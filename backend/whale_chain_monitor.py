"""
On-chain exit monitoring via Polygon eth_getLogs.

Polls the CTF Exchange contracts for OrderFilled events to detect whale sells.
Replaces activity-API exit polling (poll_exits_only) when CHAIN_EXIT_ENABLED=true.

Detection latency: ~4s worst case (2s poll interval + 2s Polygon block time).
Signal persistence: block number is monotonic — exits can never be skipped.
"""

import asyncio
import logging
from datetime import datetime, timezone

from web3 import Web3

from backend.config import settings
from backend.database import CopiedBet, MonitoringSession, SessionLocal, Whale
from backend.redemption import CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE, ORDER_FILLED_ABI

log = logging.getLogger(__name__)


class WhaleChainMonitor:
    """
    Polls Polygon CTF Exchange contracts for OrderFilled events.
    Detects whale sells by matching maker/taker addresses against tracked whales.
    Thread-safe; designed to run as an APScheduler job every 2s.
    """

    def __init__(self, bet_engine, whale_monitor):
        self._bet_engine = bet_engine
        self._whale_monitor = whale_monitor
        self._last_block: int = 0

        # Bypass the application-level HTTP proxy (set globally from PROXY_URL in config.py).
        # The VPN tunnel already routes all traffic at the network level via
        # network_mode:service:gluetun, so routing through gluetun's HTTP proxy on top
        # is a redundant double-hop that adds significant latency to every RPC call.
        self._w3 = Web3(Web3.HTTPProvider(
            settings.POLYGON_RPC_URL,
            request_kwargs={"proxies": {}},
        ))

        ctf_addr = Web3.to_checksum_address(CTF_EXCHANGE)
        neg_addr = Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE)
        self._ctf_contract = self._w3.eth.contract(address=ctf_addr, abi=ORDER_FILLED_ABI)
        self._neg_contract = self._w3.eth.contract(address=neg_addr, abi=ORDER_FILLED_ABI)

        sig_bytes = self._w3.keccak(
            text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
        )
        sig_hex = sig_bytes.hex()
        self._event_sig = "0x" + sig_hex if not sig_hex.startswith("0x") else sig_hex

        log.info(
            "WhaleChainMonitor initialized (RPC=%s, sig=%s...)",
            settings.POLYGON_RPC_URL[:40],
            self._event_sig[:18],
        )

    # ------------------------------------------------------------------
    # Scheduler entry point
    # ------------------------------------------------------------------

    def poll(self):
        """Synchronous entry point — called by APScheduler in a background thread."""
        try:
            asyncio.run(self._poll_async())
        except Exception as exc:
            log.warning("WhaleChainMonitor.poll error: %s", exc)

    # ------------------------------------------------------------------
    # Core polling logic
    # ------------------------------------------------------------------

    async def _poll_async(self):
        db = SessionLocal()
        try:
            rows = db.query(Whale.address).filter_by(is_active=True).all()
        except Exception as exc:
            log.error("WhaleChainMonitor: failed to load whales: %s", exc)
            return
        finally:
            db.close()

        if not rows:
            return

        # Map lowercase → original stored address (DB may store checksummed)
        address_map: dict[str, str] = {row[0].lower(): row[0] for row in rows}

        poll_start = datetime.now(timezone.utc)
        try:
            latest_block = self._w3.eth.block_number
        except Exception as exc:
            log.warning("WhaleChainMonitor: block_number RPC error: %s", exc)
            return

        if self._last_block == 0:
            self._last_block = max(0, latest_block - settings.CHAIN_EXIT_LOOKBACK_BLOCKS)
            log.info(
                "WhaleChainMonitor: initialized at block %d (lookback=%d)",
                self._last_block,
                settings.CHAIN_EXIT_LOOKBACK_BLOCKS,
            )

        if latest_block <= self._last_block:
            return  # No new blocks since last poll

        from_block = max(self._last_block + 1, latest_block - 500)  # cap gap after downtime
        to_block = latest_block

        whale_addresses = list(address_map.keys())  # lowercase

        try:
            logs = await self._fetch_logs(from_block, to_block, whale_addresses)
        except Exception as exc:
            log.warning(
                "WhaleChainMonitor: eth_getLogs failed (blocks %d→%d): %s",
                from_block, to_block, exc,
            )
            return

        sells = self._decode_whale_sells(logs, address_map)
        log.debug(
            "WhaleChainMonitor: blocks %d→%d, %d event(s), %d whale sell(s)",
            from_block, to_block, len(logs), len(sells),
        )

        if sells:
            detected_at = datetime.now(timezone.utc)
            db = SessionLocal()
            try:
                for trade, whale_address in sells:
                    block_ts = trade.get("timestamp")
                    if block_ts:
                        block_dt = datetime.fromtimestamp(block_ts, tz=timezone.utc)
                        lag_s = (detected_at - block_dt).total_seconds()
                        log.info(
                            "WhaleChainMonitor: EXIT detected for %s — block ts %s, detected at %s, lag=%.1fs",
                            whale_address[:10],
                            block_dt.strftime("%H:%M:%S"),
                            detected_at.strftime("%H:%M:%S"),
                            lag_s,
                        )
                    try:
                        await self._dispatch_exit(trade, whale_address, db)
                    except Exception as exc:
                        log.error(
                            "WhaleChainMonitor: dispatch error for %s: %s",
                            whale_address[:10], exc,
                        )
                        db.rollback()
            finally:
                db.close()

        self._last_block = to_block
        poll_ms = (datetime.now(timezone.utc) - poll_start).total_seconds() * 1000
        if poll_ms > 3000:
            log.warning("WhaleChainMonitor: slow poll %.0fms (blocks %d→%d)", poll_ms, from_block, to_block)

    # ------------------------------------------------------------------
    # Log fetching
    # ------------------------------------------------------------------

    # Maximum blocks per eth_getLogs request. Free-tier RPCs typically cap at 100–2000.
    # 100 is conservative and works across Ankr, Infura, Alchemy free tiers.
    _MAX_BLOCKS_PER_QUERY = 100

    async def _fetch_logs(self, from_block: int, to_block: int, whale_addresses: list[str]) -> list:
        """Fetch OrderFilled logs filtered by whale addresses.

        Topics layout (all indexed):
          [0] event signature
          [1] orderHash  (skip — any)
          [2] maker      (topic position for whale-as-maker filter)
          [3] taker      (topic position for whale-as-taker filter)

        Maker and taker queries run concurrently via asyncio.gather to halve wall-clock
        latency per chunk — each RPC call takes ~1s through the VPN, so parallel dispatch
        brings a 2-query chunk from ~2s down to ~1s.
        Large block ranges are split into chunks to stay within provider limits.
        """
        loop = asyncio.get_event_loop()

        # Pad each address to 32 bytes as required by eth_getLogs topic filtering
        padded = [
            "0x" + addr.lower().replace("0x", "").zfill(64)
            for addr in whale_addresses
        ]

        logs = []
        # Both exchanges are queried: NegRisk handles binary "Up or Down" markets;
        # standard CTF_EXCHANGE handles other conditional token markets.
        for contract_addr in (NEG_RISK_CTF_EXCHANGE, CTF_EXCHANGE):
            checksum = Web3.to_checksum_address(contract_addr)

            # Split into chunks to respect provider block range limits
            chunk_start = from_block
            while chunk_start <= to_block:
                chunk_end = min(chunk_start + self._MAX_BLOCKS_PER_QUERY - 1, to_block)
                base = {"fromBlock": chunk_start, "toBlock": chunk_end, "address": checksum}

                # Fire maker and taker queries concurrently — both are blocking I/O so
                # run_in_executor offloads each to a thread, gather awaits both at once.
                maker_filter = {**base, "topics": [self._event_sig, None, padded]}
                taker_filter = {**base, "topics": [self._event_sig, None, None, padded]}

                maker_result, taker_result = await asyncio.gather(
                    loop.run_in_executor(None, self._w3.eth.get_logs, maker_filter),
                    loop.run_in_executor(None, self._w3.eth.get_logs, taker_filter),
                )
                logs.extend(maker_result)
                logs.extend(taker_result)

                chunk_start = chunk_end + 1

        return logs

    # ------------------------------------------------------------------
    # Log decoding
    # ------------------------------------------------------------------

    def _decode_whale_sells(
        self, logs: list, address_map: dict[str, str]
    ) -> list[tuple[dict, str]]:
        """
        Decode OrderFilled logs and return (trade_dict, original_whale_address)
        for each event where a tracked whale is SELLING conditional tokens.

        Asset ID convention:  0 = USDC,  non-zero = conditional token.
        Case A (whale is maker): whale gives conditional tokens → makerAssetId != 0
        Case B (whale is taker): whale gives conditional tokens → takerAssetId != 0
        """
        whale_addrs_lower = set(address_map.keys())
        sells: list[tuple[dict, str]] = []
        block_ts_cache: dict[int, int] = {}

        for raw_log in logs:
            try:
                log_addr = raw_log["address"].lower()
                contract = (
                    self._ctf_contract
                    if log_addr == CTF_EXCHANGE.lower()
                    else self._neg_contract
                )

                event = contract.events.OrderFilled().process_log(raw_log)
                args = event["args"]

                maker = args["maker"].lower()
                taker = args["taker"].lower()
                maker_asset = args["makerAssetId"]
                taker_asset = args["takerAssetId"]
                maker_amount = args["makerAmountFilled"]
                taker_amount = args["takerAmountFilled"]

                whale_lower = None
                token_id = None
                share_amount = 0.0
                usdc_amount = 0.0

                # Case A: whale is maker, selling conditional tokens for USDC
                if maker in whale_addrs_lower and maker_asset != 0:
                    whale_lower = maker
                    token_id = str(maker_asset)
                    share_amount = maker_amount / 1e6
                    usdc_amount = taker_amount / 1e6
                # Case B: whale is taker, giving conditional tokens (gets USDC)
                elif taker in whale_addrs_lower and taker_asset != 0:
                    whale_lower = taker
                    token_id = str(taker_asset)
                    share_amount = taker_amount / 1e6
                    usdc_amount = maker_amount / 1e6

                if whale_lower is None:
                    continue

                price = usdc_amount / max(share_amount, 0.000001)

                block_num = raw_log["blockNumber"]
                if block_num not in block_ts_cache:
                    try:
                        block = self._w3.eth.get_block(block_num)
                        block_ts_cache[block_num] = int(block["timestamp"])
                    except Exception:
                        block_ts_cache[block_num] = int(
                            datetime.now(timezone.utc).timestamp()
                        )

                trade = {
                    "transactionHash": raw_log["transactionHash"].hex(),
                    "side": "SELL",
                    "asset": token_id,
                    "conditionId": "",   # enriched in _dispatch_exit from CopiedBet
                    "price": price,
                    "usdcSize": usdc_amount,
                    "shares": share_amount,
                    "timestamp": block_ts_cache[block_num],
                    "outcome": "",       # enriched in _dispatch_exit from CopiedBet
                    "question": "",      # enriched in _dispatch_exit from CopiedBet
                }

                sells.append((trade, address_map[whale_lower]))

            except Exception as exc:
                log.debug("WhaleChainMonitor: log decode error: %s", exc)

        return sells

    # ------------------------------------------------------------------
    # Exit dispatch
    # ------------------------------------------------------------------

    async def _dispatch_exit(self, trade: dict, whale_address: str, db):
        """
        Close our position matching the whale's on-chain sell.
        Calls bet_engine._handle_exit() directly — bypasses _last_seen filtering
        so block-based deduplication governs instead of timestamp consumption.
        """
        token_id = trade["asset"]
        whale_lower = whale_address.lower()

        # Find our open positions for this token/whale (case-insensitive address match)
        open_positions = (
            db.query(CopiedBet)
            .filter(
                CopiedBet.status == "OPEN",
                CopiedBet.token_id == token_id,
                CopiedBet.whale_address.ilike(whale_lower),
            )
            .order_by(CopiedBet.opened_at.asc())
            .all()
        )

        if not open_positions:
            log.debug(
                "WhaleChainMonitor: no open position for token=%s... whale=%s — skipping",
                token_id[:16], whale_address[:10],
            )
            return

        # Enrich trade dict from existing position metadata (needed by _save_whale_bet)
        open_pos = open_positions[0]
        trade["conditionId"] = open_pos.market_id or ""
        trade["outcome"] = getattr(open_pos, "outcome", "") or ""
        trade["question"] = getattr(open_pos, "question", "") or ""

        whale_rec = (
            db.query(Whale)
            .filter(Whale.address.ilike(whale_lower))
            .first()
        )
        if not whale_rec:
            log.warning(
                "WhaleChainMonitor: no Whale record for %s — skipping",
                whale_address[:10],
            )
            return

        ts = datetime.fromtimestamp(trade["timestamp"], tz=timezone.utc)
        whale_bet = await self._whale_monitor._save_whale_bet(trade, whale_rec, ts, db)
        if not whale_bet:
            log.debug(
                "WhaleChainMonitor: duplicate tx_hash for token=%s — already processed",
                token_id[:16],
            )
            return

        session = (
            db.query(MonitoringSession)
            .filter_by(mode=open_pos.mode)
            .order_by(MonitoringSession.id.desc())
            .first()
        )
        if not session:
            log.debug(
                "WhaleChainMonitor: no session for mode=%s — skipping",
                open_pos.mode,
            )
            return

        # Fetch live price for accurate simulation fills
        live_exit_price = None
        client = getattr(self._whale_monitor, "_client", None)
        if client and token_id:
            try:
                live_exit_price = await client.get_best_price(token_id)
            except Exception:
                pass

        log.info(
            "WhaleChainMonitor: closing position whale=%s token=%s...%s "
            "shares=%.4f on-chain-price=%.4f",
            whale_address[:10], token_id[:10], token_id[-6:],
            trade["shares"], trade["price"],
        )

        exit_result = self._bet_engine._handle_exit(
            whale_bet, session, db, live_exit_price=live_exit_price
        )
        db.commit()

        if exit_result is False:
            log.warning(
                "WhaleChainMonitor: sell FOK cancelled for token=%s — "
                "orphan checker will retry in ~60s",
                token_id[:16],
            )
