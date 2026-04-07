"""
On-chain entry detection via WebSocket eth_subscribe (Polygon).

Subscribes to CTF Exchange OrderFilled BUY events for tracked whales.
Runs alongside the HTTP polling path as a faster (~200ms) entry signal.
Controlled by CLOB_WS_ENABLED; uses CLOB_WS_URL (defaults to POLYGON_WS_URL).

Deduplication: the UNIQUE INDEX on whale_bets.tx_hash silently drops any
duplicate when the HTTP poller picks up the same trade later.

Architecture mirrors WhaleChainMonitor:
  - Phase 1: async HTTP (market lookup, live price) — event loop
  - Phase 2: sync DB + CLOB order placement — thread executor
  - Reconnects with exponential backoff on any connection failure.
"""

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime

import websockets
from hexbytes import HexBytes
from sqlalchemy.exc import IntegrityError, OperationalError, PendingRollbackError
from web3 import Web3
from web3.datastructures import AttributeDict

from backend.bet_engine import asset_id_matches
from backend.config import settings
from backend.database import MonitoringSession, SessionLocal, Whale, WhaleBet
from backend.db_writer import synchronized_commit, synchronized_flush
from backend.redemption import CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE, ORDER_FILLED_ABI

log = logging.getLogger(__name__)

_RECONNECT_DELAYS = [1, 2, 4, 8, 16, 30, 60]
_MAX_BACKFILL_BLOCKS = 500
_MAX_BLOCKS_PER_QUERY = 100

_dispatch_tasks: set = set()  # strong references to prevent GC of fire-and-forget tasks


class ClobWsEntryMonitor:
    """
    Subscribes to Polygon CTF Exchange OrderFilled BUY events via WebSocket.
    Provides ~200ms entry detection, complementing the 2-10s HTTP polling path.
    Runs independently of WhaleChainMonitor (which handles exits).

    On connect  : light backfill of missed buy events since last seen block.
    On event    : decodes log → dispatches entry as a concurrent Task.
    On disconnect: reconnects with exponential backoff.
    """

    _WHALE_MAP_TTL_S = 30

    def __init__(self, bet_engine, whale_monitor):
        self._bet_engine = bet_engine
        self._whale_monitor = whale_monitor
        self._running = False
        self._task: asyncio.Task | None = None
        self._last_block: int = 0

        # WS URL: prefer CLOB_WS_URL if set, else fall back to POLYGON_WS_URL / POLYGON_RPC_URL
        ws_base = settings.CLOB_WS_URL or settings.POLYGON_WS_URL or settings.POLYGON_RPC_URL
        self._ws_url = ws_base.replace("https://", "wss://").replace("http://", "ws://")

        # HTTP provider for backfill getLogs and block timestamp fetches.
        # Proxy bypassed — the VPN tunnel handles routing at the network level.
        self._http_w3 = Web3(
            Web3.HTTPProvider(
                settings.POLYGON_RPC_URL,
                request_kwargs={"proxies": {"http": None, "https": None}},
            )
        )

        # OrderFilled event signature hash
        sig_bytes = self._http_w3.keccak(
            text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
        )
        sig_hex = sig_bytes.hex()
        self._event_sig = "0x" + sig_hex if not sig_hex.startswith("0x") else sig_hex

        ctf_addr = Web3.to_checksum_address(CTF_EXCHANGE)
        neg_addr = Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE)
        self._ctf_contract = self._http_w3.eth.contract(address=ctf_addr, abi=ORDER_FILLED_ABI)
        self._neg_contract = self._http_w3.eth.contract(address=neg_addr, abi=ORDER_FILLED_ABI)

        # Whale address cache {lowercase: original} — refreshed every _WHALE_MAP_TTL_S
        self._whale_map: dict[str, str] = {}
        self._whale_map_ts: datetime | None = None

        # Active WS subscription IDs and whale set at last subscription
        self._sub_ids: list[str] = []
        self._subscribed_whales: frozenset[str] = frozenset()
        self._last_resubscribe_check: float = 0.0

        # Block timestamp cache keyed by block number
        self._block_ts_cache: dict[int, int] = {}

        # Serialise DB-write phase across concurrent dispatch tasks.
        # Multiple entries from the same block run Phase-1 HTTP concurrently
        # but Phase-2 SQLite writes are serialised to avoid "database is locked".
        self._db_write_lock: asyncio.Lock = asyncio.Lock()

        log.info(
            "ClobWsEntryMonitor initialized (WS=%s..., sig=%s...)",
            self._ws_url[:50],
            self._event_sig[:18],
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> asyncio.Task:
        """Create and return a persistent asyncio Task. Called from main.py lifespan."""
        self._running = True
        self._task = asyncio.create_task(self._run(), name="clob_ws_entry_monitor")
        return self._task

    def stop(self):
        """Signal the Task to stop and cancel it."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    # ------------------------------------------------------------------
    # Main run loop with exponential backoff
    # ------------------------------------------------------------------

    async def _run(self):
        attempt = 0
        while self._running:
            try:
                await self._connect_and_stream()
                attempt = 0
            except asyncio.CancelledError:
                log.info("ClobWsEntryMonitor: task cancelled — stopping")
                return
            except Exception as exc:
                delay = _RECONNECT_DELAYS[min(attempt, len(_RECONNECT_DELAYS) - 1)]
                log.warning(
                    "ClobWsEntryMonitor: connection lost (%s) — reconnecting in %ds",
                    exc,
                    delay,
                )
                attempt += 1
                await asyncio.sleep(delay)

    async def _connect_and_stream(self):
        log.info("ClobWsEntryMonitor: connecting to %s...", self._ws_url[:60])

        async with websockets.connect(
            self._ws_url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            log.info("ClobWsEntryMonitor: WebSocket connected")

            # Light backfill: catch any missed buy events during the reconnect gap
            current_block = await self._get_block_number()
            if current_block:
                if self._last_block == 0:
                    self._last_block = max(0, current_block - settings.CHAIN_EXIT_LOOKBACK_BLOCKS)
                    log.info(
                        "ClobWsEntryMonitor: starting from block %d (lookback=%d)",
                        self._last_block,
                        settings.CHAIN_EXIT_LOOKBACK_BLOCKS,
                    )
                elif current_block > self._last_block:
                    gap = current_block - self._last_block
                    log.info(
                        "ClobWsEntryMonitor: backfilling %d block gap (%d→%d)",
                        gap,
                        self._last_block,
                        current_block,
                    )
                    await self._backfill_buys(self._last_block + 1, current_block)
                self._last_block = current_block

            sub_ids = await self._subscribe(ws)
            if not sub_ids:
                raise RuntimeError("eth_subscribe returned no subscription IDs")

            async for raw_msg in ws:
                if not self._running:
                    return
                await self._handle_message(raw_msg, ws)

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    async def _subscribe(self, ws) -> list[str]:
        """Subscribe to OrderFilled logs filtered by tracked whale addresses.

        Creates two subscriptions — one for whale-as-maker and one for
        whale-as-taker. Only buy events (whale gives USDC, receives tokens)
        are acted on; sell events are silently ignored in _decode_whale_buys.
        """
        await self._maybe_refresh_whale_map()
        if not self._whale_map:
            raise RuntimeError("No whales configured — cannot subscribe")

        contract_addrs = [
            Web3.to_checksum_address(CTF_EXCHANGE),
            Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE),
        ]
        padded = ["0x" + addr.lower().replace("0x", "").zfill(64) for addr in self._whale_map]

        sub_ids = []
        for req_id, topics in enumerate(
            [
                [self._event_sig, None, padded],  # whale is maker
                [self._event_sig, None, None, padded],  # whale is taker
            ],
            start=1,
        ):
            await ws.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "method": "eth_subscribe",
                        "params": ["logs", {"address": contract_addrs, "topics": topics}],
                    }
                )
            )
            resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
            sub_id = resp.get("result")
            if not sub_id:
                raise RuntimeError(f"eth_subscribe (id={req_id}) returned no ID: {resp}")
            sub_ids.append(sub_id)

        self._sub_ids = sub_ids
        self._subscribed_whales = frozenset(self._whale_map)
        log.info(
            "ClobWsEntryMonitor: subscribed (%d whale(s), ids=%s.../%s...)",
            len(self._whale_map),
            sub_ids[0][:10],
            sub_ids[1][:10],
        )
        return sub_ids

    async def _resubscribe_if_needed(self, ws):
        """Resubscribe when the whale list changes. Zero-cost when unchanged."""
        import time

        if time.monotonic() - self._last_resubscribe_check < self._WHALE_MAP_TTL_S:
            return
        self._last_resubscribe_check = time.monotonic()

        await self._maybe_refresh_whale_map()
        if frozenset(self._whale_map) == self._subscribed_whales:
            return

        log.info(
            "ClobWsEntryMonitor: whale list changed (%d→%d) — resubscribing",
            len(self._subscribed_whales),
            len(self._whale_map),
        )
        for sub_id in self._sub_ids:
            with contextlib.suppress(Exception):
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 99,
                            "method": "eth_unsubscribe",
                            "params": [sub_id],
                        }
                    )
                )
                await asyncio.wait_for(ws.recv(), timeout=5.0)

        await self._subscribe(ws)

    # ------------------------------------------------------------------
    # Message handler
    # ------------------------------------------------------------------

    async def _handle_message(self, raw_msg: str, ws):
        await self._resubscribe_if_needed(ws)

        try:
            msg = json.loads(raw_msg)
        except Exception:
            return

        if msg.get("method") != "eth_subscription":
            return

        result = (msg.get("params") or {}).get("result")
        if not result or result.get("removed"):
            return

        block_num = int(result.get("blockNumber", "0x0"), 16)
        if block_num > self._last_block:
            self._last_block = block_num

        if not self._whale_map:
            return

        raw_log = self._normalize_log(result)
        if raw_log is None:
            return

        block_ts = await self._get_block_timestamp(block_num)
        self._block_ts_cache[block_num] = block_ts

        loop = asyncio.get_event_loop()
        whale_map_snapshot = self._whale_map.copy()

        buys = await loop.run_in_executor(
            None, self._decode_whale_buys, [raw_log], whale_map_snapshot, self._block_ts_cache
        )

        now = datetime.now(UTC)
        for trade, whale_address in buys:
            lag_s = now.timestamp() - block_ts
            log.info(
                "ClobWsEntryMonitor: ENTRY detected whale=%s lag=%.1fs",
                whale_address[:10],
                lag_s,
            )
            task = asyncio.create_task(self._dispatch_entry(trade, whale_address))
            _dispatch_tasks.add(task)
            task.add_done_callback(_dispatch_tasks.discard)

    # ------------------------------------------------------------------
    # Backfill (on reconnect)
    # ------------------------------------------------------------------

    async def _backfill_buys(self, from_block: int, to_block: int):
        """Fetch and process missed buy events via HTTP eth_getLogs."""
        await self._maybe_refresh_whale_map()
        if not self._whale_map:
            return

        from_block = max(from_block, to_block - _MAX_BACKFILL_BLOCKS)

        loop = asyncio.get_event_loop()
        all_logs = await loop.run_in_executor(None, self._fetch_logs_http, from_block, to_block)
        if not all_logs:
            return

        whale_map_snapshot = self._whale_map.copy()
        buys = self._decode_whale_buys(all_logs, whale_map_snapshot, {})
        log.info(
            "ClobWsEntryMonitor: backfill %d→%d — %d log(s), %d buy(s)",
            from_block,
            to_block,
            len(all_logs),
            len(buys),
        )

        for trade, whale_address in buys:
            await self._dispatch_entry(trade, whale_address)

    def _fetch_logs_http(self, from_block: int, to_block: int) -> list:
        """Synchronous eth_getLogs over HTTP. Safe to run in thread executor."""
        contract_addrs = [
            Web3.to_checksum_address(CTF_EXCHANGE),
            Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE),
        ]
        logs = []
        chunk = from_block
        while chunk <= to_block:
            end = min(chunk + _MAX_BLOCKS_PER_QUERY - 1, to_block)
            try:
                logs.extend(
                    self._http_w3.eth.get_logs(
                        {
                            "fromBlock": chunk,
                            "toBlock": end,
                            "address": contract_addrs,
                            "topics": [self._event_sig],
                        }
                    )
                )
            except Exception as exc:
                log.warning("ClobWsEntryMonitor: getLogs error blocks %d→%d: %s", chunk, end, exc)
            chunk = end + 1
        return logs

    # ------------------------------------------------------------------
    # Whale map
    # ------------------------------------------------------------------

    async def _maybe_refresh_whale_map(self):
        now = datetime.now(UTC)
        if (
            self._whale_map_ts is not None
            and (now - self._whale_map_ts).total_seconds() < self._WHALE_MAP_TTL_S
        ):
            return
        loop = asyncio.get_event_loop()

        def _load():
            db = SessionLocal()
            try:
                rows = db.query(Whale.address).filter_by(is_active=True).all()
                return {r[0].lower(): r[0] for r in rows}
            finally:
                db.close()

        self._whale_map = await loop.run_in_executor(None, _load)
        self._whale_map_ts = now

    # ------------------------------------------------------------------
    # RPC helpers
    # ------------------------------------------------------------------

    async def _get_block_number(self) -> int | None:
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self._http_w3.eth.block_number)
        except Exception as exc:
            log.warning("ClobWsEntryMonitor: block_number error: %s", exc)
            return None

    async def _get_block_timestamp(self, block_num: int) -> int:
        if block_num in self._block_ts_cache:
            return self._block_ts_cache[block_num]
        loop = asyncio.get_event_loop()
        try:
            block = await loop.run_in_executor(None, lambda: self._http_w3.eth.get_block(block_num))
            ts = int(block["timestamp"])
            self._block_ts_cache[block_num] = ts
            return ts
        except Exception:
            return int(datetime.now(UTC).timestamp())

    # ------------------------------------------------------------------
    # Log normalization: WS JSON (hex strings) → web3 AttributeDict
    # ------------------------------------------------------------------

    def _normalize_log(self, result: dict) -> AttributeDict | None:
        """Convert raw WebSocket log fields to the format web3 process_log expects."""
        try:
            return AttributeDict(
                {
                    "address": Web3.to_checksum_address(result["address"]),
                    "topics": [HexBytes(t) for t in result.get("topics", [])],
                    "data": HexBytes(result.get("data", "0x")),
                    "blockNumber": int(result["blockNumber"], 16),
                    "transactionHash": HexBytes(result["transactionHash"]),
                    "logIndex": int(result.get("logIndex", "0x0"), 16),
                    "blockHash": HexBytes(result.get("blockHash", "0x" + "0" * 64)),
                    "transactionIndex": int(result.get("transactionIndex", "0x0"), 16),
                    "removed": result.get("removed", False),
                }
            )
        except Exception as exc:
            log.debug("ClobWsEntryMonitor: normalize_log error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Buy event decoding
    # ------------------------------------------------------------------

    def _decode_whale_buys(
        self,
        logs: list,
        address_map: dict[str, str],
        block_ts_cache: dict[int, int],
    ) -> list[tuple[dict, str]]:
        """Decode OrderFilled logs and return (trade_dict, whale_address) for whale buys."""
        whale_addrs = set(address_map.keys())
        buys: list[tuple[dict, str]] = []

        for raw_log in logs:
            try:
                log_addr = raw_log["address"].lower()
                contract = (
                    self._ctf_contract if log_addr == CTF_EXCHANGE.lower() else self._neg_contract
                )
                event = contract.events.OrderFilled().process_log(raw_log)
                args = event["args"]

                maker = args["maker"].lower()
                taker = args["taker"].lower()
                maker_asset = args["makerAssetId"]
                taker_asset = args["takerAssetId"]
                maker_amount = args["makerAmountFilled"]
                taker_amount = args["takerAmountFilled"]

                whale_lower = token_id = None
                share_amount = usdc_amount = 0.0

                # Whale is maker, gives USDC (asset_id=0), receives conditional tokens
                if maker in whale_addrs and maker_asset == 0 and taker_asset != 0:
                    whale_lower = maker
                    token_id = str(taker_asset)
                    usdc_amount = maker_amount / 1e6
                    share_amount = taker_amount / 1e6
                # Whale is taker, gives USDC (asset_id=0), receives conditional tokens
                elif taker in whale_addrs and taker_asset == 0 and maker_asset != 0:
                    whale_lower = taker
                    token_id = str(maker_asset)
                    usdc_amount = taker_amount / 1e6
                    share_amount = maker_amount / 1e6

                if whale_lower is None:
                    continue

                price = usdc_amount / max(share_amount, 0.000001)
                block_num = raw_log["blockNumber"]
                if block_num not in block_ts_cache:
                    try:
                        block = self._http_w3.eth.get_block(block_num)
                        block_ts_cache[block_num] = int(block["timestamp"])
                    except Exception:
                        block_ts_cache[block_num] = int(datetime.now(UTC).timestamp())

                tx_hash = raw_log["transactionHash"]
                tx_hex = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)

                buys.append(
                    (
                        {
                            "transactionHash": tx_hex,
                            "side": "BUY",
                            "asset": token_id,
                            "conditionId": "",
                            "price": price,
                            "usdcSize": usdc_amount,
                            "shares": share_amount,
                            "timestamp": block_ts_cache[block_num],
                            "outcome": "",
                            "question": "",
                        },
                        address_map[whale_lower],
                    )
                )
            except Exception as exc:
                log.debug("ClobWsEntryMonitor: buy decode error: %s", exc)

        return buys

    # ------------------------------------------------------------------
    # Entry dispatch
    # Phase 1: async HTTP (market lookup, live price) — runs on event loop
    # Phase 2: sync DB + CLOB order placement — runs in thread executor
    # ------------------------------------------------------------------

    async def _dispatch_entry(self, trade: dict, whale_address: str):
        try:
            await self._dispatch_entry_inner(trade, whale_address)
        except Exception:
            log.exception(
                "ClobWsEntryMonitor: unhandled exception in _dispatch_entry whale=%s token=%s",
                whale_address[:10],
                trade.get("asset", "")[:16],
            )

    async def _dispatch_entry_inner(self, trade: dict, whale_address: str):
        token_id = trade["asset"]
        client = getattr(self._whale_monitor, "_client", None)
        if not client:
            log.warning("ClobWsEntryMonitor: no client for entry dispatch")
            return

        # Phase 1: market lookup, live price, taker fee, and order book in parallel
        market_result, price_result, fee_result, book_result = await asyncio.gather(
            client.get_market("", token_id=token_id),
            client.get_best_price(token_id, force_refresh=True),
            client.get_taker_fee_async(token_id),
            client.get_order_book(token_id),
            return_exceptions=True,
        )

        market_info: dict = market_result if isinstance(market_result, dict) else {}
        taker_fee_bps = fee_result if isinstance(fee_result, int) else 1000
        order_book = book_result if isinstance(book_result, dict) else None

        # Extract live_price from order book min ask; fall back to API estimate
        live_price = None
        if order_book and isinstance(order_book, dict):
            asks = order_book.get("asks", [])
            if asks and isinstance(asks, list) and len(asks) > 0:
                with contextlib.suppress(ValueError, TypeError, KeyError, IndexError):
                    min_ask_price = float(asks[-1].get("price", 0))
                    if min_ask_price > 0:
                        live_price = min_ask_price
        if live_price is None:
            live_price = price_result if isinstance(price_result, float) else None

        condition_id = market_info.get("conditionId") or market_info.get("condition_id") or ""
        question = market_info.get("question") or market_info.get("title") or ""
        tokens = market_info.get("tokens") or []

        if not tokens and condition_id:
            with contextlib.suppress(Exception):
                full = await client.get_market(condition_id) or {}
                tokens = full.get("tokens") or []

        outcome = "YES"
        for tok in tokens:
            if asset_id_matches(tok.get("token_id", ""), token_id):
                outcome = (tok.get("outcome") or "Yes").upper()
                break

        trade["conditionId"] = condition_id
        trade["question"] = question
        trade["outcome"] = outcome

        log.info(
            "ClobWsEntryMonitor: opening position whale=%s token=%s...%s "
            "outcome=%s price=%.4f usdc=%.2f",
            whale_address[:10],
            token_id[:10],
            token_id[-6:],
            outcome,
            trade["price"],
            trade["usdcSize"],
        )

        # Phase 2: sync DB + bet_engine in executor — serialised via lock
        loop = asyncio.get_event_loop()
        async with self._db_write_lock:
            await loop.run_in_executor(
                None,
                self._sync_open_position,
                trade,
                whale_address,
                market_info,
                live_price,
                taker_fee_bps,
                order_book,
            )

    def _sync_open_position(
        self,
        trade: dict,
        whale_address: str,
        market_info: dict,
        live_price: float | None,
        taker_fee_bps: int,
        order_book: dict | None = None,
    ):
        """Sync DB work for entry dispatch. Runs in thread executor."""
        whale_lower = whale_address.lower()
        db = SessionLocal()
        try:
            whale_rec = db.query(Whale).filter(Whale.address.ilike(whale_lower)).first()
            if not whale_rec:
                log.warning(
                    "ClobWsEntryMonitor: no Whale record %s — skipping entry",
                    whale_address[:10],
                )
                return

            ts = datetime.fromtimestamp(trade["timestamp"], tz=UTC)
            tx_hash = trade.get("transactionHash", "")
            if tx_hash.startswith("0x"):
                tx_hash = tx_hash[2:]

            market_id = trade.get("conditionId", "")

            # O(2) open-position check: find most recent EXIT, then check if any entry
            # exists after it. Replaces O(N+1) loop (fetch all entries, check exit for each).
            latest_exit = (
                db.query(WhaleBet.timestamp)
                .filter(
                    WhaleBet.whale_id == whale_rec.id,
                    WhaleBet.market_id == market_id,
                    WhaleBet.bet_type == "EXIT",
                    WhaleBet.timestamp < ts,
                )
                .order_by(WhaleBet.timestamp.desc())
                .first()
            )
            latest_exit_ts = latest_exit[0] if latest_exit else None

            entry_q = db.query(WhaleBet.id).filter(
                WhaleBet.whale_id == whale_rec.id,
                WhaleBet.market_id == market_id,
                WhaleBet.bet_type.in_(["OPEN", "ADD-TO-POSITION"]),
                WhaleBet.timestamp < ts,
            )
            if latest_exit_ts is not None:
                entry_q = entry_q.filter(WhaleBet.timestamp > latest_exit_ts)
            has_open_position = entry_q.first() is not None

            bet_type = "ADD-TO-POSITION" if has_open_position else "OPEN"

            log.info(
                "ClobWsEntryMonitor: %s whale=%s market=%s...%s outcome=%s price=%.4f usdc=%.2f",
                bet_type,
                whale_address[:10],
                market_id[:10],
                market_id[-6:],
                trade.get("outcome", "YES"),
                trade.get("price", 0.5),
                trade.get("usdcSize", 0.0),
            )

            whale_bet = WhaleBet(
                whale_id=whale_rec.id,
                market_id=market_id,
                token_id=trade.get("asset", ""),
                question=trade.get("question", ""),
                side="BUY",
                outcome=trade.get("outcome", "YES"),
                price=trade.get("price", 0.5),
                size_usdc=trade.get("usdcSize", 0.0),
                size_shares=trade.get("shares", 0.0),
                timestamp=ts,
                tx_hash=tx_hash or None,
                bet_type=bet_type,
            )
            whale_bet.whale = whale_rec
            db.add(whale_bet)
            try:
                synchronized_flush(db)
            except IntegrityError:
                db.rollback()
                log.debug("ClobWsEntryMonitor: duplicate entry tx %s — skipping", tx_hash[:16])
                return
            except OperationalError as e:
                db.rollback()
                if "database is locked" in str(e):
                    log.warning("ClobWsEntryMonitor: database locked during flush, retrying later")
                    return
                raise

            # Commit the whale_bet immediately so the write lock is released before
            # process_new_whale_bet runs.  That function can take several seconds
            # (market-info queries, CLOB calls, placement logic) and holding the
            # SQLite WAL write lock across all of that blocks every other writer.
            synchronized_commit(db)

            try:
                active_sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
                if not active_sessions:
                    log.warning(
                        "ClobWsEntryMonitor: no active sessions — whale bet recorded but not copied"
                    )
                    synchronized_commit(db)
                    return

                for session in active_sessions:
                    self._bet_engine.process_new_whale_bet(
                        whale_bet=whale_bet,
                        session=session,
                        db=db,
                        market_info=market_info,
                        live_price=live_price,
                        taker_fee_bps=taker_fee_bps,
                        order_book=order_book,
                    )
                synchronized_commit(db)
            except PendingRollbackError:
                db.rollback()
                log.warning("ClobWsEntryMonitor: session in pending rollback state, skipping bet")
                return
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
