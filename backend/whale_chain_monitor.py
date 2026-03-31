"""
On-chain exit monitoring via WebSocket eth_subscribe (Polygon).

Subscribes to OrderFilled events on both CTF Exchange contracts.
Replaces the APScheduler poll() approach with a persistent asyncio Task,
giving ~200ms detection latency vs ~4s for the previous polling approach.

Gap recovery: on (re)connect, eth_getLogs backfills any blocks missed
during disconnection so exits are never silently dropped.

Architecture:
  - start() creates an asyncio.Task on the FastAPI/uvicorn event loop.
  - Async HTTP work (market lookups, price fetches) runs on the event loop.
  - Sync work (DB writes, CLOB order placement) runs in thread executor.
  - Reconnects with exponential backoff on any connection failure.
"""

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime

import websockets
from hexbytes import HexBytes
from sqlalchemy.exc import IntegrityError
from web3 import Web3
from web3.datastructures import AttributeDict

from backend.bet_engine import asset_id_matches
from backend.config import settings
from backend.database import CopiedBet, MonitoringSession, SessionLocal, Whale, WhaleBet
from backend.redemption import CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE, ORDER_FILLED_ABI

log = logging.getLogger(__name__)

# Exponential backoff delays (seconds) for reconnect attempts
_RECONNECT_DELAYS = [1, 2, 4, 8, 16, 30, 60]


_dispatch_tasks: set = set()  # strong references to prevent GC of fire-and-forget tasks


class WhaleChainMonitor:
    """
    Subscribes to Polygon CTF Exchange OrderFilled events via WebSocket.
    Runs as a persistent asyncio Task started from main.py lifespan.

    On connect  : backfills missed blocks since _last_block via eth_getLogs.
    On event    : decodes log → dispatches entry/exit as concurrent Tasks.
    On disconnect: reconnects with exponential backoff; gap is backfilled on next connect.
    """

    _MAX_BACKFILL_BLOCKS = 500  # cap gap backfill after long downtime
    _MAX_BLOCKS_PER_QUERY = 100  # eth_getLogs chunk size (free-tier safe)
    _WHALE_MAP_TTL_S = 30  # seconds between whale list refreshes

    def __init__(self, bet_engine, whale_monitor):
        self._bet_engine = bet_engine
        self._whale_monitor = whale_monitor
        self._last_block: int = 0
        self._running = False
        self._task: asyncio.Task | None = None

        # WS URL: use POLYGON_WS_URL if set, else derive from POLYGON_RPC_URL
        rpc_url = settings.POLYGON_WS_URL or settings.POLYGON_RPC_URL
        self._ws_url = rpc_url.replace("https://", "wss://").replace("http://", "ws://")

        # Separate HTTP provider for backfill + block timestamp fetches.
        # Proxy is explicitly bypassed — the VPN tunnel already handles routing at
        # the network level; going through gluetun's HTTP proxy would double-hop.
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

        # Contracts for ABI-based log decoding
        ctf_addr = Web3.to_checksum_address(CTF_EXCHANGE)
        neg_addr = Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE)
        self._ctf_contract = self._http_w3.eth.contract(address=ctf_addr, abi=ORDER_FILLED_ABI)
        self._neg_contract = self._http_w3.eth.contract(address=neg_addr, abi=ORDER_FILLED_ABI)

        # Whale address cache {lowercase: original} — refreshed every _WHALE_MAP_TTL_S
        self._whale_map: dict[str, str] = {}
        self._whale_map_ts: datetime | None = None

        # Active WS subscription IDs (one for maker filter, one for taker filter)
        self._sub_ids: list[str] = []
        # Whale address set at last subscription — used to detect list changes
        self._subscribed_whales: frozenset[str] = frozenset()
        # Monotonic timestamp (time.monotonic) of last resubscribe check
        self._last_resubscribe_check: float = 0.0

        # Block timestamp cache keyed by block number — avoids repeat eth_getBlock
        # for the rare whale events (multiple events in the same block share a timestamp)
        self._block_ts_cache: dict[int, int] = {}

        # Serialise the DB-write phase across concurrent dispatch tasks.
        # Multiple _dispatch_entry/_dispatch_exit tasks can be created simultaneously
        # for trades in the same block. Their Phase-1 HTTP work runs concurrently, but
        # Phase-2 (run_in_executor → SQLite INSERT) must be serialised because SQLite
        # only allows one writer at a time. Without this lock the concurrent writes race
        # and raise OperationalError("database is locked") even with WAL mode enabled.
        self._db_write_lock: asyncio.Lock = asyncio.Lock()

        log.info(
            "WhaleChainMonitor initialized (WS=%s..., sig=%s...)",
            self._ws_url[:50],
            self._event_sig[:18],
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> asyncio.Task:
        """Create and return a persistent asyncio Task. Called from main.py lifespan."""
        self._running = True
        self._task = asyncio.create_task(self._run(), name="whale_chain_monitor")
        return self._task

    def stop(self):
        """Signal the Task to stop and cancel it."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def _run(self):
        attempt = 0
        while self._running:
            try:
                await self._connect_and_stream()
                attempt = 0  # clean disconnect resets backoff
            except asyncio.CancelledError:
                log.info("WhaleChainMonitor: task cancelled — stopping")
                return
            except Exception as exc:
                delay = _RECONNECT_DELAYS[min(attempt, len(_RECONNECT_DELAYS) - 1)]
                log.warning(
                    "WhaleChainMonitor: connection lost (%s) — reconnecting in %ds",
                    exc,
                    delay,
                )
                attempt += 1
                await asyncio.sleep(delay)

    async def _connect_and_stream(self):
        log.info("WhaleChainMonitor: connecting to %s...", self._ws_url[:60])

        # Warm market cache once on first connect so known markets are instant
        if self._last_block == 0:
            await self._warm_market_cache()

        async with websockets.connect(
            self._ws_url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            log.info("WhaleChainMonitor: WebSocket connected")

            # Backfill any blocks missed since last connection
            current_block = await self._get_block_number()
            if current_block:
                if self._last_block == 0:
                    self._last_block = max(0, current_block - settings.CHAIN_EXIT_LOOKBACK_BLOCKS)
                    log.info(
                        "WhaleChainMonitor: starting from block %d (lookback=%d)",
                        self._last_block,
                        settings.CHAIN_EXIT_LOOKBACK_BLOCKS,
                    )
                elif current_block > self._last_block:
                    gap = current_block - self._last_block
                    log.info(
                        "WhaleChainMonitor: backfilling %d block gap (%d→%d)",
                        gap,
                        self._last_block,
                        current_block,
                    )
                    await self._backfill(self._last_block + 1, current_block)
                self._last_block = current_block

            # Subscribe to OrderFilled filtered by whale addresses (two subs: maker + taker)
            sub_ids = await self._subscribe(ws)
            if not sub_ids:
                raise RuntimeError("eth_subscribe returned no subscription IDs")

            async for raw_msg in ws:
                if not self._running:
                    return
                await self._handle_message(raw_msg, ws)

    async def _subscribe(self, ws) -> list[str]:
        """Subscribe to OrderFilled logs filtered by tracked whale addresses.

        Creates two subscriptions — one for whale-as-maker (topics[2]) and one for
        whale-as-taker (topics[3]). Alchemy only delivers events where a tracked whale
        is involved, eliminating credit burn from the ~3,000+ unrelated trades/minute
        on the CTF Exchange contracts.

        Returns list of two subscription IDs.
        """
        await self._maybe_refresh_whale_map()
        if not self._whale_map:
            raise RuntimeError("No whales configured — cannot subscribe")

        contract_addrs = [
            Web3.to_checksum_address(CTF_EXCHANGE),
            Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE),
        ]
        # Pad each address to 32 bytes as required by topics filter spec
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
            "WhaleChainMonitor: subscribed (%d whale(s), ids=%s.../%s...)",
            len(self._whale_map),
            sub_ids[0][:10],
            sub_ids[1][:10],
        )
        return sub_ids

    async def _resubscribe_if_needed(self, ws):
        """Periodically check if the whale list changed; if so, update subscriptions.

        Costs zero credits when the whale list is unchanged.
        Only unsubscribes + resubscribes when a whale is added or removed.
        """
        import time

        if time.monotonic() - self._last_resubscribe_check < self._WHALE_MAP_TTL_S:
            return
        self._last_resubscribe_check = time.monotonic()

        await self._maybe_refresh_whale_map()
        if frozenset(self._whale_map) == self._subscribed_whales:
            return

        log.info(
            "WhaleChainMonitor: whale list changed (%d→%d) — resubscribing",
            len(self._subscribed_whales),
            len(self._whale_map),
        )

        # Unsubscribe old filters
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
        # Periodically resubscribe if whale list changed (zero-cost when unchanged)
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

        # Fetch block timestamp async before decode; cache avoids repeat eth_getBlock
        # for multiple whale events landing in the same block
        block_ts = await self._get_block_timestamp(block_num)
        self._block_ts_cache[block_num] = block_ts

        loop = asyncio.get_event_loop()
        whale_map_snapshot = self._whale_map.copy()

        buys, sells = await asyncio.gather(
            loop.run_in_executor(
                None, self._decode_whale_buys, [raw_log], whale_map_snapshot, self._block_ts_cache
            ),
            loop.run_in_executor(
                None,
                self._decode_whale_sells,
                [raw_log],
                whale_map_snapshot,
                dict(self._block_ts_cache),
            ),
        )

        now = datetime.now(UTC)
        for trade, whale_address in buys:
            lag_s = now.timestamp() - block_ts
            log.info(
                "WhaleChainMonitor: ENTRY detected whale=%s lag=%.1fs",
                whale_address[:10],
                lag_s,
            )
            task = asyncio.create_task(self._dispatch_entry(trade, whale_address))
            _dispatch_tasks.add(task)
            task.add_done_callback(_dispatch_tasks.discard)

        for trade, whale_address in sells:
            lag_s = now.timestamp() - block_ts
            log.info(
                "WhaleChainMonitor: EXIT detected whale=%s lag=%.1fs",
                whale_address[:10],
                lag_s,
            )
            task = asyncio.create_task(self._dispatch_exit(trade, whale_address))
            _dispatch_tasks.add(task)
            task.add_done_callback(_dispatch_tasks.discard)

    # ------------------------------------------------------------------
    # Backfill (on reconnect)
    # ------------------------------------------------------------------

    async def _backfill(self, from_block: int, to_block: int):
        """Fetch and process all missed events via HTTP eth_getLogs."""
        await self._maybe_refresh_whale_map()
        if not self._whale_map:
            return

        # Cap range to avoid massive queries after extended downtime
        from_block = max(from_block, to_block - self._MAX_BACKFILL_BLOCKS)

        loop = asyncio.get_event_loop()
        all_logs = await loop.run_in_executor(None, self._fetch_logs_http, from_block, to_block)
        if not all_logs:
            return

        whale_map_snapshot = self._whale_map.copy()
        buys = self._decode_whale_buys(all_logs, whale_map_snapshot, {})
        sells = self._decode_whale_sells(all_logs, whale_map_snapshot, {})
        log.info(
            "WhaleChainMonitor: backfill %d→%d — %d log(s), %d buy(s), %d sell(s)",
            from_block,
            to_block,
            len(all_logs),
            len(buys),
            len(sells),
        )

        # Backfill dispatches run sequentially to preserve event ordering
        for trade, whale_address in buys:
            await self._dispatch_entry(trade, whale_address)
        for trade, whale_address in sells:
            await self._dispatch_exit(trade, whale_address)

    def _fetch_logs_http(self, from_block: int, to_block: int) -> list:
        """Synchronous eth_getLogs over HTTP. Safe to run in thread executor."""
        contract_addrs = [
            Web3.to_checksum_address(CTF_EXCHANGE),
            Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE),
        ]
        logs = []
        chunk = from_block
        while chunk <= to_block:
            end = min(chunk + self._MAX_BLOCKS_PER_QUERY - 1, to_block)
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
                log.warning("WhaleChainMonitor: getLogs error blocks %d→%d: %s", chunk, end, exc)
            chunk = end + 1
        return logs

    # ------------------------------------------------------------------
    # Market cache warm-up
    # ------------------------------------------------------------------

    async def _warm_market_cache(self):
        """Pre-fetch market metadata for all tokens with open positions or recent whale bets.

        Called once on first connect so that _dispatch_entry cache hits are instant
        for markets the whales already trade in.
        """
        client = getattr(self._whale_monitor, "_client", None)
        if not client:
            return

        loop = asyncio.get_event_loop()

        def _load_token_ids() -> set[str]:
            db = SessionLocal()
            try:
                open_tokens = {
                    r[0]
                    for r in db.query(CopiedBet.token_id).filter_by(status="OPEN").all()
                    if r[0]
                }
                recent_tokens = {
                    r[0]
                    for r in db.query(WhaleBet.token_id)
                    .filter(WhaleBet.token_id.isnot(None))
                    .order_by(WhaleBet.id.desc())
                    .limit(200)
                    .all()
                    if r[0]
                }
                return open_tokens | recent_tokens
            finally:
                db.close()

        token_ids = await loop.run_in_executor(None, _load_token_ids)
        if not token_ids:
            return

        log.info("WhaleChainMonitor: warming market cache for %d token(s)...", len(token_ids))

        # Cap concurrency to avoid hammering the Gamma API
        sem = asyncio.Semaphore(10)

        async def _fetch_one(tid: str):
            async with sem:
                with contextlib.suppress(Exception):
                    await client.get_market("", token_id=tid)

        await asyncio.gather(*[_fetch_one(tid) for tid in token_ids])
        log.info("WhaleChainMonitor: market cache warm-up complete (%d tokens)", len(token_ids))

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
    # RPC helpers (all run in executor — no sync RPC on the event loop)
    # ------------------------------------------------------------------

    async def _get_block_number(self) -> int | None:
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, lambda: self._http_w3.eth.block_number)
        except Exception as exc:
            log.warning("WhaleChainMonitor: block_number error: %s", exc)
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
            log.debug("WhaleChainMonitor: normalize_log error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Log decoding
    # block_ts_cache is pre-populated for WS events (avoids RPC on event loop);
    # empty dict is passed for backfill (runs in executor, RPC is fine there).
    # ------------------------------------------------------------------

    def _decode_whale_sells(
        self,
        logs: list,
        address_map: dict[str, str],
        block_ts_cache: dict[int, int],
    ) -> list[tuple[dict, str]]:
        """Decode OrderFilled logs and return (trade_dict, whale_address) for whale sells."""
        whale_addrs = set(address_map.keys())
        sells: list[tuple[dict, str]] = []

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

                # Whale is maker, selling conditional tokens for USDC
                if maker in whale_addrs and maker_asset != 0:
                    whale_lower = maker
                    token_id = str(maker_asset)
                    share_amount = maker_amount / 1e6
                    usdc_amount = taker_amount / 1e6
                # Whale is taker, giving conditional tokens (receives USDC)
                elif taker in whale_addrs and taker_asset != 0:
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
                        block = self._http_w3.eth.get_block(block_num)
                        block_ts_cache[block_num] = int(block["timestamp"])
                    except Exception:
                        block_ts_cache[block_num] = int(datetime.now(UTC).timestamp())

                tx_hash = raw_log["transactionHash"]
                tx_hex = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)

                sells.append(
                    (
                        {
                            "transactionHash": tx_hex,
                            "side": "SELL",
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
                log.debug("WhaleChainMonitor: sell decode error: %s", exc)

        return sells

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

                # Whale is maker, gives USDC, receives conditional tokens
                if maker in whale_addrs and maker_asset == 0 and taker_asset != 0:
                    whale_lower = maker
                    token_id = str(taker_asset)
                    usdc_amount = maker_amount / 1e6
                    share_amount = taker_amount / 1e6
                # Whale is taker, gives USDC, receives conditional tokens
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
                log.debug("WhaleChainMonitor: buy decode error: %s", exc)

        return buys

    # ------------------------------------------------------------------
    # Entry dispatch
    # Phase 1: async HTTP (market lookup, live price) — runs on event loop
    # Phase 2: sync DB + CLOB order placement — runs in thread executor
    # ------------------------------------------------------------------

    async def _dispatch_entry(self, trade: dict, whale_address: str):
        token_id = trade["asset"]
        client = getattr(self._whale_monitor, "_client", None)
        if not client:
            log.warning("WhaleChainMonitor: no client for entry dispatch")
            return

        # Phase 1: fire market lookup, live price, taker fee, and order book in parallel.
        # token_id is known from the on-chain event so no sequential dependency.
        market_result, price_result, fee_result, book_result = await asyncio.gather(
            client.get_market("", token_id=token_id),
            client.get_best_price(token_id, force_refresh=True),
            client.get_taker_fee_async(token_id),
            client.get_order_book(token_id),
            return_exceptions=True,
        )

        market_info: dict = market_result if isinstance(market_result, dict) else {}
        live_price = price_result if isinstance(price_result, float) else None
        taker_fee_bps = fee_result if isinstance(fee_result, int) else 1000
        order_book = book_result if isinstance(book_result, dict) else None

        condition_id = market_info.get("conditionId") or market_info.get("condition_id") or ""
        question = market_info.get("question") or market_info.get("title") or ""
        tokens = market_info.get("tokens") or []

        # Fallback: fetch full market by condition_id if token list was absent
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
            "WhaleChainMonitor: opening position whale=%s token=%s...%s "
            "outcome=%s price=%.4f usdc=%.2f",
            whale_address[:10],
            token_id[:10],
            token_id[-6:],
            outcome,
            trade["price"],
            trade["usdcSize"],
        )

        # Phase 2: sync DB + bet_engine in executor — serialised via lock so
        # concurrent same-block dispatches don't race on the SQLite write lock.
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
                    "WhaleChainMonitor: no Whale record %s — skipping entry",
                    whale_address[:10],
                )
                return

            ts = datetime.fromtimestamp(trade["timestamp"], tz=UTC)
            tx_hash = trade.get("transactionHash", "")
            if tx_hash.startswith("0x"):
                tx_hash = tx_hash[2:]

            market_id = trade.get("conditionId", "")

            # Detect if this is an add-to-position by checking for existing open position
            # in the same market. Query for any OPEN or ADD-TO-POSITION bet before this
            # timestamp that doesn't have a matching EXIT after it.
            existing_entries = (
                db.query(WhaleBet)
                .filter(
                    WhaleBet.whale_id == whale_rec.id,
                    WhaleBet.market_id == market_id,
                    WhaleBet.bet_type.in_(["OPEN", "ADD-TO-POSITION"]),
                    WhaleBet.timestamp < ts,
                )
                .all()
            )

            # Check if any existing entry has a matching exit
            has_open_position = False
            for entry in existing_entries:
                exit_exists = (
                    db.query(WhaleBet)
                    .filter(
                        WhaleBet.whale_id == whale_rec.id,
                        WhaleBet.market_id == market_id,
                        WhaleBet.bet_type == "EXIT",
                        WhaleBet.timestamp > entry.timestamp,
                    )
                    .first()
                )
                if not exit_exists:
                    has_open_position = True
                    break

            bet_type = "ADD-TO-POSITION" if has_open_position else "OPEN"

            # Log the position classification
            log.info(
                "WhaleChainMonitor: %s whale=%s market=%s...%s outcome=%s price=%.4f usdc=%.2f",
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
            db.add(whale_bet)
            try:
                synchronized_flush(db)
            except IntegrityError:
                db.rollback()
                log.debug("WhaleChainMonitor: duplicate entry tx %s — skipping", tx_hash[:16])
                return

            active_sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
            if not active_sessions:
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
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Exit dispatch
    # Phase 1: sync DB read (load open position) — runs in executor
    # Phase 2: async HTTP (live exit price) — runs on event loop
    # Phase 3: sync DB write + CLOB sell — runs in executor
    # ------------------------------------------------------------------

    async def _dispatch_exit(self, trade: dict, whale_address: str):
        token_id = trade["asset"]
        whale_lower = whale_address.lower()

        # Phase 1: sync DB read in executor (first try direct token_id match)
        loop = asyncio.get_event_loop()
        pos_data = await loop.run_in_executor(
            None, self._sync_load_position, token_id, whale_lower, None
        )

        # Phase 1b: If direct match failed, try fallback with market_info
        if pos_data is None:
            client = getattr(self._whale_monitor, "_client", None)
            market_info = {}
            if client:
                with contextlib.suppress(Exception):
                    # Fetch market info only as fallback (on-chain token_id format mismatch)
                    market_result = await client.get_market("", token_id=token_id)
                    if market_result:
                        market_info = market_result

            # Retry position lookup with market_info for condition_id+outcome matching
            if market_info:
                pos_data = await loop.run_in_executor(
                    None, self._sync_load_position, token_id, whale_lower, market_info
                )

        if pos_data is None:
            return

        trade["conditionId"], trade["outcome"], trade["question"], pos_mode = pos_data

        # Phase 2: async HTTP — fetch live exit price
        live_exit_price: float | None = None
        client = getattr(self._whale_monitor, "_client", None)
        if client:
            with contextlib.suppress(Exception):
                live_exit_price = await client.get_best_price(token_id, side="SELL")

        log.info(
            "WhaleChainMonitor: closing position whale=%s token=%s...%s price=%.4f",
            whale_address[:10],
            token_id[:10],
            token_id[-6:],
            trade["price"],
        )

        # Phase 3: sync DB write + bet_engine in executor — serialised via lock.
        async with self._db_write_lock:
            await loop.run_in_executor(
                None,
                self._sync_close_position,
                trade,
                whale_address,
                pos_mode,
                live_exit_price,
            )

    def _sync_load_position(
        self,
        token_id: str,
        whale_lower: str,
        market_info: dict | None = None,
    ) -> tuple[str, str, str, str] | None:
        """
        Load open position metadata. Returns (condition_id, outcome, question, mode) or None.

        On-chain token_ids may differ from stored CLOB token_ids in format. This function:
        1. First tries direct token_id match with normalized comparison (handles format differences)
        2. If no match, uses market_info to find by condition_id + outcome instead

        market_info should contain: conditionId (or condition_id), tokens list with outcomes
        """
        db = SessionLocal()
        try:
            # Try direct token_id match first (with normalized comparison for format differences)
            open_positions = (
                db.query(CopiedBet)
                .filter(
                    CopiedBet.status == "OPEN",
                    CopiedBet.whale_address.ilike(whale_lower),
                )
                .order_by(CopiedBet.opened_at.asc())
                .all()
            )

            # Find match using normalized asset_id_matches() to handle format differences
            open_pos = next(
                (p for p in open_positions if asset_id_matches(p.token_id or "", token_id)),
                None,
            )

            if open_pos:
                return (
                    open_pos.market_id or "",
                    getattr(open_pos, "outcome", "") or "",
                    getattr(open_pos, "question", "") or "",
                    open_pos.mode,
                )

            # Fallback: match by market condition_id + outcome if market_info provided
            if market_info:
                condition_id = (
                    market_info.get("conditionId") or market_info.get("condition_id") or ""
                )
                if condition_id:
                    # Find outcome for this token from market_info
                    outcome = "YES"
                    for tok in market_info.get("tokens") or []:
                        if asset_id_matches(tok.get("token_id", ""), token_id):
                            outcome = (tok.get("outcome") or "Yes").upper()
                            break

                    # Now look for position by condition_id + outcome instead of token_id
                    open_pos = (
                        db.query(CopiedBet)
                        .filter(
                            CopiedBet.status == "OPEN",
                            CopiedBet.market_id == condition_id,
                            CopiedBet.outcome == outcome,
                            CopiedBet.whale_address.ilike(whale_lower),
                        )
                        .order_by(CopiedBet.opened_at.asc())
                        .first()
                    )
                    if open_pos:
                        log.debug(
                            "WhaleChainMonitor: matched position by condition_id+outcome "
                            "(on-chain token %s didn't match stored token %s)",
                            token_id[:16],
                            open_pos.token_id[:16] if open_pos.token_id else "?",
                        )
                        return (
                            condition_id,
                            outcome,
                            getattr(open_pos, "question", "") or "",
                            open_pos.mode,
                        )

            # No match found via either method
            sample = (
                db.query(CopiedBet.token_id, CopiedBet.whale_address)
                .filter_by(status="OPEN")
                .limit(5)
                .all()
            )
            log.warning(
                "WhaleChainMonitor: EXIT no match — token=%s whale=%s | open positions: %s",
                token_id,
                whale_lower,
                [(r[0], r[1][:10] if r[1] else None) for r in sample],
            )
            return None
        finally:
            db.close()

    def _sync_close_position(
        self,
        trade: dict,
        whale_address: str,
        pos_mode: str,
        live_exit_price: float | None,
    ):
        """Sync DB work for exit dispatch. Runs in thread executor."""
        whale_lower = whale_address.lower()
        db = SessionLocal()
        try:
            whale_rec = db.query(Whale).filter(Whale.address.ilike(whale_lower)).first()
            if not whale_rec:
                log.warning(
                    "WhaleChainMonitor: no Whale record %s — skipping exit",
                    whale_address[:10],
                )
                return

            ts = datetime.fromtimestamp(trade["timestamp"], tz=UTC)
            tx_hash = trade.get("transactionHash", "")
            if tx_hash.startswith("0x"):
                tx_hash = tx_hash[2:]

            whale_bet = WhaleBet(
                whale_id=whale_rec.id,
                market_id=trade.get("conditionId", ""),
                token_id=trade.get("asset", ""),
                question=trade.get("question", ""),
                side="SELL",
                outcome=trade.get("outcome", "YES"),
                price=trade.get("price", 0.5),
                size_usdc=trade.get("usdcSize", 0.0),
                size_shares=trade.get("shares", 0.0),
                timestamp=ts,
                tx_hash=tx_hash or None,
                bet_type="EXIT",
            )
            db.add(whale_bet)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()
                # The activity API may have saved this WhaleBet first, and its
                # close attempt may have failed (FOK cancelled).  Retrieve the
                # existing record and retry _handle_exit so a prior failure gets
                # another chance rather than silently falling through to the 60s
                # orphan checker.
                if tx_hash:
                    existing = (
                        db.query(WhaleBet).filter_by(tx_hash=tx_hash, bet_type="EXIT").first()
                    )
                    if existing:
                        log.debug(
                            "WhaleChainMonitor: duplicate exit tx %s — retrying close",
                            tx_hash[:16],
                        )
                        session = (
                            db.query(MonitoringSession)
                            .filter_by(mode=pos_mode)
                            .order_by(MonitoringSession.id.desc())
                            .first()
                        )
                        if session:
                            exit_result = self._bet_engine._handle_exit(
                                existing, session, db, live_exit_price=live_exit_price
                            )
                            db.commit()
                            if exit_result is False:
                                log.warning(
                                    "WhaleChainMonitor: retry close also FOK-cancelled "
                                    "for token=%s — orphan checker will retry in ~60s",
                                    (existing.token_id or "?")[:16],
                                )
                        return
                log.debug("WhaleChainMonitor: duplicate exit tx %s — skipping", tx_hash[:16])
                return

            session = (
                db.query(MonitoringSession)
                .filter_by(mode=pos_mode)
                .order_by(MonitoringSession.id.desc())
                .first()
            )
            if not session:
                db.commit()
                return

            exit_result = self._bet_engine._handle_exit(
                whale_bet, session, db, live_exit_price=live_exit_price
            )
            db.commit()

            if exit_result is False:
                log.warning(
                    "WhaleChainMonitor: sell FOK cancelled for token=%s — "
                    "orphan checker will retry in ~60s",
                    trade["asset"][:16],
                )
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
