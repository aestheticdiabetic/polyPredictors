"""
Polymarket Real-Time Data Socket (RTDS) exit monitor.

Connects to wss://ws-live-data.polymarket.com and streams the global
activity/trades feed.  Messages are filtered client-side for tracked
whale addresses.  When a whale SELL is detected, the matching open
position is closed immediately via _handle_exit_trades — the same path
used by the HTTP activity poll (poll_exits_only), so tx_hash deduplication
in _save_whale_bet prevents double-closes if the chain monitor or activity
poll also fires.

Latency: CLOB-match time (~50-200ms) vs Polygon chain (~2s) vs HTTP poll (~5s).

Architecture mirrors WhaleChainMonitor / ClobWsEntryMonitor:
  - Phase 1: async WS receive on event loop
  - Phase 2: async _handle_exit_trades (already async, reused directly)
  - Reconnects with exponential backoff on any connection failure.

Controlled by RTDS_ENABLED (default: false).
"""

import asyncio
import json
import logging
from datetime import UTC, datetime

import websockets

from backend.database import SessionLocal, Whale

log = logging.getLogger(__name__)

_RECONNECT_DELAYS = [1, 2, 4, 8, 16, 30, 60]
_RTDS_URL = "wss://ws-live-data.polymarket.com"

# RTDS PING interval — server closes connection after ~30s without activity
_PING_INTERVAL_S = 10


class RtdsExitMonitor:
    """
    Subscribes to the Polymarket RTDS global trades feed.
    Filters for whale SELL events and dispatches closes via whale_monitor.

    On connect  : sends subscription message for activity/trades.
    On message  : filters for whale sells, dispatches _handle_exit_trades.
    On disconnect: reconnects with exponential backoff.
    """

    _WHALE_MAP_TTL_S = 30

    def __init__(self, whale_monitor):
        self._whale_monitor = whale_monitor
        self._running = False
        self._task: asyncio.Task | None = None

        self._whale_map: dict[str, str] = {}  # lowercase → original
        self._whale_map_ts: datetime | None = None

        # Deduplicate: track recently dispatched tx_hashes to prevent double-
        # dispatching if the same trade arrives twice from the RTDS feed.
        self._recent_txs: set[str] = set()
        self._recent_txs_lock = asyncio.Lock()

        log.info("RtdsExitMonitor initialized (url=%s)", _RTDS_URL)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> asyncio.Task:
        self._running = True
        self._task = asyncio.create_task(self._run(), name="rtds_exit_monitor")
        return self._task

    def stop(self):
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
                log.info("RtdsExitMonitor: task cancelled — stopping")
                return
            except Exception as exc:
                delay = _RECONNECT_DELAYS[min(attempt, len(_RECONNECT_DELAYS) - 1)]
                log.warning(
                    "RtdsExitMonitor: connection lost (%s) — reconnecting in %ds",
                    exc,
                    delay,
                )
                attempt += 1
                await asyncio.sleep(delay)

    async def _connect_and_stream(self):
        log.info("RtdsExitMonitor: connecting to %s", _RTDS_URL)

        async with websockets.connect(
            _RTDS_URL,
            ping_interval=_PING_INTERVAL_S,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            log.info("RtdsExitMonitor: WebSocket connected")

            await self._maybe_refresh_whale_map()
            await self._subscribe(ws)

            async for raw_msg in ws:
                if not self._running:
                    return
                await self._handle_message(raw_msg)

    # ------------------------------------------------------------------
    # Subscription
    # ------------------------------------------------------------------

    async def _subscribe(self, ws):
        """Send subscription for the global activity/trades feed."""
        # Subscribe to the live-data activity channel.
        # Note: market-specific filters are currently broken (Issue #34),
        # so we subscribe globally and filter whale addresses client-side.
        sub_msg = json.dumps({"type": "subscribe", "channel": "activity"})
        await ws.send(sub_msg)
        log.info(
            "RtdsExitMonitor: subscribed to activity feed (%d whale(s) in filter)",
            len(self._whale_map),
        )

    # ------------------------------------------------------------------
    # Message handler
    # ------------------------------------------------------------------

    async def _handle_message(self, raw_msg: str):
        await self._maybe_refresh_whale_map()
        if not self._whale_map:
            return

        try:
            msg = json.loads(raw_msg)
        except Exception:
            return

        # RTDS sends both wrapped {"type": "...", "data": [...]} envelopes
        # and raw list payloads depending on the event type.
        events: list = []
        if isinstance(msg, list):
            events = msg
        elif isinstance(msg, dict):
            data = msg.get("data") or msg.get("events") or []
            if isinstance(data, list):
                events = data
            elif isinstance(msg.get("type"), str):
                # Single event as dict
                events = [msg]

        for event in events:
            await self._process_event(event)

    async def _process_event(self, event: dict):
        """Filter event for whale SELL and dispatch close if matched."""
        if not isinstance(event, dict):
            return

        # Accept "trade", "TRADE", "fill", "activity" event types
        event_type = (event.get("type") or event.get("event_type") or "").lower()
        if event_type not in ("trade", "fill", "sell", "activity", ""):
            return

        # Side check — we only care about SELL events
        side = (event.get("side") or event.get("outcome_side") or "").upper()
        # If side is explicit and not SELL, skip
        if side and side != "SELL":
            return

        # Extract maker address — try various field names used by different RTDS versions
        maker = (
            event.get("maker_address") or event.get("makerAddress") or event.get("maker") or ""
        ).lower()

        taker = (
            event.get("taker_address") or event.get("takerAddress") or event.get("taker") or ""
        ).lower()

        whale_lower = None
        if maker and maker in self._whale_map:
            whale_lower = maker
        elif taker and taker in self._whale_map:
            whale_lower = taker

        if whale_lower is None:
            return

        # Determine if the whale is the seller
        # In CLOB: maker sells → receives USDC (makerAsset = 0 means maker gave tokens)
        # We use the "side" field when available; otherwise infer from asset fields
        if side == "SELL":
            pass  # confirmed sell
        elif side == "BUY":
            return  # not a sell
        else:
            # No explicit side — check if this looks like a sell by examining
            # whether the maker is giving conditional tokens (non-USDC asset)
            maker_asset = event.get("makerAssetId") or event.get("maker_asset_id")
            if maker_asset is not None and str(maker_asset) == "0":
                return  # maker giving USDC = buy, not sell

        whale_address = self._whale_map[whale_lower]

        # Deduplicate by tx_hash
        tx_hash = (
            event.get("transactionHash")
            or event.get("transaction_hash")
            or event.get("id")
            or event.get("orderId")
            or ""
        )
        if tx_hash:
            async with self._recent_txs_lock:
                if tx_hash in self._recent_txs:
                    return
                self._recent_txs.add(tx_hash)
                # Trim set — keep last 500 to avoid unbounded growth
                if len(self._recent_txs) > 500:
                    # Remove oldest items (set doesn't preserve order, so just clear excess)
                    overflow = len(self._recent_txs) - 500
                    to_remove = list(self._recent_txs)[:overflow]
                    self._recent_txs -= set(to_remove)

        now_ts = datetime.now(UTC).timestamp()
        event_ts_raw = event.get("timestamp") or event.get("created_at") or now_ts
        try:
            event_ts = float(event_ts_raw)
        except (TypeError, ValueError):
            event_ts = now_ts

        lag_s = now_ts - event_ts
        log.info(
            "RtdsExitMonitor: SELL detected whale=%s lag=%.1fs token=%s",
            whale_address[:10],
            lag_s,
            str(event.get("asset_id") or event.get("asset") or "?")[:16],
        )

        # Build a trade dict compatible with _handle_exit_trades
        trade = {
            "side": "SELL",
            "asset": event.get("asset_id") or event.get("asset") or event.get("tokenId") or "",
            "conditionId": event.get("conditionId")
            or event.get("market_id")
            or event.get("market")
            or "",
            "price": self._parse_float(event.get("price")),
            "usdcSize": self._parse_float(
                event.get("amount") or event.get("cash_amount") or event.get("usdcSize")
            ),
            "shares": self._parse_float(
                event.get("size") or event.get("shares") or event.get("contracts_filled")
            ),
            "transactionHash": tx_hash,
        }

        # Derive shares from price if missing
        if trade["shares"] <= 0 and trade["usdcSize"] > 0 and trade["price"] > 0:
            trade["shares"] = trade["usdcSize"] / trade["price"]

        await self._whale_monitor._handle_exit_trades(
            whale_address, [{**trade, "timestamp": event_ts_raw}]
        )

    # ------------------------------------------------------------------
    # Whale map refresh
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
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_float(val) -> float:
        if val is None:
            return 0.0
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0
