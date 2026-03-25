"""
Background whale monitoring service.
Uses APScheduler (BackgroundScheduler) to poll whale activity
on a fixed interval without conflicting with FastAPI's event loop.
"""

import asyncio
import contextlib
import logging
import threading
from datetime import UTC, datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.exc import IntegrityError

from backend.config import settings
from backend.database import (
    CopiedBet,
    MonitoringSession,
    SessionLocal,
    Whale,
    WhaleBet,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thread-local reusable event loop
# ---------------------------------------------------------------------------
# APScheduler's BackgroundScheduler runs jobs on a thread pool.  Using
# asyncio.run() on every tick creates and destroys a new event loop each time
# (~5-10 ms overhead) and also invalidates httpx TCP connections (transports
# are tied to the event loop they were first used on).  Reusing one event loop
# per scheduler thread avoids both costs.
_tl: threading.local = threading.local()


def _run_async(coro):
    """Run *coro* on a reusable per-thread event loop."""
    if not hasattr(_tl, "loop") or _tl.loop.is_closed():
        _tl.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_tl.loop)
    return _tl.loop.run_until_complete(coro)


class WhaleMonitor:
    """
    Monitors tracked whale wallets for new trades and triggers bet copying.
    Thread-safe; designed to run alongside FastAPI via BackgroundScheduler.
    """

    def __init__(self, bet_engine, polymarket_client):
        self._bet_engine = bet_engine
        self._client = polymarket_client

        self._scheduler = BackgroundScheduler(job_defaults={"max_instances": 1, "coalesce": True})
        self._lock = threading.Lock()

        # Permanent resolution scheduler — runs regardless of session state so
        # open positions are always monitored for early-close opportunities.
        self._resolution_scheduler = BackgroundScheduler(
            job_defaults={"max_instances": 1, "coalesce": True}
        )
        self._resolution_scheduler.add_job(
            func=self._check_resolution_wrapper,
            trigger=IntervalTrigger(seconds=settings.RESOLUTION_CHECK_INTERVAL_SECONDS),
            id="check_resolution_always",
        )
        self._resolution_scheduler.add_job(
            func=self._check_orphan_positions_wrapper,
            trigger=IntervalTrigger(seconds=settings.RESOLUTION_CHECK_INTERVAL_SECONDS),
            id="check_orphan_positions_always",
        )
        self._resolution_scheduler.add_job(
            func=self.poll_exits_only,
            trigger=IntervalTrigger(seconds=settings.POLLING_INTERVAL_SECONDS),
            id="poll_exits_always",
        )
        if settings.credentials_valid():
            self._resolution_scheduler.add_job(
                func=self._auto_redemption_job,
                trigger=IntervalTrigger(seconds=settings.REDEMPTION_CHECK_INTERVAL_SECONDS),
                id="auto_redeem",
            )
            logger.info(
                "Auto-redemption job registered (every %ds)",
                settings.REDEMPTION_CHECK_INTERVAL_SECONDS,
            )

        if settings.CHAIN_EXIT_ENABLED:
            from backend.whale_chain_monitor import WhaleChainMonitor

            self._chain_monitor = WhaleChainMonitor(self._bet_engine, self)
            self._resolution_scheduler.add_job(
                func=self._chain_monitor.poll,
                trigger=IntervalTrigger(seconds=settings.CHAIN_EXIT_POLL_INTERVAL_SECONDS),
                id="chain_exit_monitor",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
            )
            logger.info(
                "On-chain exit monitor registered (every %ds) — "
                "activity-API exit polling suppressed",
                settings.CHAIN_EXIT_POLL_INTERVAL_SECONDS,
            )

        self._resolution_scheduler.add_job(
            func=self._backfill_missing_market_info,
            trigger=IntervalTrigger(minutes=15),
            id="backfill_market_info",
            next_run_time=datetime.now(UTC),
        )

        # Stop-loss monitor jobs — registered here so they run regardless of session state.
        # _stop_loss_monitor is wired in after construction from main.py.
        self._stop_loss_monitor = None
        self._resolution_scheduler.add_job(
            func=self._stop_loss_check_wrapper,
            trigger=IntervalTrigger(seconds=settings.STOP_LOSS_CHECK_INTERVAL_SECONDS),
            id="stop_loss_check",
        )
        self._resolution_scheduler.add_job(
            func=self._stop_loss_snapshot_wrapper,
            trigger=IntervalTrigger(minutes=settings.STOP_LOSS_SNAPSHOT_INTERVAL_MINUTES),
            id="stop_loss_snapshot",
        )

        self._resolution_scheduler.start()
        logger.info(
            "Permanent background scheduler started (resolution every %ds, exit polling every %ds)",
            settings.RESOLUTION_CHECK_INTERVAL_SECONDS,
            settings.POLLING_INTERVAL_SECONDS,
        )

        # { whale_address: datetime } - last seen trade timestamp per whale
        self._last_seen: dict[str, datetime] = {}

        self._session_id: int | None = None
        self._running = False

        # Load last-seen timestamps from DB on init
        self._init_last_seen()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _init_last_seen(self):
        """
        Initialise last_seen timestamps from the most recent WhaleBet
        per whale address in the database.
        """
        db = SessionLocal()
        try:
            whales = db.query(Whale).filter_by(is_active=True).all()
            for whale in whales:
                latest = (
                    db.query(WhaleBet)
                    .filter_by(whale_id=whale.id)
                    .order_by(WhaleBet.timestamp.desc())
                    .first()
                )
                if latest and latest.timestamp:
                    ts = latest.timestamp
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=UTC)
                    self._last_seen[whale.address] = ts
                    logger.debug(
                        "Initialized last_seen for %s: %s",
                        whale.address[:10],
                        ts.isoformat(),
                    )
        except Exception as exc:
            logger.error("_init_last_seen error: %s", exc)
        finally:
            db.close()

    def start_monitoring(self):
        """
        Start the polling scheduler.
        The monitor processes every active MonitoringSession found in the DB
        on each tick, so multiple modes (SIMULATION + HEDGE_SIM) run in parallel.
        Calls refresh_risk_profiles() immediately, then polls every
        POLLING_INTERVAL_SECONDS seconds.
        """
        with self._lock:
            if self._running:
                logger.debug(
                    "Monitor already running — new session will be picked up automatically"
                )
                return
            self._running = True

        logger.info(
            "Starting whale monitor (interval %ds)",
            settings.POLLING_INTERVAL_SECONDS,
        )

        # Kick off immediate risk profile refresh
        try:
            self.refresh_risk_profiles()
        except Exception as exc:
            logger.error("Initial refresh_risk_profiles error: %s", exc)

        # Main polling job — next_run_time=datetime.now() fires the first poll
        # immediately instead of waiting the full interval.
        self._scheduler.add_job(
            func=self.poll_whales,
            trigger=IntervalTrigger(seconds=settings.POLLING_INTERVAL_SECONDS),
            id="poll_whales",
            replace_existing=True,
            next_run_time=datetime.now(),
        )

        # Hourly risk profile refresh
        self._scheduler.add_job(
            func=self.refresh_risk_profiles,
            trigger=IntervalTrigger(hours=1),
            id="refresh_risk_profiles",
            replace_existing=True,
        )

        # Fast drift-retry loop — re-checks price every DRIFT_RETRY_INTERVAL_SECONDS
        # for near-expiry bets that were skipped due to price drift.
        self._scheduler.add_job(
            func=self._run_drift_retry,
            trigger=IntervalTrigger(seconds=settings.DRIFT_RETRY_INTERVAL_SECONDS),
            id="drift_retry",
            replace_existing=True,
        )

        # Price pre-fetching — keeps CLOB prices fresh for all open-position tokens
        # so the price cache is never more than PRICE_PREFETCH_INTERVAL_SECONDS stale
        # when a new whale trade fires.
        if settings.PRICE_PREFETCH_INTERVAL_SECONDS > 0:
            self._scheduler.add_job(
                func=self._run_price_prefetch,
                trigger=IntervalTrigger(seconds=settings.PRICE_PREFETCH_INTERVAL_SECONDS),
                id="price_prefetch",
                replace_existing=True,
            )
            logger.info(
                "Price pre-fetch job registered (every %ds)",
                settings.PRICE_PREFETCH_INTERVAL_SECONDS,
            )

        # Frequent wallet balance sync — keeps session.current_balance_usdc aligned
        # with the live wallet so bet sizing stays accurate.  5-minute cadence
        # ensures a failed/stuck sell that under-reports available USDC doesn't
        # block new bets for longer than one poll cycle.
        if settings.credentials_valid():
            self._scheduler.add_job(
                func=self._sync_real_balance,
                trigger=IntervalTrigger(minutes=5),
                id="sync_real_balance",
                replace_existing=True,
            )

        if not self._scheduler.running:
            self._scheduler.start()

        logger.info("Whale monitor started")

    def stop_monitoring(self):
        """Stop polling. DB session state is managed by the API layer."""
        with self._lock:
            if not self._running:
                return
            self._running = False

        logger.info("Stopping whale monitor")

        for job_id in (
            "poll_whales",
            "refresh_risk_profiles",
            "drift_retry",
            "sync_real_balance",
            "price_prefetch",
        ):
            with contextlib.suppress(Exception):
                self._scheduler.remove_job(job_id)

        logger.info("Whale monitor stopped (resolution checker still running)")

    def shutdown(self):
        """Full shutdown including the permanent resolution scheduler."""
        self.stop_monitoring()
        if self._resolution_scheduler.running:
            self._resolution_scheduler.shutdown(wait=False)
            logger.info("Permanent resolution checker stopped")

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    def _run_drift_retry(self):
        """
        Sync wrapper for the async retry_drift_watchlist method.
        Called every DRIFT_RETRY_INTERVAL_SECONDS by the scheduler.
        No-ops immediately if the watchlist is empty.
        """
        if not self._bet_engine._drift_watchlist:
            return
        db = SessionLocal()
        try:
            _run_async(self._bet_engine.retry_drift_watchlist(db))
        except Exception as exc:
            logger.error("_run_drift_retry error: %s", exc)
        finally:
            db.close()

    def _run_price_prefetch(self):
        """
        Sync wrapper for _prefetch_prices.
        Runs every PRICE_PREFETCH_INTERVAL_SECONDS to keep CLOB prices fresh
        for all tokens in open positions. No-ops if there are no open positions.
        """
        _run_async(self._prefetch_prices())

    async def _prefetch_prices(self):
        """
        Fetch fresh BUY prices for every distinct token_id that has an open
        CopiedBet. Results are stored in the client's price cache so that
        check_whale_activity finds an up-to-date price immediately on the
        next poll tick rather than waiting for a live HTTP fetch.
        """
        db = SessionLocal()
        try:
            rows = db.query(CopiedBet.token_id).filter_by(status="OPEN").distinct().all()
            token_ids = [r[0] for r in rows if r[0]]
        except Exception as exc:
            logger.error("_prefetch_prices: DB error: %s", exc)
            return
        finally:
            db.close()

        if not token_ids:
            return

        await asyncio.gather(
            *[self._client.get_best_price(tid, force_refresh=True) for tid in token_ids],
            return_exceptions=True,
        )
        logger.debug("Price pre-fetch: refreshed %d token(s)", len(token_ids))

    def poll_whales(self):
        """
        Called on each scheduler tick.
        Fetches all whale activity in parallel (single event loop + asyncio.gather),
        then processes each sequentially to avoid session-balance race conditions.
        """
        if not self._running:
            return

        # Fix #3a: query only the address column — no need for full ORM objects
        db = SessionLocal()
        try:
            active = [row[0] for row in db.query(Whale.address).filter_by(is_active=True).all()]
            # Inactive whales with open REAL positions must still be polled for exits.
            real_open = [
                row[0]
                for row in db.query(CopiedBet.whale_address)
                .filter(CopiedBet.status == "OPEN", CopiedBet.mode == "REAL")
                .distinct()
                .all()
            ]
            addresses = list({*active, *real_open})
        except Exception as exc:
            logger.error("poll_whales: failed to load whales: %s", exc)
            return
        finally:
            db.close()

        if not addresses:
            return

        logger.debug(
            "Polling %d whales (%d active, %d inactive with open REAL bets)",
            len(addresses),
            len(active),
            len(set(real_open) - set(active)),
        )

        _run_async(self._poll_all_async(addresses))

    async def _poll_all_async(self, addresses: list):
        """
        Phase 1 — parallel HTTP: fetch all whale activity at once.
        Phase 2 — sequential processing: process each result one at a time
                   so session balance updates never race each other.
        """
        fetch_results = await asyncio.gather(
            *[self._client.get_user_activity(addr, limit=100) for addr in addresses],
            return_exceptions=True,
        )

        for address, trades in zip(addresses, fetch_results, strict=False):
            if isinstance(trades, Exception):
                logger.error("Error fetching activity for whale %s: %s", address[:10], trades)
                continue
            try:
                await self.check_whale_activity(address, trades or [])
            except Exception as exc:
                logger.error("Error processing whale %s: %s", address[:10], exc)

    async def check_whale_activity(self, address: str, trades: list | None = None):
        """
        Process new trades for a whale.  When called from _poll_all_async the
        trades list is already fetched (parallel); passing None triggers a
        standalone fetch (e.g. for manual / test invocations).
        """
        if not self._running:
            return

        if trades is None:
            trades = await self._client.get_user_activity(address, limit=100)
        if not trades:
            return

        last_seen = self._last_seen.get(address)
        now_utc = datetime.now(UTC)
        max_age_seconds = settings.MAX_TRADE_AGE_HOURS * 3600
        new_trades = []

        for trade in trades:
            ts = self._parse_trade_timestamp(trade)
            if ts is None:
                continue

            # Reject trades older than MAX_TRADE_AGE_HOURS — these are historical
            # entries that likely reference already-closed markets.
            age_seconds = (now_utc - ts).total_seconds()
            if age_seconds > max_age_seconds:
                continue

            if last_seen is None or ts > last_seen:
                new_trades.append((ts, trade))

        if not new_trades:
            logger.debug("No new trades for whale %s", address[:10])
            return

        # Sort oldest first so we process in chronological order
        new_trades.sort(key=lambda x: x[0])
        logger.info(
            "Found %d new trades for whale %s",
            len(new_trades),
            address[:10],
        )

        db = SessionLocal()
        try:
            whale = db.query(Whale).filter_by(address=address).first()
            if not whale:
                return

            active_sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
            if not active_sessions:
                logger.warning("No active sessions — stopping monitor")
                self._running = False
                return

            new_last_seen = last_seen

            for ts, trade in new_trades:
                try:
                    # Extract identifiers from raw trade dict — available before DB flush.
                    # We fetch market data NOW so the DB write lock window stays minimal:
                    # previously the API calls ran *after* db.flush() (which takes a
                    # SQLite RESERVED lock), causing "database is locked" on concurrent
                    # writes (e.g. stop_session) if any API call was slow.
                    trade_market_id = (
                        trade.get("conditionId")
                        or trade.get("market")
                        or trade.get("marketId")
                        or ""
                    )
                    trade_token_id = (
                        trade.get("asset")
                        or trade.get("tokenId")
                        or trade.get("outcome_token_id")
                        or ""
                    )

                    # Fetch market metadata, live price, and taker fee in parallel
                    # BEFORE the DB flush so no lock is held during network I/O.
                    market_result, price_result, fee_result = await asyncio.gather(
                        self._client.get_market(trade_market_id, token_id=trade_token_id),
                        self._client.get_best_price(trade_token_id, force_refresh=True),
                        self._client.get_taker_fee_async(trade_token_id),
                        return_exceptions=True,
                    )
                    market_info = market_result if isinstance(market_result, dict) else {}
                    live_price = price_result if isinstance(price_result, float) else None
                    taker_fee_bps = fee_result if isinstance(fee_result, int) else 1000

                    # Now save to DB (flush takes RESERVED lock — network already done).
                    whale_bet = await self._save_whale_bet(
                        trade, whale, ts, db, market_info=market_info
                    )
                    if whale_bet:
                        # Process the whale bet for every active session (supports
                        # simultaneous SIMULATION + HEDGE_SIM for side-by-side comparison)
                        for session in active_sessions:
                            self._bet_engine.process_new_whale_bet(
                                whale_bet=whale_bet,
                                session=session,
                                db=db,
                                market_info=market_info,
                                live_price=live_price,
                                taker_fee_bps=taker_fee_bps,
                            )

                    if new_last_seen is None or ts > new_last_seen:
                        new_last_seen = ts
                except Exception as exc:
                    logger.error("Error processing trade for %s: %s", address[:10], exc)
                    db.rollback()

            # Update whale stats
            whale.total_bets_tracked = db.query(WhaleBet).filter_by(whale_id=whale.id).count()
            db.commit()

            # Update in-memory last_seen
            with self._lock:
                if new_last_seen:
                    self._last_seen[address] = new_last_seen

        except Exception as exc:
            logger.error("check_whale_activity error for %s: %s", address[:10], exc)
            db.rollback()
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Permanent exit polling (runs even when main scan is stopped)
    # ------------------------------------------------------------------

    def poll_exits_only(self):
        """
        Always-on background job: poll tracked whales for EXIT (SELL) trades
        and close our matching open positions immediately.

        Skipped when the main scanner is active — it already handles exits
        as part of full trade processing.
        Skipped when on-chain monitoring is active — chain monitor handles exits
        with block-number deduplication and no _last_seen signal-loss risk.
        """
        if self._running:
            return  # main scanner covers this
        if settings.CHAIN_EXIT_ENABLED:
            return  # chain monitor covers this

        db = SessionLocal()
        try:
            active = [row[0] for row in db.query(Whale.address).filter_by(is_active=True).all()]
            real_open = [
                row[0]
                for row in db.query(CopiedBet.whale_address)
                .filter(CopiedBet.status == "OPEN", CopiedBet.mode == "REAL")
                .distinct()
                .all()
            ]
            addresses = list({*active, *real_open})
        except Exception as exc:
            logger.error("poll_exits_only: failed to load whales: %s", exc)
            return
        finally:
            db.close()

        if not addresses:
            return

        _run_async(self._poll_exits_async(addresses))

    async def _poll_exits_async(self, addresses: list):
        """Fetch recent activity for all whales in parallel, then handle exits."""
        fetch_results = await asyncio.gather(
            *[self._client.get_user_activity(addr, limit=100) for addr in addresses],
            return_exceptions=True,
        )
        for address, trades in zip(addresses, fetch_results, strict=False):
            if isinstance(trades, Exception):
                logger.debug("poll_exits_only fetch error for %s: %s", address[:10], trades)
                continue
            try:
                await self._handle_exit_trades(address, trades or [])
            except Exception as exc:
                logger.error("poll_exits_only error for %s: %s", address[:10], exc)

    async def _handle_exit_trades(self, address: str, trades: list):
        """
        Filter trades to new EXIT signals and close any matching open positions.
        Only acts if we actually hold a position in the exited market.
        """
        last_seen = self._last_seen.get(address)
        now_utc = datetime.now(UTC)
        max_age_seconds = settings.MAX_TRADE_AGE_HOURS * 3600

        new_exits = []
        for trade in trades:
            ts = self._parse_trade_timestamp(trade)
            if ts is None:
                continue
            if (now_utc - ts).total_seconds() > max_age_seconds:
                continue
            if last_seen is not None and ts <= last_seen:
                continue
            side = (trade.get("side") or trade.get("type") or "").upper()
            if side != "SELL":
                continue
            new_exits.append((ts, trade))

        if not new_exits:
            return

        new_exits.sort(key=lambda x: x[0])
        logger.info(
            "Background exit poll: %d new EXIT trade(s) from whale %s",
            len(new_exits),
            address[:10],
        )

        db = SessionLocal()
        try:
            whale = db.query(Whale).filter_by(address=address).first()
            if not whale:
                return

            new_last_seen = last_seen

            for ts, trade in new_exits:
                try:
                    token_id = (
                        trade.get("asset")
                        or trade.get("tokenId")
                        or trade.get("outcome_token_id")
                        or ""
                    )
                    market_id = (
                        trade.get("conditionId")
                        or trade.get("market")
                        or trade.get("marketId")
                        or ""
                    )

                    # Find ALL open positions for this token from this whale — each
                    # whale BUY creates its own CopiedBet, so an EXIT should close
                    # every tranche we hold simultaneously.
                    open_positions = []
                    if token_id:
                        open_positions = (
                            db.query(CopiedBet)
                            .filter_by(status="OPEN", token_id=token_id, whale_address=address)
                            .order_by(CopiedBet.opened_at.asc())
                            .all()
                        )
                    if not open_positions and market_id:
                        open_positions = (
                            db.query(CopiedBet)
                            .filter_by(status="OPEN", market_id=market_id, whale_address=address)
                            .order_by(CopiedBet.opened_at.asc())
                            .all()
                        )

                    if not open_positions:
                        # No positions — still update last_seen so we don't re-check
                        if new_last_seen is None or ts > new_last_seen:
                            new_last_seen = ts
                        continue

                    # Persist the whale's exit bet for record keeping
                    whale_bet = await self._save_whale_bet(trade, whale, ts, db)
                    if not whale_bet:
                        # Duplicate tx_hash — already processed
                        if new_last_seen is None or ts > new_last_seen:
                            new_last_seen = ts
                        continue

                    # Find the most recent session matching this position's mode
                    open_pos = open_positions[0]
                    session = (
                        db.query(MonitoringSession)
                        .filter_by(mode=open_pos.mode)
                        .order_by(MonitoringSession.id.desc())
                        .first()
                    )
                    if not session:
                        if new_last_seen is None or ts > new_last_seen:
                            new_last_seen = ts
                        continue

                    # Fetch current live price so simulation fills at market price
                    # rather than the whale's historical sell price.
                    live_exit_price = None
                    if token_id:
                        with contextlib.suppress(Exception):
                            live_exit_price = await self._client.get_best_price(
                                token_id, side="SELL"
                            )

                    # Route through _handle_exit for consistent partial/full exit
                    # logic (fraction-based tranche selection, same path as main scanner)
                    exit_result = self._bet_engine._handle_exit(
                        whale_bet, session, db, live_exit_price=live_exit_price
                    )
                    db.commit()  # safety net — _handle_exit commits internally

                except Exception as exc:
                    logger.error("_handle_exit_trades error for %s: %s", address[:10], exc)
                    db.rollback()
                    exit_result = None  # exception path — treat as "no position found"

                # Only advance _last_seen when the exit was handled successfully
                # (sell went through) or when we had no position to close (None).
                # If _handle_exit returned False the CLOB sell failed — don't
                # consume the signal so the next poll retries automatically.
                if exit_result is not False and (new_last_seen is None or ts > new_last_seen):
                    new_last_seen = ts

            with self._lock:
                if new_last_seen:
                    self._last_seen[address] = new_last_seen

        except Exception as exc:
            logger.error("_handle_exit_trades outer error for %s: %s", address[:10], exc)
            db.rollback()
        finally:
            db.close()

    async def _save_whale_bet(
        self, trade: dict, whale: Whale, ts: datetime, db, market_info: dict | None = None
    ) -> WhaleBet | None:
        """
        Parse a raw trade dict and persist a WhaleBet record.
        Returns None if the trade is a duplicate or cannot be parsed.
        """
        tx_hash = trade.get("transactionHash") or trade.get("id") or trade.get("orderId", "")
        # Normalise: strip 0x prefix so activity-API hashes (no prefix) and
        # chain-monitor hashes (0x-prefixed HexBytes.hex()) always match.
        if tx_hash and tx_hash.startswith("0x"):
            tx_hash = tx_hash[2:]

        # Dedup by tx_hash — rely on the UNIQUE index rather than a racy SELECT-first.
        # IntegrityError is raised if a concurrent scanner already inserted the same hash.

        # Parse fields - Polymarket Data API field names
        side = (trade.get("side") or trade.get("type") or "BUY").upper()
        if side not in ("BUY", "SELL"):
            side = "BUY"

        outcome = (trade.get("outcome") or "YES").upper()
        if outcome not in ("YES", "NO"):
            outcome = "YES"

        price_raw = trade.get("price") or trade.get("avgPrice") or 0.5
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            price = 0.5

        size_usdc_raw = (
            trade.get("usdcSize")
            or trade.get("amount")
            or trade.get("cost")
            or trade.get("cashAmount")
            or 0.0
        )
        try:
            size_usdc = float(size_usdc_raw)
        except (TypeError, ValueError):
            size_usdc = 0.0

        size_shares_raw = trade.get("shares") or trade.get("size") or trade.get("contractsFilled")
        try:
            size_shares = float(size_shares_raw) if size_shares_raw is not None else 0.0
        except (TypeError, ValueError):
            size_shares = 0.0
        # Fall back to deriving from USDC cost when the API omits shares or returns 0
        if size_shares <= 0:
            size_shares = size_usdc / max(price, 0.001)

        # Market identifiers — fall back to market_info fetched from Gamma API
        _mi = market_info or {}
        market_id = (
            trade.get("conditionId")
            or trade.get("market")
            or trade.get("marketId")
            or _mi.get("conditionId")
            or _mi.get("condition_id")
            or ""
        )
        token_id = trade.get("asset") or trade.get("tokenId") or trade.get("outcome_token_id") or ""
        question = (
            trade.get("title")
            or trade.get("question")
            or trade.get("market_question")
            or _mi.get("question")
            or _mi.get("title")
            or ""
        )

        # Determine bet type
        bet_type = "EXIT" if side == "SELL" else "OPEN"

        whale_bet = WhaleBet(
            whale_id=whale.id,
            market_id=market_id,
            token_id=token_id,
            question=question,
            side=side,
            outcome=outcome,
            price=price,
            size_usdc=size_usdc,
            size_shares=size_shares,
            timestamp=ts,
            tx_hash=tx_hash or None,
            bet_type=bet_type,
        )
        db.add(whale_bet)
        try:
            db.flush()  # Get ID without committing
        except IntegrityError:
            db.rollback()
            logger.debug("Duplicate tx_hash %s — skipping", tx_hash)
            return None
        return whale_bet

    # ------------------------------------------------------------------
    # Risk profile refresh
    # ------------------------------------------------------------------

    def refresh_risk_profiles(self):
        """
        Recalculate avg_bet_size_usdc for all active whales from their
        last 100 WhaleBet records in the database.
        """
        db = SessionLocal()
        try:
            whales = db.query(Whale).filter_by(is_active=True).all()
            for whale in whales:
                try:
                    recent_bets = (
                        db.query(WhaleBet)
                        .filter_by(whale_id=whale.id, bet_type="OPEN")
                        .order_by(WhaleBet.timestamp.desc())
                        .limit(100)
                        .all()
                    )
                    if recent_bets:
                        avg = sum(b.size_usdc for b in recent_bets) / len(recent_bets)
                        whale.avg_bet_size_usdc = round(avg, 2)
                        whale.risk_profile_calculated_at = datetime.utcnow()
                        logger.debug(
                            "Updated avg bet for %s: $%.2f (%d bets)",
                            whale.address[:10],
                            avg,
                            len(recent_bets),
                        )
                except Exception as exc:
                    logger.error(
                        "refresh_risk_profiles error for %s: %s",
                        whale.address[:10],
                        exc,
                    )

            db.commit()
            logger.info("Refreshed risk profiles for %d whales", len(whales))
        except Exception as exc:
            logger.error("refresh_risk_profiles outer error: %s", exc)
            db.rollback()
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Wallet balance sync (REAL mode)
    # ------------------------------------------------------------------

    def _sync_real_balance(self):
        """
        Re-fetch the live wallet balance from Polymarket and update any active
        REAL session's current_balance_usdc.  Runs every 5 minutes so a failed
        or stuck sell that under-reports available USDC doesn't block new bets
        for an extended period (e.g. partial fills, gas costs, manual trades).
        """
        try:
            balance = _run_async(self._client.get_wallet_balance())
        except Exception as exc:
            logger.error("_sync_real_balance: wallet fetch error: %s", exc)
            return

        if balance is None:
            logger.debug("_sync_real_balance: no balance returned — skipping")
            return

        db = SessionLocal()
        try:
            session = (
                db.query(MonitoringSession)
                .filter_by(mode="REAL", is_active=True)
                .order_by(MonitoringSession.id.desc())
                .first()
            )
            if session:
                old = session.current_balance_usdc
                session.current_balance_usdc = balance
                db.commit()
                logger.info(
                    "Balance sync: REAL session balance $%.2f → $%.2f (live wallet)",
                    old,
                    balance,
                )
        except Exception as exc:
            logger.error("_sync_real_balance: DB error: %s", exc)
            db.rollback()
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Resolution wrapper (sync -> runs in scheduler thread)
    # ------------------------------------------------------------------

    def _check_resolution_wrapper(self):
        try:
            self._bet_engine.check_resolution()
        except Exception as exc:
            logger.error("check_resolution error: %s", exc)

    def _check_orphan_positions_wrapper(self):
        try:
            self._bet_engine.check_orphan_positions()
        except Exception as exc:
            logger.error("check_orphan_positions error: %s", exc)

    def _stop_loss_check_wrapper(self):
        if self._stop_loss_monitor is None:
            return
        try:
            _run_async(self._stop_loss_monitor.check_async())
        except Exception as exc:
            logger.error("stop_loss_check error: %s", exc)

    def _stop_loss_snapshot_wrapper(self):
        if self._stop_loss_monitor is None:
            return
        try:
            _run_async(self._stop_loss_monitor.snapshot_async())
        except Exception as exc:
            logger.error("stop_loss_snapshot error: %s", exc)

    def _auto_redemption_job(self):
        """Periodically redeem resolved positions on-chain. Runs in permanent scheduler."""
        from backend.database import CopiedBet, MonitoringSession, SessionLocal
        from backend.redemption import check_and_redeem

        try:
            result = _run_async(check_and_redeem())
            redeemed = result.get("redeemed", 0)
            total_gas_matic = result.get("total_gas_matic", 0.0)
            if redeemed > 0 or total_gas_matic > 0:
                total = result.get("total_value", 0.0)
                total_gas_usdc = round(total_gas_matic * settings.MATIC_USD_PRICE, 6)
                logger.info(
                    "Auto-redeemed %d position(s) for $%.2f (gas=%.6f MATIC ≈ $%.4f)",
                    redeemed,
                    total,
                    total_gas_matic,
                    total_gas_usdc,
                )
                if total_gas_usdc > 0:
                    db = SessionLocal()
                    try:
                        # Allocate gas per-bet: distribute each position's gas cost
                        # proportionally across all closed tranches for that market_id.
                        for pos_info in result.get("redeemed_positions", []):
                            condition_id = pos_info.get("condition_id", "")
                            pos_gas_usdc = round(
                                pos_info.get("gas_matic", 0.0) * settings.MATIC_USD_PRICE, 6
                            )
                            if not condition_id or pos_gas_usdc <= 0:
                                continue
                            tranches = (
                                db.query(CopiedBet)
                                .filter(
                                    CopiedBet.market_id == condition_id,
                                    CopiedBet.mode == "REAL",
                                    CopiedBet.status.in_(
                                        ["CLOSED_WIN", "CLOSED_LOSS", "CLOSED_NEUTRAL"]
                                    ),
                                    CopiedBet.gas_fees_usdc == None,  # noqa: E711 — SQLAlchemy IS NULL
                                )
                                .all()
                            )
                            if not tranches:
                                continue
                            total_size = sum(t.size_usdc for t in tranches) or 1.0
                            for tranche in tranches:
                                share = tranche.size_usdc / total_size
                                tranche.gas_fees_usdc = round(pos_gas_usdc * share, 6)
                                db.add(tranche)

                        # Accumulate on session totals
                        session = (
                            db.query(MonitoringSession)
                            .filter_by(mode="REAL", is_active=True)
                            .order_by(MonitoringSession.id.desc())
                            .first()
                        )
                        if session:
                            session.total_gas_fees_usdc = round(
                                (session.total_gas_fees_usdc or 0.0) + total_gas_usdc, 6
                            )
                            db.add(session)
                        db.commit()
                    except Exception as db_exc:
                        logger.error("Failed to record gas fees: %s", db_exc)
                        db.rollback()
                    finally:
                        db.close()
        except Exception as exc:
            logger.error("Auto-redemption job error: %s", exc)

    def _backfill_missing_market_info(self):
        """
        Periodic job: find whale_bets and copied_bets with empty question or
        market_id and backfill them from the Gamma API.  Runs every 15 minutes
        on the permanent scheduler so records missed at insertion time are
        corrected quickly.
        """
        import httpx

        from backend.categorizer import classify
        from backend.database import CopiedBet, SessionLocal, WhaleBet

        db = SessionLocal()
        try:
            missing_wb = (
                db.query(WhaleBet.token_id)
                .filter(
                    WhaleBet.token_id != "",
                    WhaleBet.token_id.isnot(None),
                    (WhaleBet.question == "") | WhaleBet.question.is_(None),
                )
                .distinct()
                .all()
            )
            missing_cb = (
                db.query(CopiedBet.token_id)
                .filter(
                    CopiedBet.token_id != "",
                    CopiedBet.token_id.isnot(None),
                    (CopiedBet.question == "") | CopiedBet.question.is_(None),
                )
                .distinct()
                .all()
            )

            token_ids = {r[0] for r in missing_wb} | {r[0] for r in missing_cb}
            if not token_ids:
                return

            logger.info("Backfill: fetching market info for %d token_id(s)", len(token_ids))
            client = httpx.Client(timeout=15)
            updated_wb = updated_cb = 0
            try:
                for token_id in token_ids:
                    try:
                        r = client.get(
                            "https://gamma-api.polymarket.com/markets",
                            params={"clob_token_ids": token_id},
                        )
                        data = r.json()
                        market = data[0] if isinstance(data, list) and data else None
                        if not market:
                            continue
                        question = market.get("question") or market.get("title") or ""
                        condition_id = market.get("conditionId") or market.get("condition_id") or ""
                        if not question:
                            continue
                        market_category, _ = classify(question)

                        wb_count = (
                            db.query(WhaleBet)
                            .filter(
                                WhaleBet.token_id == token_id,
                                (WhaleBet.question == "") | WhaleBet.question.is_(None),
                            )
                            .update(
                                {"question": question, "market_id": condition_id},
                                synchronize_session=False,
                            )
                        )
                        cb_count = (
                            db.query(CopiedBet)
                            .filter(
                                CopiedBet.token_id == token_id,
                                (CopiedBet.question == "") | CopiedBet.question.is_(None),
                            )
                            .update(
                                {
                                    "question": question,
                                    "market_id": condition_id,
                                    "market_category": market_category,
                                },
                                synchronize_session=False,
                            )
                        )
                        updated_wb += wb_count
                        updated_cb += cb_count
                    except Exception as fetch_exc:
                        logger.warning(
                            "Backfill fetch error for token %s: %s", token_id[:16], fetch_exc
                        )
            finally:
                client.close()

            if updated_wb or updated_cb:
                db.commit()
                logger.info(
                    "Backfill complete: %d whale_bet(s), %d copied_bet(s) updated",
                    updated_wb,
                    updated_cb,
                )
        except Exception as exc:
            logger.error("Backfill market info job error: %s", exc)
            db.rollback()
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_trade_timestamp(trade: dict) -> datetime | None:
        """Parse timestamp from a trade dict, returning a UTC-aware datetime."""
        raw = (
            trade.get("timestamp")
            or trade.get("createdAt")
            or trade.get("created_at")
            or trade.get("time")
        )
        if raw is None:
            return None

        if isinstance(raw, (int, float)):
            # Unix epoch seconds or milliseconds
            if raw > 1e12:
                raw = raw / 1000.0
            try:
                return datetime.fromtimestamp(raw, tz=UTC)
            except Exception:
                return None

        if isinstance(raw, str):
            raw = raw.rstrip("Z")
            try:
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt
            except (ValueError, TypeError):
                return None

        return None

    def get_status(self) -> dict:
        """Return current monitor status for API."""
        return {
            "running": self._running,
            "tracked_whales": len(self._last_seen),
        }

    def reset_last_seen(self, address: str):
        """Clear the last-seen timestamp for a whale (forces re-scan)."""
        with self._lock:
            self._last_seen.pop(address, None)
