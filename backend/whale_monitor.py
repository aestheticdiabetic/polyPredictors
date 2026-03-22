"""
Background whale monitoring service.
Uses APScheduler (BackgroundScheduler) to poll whale activity
on a fixed interval without conflicting with FastAPI's event loop.
"""

import asyncio
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from backend.config import settings
from backend.database import (
    CopiedBet,
    MonitoringSession,
    SessionLocal,
    Whale,
    WhaleBet,
)

logger = logging.getLogger(__name__)


class WhaleMonitor:
    """
    Monitors tracked whale wallets for new trades and triggers bet copying.
    Thread-safe; designed to run alongside FastAPI via BackgroundScheduler.
    """

    def __init__(self, bet_engine, polymarket_client):
        self._bet_engine = bet_engine
        self._client = polymarket_client

        self._scheduler = BackgroundScheduler(
            job_defaults={"max_instances": 1, "coalesce": True}
        )
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
                max_instances=2,
            )
            logger.info(
                "On-chain exit monitor registered (every %ds) — "
                "activity-API exit polling suppressed",
                settings.CHAIN_EXIT_POLL_INTERVAL_SECONDS,
            )

        self._resolution_scheduler.start()
        logger.info(
            "Permanent background scheduler started "
            "(resolution every %ds, exit polling every %ds)",
            settings.RESOLUTION_CHECK_INTERVAL_SECONDS,
            settings.POLLING_INTERVAL_SECONDS,
        )

        # { whale_address: datetime } - last seen trade timestamp per whale
        self._last_seen: dict[str, datetime] = {}

        self._session_id: Optional[int] = None
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
                        ts = ts.replace(tzinfo=timezone.utc)
                    self._last_seen[whale.address] = ts
                    logger.debug(
                        "Initialized last_seen for %s: %s",
                        whale.address[:10], ts.isoformat(),
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
                logger.debug("Monitor already running — new session will be picked up automatically")
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

        for job_id in ("poll_whales", "refresh_risk_profiles", "drift_retry", "sync_real_balance"):
            try:
                self._scheduler.remove_job(job_id)
            except Exception:
                pass

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
            asyncio.run(self._bet_engine.retry_drift_watchlist(db))
        except Exception as exc:
            logger.error("_run_drift_retry error: %s", exc)
        finally:
            db.close()

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
            addresses = [
                row[0]
                for row in db.query(Whale.address).filter_by(is_active=True).all()
            ]
        except Exception as exc:
            logger.error("poll_whales: failed to load whales: %s", exc)
            return
        finally:
            db.close()

        if not addresses:
            return

        logger.debug("Polling %d active whales", len(addresses))

        asyncio.run(self._poll_all_async(addresses))

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

        for address, trades in zip(addresses, fetch_results):
            if isinstance(trades, Exception):
                logger.error(
                    "Error fetching activity for whale %s: %s", address[:10], trades
                )
                continue
            try:
                await self.check_whale_activity(address, trades or [])
            except Exception as exc:
                logger.error("Error processing whale %s: %s", address[:10], exc)

    async def check_whale_activity(self, address: str, trades: list = None):
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
        now_utc = datetime.now(timezone.utc)
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
            len(new_trades), address[:10],
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
                    whale_bet = await self._save_whale_bet(trade, whale, ts, db)
                    if whale_bet:
                        # Fetch market metadata, live price, and taker fee in parallel.
                        # The fee is used by the REAL mode fee-viability gate before
                        # placing any order, avoiding the error-driven retry path.
                        market_result, price_result, fee_result = await asyncio.gather(
                            self._client.get_market(
                                whale_bet.market_id, token_id=whale_bet.token_id
                            ),
                            self._client.get_best_price(whale_bet.token_id),
                            self._client.get_taker_fee_async(whale_bet.token_id),
                            return_exceptions=True,
                        )
                        market_info = market_result if isinstance(market_result, dict) else {}
                        live_price = price_result if isinstance(price_result, float) else None
                        taker_fee_bps = fee_result if isinstance(fee_result, int) else 1000

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
                    logger.error(
                        "Error processing trade for %s: %s", address[:10], exc
                    )
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
            addresses = [
                row[0]
                for row in db.query(Whale.address).filter_by(is_active=True).all()
            ]
        except Exception as exc:
            logger.error("poll_exits_only: failed to load whales: %s", exc)
            return
        finally:
            db.close()

        if not addresses:
            return

        asyncio.run(self._poll_exits_async(addresses))

    async def _poll_exits_async(self, addresses: list):
        """Fetch recent activity for all whales in parallel, then handle exits."""
        fetch_results = await asyncio.gather(
            *[self._client.get_user_activity(addr, limit=100) for addr in addresses],
            return_exceptions=True,
        )
        for address, trades in zip(addresses, fetch_results):
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
        now_utc = datetime.now(timezone.utc)
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
            len(new_exits), address[:10],
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
                            .filter_by(status="OPEN", token_id=token_id,
                                       whale_address=address)
                            .order_by(CopiedBet.opened_at.asc())
                            .all()
                        )
                    if not open_positions and market_id:
                        open_positions = (
                            db.query(CopiedBet)
                            .filter_by(status="OPEN", market_id=market_id,
                                       whale_address=address)
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
                        try:
                            live_exit_price = await self._client.get_best_price(token_id)
                        except Exception:
                            pass

                    # Route through _handle_exit for consistent partial/full exit
                    # logic (fraction-based tranche selection, same path as main scanner)
                    exit_result = self._bet_engine._handle_exit(whale_bet, session, db, live_exit_price=live_exit_price)
                    db.commit()  # safety net — _handle_exit commits internally

                except Exception as exc:
                    logger.error("_handle_exit_trades error for %s: %s", address[:10], exc)
                    db.rollback()
                    exit_result = None  # exception path — treat as "no position found"

                # Only advance _last_seen when the exit was handled successfully
                # (sell went through) or when we had no position to close (None).
                # If _handle_exit returned False the CLOB sell failed — don't
                # consume the signal so the next poll retries automatically.
                if exit_result is not False:
                    if new_last_seen is None or ts > new_last_seen:
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
        self, trade: dict, whale: Whale, ts: datetime, db
    ) -> Optional[WhaleBet]:
        """
        Parse a raw trade dict and persist a WhaleBet record.
        Returns None if the trade is a duplicate or cannot be parsed.
        """
        tx_hash = trade.get("transactionHash") or trade.get("id") or trade.get("orderId", "")

        # Dedup by tx_hash
        if tx_hash:
            existing = db.query(WhaleBet).filter_by(tx_hash=tx_hash).first()
            if existing:
                return None

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

        size_shares_raw = (
            trade.get("shares")
            or trade.get("size")
            or trade.get("contractsFilled")
        )
        try:
            size_shares = float(size_shares_raw) if size_shares_raw is not None else 0.0
        except (TypeError, ValueError):
            size_shares = 0.0
        # Fall back to deriving from USDC cost when the API omits shares or returns 0
        if size_shares <= 0:
            size_shares = size_usdc / max(price, 0.001)

        # Market identifiers
        market_id = (
            trade.get("conditionId")
            or trade.get("market")
            or trade.get("marketId")
            or ""
        )
        token_id = (
            trade.get("asset")
            or trade.get("tokenId")
            or trade.get("outcome_token_id")
            or ""
        )
        question = (
            trade.get("title")
            or trade.get("question")
            or trade.get("market_question")
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
        db.flush()  # Get ID without committing
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
                            whale.address[:10], avg, len(recent_bets),
                        )
                except Exception as exc:
                    logger.error(
                        "refresh_risk_profiles error for %s: %s",
                        whale.address[:10], exc,
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
            balance = asyncio.run(self._client.get_wallet_balance())
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
                    old, balance,
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

    def _auto_redemption_job(self):
        """Periodically redeem resolved positions on-chain. Runs in permanent scheduler."""
        from backend.redemption import check_and_redeem
        from backend.database import CopiedBet, MonitoringSession, SessionLocal
        try:
            result = asyncio.run(check_and_redeem())
            redeemed = result.get("redeemed", 0)
            total_gas_matic = result.get("total_gas_matic", 0.0)
            if redeemed > 0 or total_gas_matic > 0:
                total = result.get("total_value", 0.0)
                total_gas_usdc = round(total_gas_matic * settings.MATIC_USD_PRICE, 6)
                logger.info(
                    "Auto-redeemed %d position(s) for $%.2f (gas=%.6f MATIC ≈ $%.4f)",
                    redeemed, total, total_gas_matic, total_gas_usdc,
                )
                if total_gas_usdc > 0:
                    db = SessionLocal()
                    try:
                        # Allocate gas per-bet: distribute each position's gas cost
                        # proportionally across all closed tranches for that market_id.
                        for pos_info in result.get("redeemed_positions", []):
                            condition_id = pos_info.get("condition_id", "")
                            pos_gas_usdc = round(pos_info.get("gas_matic", 0.0) * settings.MATIC_USD_PRICE, 6)
                            if not condition_id or pos_gas_usdc <= 0:
                                continue
                            tranches = (
                                db.query(CopiedBet)
                                .filter(
                                    CopiedBet.market_id == condition_id,
                                    CopiedBet.mode == "REAL",
                                    CopiedBet.status.in_(["CLOSED_WIN", "CLOSED_LOSS", "CLOSED_NEUTRAL"]),
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

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_trade_timestamp(trade: dict) -> Optional[datetime]:
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
                return datetime.fromtimestamp(raw, tz=timezone.utc)
            except Exception:
                return None

        if isinstance(raw, str):
            raw = raw.rstrip("Z")
            try:
                dt = datetime.fromisoformat(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
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
