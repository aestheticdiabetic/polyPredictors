"""
Stop-loss monitor: checks portfolio value every N minutes and triggers
Discord alerts (or auto-stops the bot) when 24h losses exceed configured thresholds.
"""

import logging
from datetime import UTC, datetime, timedelta

from backend.config import settings
from backend.database import CopiedBet, MonitoringSession, PortfolioSnapshot, SessionLocal

logger = logging.getLogger(__name__)


class StopLossMonitor:
    """
    Runs as APScheduler jobs (called via _run_async in whale_monitor.py).

    Two jobs:
      - check_async()    — every STOP_LOSS_CHECK_INTERVAL_SECONDS (default 5 min)
      - snapshot_async() — every STOP_LOSS_SNAPSHOT_INTERVAL_MINUTES (default 30 min)
    """

    def __init__(self, polymarket_client, discord_bot, whale_monitor_ref):
        self._client = polymarket_client
        self._discord_bot = discord_bot  # may be None when Discord is disabled
        self._whale_monitor = whale_monitor_ref
        # In-memory cooldown: (session_id, threshold_pct) -> last_alerted_at
        self._cooldowns: dict[tuple[int, float], datetime] = {}

    # ------------------------------------------------------------------
    # Scheduler entry points
    # ------------------------------------------------------------------

    async def check_async(self) -> None:
        """Check portfolio loss thresholds for all active sessions."""
        db = SessionLocal()
        try:
            active_sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
            for session in active_sessions:
                await self._check_session(session, db)
        except Exception as exc:
            logger.error("StopLossMonitor.check_async error: %s", exc, exc_info=True)
        finally:
            db.close()

    async def snapshot_async(self) -> None:
        """Record a portfolio snapshot for each active session."""
        db = SessionLocal()
        try:
            active_sessions = db.query(MonitoringSession).filter_by(is_active=True).all()
            for session in active_sessions:
                try:
                    balance, open_val = await self._get_balance_and_open(session, db)
                    snap = PortfolioSnapshot(
                        session_id=session.id,
                        timestamp=datetime.now(UTC).replace(tzinfo=None),
                        balance_usdc=balance,
                        open_positions_value_usdc=open_val,
                        total_portfolio_usdc=balance + open_val,
                    )
                    db.add(snap)
                    db.commit()
                    logger.debug(
                        "Portfolio snapshot: session=%d balance=%.2f open=%.2f total=%.2f",
                        session.id,
                        balance,
                        open_val,
                        balance + open_val,
                    )
                except Exception as exc:
                    logger.error("Snapshot failed for session %d: %s", session.id, exc)
                    db.rollback()
        except Exception as exc:
            logger.error("StopLossMonitor.snapshot_async error: %s", exc, exc_info=True)
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Core check logic
    # ------------------------------------------------------------------

    async def _check_session(self, session: MonitoringSession, db) -> None:
        balance, open_val = await self._get_balance_and_open(session, db)
        current_portfolio = balance + open_val
        baseline = self._get_baseline(session, db)

        if baseline <= 0:
            return

        loss = baseline - current_portfolio
        loss_pct = loss / baseline

        if loss_pct < settings.STOP_LOSS_WARN_PCT:
            return  # within acceptable range

        logger.warning(
            "Stop-loss check: session=%d baseline=%.2f current=%.2f loss=%.1f%%",
            session.id,
            baseline,
            current_portfolio,
            loss_pct * 100,
        )

        auto_stop_threshold = settings.STOP_LOSS_AUTO_STOP_PCT
        warn_threshold = settings.STOP_LOSS_WARN_PCT

        if loss_pct >= auto_stop_threshold and not self._is_on_cooldown(
            session.id, auto_stop_threshold
        ):
            # Auto-stop the session
            self._stop_session(session, db)
            self._record_cooldown(session.id, auto_stop_threshold)
            if self._discord_bot is not None:
                await self._discord_bot.send_auto_stop_alert(
                    session, current_portfolio, baseline, loss_pct
                )
            else:
                logger.warning(
                    "AUTO-STOP triggered: session=%d loss=%.1f%% (Discord disabled)",
                    session.id,
                    loss_pct * 100,
                )

        elif loss_pct >= warn_threshold and not self._is_on_cooldown(session.id, warn_threshold):
            self._record_cooldown(session.id, warn_threshold)
            if self._discord_bot is not None:
                await self._discord_bot.send_warning_alert(
                    session, current_portfolio, baseline, loss_pct
                )
            else:
                logger.warning(
                    "STOP-LOSS WARNING: session=%d loss=%.1f%% — threshold %.0f%% exceeded (Discord disabled)",
                    session.id,
                    loss_pct * 100,
                    warn_threshold * 100,
                )

    def _stop_session(self, session: MonitoringSession, db) -> None:
        """Stop the session in-place (session object already loaded)."""
        session.is_active = False
        session.stopped_at = datetime.utcnow()
        db.commit()
        # Stop whale poller if no other sessions remain
        remaining = db.query(MonitoringSession).filter_by(is_active=True).count()
        if remaining == 0:
            self._whale_monitor.stop_monitoring()
        logger.info("Auto-stopped session %d due to stop-loss trigger", session.id)

    # ------------------------------------------------------------------
    # Portfolio computation
    # ------------------------------------------------------------------

    async def _get_balance_and_open(self, session: MonitoringSession, db) -> tuple[float, float]:
        """Return (available_balance, open_positions_cost_basis)."""
        if session.mode == "REAL":
            balance = await self._client.get_wallet_balance() or 0.0
        else:
            balance = session.current_balance_usdc or 0.0

        open_val = (
            db.query(CopiedBet)
            .filter(
                CopiedBet.session_id == session.id,
                CopiedBet.status == "OPEN",
            )
            .with_entities(CopiedBet.size_usdc)
            .all()
        )
        open_positions_total = sum(r.size_usdc for r in open_val)
        return balance, open_positions_total

    def _get_baseline(self, session: MonitoringSession, db) -> float:
        """
        Return the portfolio value from STOP_LOSS_WINDOW_HOURS ago.
        Falls back to session.starting_balance_usdc if no snapshot exists yet.
        """
        cutoff = datetime.utcnow() - timedelta(hours=settings.STOP_LOSS_WINDOW_HOURS)
        snap = (
            db.query(PortfolioSnapshot)
            .filter(
                PortfolioSnapshot.session_id == session.id,
                PortfolioSnapshot.timestamp <= cutoff,
            )
            .order_by(PortfolioSnapshot.timestamp.desc())
            .first()
        )
        if snap:
            return snap.total_portfolio_usdc
        return session.starting_balance_usdc or 0.0

    # ------------------------------------------------------------------
    # Cooldown helpers
    # ------------------------------------------------------------------

    def _is_on_cooldown(self, session_id: int, threshold_pct: float) -> bool:
        key = (session_id, threshold_pct)
        last = self._cooldowns.get(key)
        if last is None:
            return False
        elapsed = (datetime.utcnow() - last).total_seconds()
        return elapsed < settings.STOP_LOSS_COOLDOWN_HOURS * 3600

    def _record_cooldown(self, session_id: int, threshold_pct: float) -> None:
        self._cooldowns[(session_id, threshold_pct)] = datetime.utcnow()
