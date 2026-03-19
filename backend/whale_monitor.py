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

    def start_monitoring(self, session_id: int):
        """
        Start the polling scheduler for the given session.
        Calls refresh_risk_profiles() immediately, then polls every
        POLLING_INTERVAL_SECONDS seconds.
        """
        with self._lock:
            if self._running:
                logger.warning("Monitor already running, ignoring start request")
                return

            self._session_id = session_id
            self._running = True

        logger.info(
            "Starting whale monitor (session %d, interval %ds)",
            session_id, settings.POLLING_INTERVAL_SECONDS,
        )

        # Kick off immediate risk profile refresh
        try:
            self.refresh_risk_profiles()
        except Exception as exc:
            logger.error("Initial refresh_risk_profiles error: %s", exc)

        # Main polling job
        self._scheduler.add_job(
            func=self.poll_whales,
            trigger=IntervalTrigger(seconds=settings.POLLING_INTERVAL_SECONDS),
            id="poll_whales",
            replace_existing=True,
        )

        # Hourly risk profile refresh
        self._scheduler.add_job(
            func=self.refresh_risk_profiles,
            trigger=IntervalTrigger(hours=1),
            id="refresh_risk_profiles",
            replace_existing=True,
        )

        # Resolution checker every 5 minutes
        self._scheduler.add_job(
            func=self._check_resolution_wrapper,
            trigger=IntervalTrigger(minutes=5),
            id="check_resolution",
            replace_existing=True,
        )

        if not self._scheduler.running:
            self._scheduler.start()

        logger.info("Whale monitor started")

    def stop_monitoring(self):
        """Stop all scheduled jobs and mark the current session as stopped."""
        with self._lock:
            if not self._running:
                return
            self._running = False

        logger.info("Stopping whale monitor")

        for job_id in ("poll_whales", "refresh_risk_profiles", "check_resolution"):
            try:
                self._scheduler.remove_job(job_id)
            except Exception:
                pass

        if self._session_id:
            db = SessionLocal()
            try:
                session = db.query(MonitoringSession).filter_by(id=self._session_id).first()
                if session:
                    session.is_active = False
                    session.stopped_at = datetime.utcnow()
                    db.commit()
            except Exception as exc:
                logger.error("Error stopping session in DB: %s", exc)
            finally:
                db.close()

        self._session_id = None
        logger.info("Whale monitor stopped")

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    def poll_whales(self):
        """
        Called on each scheduler tick.
        Checks activity for every active whale.
        """
        if not self._running:
            return

        db = SessionLocal()
        try:
            whales = db.query(Whale).filter_by(is_active=True).all()
            whale_list = [{"id": w.id, "address": w.address} for w in whales]
        except Exception as exc:
            logger.error("poll_whales: failed to load whales: %s", exc)
            return
        finally:
            db.close()

        logger.debug("Polling %d active whales", len(whale_list))

        for w in whale_list:
            try:
                self._check_whale_activity_sync(w["address"])
            except Exception as exc:
                logger.error("Error checking whale %s: %s", w["address"][:10], exc)

    def _check_whale_activity_sync(self, address: str):
        """
        Run async whale check in a new event loop (we're in a background thread).
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.check_whale_activity(address))
        finally:
            loop.close()

    async def check_whale_activity(self, address: str):
        """
        Fetch the last 100 trades for a whale and process any new ones.
        """
        if not self._running or not self._session_id:
            return

        trades = await self._client.get_user_activity(address, limit=100)
        if not trades:
            return

        last_seen = self._last_seen.get(address)
        new_trades = []

        for trade in trades:
            ts = self._parse_trade_timestamp(trade)
            if ts is None:
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

            session = db.query(MonitoringSession).filter_by(
                id=self._session_id, is_active=True
            ).first()
            if not session:
                logger.warning("Session %d no longer active", self._session_id)
                self._running = False
                return

            new_last_seen = last_seen

            for ts, trade in new_trades:
                try:
                    whale_bet = await self._save_whale_bet(trade, whale, ts, db)
                    if whale_bet:
                        # Fetch market info for guard checks
                        market_info = {}
                        try:
                            market_info = await self._client.get_market(whale_bet.market_id) or {}
                        except Exception:
                            pass

                        self._bet_engine.process_new_whale_bet(
                            whale_bet=whale_bet,
                            session=session,
                            db=db,
                            market_info=market_info,
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
            or 0.0
        )
        try:
            size_shares = float(size_shares_raw)
        except (TypeError, ValueError):
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
    # Resolution wrapper (sync -> runs in scheduler thread)
    # ------------------------------------------------------------------

    def _check_resolution_wrapper(self):
        if self._session_id:
            try:
                self._bet_engine.check_resolution(self._session_id)
            except Exception as exc:
                logger.error("check_resolution error: %s", exc)

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
            "session_id": self._session_id,
            "tracked_whales": len(self._last_seen),
        }

    def reset_last_seen(self, address: str):
        """Clear the last-seen timestamp for a whale (forces re-scan)."""
        with self._lock:
            self._last_seen.pop(address, None)
