"""
Bet placement engine.
Handles both SIMULATION and REAL mode bet placement,
position closing, and resolution checking.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

from sqlalchemy.orm import Session as DBSession

from backend.categorizer import classify
from backend.config import settings
from backend.database import (
    AddToPositionSignal,
    CopiedBet,
    MonitoringSession,
    SessionLocal,
    WhaleBet,
)
from backend.risk_calculator import RiskCalculator

logger = logging.getLogger(__name__)

risk_calc = RiskCalculator()


@dataclass
class _WatchItem:
    """Tracks a drift-skipped bet on a near-expiry market for fast retry."""
    copied_bet_id: int        # existing SKIPPED CopiedBet to upgrade on success
    whale_bet_id: int
    session_id: int
    token_id: str
    market_info: dict
    whale_price: float
    bet_size_usdc: float
    risk_factor: float
    whale_avg: float
    expires_at: float         # time.monotonic() deadline


class BetEngine:
    """Processes whale bets and manages our copied positions."""

    def __init__(self, polymarket_client=None):
        self._client = polymarket_client
        # whale_bet_id → _WatchItem: near-expiry bets that were drift-skipped,
        # retried every DRIFT_RETRY_INTERVAL_SECONDS with a fresh price.
        self._drift_watchlist: dict[int, _WatchItem] = {}

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def process_new_whale_bet(
        self,
        whale_bet: WhaleBet,
        session: MonitoringSession,
        db: DBSession,
        market_info: Optional[dict] = None,
        live_price: Optional[float] = None,
    ) -> Optional[CopiedBet]:
        """
        Decide what to do with a newly detected whale bet and create
        the corresponding CopiedBet record.

        Flow:
        1. If bet_type == EXIT: find our open position and close it.
        2. If bet_type == OPEN:
           a. Check if we already have an open position in this market
              -> if yes, record as AddToPositionSignal, skip copy.
           b. Otherwise: calculate risk, size, validate market, place/simulate.
        """
        whale = whale_bet.whale
        mode = session.mode

        # ---- CASE 1: Whale is exiting a position ----------------------
        if whale_bet.bet_type == "EXIT":
            return self._handle_exit(whale_bet, session, db)

        # ---- CASE 2: Whale is opening / adding to a position ----------
        # Scope to THIS whale's position so that a different whale entering the
        # same market doesn't incorrectly trigger "add to position" logic.
        # Each whale's positions are tracked independently — EXIT signals are
        # also scoped by whale address so one whale's exit can never close
        # a position that was opened by following a different whale.
        whale_address = whale.address if whale else None
        existing_open = self._find_open_position(
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            session_mode=session.mode,
            db=db,
            whale_address=whale_address,
        )

        if existing_open is not None:
            # Same whale is adding to a position we already hold -> signal only
            return self._handle_add_to_position(whale_bet, existing_open, session, db)

        # ---- HEDGE GUARD (HEDGE_SIM only): same whale, same market, opposite outcome ---
        if session.mode == "HEDGE_SIM" and whale_address:
            opposite_pos = self._find_opposite_outcome_position(
                market_id=whale_bet.market_id,
                token_id=whale_bet.token_id,
                session_mode=session.mode,
                db=db,
                whale_address=whale_address,
            )
            if opposite_pos is not None:
                existing_outcome = opposite_pos.outcome or opposite_pos.token_id[:8]
                new_outcome = whale_bet.outcome or whale_bet.token_id[:8]
                skip_reason = (
                    f"Whale hedging — already hold {existing_outcome} in this market "
                    f"(whale entered {new_outcome} on opposite side)"
                )
                logger.info(
                    "Hedge detected for whale %s in market %s: hold %s, new signal %s — skipping",
                    whale_address[:10], whale_bet.market_id[:16], existing_outcome, new_outcome,
                )
                whale_avg = whale.avg_bet_size_usdc if whale else 0.0
                risk_factor = risk_calc.calculate_risk_factor(whale_bet.size_usdc, whale_avg)
                bet_size_usdc = risk_calc.calculate_bet_size(
                    session_balance=session.current_balance_usdc,
                    risk_factor=risk_factor,
                    max_bet_pct=settings.MAX_BET_PCT,
                )
                return self._create_skipped_bet(
                    whale_bet=whale_bet, session=session, bet_size_usdc=bet_size_usdc,
                    risk_factor=risk_factor, whale_avg=whale_avg, skip_reason=skip_reason, db=db,
                )

        # ---- CASE 3: Fresh position ------------------------------------

        # Risk calculations
        # For the first WHALE_CALIBRATION_BETS placed bets for this whale we don't
        # yet have a reliable average, so use the minimum as a tracker bet.  Once
        # we have enough history the normal risk-factor sizing kicks in.
        _CALIBRATION_THRESHOLD = 15
        whale_address = whale.address if whale else None
        placed_count = (
            db.query(CopiedBet)
            .filter(
                CopiedBet.whale_address == whale_address,
                CopiedBet.mode == session.mode,
                CopiedBet.status != "SKIPPED",
            )
            .count()
            if whale_address else 0
        )

        if placed_count < _CALIBRATION_THRESHOLD:
            bet_size_usdc = settings.MIN_BET_USDC
            risk_factor = 0.0  # sentinel — no risk data yet
            whale_avg = 0.0
            logger.info(
                "Calibration mode: whale %s has %d/%d placed bets — using minimum $%.2f",
                (whale_address or "?")[:10], placed_count, _CALIBRATION_THRESHOLD, settings.MIN_BET_USDC,
            )
        else:
            whale_avg = whale.avg_bet_size_usdc if whale else 0.0
            risk_factor = risk_calc.calculate_risk_factor(whale_bet.size_usdc, whale_avg)
            bet_size_usdc = risk_calc.calculate_bet_size(
                session_balance=session.current_balance_usdc,
                risk_factor=risk_factor,
                max_bet_pct=settings.MAX_BET_PCT,
            )

        # Market guard
        ok, skip_reason = risk_calc.should_place_bet(
            market_info or {},
            min_hours_to_close=settings.MIN_MARKET_HOURS_TO_CLOSE,
            whale_price=whale_bet.price,
            live_price=live_price,
        )
        if not ok:
            # If the market is already closed/resolved, drop silently — no ledger entry.
            # Policy skips (balance, price drift, etc.) still get a SKIPPED record.
            if self._is_closed_market_skip(market_info or {}, skip_reason):
                logger.debug(
                    "Dropping whale_bet %d silently — market already closed: %s",
                    whale_bet.id, skip_reason,
                )
                return None
            skipped = self._create_skipped_bet(
                whale_bet=whale_bet,
                session=session,
                bet_size_usdc=bet_size_usdc,
                risk_factor=risk_factor,
                whale_avg=whale_avg,
                skip_reason=skip_reason,
                db=db,
            )
            # Near-expiry drift skip → add to watchlist for fast retry.
            # The retry loop checks price every DRIFT_RETRY_INTERVAL_SECONDS and
            # upgrades this SKIPPED record to OPEN if price comes back within range.
            if "price drift" in skip_reason and "> 2%" in skip_reason:
                self._add_to_drift_watchlist(
                    copied_bet_id=skipped.id,
                    whale_bet=whale_bet,
                    session=session,
                    market_info=market_info or {},
                    bet_size_usdc=bet_size_usdc,
                    risk_factor=risk_factor,
                    whale_avg=whale_avg,
                )
            return skipped

        # Balance check
        if session.current_balance_usdc < 1.0:
            return self._create_skipped_bet(
                whale_bet=whale_bet,
                session=session,
                bet_size_usdc=0.0,
                risk_factor=risk_factor,
                whale_avg=whale_avg,
                skip_reason="Insufficient balance",
                db=db,
            )

        if bet_size_usdc < settings.MIN_BET_USDC:
            bet_size_usdc = settings.MIN_BET_USDC
            logger.debug(
                "Bet size below minimum — clamping to $%.2f for whale_bet %d",
                settings.MIN_BET_USDC, whale_bet.id,
            )

        # Category filter — skip if this whale has disabled this sport or bet type
        cat_sport, cat_bet_type = classify(whale_bet.question)
        if whale and whale.category_filters:
            import json as _json
            try:
                filters = _json.loads(whale.category_filters)
                disabled_sports = filters.get("disabled_sports", [])
                disabled_bet_types = filters.get("disabled_bet_types", [])
                if cat_sport in disabled_sports or cat_bet_type in disabled_bet_types:
                    return self._create_skipped_bet(
                        whale_bet=whale_bet,
                        session=session,
                        bet_size_usdc=bet_size_usdc,
                        risk_factor=risk_factor,
                        whale_avg=whale_avg,
                        skip_reason=f"Category filtered: {cat_sport} / {cat_bet_type}",
                        db=db,
                    )
            except Exception:
                pass

        # Price staleness guard — skip if odds have moved too far since the whale bet
        price_ok, price_reason = risk_calc.check_price_staleness(
            whale_price=whale_bet.price,
            live_price=live_price,
            max_drift=settings.MAX_PRICE_DRIFT_PCT,
        )
        if not price_ok:
            return self._create_skipped_bet(
                whale_bet=whale_bet,
                session=session,
                bet_size_usdc=bet_size_usdc,
                risk_factor=risk_factor,
                whale_avg=whale_avg,
                skip_reason=price_reason,
                db=db,
            )

        # Place or simulate
        entry_price = whale_bet.price
        try:
            if mode in ("SIMULATION", "HEDGE_SIM"):
                shares, actual_price = self.simulate_buy(
                    session=session,
                    market_id=whale_bet.market_id,
                    token_id=whale_bet.token_id,
                    question=whale_bet.question,
                    outcome=whale_bet.outcome,
                    price=whale_bet.price,
                    size_usdc=bet_size_usdc,
                    db=db,
                )
                entry_price = actual_price
                size_shares = shares
                bet_status = "OPEN"
                skip_reason_val = None
            else:
                order_resp = self.place_real_buy(
                    token_id=whale_bet.token_id,
                    size_usdc=bet_size_usdc,
                    price=whale_bet.price,
                )
                # Extract actual fill size from order response
                shares_filled = order_resp.get("size_matched", bet_size_usdc / max(whale_bet.price, 0.01))
                size_shares = float(shares_filled)
                # Use actual fill price if the exchange reports it (avoids slippage mismatch)
                fill_price = (
                    order_resp.get("price")
                    or order_resp.get("avgPrice")
                    or order_resp.get("average_price")
                )
                if fill_price:
                    entry_price = float(fill_price)
                bet_status = "OPEN"
                skip_reason_val = None
        except Exception as exc:
            logger.error("Failed to place bet for whale_bet %d: %s", whale_bet.id, exc)
            return self._create_skipped_bet(
                whale_bet=whale_bet,
                session=session,
                bet_size_usdc=bet_size_usdc,
                risk_factor=risk_factor,
                whale_avg=whale_avg,
                skip_reason=f"Order failed: {exc}",
                db=db,
            )

        # Parse market close date from market_info for ledger display.
        # Prefer gameStartTime (actual tip-off / kick-off) over endDate (trading
        # cutoff) when it is available and later, so the countdown reflects the
        # real event time rather than Polymarket's arbitrary betting deadline.
        market_close_at = None
        if market_info:
            end_str = (
                market_info.get("endDate")
                or market_info.get("endDateIso")
                or market_info.get("end_date_iso")
                or market_info.get("end_date")
            )
            if end_str:
                try:
                    dt = datetime.fromisoformat(end_str.rstrip("Z"))
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)  # store as naive UTC
                    market_close_at = dt
                except (ValueError, TypeError):
                    pass

            # Override with gameStartTime when available and later than endDate
            game_start_str = market_info.get("gameStartTime")
            if game_start_str:
                try:
                    gst = datetime.fromisoformat(str(game_start_str).rstrip("Z"))
                    if gst.tzinfo is not None:
                        gst = gst.replace(tzinfo=None)
                    if market_close_at is None or gst > market_close_at:
                        market_close_at = gst
                except (ValueError, TypeError):
                    pass

        # Classify market category and bet type
        market_category, bet_type_cat = classify(whale_bet.question)

        # Persist CopiedBet
        copied_bet = CopiedBet(
            whale_bet_id=whale_bet.id,
            whale_address=whale_bet.whale.address,
            mode=mode,
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            question=whale_bet.question,
            side=whale_bet.side,
            outcome=whale_bet.outcome,
            price_at_entry=entry_price,
            size_usdc=bet_size_usdc,
            size_shares=size_shares,
            risk_factor=risk_factor,
            whale_bet_usdc=whale_bet.size_usdc,
            whale_avg_bet_usdc=whale_avg,
            status=bet_status,
            skip_reason=skip_reason_val,
            market_category=market_category,
            bet_type=bet_type_cat,
            opened_at=datetime.utcnow(),
            market_close_at=market_close_at,
        )
        db.add(copied_bet)

        # Update session counters
        session.total_bets_placed += 1
        db.commit()
        db.refresh(copied_bet)

        logger.info(
            "Placed %s bet: %s %s @ %.3f for $%.2f (rf=%.2f)",
            mode, whale_bet.outcome, whale_bet.market_id[:16],
            entry_price, bet_size_usdc, risk_factor,
        )
        return copied_bet

    # ------------------------------------------------------------------
    # Simulation helpers
    # ------------------------------------------------------------------

    def simulate_buy(
        self,
        session: MonitoringSession,
        market_id: str,
        token_id: str,
        question: str,
        outcome: str,
        price: float,
        size_usdc: float,
        db: DBSession,
    ) -> tuple[float, float]:
        """
        Simulate buying `size_usdc` worth of shares at `price`.
        Deducts from session balance.
        Returns (shares_bought, entry_price).
        """
        # Clamp price to avoid zero division
        effective_price = max(price, 0.001)
        shares = size_usdc / effective_price

        session.current_balance_usdc = max(0.0, session.current_balance_usdc - size_usdc)
        db.add(session)

        logger.debug(
            "SIM BUY: $%.2f @ %.4f = %.4f shares | balance -> $%.2f",
            size_usdc, effective_price, shares, session.current_balance_usdc,
        )
        return round(shares, 6), round(effective_price, 6)

    def simulate_sell(
        self,
        copied_bet: CopiedBet,
        current_price: float,
        session: MonitoringSession,
        db: DBSession,
        close_reason: str = "",
    ) -> float:
        """
        Simulate selling all shares at `current_price`.
        Calculates P&L, updates session balance and CopiedBet record.
        Returns pnl_usdc.
        """
        proceeds = copied_bet.size_shares * current_price
        pnl = proceeds - copied_bet.size_usdc

        # Update session balance
        session.current_balance_usdc += proceeds
        session.total_pnl_usdc = round(session.total_pnl_usdc + pnl, 2)

        if pnl > 0.01:
            copied_bet.status = "CLOSED_WIN"
            session.total_wins += 1
        elif pnl < -0.01:
            copied_bet.status = "CLOSED_LOSS"
            session.total_losses += 1
        else:
            copied_bet.status = "CLOSED_NEUTRAL"

        copied_bet.pnl_usdc = round(pnl, 2)
        copied_bet.resolution_price = current_price
        copied_bet.closed_at = datetime.utcnow()
        if close_reason:
            copied_bet.close_reason = close_reason

        db.add(copied_bet)
        db.add(session)

        logger.info(
            "SIM SELL: %.4f shares @ %.4f = $%.2f (pnl $%.2f) | balance -> $%.2f",
            copied_bet.size_shares, current_price, proceeds, pnl,
            session.current_balance_usdc,
        )
        return round(pnl, 2)

    # ------------------------------------------------------------------
    # Real mode helpers
    # ------------------------------------------------------------------

    def place_real_buy(
        self, token_id: str, size_usdc: float, price: float
    ) -> dict:
        """Place a real market buy via py-clob-client."""
        logger.info("REAL BUY: token=%s size=$%.2f price=%.4f", token_id[:16], size_usdc, price)
        return self._client.place_market_buy(token_id, size_usdc)

    def place_real_sell(self, token_id: str, size_shares: float) -> dict:
        """Place a real market sell via py-clob-client."""
        logger.info("REAL SELL: token=%s shares=%.4f", token_id[:16], size_shares)
        return self._client.place_market_sell(token_id, size_shares)

    # ------------------------------------------------------------------
    # Resolution checker
    # ------------------------------------------------------------------

    def check_resolution(self):
        """
        Check ALL open CopiedBets across every session, regardless of whether
        the whale monitor is currently running.  This runs on a permanent
        background scheduler so positions are never left unattended.

        Resolution rules:
        - Market already ended (hours_remaining < 0):
            Force-close at current price — the market has settled.
        - Near-expiry (0 < hours_remaining < MIN_MARKET_HOURS_TO_CLOSE):
            Close early if price crosses 0.80 / 0.20 — mirrors the whale
            cashing out before expiry.
        - Standard open market:
            Close only when price is very decisive (>= 0.95 or <= 0.05).
        """
        db = SessionLocal()
        try:
            open_bets = db.query(CopiedBet).filter(CopiedBet.status == "OPEN").all()
            logger.info("check_resolution: %d open bet(s) to check", len(open_bets))
            if not open_bets:
                return

            # Fetch price + endDate for all open bets in parallel
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                resolution_data = loop.run_until_complete(
                    self._fetch_resolution_data([b.token_id for b in open_bets])
                )
            finally:
                loop.close()

            # Cache sessions by mode so we only query once per mode
            session_cache: dict[str, Optional[MonitoringSession]] = {}

            now = datetime.now(timezone.utc)
            min_hours = settings.MIN_MARKET_HOURS_TO_CLOSE

            _LIVE_SPORTS = {
                "Soccer", "Basketball", "Tennis", "eSports",
                "American Football", "Baseball", "Hockey",
            }

            for bet, (price, end_dt, is_resolved) in zip(open_bets, resolution_data):
                if price is None:
                    continue

                # Must be computed before any case that references it
                is_live_sport = bet.market_category in _LIVE_SPORTS or bet.market_category == "Crypto"

                # Resolve the session for this bet's mode (cached)
                if bet.mode not in session_cache:
                    session_cache[bet.mode] = (
                        db.query(MonitoringSession)
                        .filter_by(mode=bet.mode)
                        .order_by(MonitoringSession.id.desc())
                        .first()
                    )
                session = session_cache.get(bet.mode)
                if not session:
                    continue

                # Refresh market_close_at from the API if it has changed or was missing.
                # Polymarket occasionally updates endDate; keeping the DB in sync
                # ensures the frontend always shows the latest trading-close time.
                if end_dt is not None:
                    # Convert to naive UTC for comparison (DB stores naive UTC)
                    end_dt_naive = end_dt.replace(tzinfo=None) if end_dt.tzinfo else end_dt
                    if bet.market_close_at != end_dt_naive:
                        bet.market_close_at = end_dt_naive

                # Time remaining to market close
                hours_remaining: Optional[float] = None
                if end_dt is not None:
                    hours_remaining = (end_dt - now).total_seconds() / 3600.0

                # Guard: price of exactly 0.0 on an active market is API noise.
                # Only a market that has already passed its end date can legitimately
                # settle at 0.0. Skipping prevents a transient API glitch from
                # wiping open positions with a 100% loss.
                market_ended = hours_remaining is not None and hours_remaining < 0
                if price == 0.0 and not market_ended and not is_resolved:
                    # price=0.0 on an active market is API noise *unless* the oracle
                    # has confirmed it (is_resolved=True means opposing token = 1.0).
                    # Some markets use endDate as the UMA settlement deadline (days in
                    # the future), so market_ended is False even for finished events.
                    # We must not skip those — they are legitimately settled at 0.
                    logger.warning(
                        "Skipping price=0.0 for active bet %d (%.1fh to close) — "
                        "likely API noise, not closing",
                        bet.id,
                        hours_remaining if hours_remaining is not None else 0,
                    )
                    continue

                # --- Case 1: Market has already ended ---
                # Polymarket sets endDate to game START time for many live sports
                # markets, so hours_remaining < 0 fires immediately when play begins —
                # well before the final outcome is known.  The oracle (UMA) then takes
                # hours-to-days to settle on-chain.  Until it does, outcomePrices for
                # unsettled binary markets often reads ["0","0"] or the last traded
                # price, neither of which is a reliable resolution signal.
                #
                # Rules:
                #  - price >= 0.95               → YES clearly won; close regardless of resolved flag
                #  - price <= 0.05 AND is_resolved → oracle confirmed NO won; close
                #  - price <= 0.05 AND NOT is_resolved → likely unresolved 0.0 default; wait
                #  - 0.05 < price < 0.95         → mid-range; definitely not settled; wait
                #
                # Additional guard for live sports NO resolutions: Polymarket sometimes
                # cancels/voids a duplicate or deprecated market line right at game start,
                # resolving it to ["0","1"] (NO wins) within minutes of endDate. A real
                # game outcome cannot be known this quickly. For live sports, require at
                # least MIN_HOURS_AFTER_CLOSE hours past market end before accepting a
                # NO oracle resolution, to avoid treating a market cancellation as a loss.
                # Minimum hours after market_close_at before a NO oracle resolution
                # is trusted for live sports bets.  Rationale:
                #   Shortest game + minimum UMA liveness period ≈ 4h
                #   (Soccer ~2h game + 2h oracle, Basketball ~2.5h + 2h, Hockey ~3h + 2h).
                # All observed premature cancellations resolved within 2.5h; confirmed
                # legitimate resolutions took 7h+.  5h provides a clear margin on both sides.
                MIN_HOURS_AFTER_CLOSE = 5.0
                if market_ended:
                    hours_since_close = abs(hours_remaining)
                    if price >= 0.95:
                        logger.info(
                            "Force-close: bet %d market ended %.1fh ago, price=%.4f (YES resolved)",
                            bet.id, hours_since_close, price,
                        )
                        self.simulate_sell(
                            copied_bet=bet, current_price=price, session=session, db=db,
                            close_reason="Market ended",
                        )
                    elif price <= 0.05 and is_resolved:
                        if is_live_sport and hours_since_close < MIN_HOURS_AFTER_CLOSE:
                            logger.warning(
                                "Skipping premature NO resolution for live sport bet %d "
                                "(%.1fh after market close < %.1fh minimum) — "
                                "likely market cancellation, not game outcome",
                                bet.id, hours_since_close, MIN_HOURS_AFTER_CLOSE,
                            )
                        else:
                            logger.info(
                                "Force-close: bet %d market ended %.1fh ago, price=%.4f (NO resolved, oracle confirmed)",
                                bet.id, hours_since_close, price,
                            )
                            self.simulate_sell(
                                copied_bet=bet, current_price=price, session=session, db=db,
                                close_reason="Market ended",
                            )
                    else:
                        logger.debug(
                            "Market ended %.1fh ago but not yet oracle-resolved "
                            "(price=%.4f, resolved=%s) — waiting for on-chain settlement (bet %d)",
                            hours_since_close, price, is_resolved, bet.id,
                        )
                    continue

                # --- Case 2: Near-expiry window ---
                if hours_remaining is not None and 0 <= hours_remaining < min_hours:
                    logger.debug(
                        "Near-expiry monitoring: bet %d closes in %.2fh, price=%.4f",
                        bet.id, hours_remaining, price,
                    )
                    if not is_live_sport and (price >= 0.80 or price <= 0.20):
                        logger.info(
                            "Near-expiry close: bet %d price=%.4f crosses 0.80/0.20 "
                            "with %.2fh remaining",
                            bet.id, price, hours_remaining,
                        )
                        self.simulate_sell(
                            copied_bet=bet, current_price=price, session=session, db=db,
                            close_reason=f"Near-expiry exit ({hours_remaining:.1f}h left, price {price:.3f})",
                        )
                    elif is_live_sport:
                        logger.debug(
                            "Near-expiry: skipping price-based close for live sport bet %d "
                            "(%s, %.2fh left, price=%.4f) — waiting for on-chain settlement",
                            bet.id, bet.market_category, hours_remaining, price,
                        )
                    continue

                # --- Case 3: Standard resolution threshold ---
                # For non-sports: price converging to an extreme signals outcome.
                # For live sports: ignore price unless the oracle has confirmed it
                # (is_resolved=True), because live in-play prices fluctuate wildly
                # and must not be used as resolution signals before the event ends.
                # When is_resolved=True the endDate may be a future UMA settlement
                # deadline rather than the game end time, so market_ended is False
                # even for long-finished matches — we close them here instead.
                if (not is_live_sport or is_resolved) and (price >= 0.95 or price <= 0.05):
                    self.simulate_sell(
                        copied_bet=bet, current_price=price, session=session, db=db,
                        close_reason=f"Price reached resolution threshold ({price:.3f})",
                    )
                    logger.info(
                        "Resolved bet %d at price %.4f status=%s",
                        bet.id, price, bet.status,
                    )

            db.commit()

        except Exception as exc:
            logger.error("check_resolution error: %s", exc)
            db.rollback()
        finally:
            db.close()

    async def _fetch_resolution_data(
        self, token_ids: list
    ) -> list[tuple[Optional[float], Optional[datetime], bool]]:
        """Fetch price, endDate and resolved flag for multiple tokens.

        Requests are throttled to at most MAX_CONCURRENT concurrent connections
        to avoid rate-limiting on the Gamma API.  Each failed request is retried
        once after a short back-off before giving up and returning (None, None, False).
        """
        MAX_CONCURRENT = 4
        RETRY_DELAY = 1.5  # seconds

        sem = asyncio.Semaphore(MAX_CONCURRENT)

        async def _fetch_with_limit(tid: str, http: httpx.AsyncClient):
            async with sem:
                result = await self._get_resolution_data_async(tid, http)
                if result[0] is None:
                    # Single retry after a short back-off
                    await asyncio.sleep(RETRY_DELAY)
                    result = await self._get_resolution_data_async(tid, http)
                return result

        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as http:
            return await asyncio.gather(
                *[_fetch_with_limit(tid, http) for tid in token_ids]
            )

    async def _get_resolution_data_async(
        self, token_id: str, http: httpx.AsyncClient
    ) -> tuple[Optional[float], Optional[datetime], bool]:
        """
        Async fetch of (price, end_dt, is_resolved) from Gamma API for a single token.
        is_resolved is True only when the market's oracle has actually settled the outcome.
        Returns (None, None, False) on error.
        """
        try:
            url = f"{settings.GAMMA_API_BASE}/markets"
            resp = await http.get(url, params={"clob_token_ids": token_id})
            resp.raise_for_status()
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            if not markets:
                logger.warning(
                    "_get_resolution_data_async: empty markets list for token %s "
                    "(response keys: %s)",
                    token_id[:16],
                    list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                )
                return None, None, False
            for market in markets:
                tokens = market.get("clobTokenIds", [])
                if isinstance(tokens, str):
                    try:
                        tokens = json.loads(tokens)
                    except (json.JSONDecodeError, ValueError):
                        tokens = []
                outcomes = market.get("outcomePrices", [])
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except (json.JSONDecodeError, ValueError):
                        outcomes = []
                if token_id not in tokens:
                    logger.debug(
                        "_get_resolution_data_async: token %s not in clobTokenIds %s",
                        token_id[:16],
                        [t[:8] for t in tokens],
                    )
                    continue

                # Parse price
                price: Optional[float] = None
                idx = tokens.index(token_id)
                if outcomes and idx < len(outcomes):
                    try:
                        price = float(outcomes[idx])
                    except (TypeError, ValueError):
                        pass

                # Parse endDate (Polymarket trading cutoff) and gameStartTime.
                # For sports markets, endDate is often the betting cutoff set to
                # midnight before the game, while gameStartTime is the actual tip-off /
                # kick-off. We prefer gameStartTime when it is later than endDate so
                # that market_close_at (and thus the ledger countdown) reflects the
                # real event time rather than an arbitrary trading cutoff.
                end_dt: Optional[datetime] = None
                end_str = (
                    market.get("endDate")
                    or market.get("endDateIso")
                    or market.get("end_date_iso")
                    or market.get("end_date")
                )
                if end_str:
                    try:
                        dt = datetime.fromisoformat(end_str.rstrip("Z"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        end_dt = dt
                    except (ValueError, TypeError):
                        pass

                # Override with gameStartTime when available and later than endDate
                game_start_str = market.get("gameStartTime")
                if game_start_str:
                    try:
                        gst = datetime.fromisoformat(str(game_start_str).rstrip("Z"))
                        if gst.tzinfo is None:
                            gst = gst.replace(tzinfo=timezone.utc)
                        if end_dt is None or gst > end_dt:
                            end_dt = gst
                    except (ValueError, TypeError):
                        pass

                # Parse oracle resolution status.
                # The market is only truly settled when the UMA/oracle has resolved
                # it on-chain. "active=False" and "resolved=True" alone are NOT
                # enough — Polymarket sets these flags when trading stops (e.g. at
                # game tipoff) but the oracle outcome can still be pending for hours.
                #
                # The reliable signal is price convergence: when the oracle finalises,
                # outcomePrices becomes ["0","1"] or ["1","0"] (one side wins, one
                # loses). Before settlement it reads ["0","0"] — both zero, sum = 0.
                #
                # IMPORTANT: Polymarket live-odds markets update outcomePrices during
                # play (e.g. ["0.03","0.97"] for a low-scoring game in the 3rd period).
                # These are NOT oracle-settled — they are live trading prices.
                # Polymarket never allows trading at exactly 0 or 1; the oracle sets
                # prices to the integer strings "0" and "1" only after final settlement.
                #
                # We distinguish oracle settlement from live pricing by requiring the
                # opposing token to be ≥ 0.99 (only an exact "1" passes; a live "0.97"
                # does not). This correctly handles all three states:
                #   ["0","0"]      → other_price=0.0  → not settled (wait)
                #   ["0.03","0.97"]→ other_price=0.97 → live trading  (wait)
                #   ["0","1"]      → other_price=1.0  → oracle settled (close)
                other_price_val: float = 0.0
                if len(outcomes) >= 2:
                    try:
                        other_price_val = float(outcomes[1 - idx])
                    except (TypeError, ValueError, IndexError):
                        pass

                # Primary signal: opposing token ≥ 0.99 (oracle set price to "1")
                is_resolved = other_price_val >= 0.99

                # Secondary signal: umaResolutionStatus="resolved" means the
                # oracle has finalised on-chain even if outcomePrices hasn't
                # updated yet (e.g. still shows ["0","0"] due to API lag).
                uma_status = (market.get("umaResolutionStatus") or "").lower()
                if uma_status == "resolved" and not is_resolved:
                    logger.info(
                        "_get_resolution_data_async: umaResolutionStatus=resolved "
                        "but outcomePrices=%s (price lag) — treating as resolved for token %s",
                        outcomes, token_id[:16],
                    )
                    is_resolved = True

                return price, end_dt, is_resolved
        except Exception as exc:
            logger.warning(
                "_get_resolution_data_async error for %s: %s (%s)",
                token_id, exc, type(exc).__name__,
            )
        return None, None, False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _handle_exit(
        self,
        whale_bet: WhaleBet,
        session: MonitoringSession,
        db: DBSession,
    ) -> CopiedBet:
        """Whale is selling out of a position - close ours if we have one."""
        # Use the exiting whale's address to scope the search — prevents a
        # different whale's EXIT signal from closing a position we opened by
        # following a different whale in the same market.
        exiting_whale_address = whale_bet.whale.address if whale_bet.whale else None
        open_pos = self._find_open_position(
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            session_mode=session.mode,
            db=db,
            whale_address=exiting_whale_address,
        )

        if open_pos is None:
            # We never had a position here; log a skipped EXIT
            copied_bet = CopiedBet(
                whale_bet_id=whale_bet.id,
                whale_address=whale_bet.whale.address,
                mode=session.mode,
                market_id=whale_bet.market_id,
                token_id=whale_bet.token_id,
                question=whale_bet.question,
                side=whale_bet.side,
                outcome=whale_bet.outcome,
                price_at_entry=whale_bet.price,
                size_usdc=0.0,
                size_shares=0.0,
                risk_factor=1.0,
                whale_bet_usdc=whale_bet.size_usdc,
                whale_avg_bet_usdc=whale_bet.whale.avg_bet_size_usdc if whale_bet.whale else 0.0,
                status="SKIPPED",
                skip_reason="EXIT signal but no open position to close",
                opened_at=datetime.utcnow(),
            )
            db.add(copied_bet)
            db.commit()
            return copied_bet

        # Close existing position
        exit_price = whale_bet.price
        whale_alias = whale_bet.whale.alias if whale_bet.whale else whale_bet.whale_address[:10]
        exit_reason = f"Whale exited ({whale_alias} sold @ {exit_price:.3f})"

        if session.mode in ("SIMULATION", "HEDGE_SIM"):
            self.simulate_sell(
                copied_bet=open_pos,
                current_price=exit_price,
                session=session,
                db=db,
                close_reason=exit_reason,
            )
        else:
            try:
                self.place_real_sell(open_pos.token_id, open_pos.size_shares)
                open_pos.status = "CLOSED_NEUTRAL"
                open_pos.pnl_usdc = 0.0
                open_pos.resolution_price = exit_price
                open_pos.closed_at = datetime.utcnow()
                open_pos.close_reason = exit_reason
            except Exception as exc:
                logger.error("Real sell failed for copied_bet %d: %s", open_pos.id, exc)

        db.commit()
        return open_pos

    def _handle_add_to_position(
        self,
        whale_bet: WhaleBet,
        existing_open: CopiedBet,
        session: MonitoringSession,
        db: DBSession,
    ) -> CopiedBet:
        """Whale is adding to a market we already hold - record signal only, no new ledger entry.

        Calculates suggested_add_usdc using the same risk-factor sizing as the
        main bet engine, based on the session balance *after* the original
        position's cost (already deducted from session.current_balance_usdc).
        """
        whale = whale_bet.whale
        whale_avg = whale.avg_bet_size_usdc if whale else 0.0
        risk_factor = risk_calc.calculate_risk_factor(whale_bet.size_usdc, whale_avg)
        suggested_add_usdc = risk_calc.calculate_bet_size(
            session_balance=session.current_balance_usdc,
            risk_factor=risk_factor,
            max_bet_pct=settings.MAX_BET_PCT,
        )

        # Upsert: accumulate into the existing signal for this position rather than
        # creating a new row for every addition.  This keeps one signal per position
        # showing the total the whale has added and how many times they've added.
        existing_signal = (
            db.query(AddToPositionSignal)
            .filter_by(copied_bet_id=existing_open.id)
            .first()
        )
        if existing_signal:
            existing_signal.whale_additional_usdc += whale_bet.size_usdc
            existing_signal.whale_additional_shares += whale_bet.size_shares
            existing_signal.addition_count = (existing_signal.addition_count or 1) + 1
            existing_signal.last_addition_at = whale_bet.timestamp
            existing_signal.price = whale_bet.price          # latest addition price
            existing_signal.suggested_add_usdc = suggested_add_usdc
            db.add(existing_signal)
            logger.info(
                "Updated add-to-position signal for copied_bet=%d: "
                "total=$%.2f additions=%d",
                existing_open.id,
                existing_signal.whale_additional_usdc,
                existing_signal.addition_count,
            )
        else:
            signal = AddToPositionSignal(
                whale_bet_id=whale_bet.id,
                copied_bet_id=existing_open.id,
                whale_additional_usdc=whale_bet.size_usdc,
                whale_additional_shares=whale_bet.size_shares,
                price=whale_bet.price,
                timestamp=whale_bet.timestamp,
                addition_count=1,
                last_addition_at=whale_bet.timestamp,
                suggested_add_usdc=suggested_add_usdc,
                note="Whale added to position while we hold",
            )
            db.add(signal)

        db.commit()
        # Return the existing open position — no new ledger entry created
        return existing_open

    # ------------------------------------------------------------------
    # Drift-skip watchlist — fast retry for near-expiry markets
    # ------------------------------------------------------------------

    def _add_to_drift_watchlist(
        self,
        copied_bet_id: int,
        whale_bet: WhaleBet,
        session: MonitoringSession,
        market_info: dict,
        bet_size_usdc: float,
        risk_factor: float,
        whale_avg: float,
    ) -> None:
        """Register a drift-skipped bet for fast-polling retry."""
        expires_at = time.monotonic() + settings.DRIFT_RETRY_WINDOW_SECONDS
        self._drift_watchlist[whale_bet.id] = _WatchItem(
            copied_bet_id=copied_bet_id,
            whale_bet_id=whale_bet.id,
            session_id=session.id,
            token_id=whale_bet.token_id,
            market_info=market_info,
            whale_price=whale_bet.price,
            bet_size_usdc=bet_size_usdc,
            risk_factor=risk_factor,
            whale_avg=whale_avg,
            expires_at=expires_at,
        )
        logger.info(
            "Drift watchlist: added whale_bet=%d token=%s…  expires_in=%ds  (watchlist size=%d)",
            whale_bet.id, whale_bet.token_id[:12],
            settings.DRIFT_RETRY_WINDOW_SECONDS, len(self._drift_watchlist),
        )

    async def retry_drift_watchlist(self, db: DBSession) -> None:
        """
        Called by the fast-poll scheduler every DRIFT_RETRY_INTERVAL_SECONDS.
        For each watchlist item: fetch a fresh (uncached) price and place the bet
        if the price has come back within the drift threshold.  On success the
        existing SKIPPED CopiedBet record is updated to OPEN in-place.
        """
        if not self._drift_watchlist:
            return

        now = time.monotonic()
        for whale_bet_id, item in list(self._drift_watchlist.items()):
            # 1. Expiry check
            if now > item.expires_at:
                logger.debug("Drift watchlist: whale_bet=%d expired — removing", whale_bet_id)
                del self._drift_watchlist[whale_bet_id]
                continue

            # 2. Load and validate session
            session = db.get(MonitoringSession, item.session_id)
            if not session or not getattr(session, "active", True):
                logger.debug("Drift watchlist: session %d gone — removing whale_bet=%d", item.session_id, whale_bet_id)
                del self._drift_watchlist[whale_bet_id]
                continue

            # 3. Fetch a live price, bypassing the 30 s cache
            try:
                live_price = await self._client.get_best_price(item.token_id, force_refresh=True)
            except Exception as exc:
                logger.debug("Drift watchlist: price fetch failed for whale_bet=%d: %s", whale_bet_id, exc)
                continue

            # 4. Re-check drift only — all other guards already passed
            price_ok, price_reason = risk_calc.check_price_staleness(
                whale_price=item.whale_price,
                live_price=live_price,
                max_drift=settings.MAX_PRICE_DRIFT_PCT,
            )
            if not price_ok:
                logger.debug(
                    "Drift watchlist: whale_bet=%d still drifted (%s) — retrying",
                    whale_bet_id, price_reason,
                )
                continue

            # 5. Price is back in range — execute placement
            mode = session.mode
            try:
                if mode in ("SIMULATION", "HEDGE_SIM"):
                    size_shares, actual_price = self.simulate_buy(
                        session=session,
                        market_id=item.market_info.get("conditionId", ""),
                        token_id=item.token_id,
                        question=item.market_info.get("question", ""),
                        outcome=item.market_info.get("outcome", ""),
                        price=live_price if live_price is not None else item.whale_price,
                        size_usdc=item.bet_size_usdc,
                        db=db,
                    )
                else:
                    order_resp = self.place_real_buy(
                        token_id=item.token_id,
                        size_usdc=item.bet_size_usdc,
                        price=item.whale_price,
                    )
                    size_shares = float(order_resp.get("size_matched", item.bet_size_usdc / max(item.whale_price, 0.01)))
                    fill_price = (
                        order_resp.get("price")
                        or order_resp.get("avgPrice")
                        or order_resp.get("average_price")
                    )
                    actual_price = float(fill_price) if fill_price else (live_price or item.whale_price)
            except Exception as exc:
                logger.error("Drift watchlist: placement failed for whale_bet=%d: %s", whale_bet_id, exc)
                del self._drift_watchlist[whale_bet_id]
                continue

            # 6. Upgrade the existing SKIPPED record to OPEN in-place
            copied_bet = db.get(CopiedBet, item.copied_bet_id)
            if copied_bet is None:
                logger.warning("Drift watchlist: CopiedBet %d not found — removing", item.copied_bet_id)
                del self._drift_watchlist[whale_bet_id]
                continue

            copied_bet.status = "OPEN"
            copied_bet.price_at_entry = actual_price
            copied_bet.size_shares = size_shares
            copied_bet.skip_reason = None
            copied_bet.opened_at = datetime.utcnow()
            session.total_bets_placed += 1
            db.add(copied_bet)
            db.add(session)
            db.commit()
            db.refresh(copied_bet)

            del self._drift_watchlist[whale_bet_id]
            logger.info(
                "Drift watchlist: RETRY SUCCESS whale_bet=%d — placed %s @ %.4f for $%.2f  (watchlist size=%d)",
                whale_bet_id, mode, actual_price, item.bet_size_usdc, len(self._drift_watchlist),
            )

    def _create_skipped_bet(
        self,
        whale_bet: WhaleBet,
        session: MonitoringSession,
        bet_size_usdc: float,
        risk_factor: float,
        whale_avg: float,
        skip_reason: str,
        db: DBSession,
    ) -> CopiedBet:
        market_category, bet_type_cat = classify(whale_bet.question)
        copied_bet = CopiedBet(
            whale_bet_id=whale_bet.id,
            whale_address=whale_bet.whale.address,
            mode=session.mode,
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            question=whale_bet.question,
            side=whale_bet.side,
            outcome=whale_bet.outcome,
            price_at_entry=whale_bet.price,
            size_usdc=bet_size_usdc,
            size_shares=0.0,
            risk_factor=risk_factor,
            whale_bet_usdc=whale_bet.size_usdc,
            whale_avg_bet_usdc=whale_avg,
            status="SKIPPED",
            skip_reason=skip_reason,
            market_category=market_category,
            bet_type=bet_type_cat,
            opened_at=datetime.utcnow(),
        )
        db.add(copied_bet)
        db.commit()
        db.refresh(copied_bet)
        logger.info("Skipped bet for whale_bet %d: %s", whale_bet.id, skip_reason)
        return copied_bet

    @staticmethod
    def _is_closed_market_skip(market_info: dict, skip_reason: str) -> bool:
        """
        Return True if the skip is because the market is already closed/expired.
        These should be dropped silently rather than cluttering the ledger.
        Policy skips (balance, min bet, price drift, closing soon) return False.
        """
        # Explicit closed/resolved flags from market metadata
        if market_info.get("closed") or market_info.get("resolved"):
            return True
        if not market_info.get("active", True):
            return True
        status = str(market_info.get("status", "")).lower()
        if status in ("closed", "resolved", "finalized", "settled"):
            return True

        # Reason-string patterns produced by should_place_bet for past-end-date markets
        closed_phrases = (
            "market is already closed",
            "market is already resolved",
            "market is inactive",
            "closed ",       # "Market closed 3.2h ago"
            "market status is",
            "market end date unknown",
        )
        reason_lower = skip_reason.lower()
        return any(phrase in reason_lower for phrase in closed_phrases)

    def _find_open_position(
        self,
        market_id: str,
        token_id: str,
        session_mode: str,
        db: DBSession,
        whale_address: Optional[str] = None,
    ) -> Optional[CopiedBet]:
        """Find an OPEN CopiedBet for this market/token in the given session.

        If whale_address is provided (recommended for exits and add-to-position
        signals), the search is scoped to that whale's positions only.  This
        prevents a different whale's EXIT signal from accidentally closing a
        position that was opened by following a different whale.
        """
        q = db.query(CopiedBet).filter(
            CopiedBet.market_id == market_id,
            CopiedBet.token_id == token_id,
            CopiedBet.status == "OPEN",
            CopiedBet.mode == session_mode,
        )
        if whale_address is not None:
            q = q.filter(CopiedBet.whale_address == whale_address)
        return q.first()

    def _find_opposite_outcome_position(
        self,
        market_id: str,
        token_id: str,
        session_mode: str,
        db: DBSession,
        whale_address: str,
    ) -> Optional[CopiedBet]:
        """Return an OPEN position for the same whale in the same market but on a
        different token_id (opposite outcome). Used for hedge detection in HEDGE_SIM mode."""
        return (
            db.query(CopiedBet)
            .filter(
                CopiedBet.market_id == market_id,
                CopiedBet.token_id != token_id,
                CopiedBet.whale_address == whale_address,
                CopiedBet.status == "OPEN",
                CopiedBet.mode == session_mode,
            )
            .first()
        )
