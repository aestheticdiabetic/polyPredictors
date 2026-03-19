"""
Bet placement engine.
Handles both SIMULATION and REAL mode bet placement,
position closing, and resolution checking.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from sqlalchemy.orm import Session as DBSession

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


class BetEngine:
    """Processes whale bets and manages our copied positions."""

    def __init__(self, polymarket_client=None):
        self._client = polymarket_client

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
        existing_open = self._find_open_position(
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            session_mode=session.mode,
            db=db,
        )

        if existing_open is not None:
            # Whale is adding to a position we already hold -> signal only
            return self._handle_add_to_position(whale_bet, existing_open, db)

        # ---- CASE 3: Fresh position ------------------------------------

        # Risk calculations
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
            return self._create_skipped_bet(
                whale_bet=whale_bet,
                session=session,
                bet_size_usdc=bet_size_usdc,
                risk_factor=risk_factor,
                whale_avg=whale_avg,
                skip_reason=skip_reason,
                db=db,
            )

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

        if bet_size_usdc < 1.0:
            return self._create_skipped_bet(
                whale_bet=whale_bet,
                session=session,
                bet_size_usdc=bet_size_usdc,
                risk_factor=risk_factor,
                whale_avg=whale_avg,
                skip_reason="Calculated bet size below minimum ($1.00)",
                db=db,
            )

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
            if mode == "SIMULATION":
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

        # Parse market close date from market_info for ledger display
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
                        dt = dt.replace(tzinfo=None)  # store as naive UTC for consistency
                    market_close_at = dt
                except (ValueError, TypeError):
                    pass

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
            if not open_bets:
                return

            logger.debug("check_resolution: checking %d open bet(s)", len(open_bets))

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

            for bet, (price, end_dt) in zip(open_bets, resolution_data):
                if price is None:
                    continue

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

                # Time remaining to market close
                hours_remaining: Optional[float] = None
                if end_dt is not None:
                    hours_remaining = (end_dt - now).total_seconds() / 3600.0

                # Guard: price of exactly 0.0 on an active market is API noise.
                # Only a market that has already passed its end date can legitimately
                # settle at 0.0. Skipping prevents a transient API glitch from
                # wiping open positions with a 100% loss.
                market_ended = hours_remaining is not None and hours_remaining < 0
                if price == 0.0 and not market_ended:
                    logger.warning(
                        "Skipping price=0.0 for active bet %d (%.1fh to close) — "
                        "likely API noise, not closing",
                        bet.id,
                        hours_remaining if hours_remaining is not None else 0,
                    )
                    continue

                # --- Case 1: Market has already ended ---
                if market_ended:
                    logger.info(
                        "Force-close: bet %d market ended %.1fh ago, price=%.4f",
                        bet.id, abs(hours_remaining), price,
                    )
                    self.simulate_sell(
                        copied_bet=bet, current_price=price, session=session, db=db,
                        close_reason="Market ended",
                    )
                    continue

                # --- Case 2: Near-expiry window ---
                if hours_remaining is not None and 0 <= hours_remaining < min_hours:
                    logger.debug(
                        "Near-expiry monitoring: bet %d closes in %.2fh, price=%.4f",
                        bet.id, hours_remaining, price,
                    )
                    if price >= 0.80 or price <= 0.20:
                        logger.info(
                            "Near-expiry close: bet %d price=%.4f crosses 0.80/0.20 "
                            "with %.2fh remaining",
                            bet.id, price, hours_remaining,
                        )
                        self.simulate_sell(
                            copied_bet=bet, current_price=price, session=session, db=db,
                            close_reason=f"Near-expiry exit ({hours_remaining:.1f}h left, price {price:.3f})",
                        )
                    continue

                # --- Case 3: Standard resolution threshold ---
                if price >= 0.95 or price <= 0.05:
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
    ) -> list[tuple[Optional[float], Optional[datetime]]]:
        """Fetch price and endDate for multiple tokens in parallel."""
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as http:
            return await asyncio.gather(
                *[self._get_resolution_data_async(tid, http) for tid in token_ids]
            )

    async def _get_resolution_data_async(
        self, token_id: str, http: httpx.AsyncClient
    ) -> tuple[Optional[float], Optional[datetime]]:
        """
        Async fetch of (price, end_dt) from Gamma API for a single token.
        Returns (None, None) on error.
        """
        try:
            url = f"{settings.GAMMA_API_BASE}/markets"
            resp = await http.get(url, params={"clob_token_ids": token_id})
            resp.raise_for_status()
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            for market in markets:
                tokens = market.get("clobTokenIds", [])
                outcomes = market.get("outcomePrices", [])
                if token_id not in tokens:
                    continue

                # Parse price
                price: Optional[float] = None
                idx = tokens.index(token_id)
                if outcomes and idx < len(outcomes):
                    try:
                        price = float(outcomes[idx])
                    except (TypeError, ValueError):
                        pass

                # Parse endDate
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

                return price, end_dt
        except Exception as exc:
            logger.debug("_get_resolution_data_async error for %s: %s", token_id, exc)
        return None, None

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
        open_pos = self._find_open_position(
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            session_mode=session.mode,
            db=db,
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
            )
            db.add(copied_bet)
            db.commit()
            return copied_bet

        # Close existing position
        exit_price = whale_bet.price
        whale_alias = whale_bet.whale.alias if whale_bet.whale else whale_bet.whale_address[:10]
        exit_reason = f"Whale exited ({whale_alias} sold @ {exit_price:.3f})"

        if session.mode == "SIMULATION":
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
        db: DBSession,
    ) -> CopiedBet:
        """Whale is adding to a market we already hold - record signal only, no new ledger entry."""
        signal = AddToPositionSignal(
            whale_bet_id=whale_bet.id,
            copied_bet_id=existing_open.id,
            whale_additional_usdc=whale_bet.size_usdc,
            whale_additional_shares=whale_bet.size_shares,
            price=whale_bet.price,
            timestamp=whale_bet.timestamp,
            note="Whale added to position while we hold",
        )
        db.add(signal)
        db.commit()
        # Return the existing open position — no new ledger entry created
        return existing_open

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
    ) -> Optional[CopiedBet]:
        """Find an OPEN CopiedBet for this market/token in the given session.
        Accepts session_mode directly to avoid a redundant DB round-trip."""
        return (
            db.query(CopiedBet)
            .filter(
                CopiedBet.market_id == market_id,
                CopiedBet.token_id == token_id,
                CopiedBet.status == "OPEN",
                CopiedBet.mode == session_mode,
            )
            .first()
        )
