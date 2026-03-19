"""
Bet placement engine.
Handles both SIMULATION and REAL mode bet placement,
position closing, and resolution checking.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

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
    ) -> CopiedBet:
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
            session_id=session.id,
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
                # Try to extract fill info from response
                shares_filled = order_resp.get("size_matched", bet_size_usdc / max(whale_bet.price, 0.01))
                size_shares = float(shares_filled)
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

    def check_resolution(self, session_id: int):
        """
        Check all OPEN CopiedBets for the given session.
        For each OPEN bet, look up the current market price.
        If the price is at or near 0 or 1, treat the market as resolved.
        Called periodically (every 5 minutes).
        """
        db = SessionLocal()
        try:
            session = db.query(MonitoringSession).filter_by(id=session_id).first()
            if not session:
                return

            open_bets = (
                db.query(CopiedBet)
                .filter(
                    CopiedBet.status == "OPEN",
                    CopiedBet.mode == session.mode,
                )
                .all()
            )

            if not open_bets:
                return

            # We need the async client but we're in a sync context here.
            # Use a synchronous fallback: check market data via httpx.get (sync)
            import httpx

            http = httpx.Client(timeout=10.0, follow_redirects=True)
            try:
                for bet in open_bets:
                    resolution_price = self._get_resolution_price_sync(
                        bet.token_id, http
                    )
                    if resolution_price is None:
                        continue

                    # Consider resolved if price is very near 0 or 1
                    if resolution_price >= 0.95 or resolution_price <= 0.05:
                        self.simulate_sell(
                            copied_bet=bet,
                            current_price=resolution_price,
                            session=session,
                            db=db,
                        )
                        logger.info(
                            "Resolved bet %d at price %.4f status=%s",
                            bet.id, resolution_price, bet.status,
                        )

                db.commit()
            finally:
                http.close()

        except Exception as exc:
            logger.error("check_resolution error: %s", exc)
            db.rollback()
        finally:
            db.close()

    def _get_resolution_price_sync(self, token_id: str, http: "httpx.Client") -> Optional[float]:
        """Synchronous fetch of last trade price from Gamma API."""
        try:
            url = f"{settings.GAMMA_API_BASE}/markets"
            resp = http.get(url, params={"clob_token_ids": token_id})
            resp.raise_for_status()
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            for market in markets:
                tokens = market.get("clobTokenIds", [])
                outcomes = market.get("outcomePrices", [])
                if token_id in tokens and outcomes:
                    idx = tokens.index(token_id)
                    if idx < len(outcomes):
                        return float(outcomes[idx])
        except Exception as exc:
            logger.debug("_get_resolution_price_sync error: %s", exc)
        return None

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
            session_id=session.id,
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
        if session.mode == "SIMULATION":
            self.simulate_sell(
                copied_bet=open_pos,
                current_price=exit_price,
                session=session,
                db=db,
            )
        else:
            try:
                self.place_real_sell(open_pos.token_id, open_pos.size_shares)
                open_pos.status = "CLOSED_NEUTRAL"
                open_pos.pnl_usdc = 0.0
                open_pos.resolution_price = exit_price
                open_pos.closed_at = datetime.utcnow()
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
        """Whale is adding to a market we already hold - record signal, skip copy."""
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

        # Mark the whale_bet as leading to a SKIPPED copy
        copied_bet = CopiedBet(
            whale_bet_id=whale_bet.id,
            whale_address=whale_bet.whale.address,
            mode=existing_open.mode,
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
            skip_reason="Already have open position in this market",
        )
        db.add(copied_bet)
        db.commit()
        return copied_bet

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

    def _find_open_position(
        self,
        market_id: str,
        token_id: str,
        session_id: int,
        db: DBSession,
    ) -> Optional[CopiedBet]:
        """Find an OPEN CopiedBet for this market/token in the given session."""
        # Get session mode first
        session = db.query(MonitoringSession).filter_by(id=session_id).first()
        if not session:
            return None

        return (
            db.query(CopiedBet)
            .filter(
                CopiedBet.market_id == market_id,
                CopiedBet.token_id == token_id,
                CopiedBet.status == "OPEN",
                CopiedBet.mode == session.mode,
            )
            .first()
        )
