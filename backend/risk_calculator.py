"""
Risk factor calculation engine.
Determines bet sizing based on whale's relative bet size vs their average.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_MIN_RISK_FACTOR = 0.5
_MAX_RISK_FACTOR = 3.0
_MIN_BET_USDC = 1.0


class RiskCalculator:
    """Calculates risk-adjusted bet sizes based on whale behaviour."""

    # ------------------------------------------------------------------
    # Core calculations
    # ------------------------------------------------------------------

    def calculate_risk_factor(
        self,
        whale_bet_usdc: float,
        whale_avg_bet_usdc: float,
    ) -> float:
        """
        Compute risk factor as ratio of current bet to whale's average bet.

        Formula:  risk_factor = whale_bet_usdc / whale_avg_bet_usdc

        Clamped to [MIN_RISK_FACTOR, MAX_RISK_FACTOR].
        If whale average is zero or unknown, returns 1.0 (neutral).
        """
        if whale_avg_bet_usdc <= 0:
            logger.debug(
                "Whale average bet is %.2f, defaulting risk factor to 1.0",
                whale_avg_bet_usdc,
            )
            return 1.0

        raw = whale_bet_usdc / whale_avg_bet_usdc
        clamped = max(_MIN_RISK_FACTOR, min(_MAX_RISK_FACTOR, raw))
        logger.debug(
            "Risk factor: %.2f / %.2f = %.3f -> clamped %.3f",
            whale_bet_usdc,
            whale_avg_bet_usdc,
            raw,
            clamped,
        )
        return clamped

    def calculate_bet_size(
        self,
        session_balance: float,
        risk_factor: float,
        max_bet_pct: float = 0.05,
    ) -> float:
        """
        Compute the USDC amount to bet.

        Logic:
            base_bet  = session_balance * max_bet_pct
            bet_size  = base_bet * risk_factor
            hard_cap  = base_bet * MAX_RISK_FACTOR
            result    = max(_MIN_BET_USDC, min(bet_size, hard_cap))
        """
        if session_balance <= 0:
            return 0.0

        base_bet = session_balance * max_bet_pct
        bet_size = base_bet * risk_factor
        hard_cap = base_bet * _MAX_RISK_FACTOR  # absolute ceiling

        result = min(bet_size, hard_cap)
        result = max(_MIN_BET_USDC, result)

        # Never bet more than the full balance
        result = min(result, session_balance)

        logger.debug(
            "Bet size: balance=%.2f pct=%.3f base=%.2f rf=%.3f raw=%.2f cap=%.2f -> %.2f",
            session_balance,
            max_bet_pct,
            base_bet,
            risk_factor,
            bet_size,
            hard_cap,
            result,
        )
        return round(result, 2)

    def should_place_bet(
        self,
        market_info: dict,
        min_hours_to_close: float = 1.0,
    ) -> tuple[bool, str]:
        """
        Decide whether to place a bet given market metadata.

        Checks:
        - Market is still open (endDate in the future by at least min_hours_to_close)
        - Market is active (not resolved / closed)

        Returns:
            (True, "") if safe to proceed
            (False, reason_string) if the bet should be skipped
        """
        if not market_info:
            return False, "Market info unavailable"

        # Check active flag if present
        active = market_info.get("active", True)
        if not active:
            return False, "Market is inactive"

        closed_flag = market_info.get("closed", False)
        if closed_flag:
            return False, "Market is already closed"

        # Check endDate
        end_date_str = (
            market_info.get("endDate")
            or market_info.get("end_date_iso")
            or market_info.get("end_date")
        )
        if not end_date_str:
            # No end date info - allow the bet but flag uncertainty
            logger.debug("Market has no endDate, allowing bet anyway: %s", market_info.get("id"))
            return True, ""

        try:
            # Handle various ISO formats
            end_date_str = end_date_str.rstrip("Z")
            if "." in end_date_str:
                end_dt = datetime.fromisoformat(end_date_str)
            else:
                end_dt = datetime.fromisoformat(end_date_str)

            # Make timezone-aware if naive
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            hours_remaining = (end_dt - now).total_seconds() / 3600.0

            if hours_remaining < min_hours_to_close:
                return (
                    False,
                    f"Market closes in {hours_remaining:.1f}h (min {min_hours_to_close}h required)",
                )
        except (ValueError, TypeError) as exc:
            logger.warning("Could not parse endDate '%s': %s", end_date_str, exc)
            # Allow the bet - don't block on parse error
            return True, ""

        return True, ""

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def estimate_pnl(
        self,
        entry_price: float,
        resolution_price: float,
        size_shares: float,
        size_usdc: float,
    ) -> tuple[float, str]:
        """
        Calculate realised P&L and determine win/loss/neutral.

        resolution_price = 1.0 for YES win, 0.0 for NO win (YES loss).

        Returns:
            (pnl_usdc, status_string)
        """
        if resolution_price is None:
            return 0.0, "CLOSED_NEUTRAL"

        # Each share pays resolution_price at settlement
        proceeds = size_shares * resolution_price
        pnl = proceeds - size_usdc

        if pnl > 0.01:
            status = "CLOSED_WIN"
        elif pnl < -0.01:
            status = "CLOSED_LOSS"
        else:
            status = "CLOSED_NEUTRAL"

        return round(pnl, 2), status
