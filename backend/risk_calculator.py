"""
Risk factor calculation engine.
Determines bet sizing based on whale's relative bet size vs their average.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from backend.config import settings

logger = logging.getLogger(__name__)

_MIN_RISK_FACTOR = 0.5
_MAX_RISK_FACTOR = 3.0


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
        result = max(settings.MIN_BET_USDC, result)

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
        whale_price: Optional[float] = None,
        live_price: Optional[float] = None,
    ) -> tuple[bool, str]:
        """
        Decide whether to place a bet given market metadata.

        Checks:
        - Market is still open and active (not resolved / closed)
        - Time-to-close rules:
            * < 1 minute remaining → always skip (hard floor)
            * 1 min – min_hours_to_close remaining → skip UNLESS live price is
              within 2% of the whale's entry price (price still representative)
            * >= min_hours_to_close remaining → allow through

        Returns:
            (True, "") if safe to proceed
            (False, reason_string) if the bet should be skipped
        """
        if not market_info:
            return False, "Market info unavailable"

        # Explicit closed / resolved / inactive flags
        if not market_info.get("active", True):
            return False, "Market is inactive"

        if market_info.get("closed", False):
            return False, "Market is already closed"

        if market_info.get("resolved", False):
            return False, "Market is already resolved"

        # Some APIs use a string status field
        status = str(market_info.get("status", "")).lower()
        if status in ("closed", "resolved", "finalized", "settled"):
            return False, f"Market status is '{status}'"

        # Check endDate — deny if missing (conservative: unknown close time = unsafe)
        end_date_str = (
            market_info.get("endDate")
            or market_info.get("endDateIso")
            or market_info.get("end_date_iso")
            or market_info.get("end_date")
        )
        if not end_date_str:
            logger.warning(
                "Market has no endDate field — skipping to avoid betting on a "
                "closed market (id=%s)", market_info.get("id", "unknown")
            )
            return False, "Market end date unknown — skipping"

        try:
            end_dt = datetime.fromisoformat(end_date_str.rstrip("Z"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            hours_remaining = (end_dt - now).total_seconds() / 3600.0

            if hours_remaining < 0:
                return (
                    False,
                    f"Market closed {abs(hours_remaining):.1f}h ago",
                )

            HARD_MIN_HOURS = 1.0 / 60.0  # 1 minute — never bet this close to close
            if hours_remaining < HARD_MIN_HOURS:
                mins = hours_remaining * 60
                return (
                    False,
                    f"Market closes in {mins:.1f}min — too close to close",
                )

            if hours_remaining < min_hours_to_close:
                # Allow through if live price is within 2% of the whale's entry price,
                # meaning the market is still pricing correctly despite the short window.
                if whale_price is not None and live_price is not None:
                    drift = abs(live_price - whale_price)
                    if drift <= 0.02:
                        pass  # price looks good — allow the bet
                    else:
                        mins = hours_remaining * 60
                        return (
                            False,
                            f"Market closes in {mins:.0f}min, price drift {drift:.3f} > 2% — skipping",
                        )
                else:
                    # No price data to validate — apply the original stricter rule
                    return (
                        False,
                        f"Market closes in {hours_remaining:.1f}h (min {min_hours_to_close}h required, no price data)",
                    )
        except (ValueError, TypeError) as exc:
            logger.warning("Could not parse endDate '%s': %s", end_date_str, exc)
            return False, f"Could not parse market end date: {end_date_str!r}"

        return True, ""

    def check_price_staleness(
        self,
        whale_price: float,
        live_price: Optional[float],
        max_drift: float,
    ) -> tuple[bool, str]:
        """
        Guard against copying a bet whose odds have moved significantly since
        the whale placed it.

        Logic:
            drift = |live_price - whale_price|
            If drift > max_drift  →  skip

        A drift in either direction is treated as a skip signal:
          - Price went UP   (e.g. 0.20 → 0.30): we'd be paying too much per share,
            reducing or eliminating the expected edge.
          - Price went DOWN (e.g. 0.20 → 0.10): the market has moved against the
            whale's thesis, which is a warning sign.

        If live_price is None (fetch failed) the bet is allowed through — we
        prefer not to block on network uncertainty.

        Returns:
            (True,  "")             if safe to proceed
            (False, reason_string)  if the bet should be skipped
        """
        if live_price is None:
            logger.debug("Live price unavailable — skipping staleness check")
            return True, ""

        drift = abs(live_price - whale_price)

        if drift <= max_drift:
            logger.debug(
                "Price OK: whale=%.4f live=%.4f drift=%.4f <= max %.4f",
                whale_price, live_price, drift, max_drift,
            )
            return True, ""

        direction = "risen" if live_price > whale_price else "fallen"
        return (
            False,
            f"Odds have {direction} from {whale_price:.3f} to {live_price:.3f} "
            f"(drift {drift:.3f} > max {max_drift:.3f})",
        )

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
