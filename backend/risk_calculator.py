"""
Risk factor calculation engine.
Determines bet sizing based on whale's relative bet size vs their average.
"""

import logging
from datetime import UTC, datetime

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
        conviction_exponent: float = 1.0,
    ) -> float:
        """
        Compute risk factor as ratio of current bet to whale's average bet.

        When conviction_exponent == 1.0 (default): linear scaling, identical to
        previous behaviour — clamped to [MIN_RISK_FACTOR, MAX_RISK_FACTOR].

        When conviction_exponent > 1.0: convex curve applied to above-average
        conviction (conviction >= 1.0).  This rewards high-conviction bets
        disproportionately while keeping the below-average range linear.

            conviction = whale_bet / whale_avg
            if conviction >= 1.0:
                risk_factor = min(conviction ** exponent, MAX_RISK_FACTOR)
            else:
                risk_factor = max(conviction, MIN_RISK_FACTOR)

        If whale average is zero or unknown, returns 1.0 (neutral).
        """
        if whale_avg_bet_usdc <= 0:
            logger.debug(
                "Whale average bet is %.2f, defaulting risk factor to 1.0",
                whale_avg_bet_usdc,
            )
            return 1.0

        conviction = whale_bet_usdc / whale_avg_bet_usdc

        if conviction >= 1.0:
            raw = min(conviction**conviction_exponent, _MAX_RISK_FACTOR)
        else:
            raw = max(conviction, _MIN_RISK_FACTOR)

        logger.debug(
            "Risk factor: %.2f / %.2f = conviction %.3f -> rf %.3f (exponent=%.2f)",
            whale_bet_usdc,
            whale_avg_bet_usdc,
            conviction,
            raw,
            conviction_exponent,
        )
        return raw

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

        # For low-conviction signals (rf < 1.0), never inflate to the MIN_BET floor.
        # The floor exists to meet exchange minimums; applying it to below-average
        # whale bets would place a 5-10x oversized position vs what the model recommends.
        # Return 0.0 to signal "skip this bet" to the caller.
        if result < settings.MIN_BET_USDC and risk_factor < 1.0:
            logger.debug(
                "Bet size below minimum for rf=%.3f — returning 0.0 to skip (raw=%.2f < min=%.2f)",
                risk_factor,
                result,
                settings.MIN_BET_USDC,
            )
            return 0.0

        result = max(settings.MIN_BET_USDC, result)

        # Never bet more than the full balance
        result = min(result, session_balance)

        scalar = settings.BET_SIZE_SCALAR
        if scalar != 1.0:
            result = round(result * scalar, 2)

        logger.debug(
            "Bet size: balance=%.2f pct=%.3f base=%.2f rf=%.3f raw=%.2f cap=%.2f scalar=%.2f -> %.2f",
            session_balance,
            max_bet_pct,
            base_bet,
            risk_factor,
            bet_size,
            hard_cap,
            scalar,
            result,
        )
        return round(result, 2)

    def should_place_bet(
        self,
        market_info: dict,
        min_hours_to_close: float = 1.0,
        whale_price: float | None = None,
        live_price: float | None = None,
    ) -> tuple[bool, str]:
        """
        Decide whether to place a bet given market metadata.

        Checks:
        - Market is still open and active (not resolved / closed)
        - Time-to-close rules:
            * < 1 minute remaining → always skip (hard floor)
            * 1 min - min_hours_to_close remaining → skip UNLESS live price is
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
            # If we have a live price the market is demonstrably still active
            # (CLOB order books only exist for open markets).  Allow through —
            # we can't do time-to-close checks but the price staleness guard
            # downstream will still protect against copying stale odds.
            if live_price is not None:
                logger.debug(
                    "Market has no endDate but live_price=%.4f — allowing through "
                    "(market is demonstrably active, id=%s)",
                    live_price,
                    market_info.get("id", "unknown"),
                )
                return True, ""
            logger.warning(
                "Market has no endDate field — skipping to avoid betting on a "
                "closed market (id=%s)",
                market_info.get("id", "unknown"),
            )
            return False, "Market end date unknown — skipping"

        try:
            end_dt = datetime.fromisoformat(end_date_str.rstrip("Z"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=UTC)

            now = datetime.now(UTC)
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

    def check_fee_viability(
        self,
        entry_price: float,
        fee_bps: int,
        max_entry_price: float,
        min_net_upside: float,
    ) -> tuple[bool, str]:
        """
        Guard against REAL mode entries where the taker fee eliminates all upside.

        Logic:
            net_upside = (1 - entry_price) - (fee_bps / 10000)
            If entry_price >= max_entry_price  →  skip
            If net_upside < min_net_upside     →  skip

        Returns:
            (True,  "")             if safe to proceed
            (False, reason_string)  if the bet should be skipped
        """
        if entry_price >= max_entry_price:
            return (
                False,
                f"Entry price {entry_price:.3f} >= REAL_MAX_ENTRY_PRICE {max_entry_price:.3f} "
                f"(insufficient upside after fees)",
            )
        upside = 1.0 - entry_price
        fee_cost = fee_bps / 10000
        net_upside = upside - fee_cost
        if net_upside < min_net_upside:
            return (
                False,
                f"Net upside after {fee_bps}bps fee = {net_upside:.3f} "
                f"< REAL_MIN_NET_UPSIDE {min_net_upside:.3f} (entry={entry_price:.3f})",
            )
        logger.debug(
            "Fee viability OK: entry=%.4f fee=%dbps upside=%.4f net=%.4f >= min %.4f",
            entry_price,
            fee_bps,
            upside,
            net_upside,
            min_net_upside,
        )
        return True, ""

    def check_price_staleness(
        self,
        whale_price: float,
        live_price: float | None,
        max_upward_drift: float,
        max_downward_drift: float,
    ) -> tuple[bool, str]:
        """
        Guard against copying a bet whose odds have moved significantly since
        the whale placed it.  Uses asymmetric thresholds:

          - Price UP   (live > whale): we'd overpay relative to the whale, which
            compounds with fees to erode edge.  Capped tightly (default 0.02).
          - Price DOWN (live < whale): we'd pay less than the whale — favourable.
            Allowed a wider band (default 0.10) before treating as a stale signal.

        If live_price is None (fetch failed) the bet is allowed through — we
        prefer not to block on network uncertainty.

        Returns:
            (True,  "")             if safe to proceed
            (False, reason_string)  if the bet should be skipped
        """
        if live_price is None:
            logger.debug("Live price unavailable — skipping staleness check")
            return True, ""

        if live_price > whale_price:
            upward = live_price - whale_price
            if upward > max_upward_drift:
                return (
                    False,
                    f"Price has risen from {whale_price:.3f} to {live_price:.3f} "
                    f"(+{upward:.3f} > MAX_UPWARD_DRIFT {max_upward_drift:.3f})",
                )
        else:
            downward = whale_price - live_price
            if downward > max_downward_drift:
                return (
                    False,
                    f"Price has fallen from {whale_price:.3f} to {live_price:.3f} "
                    f"(-{downward:.3f} > MAX_DOWNWARD_DRIFT {max_downward_drift:.3f})",
                )

        logger.debug(
            "Price OK: whale=%.4f live=%.4f (upward_max=%.4f downward_max=%.4f)",
            whale_price,
            live_price,
            max_upward_drift,
            max_downward_drift,
        )
        return True, ""

    def calculate_arb_bet_size(
        self,
        session_balance: float,
        whale_bet_usdc: float,
        whale_avg_bet_usdc: float,
    ) -> float:
        """
        Bet sizing for arb-mode whales: portfolio-fraction approach.

        Arb whales hold ~ARB_CONCURRENT_POSITIONS open at a time, using their
        full capital. We mirror that by dividing our wallet equally across N
        positions, then scaling by the whale's relative bet size so larger
        arb positions get more allocation.

            base = session_balance / ARB_CONCURRENT_POSITIONS
            rel  = whale_bet_usdc / whale_avg_bet_usdc   (1.0 if avg unknown)
            bet  = base * rel

        Always places a bet (no skip for low rf) — arb requires capturing both
        sides of a position regardless of price proximity to fair odds.
        Hard floor: MIN_BET_USDC. Hard cap: session_balance.
        """
        if session_balance <= 0:
            return 0.0

        n = max(1, settings.ARB_CONCURRENT_POSITIONS)
        base = session_balance / n

        rel = whale_bet_usdc / whale_avg_bet_usdc if whale_avg_bet_usdc > 0 else 1.0
        rel = min(rel, _MAX_RISK_FACTOR)  # cap at same ceiling as standard mode

        result = base * rel
        result = min(result, session_balance)
        result = max(result, settings.MIN_BET_USDC)

        scalar = settings.BET_SIZE_SCALAR
        if scalar != 1.0:
            result = round(result * scalar, 2)

        logger.debug(
            "Arb bet size: balance=%.2f N=%d base=%.2f "
            "whale_bet=%.2f whale_avg=%.2f rel=%.3f scalar=%.2f -> %.2f",
            session_balance,
            n,
            base,
            whale_bet_usdc,
            whale_avg_bet_usdc,
            rel,
            scalar,
            result,
        )
        return round(result, 2)

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
