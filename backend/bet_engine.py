"""
Bet placement engine.
Handles both SIMULATION and REAL mode bet placement,
position closing, and resolution checking.
"""

import asyncio
import contextlib
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from sqlalchemy.orm import Session as DBSession

from backend.categorizer import classify
from backend.config import settings
from backend.database import (
    AddToPositionSignal,
    CopiedBet,
    MonitoringSession,
    SessionLocal,
    Whale,
    WhaleBet,
)
from backend.risk_calculator import RiskCalculator

logger = logging.getLogger(__name__)

risk_calc = RiskCalculator()


@dataclass
class _WatchItem:
    """Tracks a drift-skipped bet on a near-expiry market for fast retry."""

    copied_bet_id: int  # existing SKIPPED CopiedBet to upgrade on success
    whale_bet_id: int
    session_id: int
    token_id: str
    market_info: dict
    whale_price: float
    bet_size_usdc: float
    risk_factor: float
    whale_avg: float
    expires_at: float  # time.monotonic() deadline


class BetEngine:
    """Processes whale bets and manages our copied positions."""

    def __init__(self, polymarket_client=None):
        self._client = polymarket_client
        # whale_bet_id → _WatchItem: near-expiry bets that were drift-skipped,
        # retried every DRIFT_RETRY_INTERVAL_SECONDS with a fresh price.
        self._drift_watchlist: dict[int, _WatchItem] = {}
        # Guards _drift_watchlist against concurrent access from poll_whales
        # and drift_retry scheduler jobs running in different threads.
        self._drift_lock = threading.Lock()
        # Serialises the balance-read → bet-size → deduct cycle so that
        # concurrent scanner threads (chain monitor + activity poller + drift
        # retry) cannot both read the same committed balance and over-commit.
        self._placement_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def process_new_whale_bet(
        self,
        whale_bet: WhaleBet,
        session: MonitoringSession,
        db: DBSession,
        market_info: dict | None = None,
        live_price: float | None = None,
        taker_fee_bps: int = 1000,
    ) -> CopiedBet | None:
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
        market_info = market_info or {}

        # Backfill missing WhaleBet metadata from market_info when the activity
        # API omitted conditionId / question (common for some trade types).
        # Persists to DB so downstream display and exit matching work correctly.
        if market_info:
            if not whale_bet.market_id:
                whale_bet.market_id = (
                    market_info.get("conditionId") or market_info.get("condition_id") or ""
                )
            if not whale_bet.question:
                whale_bet.question = market_info.get("question") or market_info.get("title") or ""
            if not whale_bet.outcome:
                for tok in market_info.get("tokens") or []:
                    if str(tok.get("token_id", "")) == whale_bet.token_id:
                        whale_bet.outcome = (tok.get("outcome") or "Yes").upper()
                        break
            if whale_bet.market_id or whale_bet.question:
                try:
                    db.add(whale_bet)
                    db.flush()
                except Exception:
                    pass

        # ---- CASE 1: Whale is exiting a position ----------------------
        if whale_bet.bet_type == "EXIT":
            return self._handle_exit(whale_bet, session, db, live_exit_price=live_price)

        # ---- CASE 2: Whale is opening / adding to a position ----------
        # Two sub-cases:
        #   a) Same price (within _SAME_PRICE_TOLERANCE): whale is adding more capital
        #      to an existing position at the same entry level → AddToPositionSignal.
        #   b) Different price: whale is opening a NEW independent tranche at a different
        #      entry level → new CopiedBet tracked separately for per-tranche exit.
        # EXIT signals close tranches proportionally (see _handle_exit).
        whale_address = whale.address if whale else None

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
                    whale_address[:10],
                    whale_bet.market_id[:16],
                    existing_outcome,
                    new_outcome,
                )
                whale_avg = whale.avg_bet_size_usdc if whale else 0.0
                risk_factor = risk_calc.calculate_risk_factor(whale_bet.size_usdc, whale_avg)
                bet_size_usdc = risk_calc.calculate_bet_size(
                    session_balance=session.current_balance_usdc,
                    risk_factor=risk_factor,
                    max_bet_pct=settings.MAX_BET_PCT,
                )
                return self._create_skipped_bet(
                    whale_bet=whale_bet,
                    session=session,
                    bet_size_usdc=bet_size_usdc,
                    risk_factor=risk_factor,
                    whale_avg=whale_avg,
                    skip_reason=skip_reason,
                    db=db,
                )

        # ---- SAME-PRICE ADD-TO-POSITION GUARD ---------------------------------
        # If the whale already holds an OPEN position in this token at approximately
        # the same price, record as an add-to-position signal rather than opening
        # a new tranche.  Trades at a meaningfully different price are independent
        # positions held until the whale exits that specific entry level.
        _SAME_PRICE_TOLERANCE = 0.03  # 3 cents absolute — same entry level (tick variation)
        if whale_address:
            existing_for_token = (
                db.query(CopiedBet)
                .filter(
                    CopiedBet.whale_address == whale_address,
                    CopiedBet.token_id == whale_bet.token_id,
                    CopiedBet.status == "OPEN",
                    CopiedBet.mode == mode,
                )
                .all()
            )
            if existing_for_token:
                closest = min(
                    existing_for_token,
                    key=lambda p: abs(whale_bet.price - p.price_at_entry),
                )
                price_diff = abs(whale_bet.price - closest.price_at_entry)
                if price_diff <= _SAME_PRICE_TOLERANCE:
                    logger.info(
                        "Add-to-position: whale %s bought %s @ %.3f within %.2f of "
                        "existing entry %.3f — recording signal, no new tranche",
                        whale_address[:10],
                        whale_bet.token_id[:16],
                        whale_bet.price,
                        _SAME_PRICE_TOLERANCE,
                        closest.price_at_entry,
                    )
                    return self._handle_add_to_position(whale_bet, closest, session, db)
                logger.info(
                    "New price tranche: whale %s bought %s @ %.3f "
                    "(diff=%.3f from existing %.3f > %.2f tolerance) — opening new tranche",
                    whale_address[:10],
                    whale_bet.token_id[:16],
                    whale_bet.price,
                    price_diff,
                    closest.price_at_entry,
                    _SAME_PRICE_TOLERANCE,
                )

        # ---- CASE 3: Fresh position / new price tranche -------------------

        with self._placement_lock:
            # Re-read session balance now that we hold the lock —
            # another thread may have committed a deduction since
            # this thread first loaded the session object.
            db.refresh(session)
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
                if whale_address
                else 0
            )

            if whale and whale.arb_mode:
                # Arb mode bypasses calibration: portfolio-fraction sizing works from
                # bet 1 and never skips — capturing both sides of every arb is mandatory.
                whale_avg = whale.avg_bet_size_usdc if whale else 0.0
                rel = whale_bet.size_usdc / whale_avg if whale_avg > 0 else 1.0
                risk_factor = rel  # logged as relative size, not conviction
                bet_size_usdc = risk_calc.calculate_arb_bet_size(
                    session_balance=session.current_balance_usdc,
                    whale_bet_usdc=whale_bet.size_usdc,
                    whale_avg_bet_usdc=whale_avg,
                )
                logger.info(
                    "Arb mode: whale %s price=%.4f whale_bet=$%.2f avg=$%.2f rel=%.3f bet=$%.2f",
                    whale.address[:10],
                    whale_bet.price,
                    whale_bet.size_usdc,
                    whale_avg,
                    rel,
                    bet_size_usdc,
                )
            elif placed_count < _CALIBRATION_THRESHOLD:
                bet_size_usdc = settings.MIN_BET_USDC
                risk_factor = 0.0  # sentinel — no risk data yet
                whale_avg = 0.0
                logger.info(
                    "Calibration mode: whale %s has %d/%d placed bets — using minimum $%.2f",
                    (whale_address or "?")[:10],
                    placed_count,
                    _CALIBRATION_THRESHOLD,
                    settings.MIN_BET_USDC,
                )
            else:
                whale_avg = whale.avg_bet_size_usdc if whale else 0.0
                risk_factor = risk_calc.calculate_risk_factor(
                    whale_bet.size_usdc,
                    whale_avg,
                    conviction_exponent=settings.CONVICTION_EXPONENT,
                )
                bet_size_usdc = risk_calc.calculate_bet_size(
                    session_balance=session.current_balance_usdc,
                    risk_factor=risk_factor,
                    max_bet_pct=settings.MAX_BET_PCT,
                )

            # Fix: risk_calculator returns 0.0 when rf < 1.0 and raw size < MIN_BET_USDC
            # to avoid inflating a low-conviction signal to an oversized position.
            if bet_size_usdc == 0.0:
                return self._create_skipped_bet(
                    whale_bet=whale_bet,
                    session=session,
                    bet_size_usdc=0.0,
                    risk_factor=risk_factor,
                    whale_avg=whale_avg,
                    skip_reason="Low-conviction signal below minimum bet size — skipping to avoid MIN_BET inflation",
                    db=db,
                )

            # Market guard
            # If Gamma couldn't return market metadata but the CLOB has a live price,
            # the market is demonstrably still active (CLOB order books only exist for
            # open markets).  In that case bypass the metadata check entirely — we don't
            # want to miss the initial bet just because the Gamma API had a transient
            # failure or negatively cached a bad condition_id format.  Price staleness
            # is still validated below via check_price_staleness.
            effective_market_info = market_info or {}
            if not effective_market_info and live_price is not None:
                ok, skip_reason = True, ""
            else:
                ok, skip_reason = risk_calc.should_place_bet(
                    effective_market_info,
                    min_hours_to_close=settings.MIN_MARKET_HOURS_TO_CLOSE,
                    whale_price=whale_bet.price,
                    live_price=live_price,
                )
            if not ok:
                # If the market is already closed/resolved, drop silently — no ledger entry.
                # Policy skips (balance, price drift, etc.) still get a SKIPPED record.
                if self._is_closed_market_skip(effective_market_info, skip_reason):
                    logger.debug(
                        "Dropping whale_bet %d silently — market already closed: %s",
                        whale_bet.id,
                        skip_reason,
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

            # Per-whale filters — category, price threshold, bet size, keywords
            cat_sport, cat_bet_type = classify(whale_bet.question)
            if whale and whale.category_filters:
                import json as _json

                try:
                    filters = _json.loads(whale.category_filters)

                    # Sport: opt-in allowlist takes precedence over opt-out list
                    allowed_sports = filters.get("allowed_sports", [])
                    disabled_sports = filters.get("disabled_sports", [])
                    disabled_bet_types = filters.get("disabled_bet_types", [])
                    if allowed_sports:
                        if cat_sport not in allowed_sports:
                            return self._create_skipped_bet(
                                whale_bet=whale_bet,
                                session=session,
                                bet_size_usdc=bet_size_usdc,
                                risk_factor=risk_factor,
                                whale_avg=whale_avg,
                                skip_reason=f"Sport not in allowlist: {cat_sport}",
                                db=db,
                            )
                    elif cat_sport in disabled_sports:
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Category filtered: {cat_sport}",
                            db=db,
                        )
                    if cat_bet_type in disabled_bet_types:
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Bet type filtered: {cat_bet_type}",
                            db=db,
                        )

                    # Whale entry price threshold
                    min_price = filters.get("min_entry_price")
                    max_price = filters.get("max_entry_price")
                    if min_price is not None and whale_bet.price < min_price:
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Whale price {whale_bet.price:.3f} below min {min_price:.3f}",
                            db=db,
                        )
                    if max_price is not None and whale_bet.price > max_price:
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Whale price {whale_bet.price:.3f} above max {max_price:.3f}",
                            db=db,
                        )

                    # Whale bet size threshold
                    min_size = filters.get("min_whale_bet_usdc")
                    max_size = filters.get("max_whale_bet_usdc")
                    if min_size is not None and whale_bet.size_usdc < min_size:
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Whale bet ${whale_bet.size_usdc:.0f} below min ${min_size:.0f}",
                            db=db,
                        )
                    if max_size is not None and whale_bet.size_usdc > max_size:
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Whale bet ${whale_bet.size_usdc:.0f} above max ${max_size:.0f}",
                            db=db,
                        )

                    # Keyword filters on market question (case-insensitive)
                    question_lower = whale_bet.question.lower()
                    required_kw = filters.get("required_keywords", [])
                    excluded_kw = filters.get("excluded_keywords", [])
                    for kw in required_kw:
                        if kw.lower() not in question_lower:
                            return self._create_skipped_bet(
                                whale_bet=whale_bet,
                                session=session,
                                bet_size_usdc=bet_size_usdc,
                                risk_factor=risk_factor,
                                whale_avg=whale_avg,
                                skip_reason=f"Missing required keyword: {kw!r}",
                                db=db,
                            )
                    for kw in excluded_kw:
                        if kw.lower() in question_lower:
                            return self._create_skipped_bet(
                                whale_bet=whale_bet,
                                session=session,
                                bet_size_usdc=bet_size_usdc,
                                risk_factor=risk_factor,
                                whale_avg=whale_avg,
                                skip_reason=f"Excluded keyword matched: {kw!r}",
                                db=db,
                            )

                except Exception:
                    pass

            # Price staleness guard — skip if odds have moved too far since the whale bet.
            # REAL mode uses a tighter upward cap: buying above whale entry compounds
            # with fees, so we tolerate less overpayment than downward moves.
            price_ok, price_reason = risk_calc.check_price_staleness(
                whale_price=whale_bet.price,
                live_price=live_price,
                max_upward_drift=settings.MAX_UPWARD_DRIFT_PCT,
                max_downward_drift=settings.MAX_DOWNWARD_DRIFT_PCT,
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

            # REAL mode fee-viability gate — skip trades where fees eliminate all upside.
            # E.g. buying at 0.85 with 10% fee leaves only 5% net upside; break-even
            # requires the whale to win 93.5% of the time.
            if mode == "REAL":
                check_price = live_price if live_price is not None else whale_bet.price
                viable, viability_reason = risk_calc.check_fee_viability(
                    entry_price=check_price,
                    fee_bps=taker_fee_bps,
                    max_entry_price=settings.REAL_MAX_ENTRY_PRICE,
                    min_net_upside=settings.REAL_MIN_NET_UPSIDE,
                )
                if not viable:
                    return self._create_skipped_bet(
                        whale_bet=whale_bet,
                        session=session,
                        bet_size_usdc=bet_size_usdc,
                        risk_factor=risk_factor,
                        whale_avg=whale_avg,
                        skip_reason=viability_reason,
                        db=db,
                    )

            # Clamp to exchange minimum of $1.
            if bet_size_usdc < 1.0:
                logger.info(
                    "Clamping bet size from $%.4f to $1.00 (exchange minimum) for whale_bet %d",
                    bet_size_usdc,
                    whale_bet.id,
                )
                bet_size_usdc = 1.0

            # Place or simulate
            entry_price = whale_bet.price
            _neg_risk = bool(market_info.get("negRisk", False))
            _post_fill_pnl: float | None = None
            _post_fill_sell_price: float | None = None
            try:
                if mode in ("SIMULATION", "HEDGE_SIM"):
                    shares, actual_price, actual_cost = self.simulate_buy(
                        session=session,
                        market_id=whale_bet.market_id,
                        token_id=whale_bet.token_id,
                        question=whale_bet.question,
                        outcome=whale_bet.outcome,
                        price=whale_bet.price,
                        size_usdc=bet_size_usdc,
                        db=db,
                        live_price=live_price,
                    )
                    entry_price = actual_price
                    size_shares = shares
                    bet_size_usdc = actual_cost  # use fee-inclusive cost as DB cost basis
                    bet_status = "OPEN"
                    skip_reason_val = None
                else:
                    order_resp = self.place_real_buy(
                        token_id=whale_bet.token_id,
                        size_usdc=bet_size_usdc,
                        price=whale_bet.price,
                        neg_risk=_neg_risk,
                    )
                    # Detect FOK/unmatched cancellations — no tokens were actually received
                    order_status_raw = str(order_resp.get("status", "")).upper()
                    if order_status_raw in ("UNMATCHED", "CANCELLED", "CANCELED"):
                        size_shares = 0.0
                        logger.warning(
                            "REAL BUY status=%s for token %s — order not filled, recording size_shares=0",
                            order_status_raw,
                            whale_bet.token_id[:16],
                        )
                    else:
                        import math as _math

                        # Log the raw response so we can diagnose field names in prod.
                        logger.info("REAL BUY response: %s", order_resp)

                        # CLOB BUY response fields (confirmed via py-clob-client source):
                        #   Submitter is the ORDER MAKER (maker=self.funder in builder).
                        #   makingAmount = USDC the maker (us) provided (e.g. "0.7548")
                        #   takingAmount = YES shares the taker provided to us (e.g. "1.02")
                        # entry_price = makingAmount / takingAmount = USDC / shares
                        # Legacy fields like price/avgPrice/size_matched are NOT present.
                        taking_raw = order_resp.get("takingAmount")
                        making_raw = order_resp.get("makingAmount")

                        if taking_raw is not None and making_raw is not None:
                            try:
                                taking_f = float(taking_raw)
                                making_f = float(making_raw)
                                if making_f > 0 and taking_f > 0:
                                    size_shares = _math.floor(taking_f * 100) / 100
                                    actual_cost = round(making_f, 4)
                                    bet_size_usdc = actual_cost
                                    entry_price = round(making_f / taking_f, 6)
                                    logger.info(
                                        "REAL BUY: makingAmount=%.4f (USDC) takingAmount=%.4f (shares)"
                                        " → entry_price=%.4f actual_cost=$%.4f",
                                        making_f,
                                        taking_f,
                                        entry_price,
                                        actual_cost,
                                    )
                                    # Post-fill guard: CLOB slippage can push the actual fill
                                    # above REAL_MAX_ENTRY_PRICE checked pre-order. Sell back
                                    # immediately to avoid being trapped in a fee-dominant trade.
                                    if entry_price >= settings.REAL_MAX_ENTRY_PRICE:
                                        logger.warning(
                                            "REAL BUY: fill %.4f >= REAL_MAX_ENTRY_PRICE %.4f "
                                            "— selling back immediately",
                                            entry_price,
                                            settings.REAL_MAX_ENTRY_PRICE,
                                        )
                                        try:
                                            _sell_r = self.place_real_sell(
                                                token_id=whale_bet.token_id,
                                                size_shares=size_shares,
                                            )
                                            _post_fill_sell_price = float(
                                                _sell_r.get("price")
                                                or _sell_r.get("avgPrice")
                                                or _sell_r.get("average_price")
                                                or entry_price * 0.97
                                            )
                                        except Exception as _se:
                                            logger.error("Post-fill emergency sell failed: %s", _se)
                                            _post_fill_sell_price = entry_price * 0.97
                                        _post_fill_pnl = round(
                                            size_shares * _post_fill_sell_price - actual_cost, 4
                                        )
                                else:
                                    # Zero amounts — fall back to whale price estimate
                                    entry_price = float(whale_bet.price)
                                    size_shares = (
                                        _math.floor(bet_size_usdc / entry_price * 100) / 100
                                    )
                                    logger.warning(
                                        "REAL BUY: takingAmount/makingAmount zero — "
                                        "falling back to whale price %.4f",
                                        entry_price,
                                    )
                            except (TypeError, ValueError) as _e:
                                entry_price = float(whale_bet.price)
                                size_shares = _math.floor(bet_size_usdc / entry_price * 100) / 100
                                logger.warning(
                                    "REAL BUY: could not parse amounts (%s) — fallback", _e
                                )
                        else:
                            # Fields absent — fall back to whale price estimate
                            entry_price = float(whale_bet.price)
                            size_shares = _math.floor(bet_size_usdc / entry_price * 100) / 100
                            logger.warning(
                                "REAL BUY: takingAmount/makingAmount absent in response %s"
                                " — falling back to whale price %.4f",
                                order_resp,
                                entry_price,
                            )
                    # Deduct actual cost from session balance, then refund sell proceeds
                    # if we immediately sold back due to post-fill overpay.
                    session.current_balance_usdc = max(
                        0.0, session.current_balance_usdc - bet_size_usdc
                    )
                    if _post_fill_sell_price is not None:
                        sell_proceeds = round(size_shares * _post_fill_sell_price, 4)
                        session.current_balance_usdc = min(
                            session.current_balance_usdc + sell_proceeds,
                            session.starting_balance_usdc,
                        )
                        bet_status = "CLOSED_LOSS"
                        skip_reason_val = (
                            f"Post-fill overpay: fill {entry_price:.4f} >= "
                            f"REAL_MAX_ENTRY_PRICE {settings.REAL_MAX_ENTRY_PRICE:.4f}"
                        )
                    else:
                        bet_status = "OPEN"
                        skip_reason_val = None
                    db.add(session)
            except Exception as exc:
                logger.error("Failed to place bet for whale_bet %d: %s", whale_bet.id, exc)
                # Retry once on PolyApiException if price is still within drift parameters.
                try:
                    from py_clob_client.exceptions import PolyApiException as _PolyApiExc

                    _is_poly_exc = isinstance(exc, _PolyApiExc)
                except ImportError:
                    _is_poly_exc = "PolyApiException" in type(exc).__name__
                if _is_poly_exc and mode not in ("SIMULATION", "HEDGE_SIM"):
                    retry_price_ok, retry_price_reason = risk_calc.check_price_staleness(
                        whale_price=whale_bet.price,
                        live_price=live_price,
                        max_upward_drift=settings.MAX_UPWARD_DRIFT_PCT,
                        max_downward_drift=settings.MAX_DOWNWARD_DRIFT_PCT,
                    )
                    if retry_price_ok:
                        logger.warning(
                            "PolyApiException on whale_bet %d — price still within drift, retrying buy",
                            whale_bet.id,
                        )
                        try:
                            order_resp = self.place_real_buy(
                                token_id=whale_bet.token_id,
                                size_usdc=bet_size_usdc,
                                price=whale_bet.price,
                                neg_risk=_neg_risk,
                            )
                            # Retry succeeded — re-enter the success path inline
                            import math as _math

                            order_status_raw = str(order_resp.get("status", "")).upper()
                            if order_status_raw in ("UNMATCHED", "CANCELLED", "CANCELED"):
                                size_shares = 0.0
                                bet_status = "OPEN"
                                skip_reason_val = None
                            else:
                                taking_raw = order_resp.get("takingAmount")
                                making_raw = order_resp.get("makingAmount")
                                if taking_raw is not None and making_raw is not None:
                                    try:
                                        taking_f = float(taking_raw)
                                        making_f = float(making_raw)
                                        if making_f > 0 and taking_f > 0:
                                            size_shares = _math.floor(taking_f * 100) / 100
                                            bet_size_usdc = round(making_f, 4)
                                            entry_price = round(making_f / taking_f, 6)
                                        else:
                                            entry_price = float(whale_bet.price)
                                            size_shares = (
                                                _math.floor(bet_size_usdc / entry_price * 100) / 100
                                            )
                                    except (TypeError, ValueError):
                                        entry_price = float(whale_bet.price)
                                        size_shares = (
                                            _math.floor(bet_size_usdc / entry_price * 100) / 100
                                        )
                                else:
                                    entry_price = float(whale_bet.price)
                                    size_shares = (
                                        _math.floor(bet_size_usdc / entry_price * 100) / 100
                                    )
                                bet_status = "OPEN"
                                skip_reason_val = None
                            session.current_balance_usdc = max(
                                0.0, session.current_balance_usdc - bet_size_usdc
                            )
                            db.add(session)
                            logger.info(
                                "Retry BUY succeeded for whale_bet %d: entry=%.4f shares=%.4f",
                                whale_bet.id,
                                entry_price,
                                size_shares,
                            )
                        except Exception as retry_exc:
                            logger.error(
                                "Retry buy also failed for whale_bet %d: %s",
                                whale_bet.id,
                                retry_exc,
                            )
                            return self._create_skipped_bet(
                                whale_bet=whale_bet,
                                session=session,
                                bet_size_usdc=bet_size_usdc,
                                risk_factor=risk_factor,
                                whale_avg=whale_avg,
                                skip_reason=f"Order failed (retry): {retry_exc}",
                                db=db,
                            )
                    else:
                        logger.info(
                            "PolyApiException on whale_bet %d — price out of drift after failure (%s), skipping",
                            whale_bet.id,
                            retry_price_reason,
                        )
                        return self._create_skipped_bet(
                            whale_bet=whale_bet,
                            session=session,
                            bet_size_usdc=bet_size_usdc,
                            risk_factor=risk_factor,
                            whale_avg=whale_avg,
                            skip_reason=f"Order failed + price drifted: {exc}",
                            db=db,
                        )
                else:
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
                session_id=session.id,
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

            # If we immediately sold back due to post-fill overpay, record the realised loss.
            if _post_fill_pnl is not None:
                copied_bet.pnl_usdc = _post_fill_pnl
                copied_bet.resolution_price = _post_fill_sell_price
                copied_bet.closed_at = datetime.utcnow()
                copied_bet.close_reason = skip_reason_val
                session.total_losses += 1
                session.total_pnl_usdc = round(session.total_pnl_usdc + _post_fill_pnl, 4)
                db.add(session)

            db.commit()
            db.refresh(copied_bet)

            logger.info(
                "Placed %s bet: %s %s @ %.3f for $%.2f (rf=%.2f)",
                mode,
                whale_bet.outcome,
                whale_bet.market_id[:16],
                entry_price,
                bet_size_usdc,
                risk_factor,
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
        live_price: float | None = None,
    ) -> tuple[float, float, float]:
        """
        Simulate buying `size_usdc` worth of shares, mirroring real mode behaviour:
          - Uses live_price as entry (falling back to whale price) — Gap 1 fix
          - Floor-rounds shares to 2 dp to match CLOB fill rounding — Gap 3 fix
          - When SIM_APPLY_FEES: fee baked into cost basis stored in DB — Gap 2 fix
        Returns (shares_bought, entry_price, actual_cost).
        actual_cost should be stored as CopiedBet.size_usdc so that P&L uses
        the true cost basis (including fee) just like real mode does.
        """
        import math as _math

        # Gap 1: use live market price as entry, not the whale's historical price.
        # Real mode always fills at the current market price (which has moved since
        # the whale traded). Fall back to whale price only if live price unavailable.
        effective_price = max(live_price if live_price is not None else price, 0.001)

        # Gap 3: truncate shares to 2 dp, matching CLOB floor rounding in real mode.
        raw_shares = size_usdc / effective_price
        shares = _math.floor(raw_shares * 100) / 100

        # Gap 2: when fee simulation is on, bake the fee into the cost basis so
        # that pnl = proceeds - actual_cost is consistent with real mode, where
        # actual_cost = takingAmount (which already includes the fee).
        actual_cost = size_usdc
        if settings.SIM_APPLY_FEES and settings.SIM_ASSUMED_FEE_BPS > 0:
            fee_amount = size_usdc * (settings.SIM_ASSUMED_FEE_BPS / 10000)
            actual_cost = size_usdc + fee_amount
            logger.debug(
                "SIM BUY (fee-adjusted): $%.4f position + $%.4f fee (%dbps) = $%.4f cost basis",
                size_usdc,
                fee_amount,
                settings.SIM_ASSUMED_FEE_BPS,
                actual_cost,
            )

        session.current_balance_usdc = max(0.0, session.current_balance_usdc - actual_cost)
        db.add(session)

        logger.debug(
            "SIM BUY: $%.2f (cost $%.2f) @ %.4f live (whale %.4f) = %.4f shares | balance -> $%.2f",
            size_usdc,
            actual_cost,
            effective_price,
            price,
            shares,
            session.current_balance_usdc,
        )
        return round(shares, 6), round(effective_price, 6), round(actual_cost, 4)

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
        # Gap 4: apply sell-side taker fee when fee simulation is enabled.
        # Real mode FOK sell orders also carry fee_rate_bps, so proceeds are reduced.
        raw_proceeds = copied_bet.size_shares * current_price
        if settings.SIM_APPLY_FEES and settings.SIM_ASSUMED_FEE_BPS > 0:
            sell_fee = raw_proceeds * (settings.SIM_ASSUMED_FEE_BPS / 10000)
            proceeds = raw_proceeds - sell_fee
            logger.debug(
                "SIM SELL (fee-adjusted): gross $%.4f - fee $%.4f (%dbps) = $%.4f net",
                raw_proceeds,
                sell_fee,
                settings.SIM_ASSUMED_FEE_BPS,
                proceeds,
            )
        else:
            proceeds = raw_proceeds
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
            copied_bet.size_shares,
            current_price,
            proceeds,
            pnl,
            session.current_balance_usdc,
        )
        return round(pnl, 2)

    # ------------------------------------------------------------------
    # Real mode helpers
    # ------------------------------------------------------------------

    def place_real_buy(
        self, token_id: str, size_usdc: float, price: float, neg_risk: bool = False
    ) -> dict:
        """Place a real market buy via py-clob-client."""
        logger.info(
            "REAL BUY: token=%s size=$%.2f price=%.4f neg_risk=%s",
            token_id[:16],
            size_usdc,
            price,
            neg_risk,
        )
        return self._client.place_market_buy(token_id, size_usdc, neg_risk=neg_risk)

    def place_real_sell(
        self, token_id: str, size_shares: float, whale_price: float | None = None
    ) -> dict:
        """Place a real market sell via py-clob-client."""
        logger.info(
            "REAL SELL: token=%s shares=%.4f whale_price=%s",
            token_id[:16],
            size_shares,
            whale_price,
        )
        return self._client.place_market_sell(token_id, size_shares, whale_price=whale_price)

    def _sell_with_retry(
        self,
        copied_bet: CopiedBet,
        whale_price: float,
    ) -> tuple[float | None, float | None]:
        """
        Attempt a CLOB sell for `copied_bet`, retrying up to
        settings.SELL_CLOSE_RETRIES times on transient failures (FOK cancelled,
        zero-fill response, no_position size mismatch, or exception).

        Returns (fill_price, taking_amount_usdc) on a confirmed fill.
        Returns (copied_bet.price_at_entry, None) when the position is
        confirmed absent on-chain (neutral close — no actual sell needed).
        Returns (None, None) when all attempts are exhausted (leave OPEN).

        Side-effect: may update copied_bet.size_shares and price_at_entry
        when the on-chain balance differs from the DB record.
        """
        import time as _time

        max_attempts = settings.SELL_CLOSE_RETRIES + 1
        delay = settings.SELL_CLOSE_RETRY_DELAY_SECONDS

        for attempt in range(max_attempts):
            last = attempt + 1 >= max_attempts

            def _log_retry(reason: str, _attempt: int = attempt, _last: bool = last) -> None:
                if not _last:
                    logger.warning(
                        "Real sell bet %d: %s — retrying in %ds (attempt %d/%d)",
                        copied_bet.id,
                        reason,
                        delay,
                        _attempt + 1,
                        max_attempts,
                    )
                else:
                    logger.warning(
                        "Real sell bet %d: %s — all %d attempts exhausted, leaving OPEN",
                        copied_bet.id,
                        reason,
                        max_attempts,
                    )

            try:
                order_resp = self.place_real_sell(
                    copied_bet.token_id, copied_bet.size_shares, whale_price=whale_price
                )
                logger.info(
                    "REAL SELL response bet %d (attempt %d/%d): %s",
                    copied_bet.id,
                    attempt + 1,
                    max_attempts,
                    order_resp,
                )
                sell_status = (order_resp.get("status") or "").lower()

                # ---- FOK cancelled ----
                if sell_status in ("cancelled", "canceled"):
                    _log_retry("FOK cancelled")
                    if not last:
                        _time.sleep(delay)
                        continue
                    return None, None

                # ---- no_position: DB size may be off, fetch actual balance ----
                if sell_status == "no_position":
                    actual_shares = self._get_actual_position_size(copied_bet.token_id)
                    if not actual_shares or actual_shares <= 0:
                        # Genuinely absent — neutral close (no sell needed).
                        logger.warning(
                            "Real sell bet %d: no on-chain position confirmed (attempt %d/%d) — "
                            "closing neutral",
                            copied_bet.id,
                            attempt + 1,
                            max_attempts,
                        )
                        return copied_bet.price_at_entry, None
                    # Update DB size to match on-chain reality and retry.
                    logger.warning(
                        "Real sell bet %d: CLOB rejected %.4f shares but on-chain shows "
                        "%.4f — updating size and retrying (attempt %d/%d)",
                        copied_bet.id,
                        copied_bet.size_shares,
                        actual_shares,
                        attempt + 1,
                        max_attempts,
                    )
                    copied_bet.size_shares = actual_shares
                    copied_bet.price_at_entry = round(copied_bet.size_usdc / actual_shares, 4)
                    if not last:
                        _time.sleep(delay)
                    continue

                # ---- apparent success: validate fill is non-zero ----
                raw_price = (
                    order_resp.get("price")
                    or order_resp.get("avgPrice")
                    or order_resp.get("average_price")
                )
                fill_price = float(raw_price) if raw_price else whale_price
                raw_taking = order_resp.get("takingAmount")
                taking_amount_usdc = float(raw_taking) if raw_taking else None

                if taking_amount_usdc is not None and taking_amount_usdc == 0.0:
                    _log_retry(f"takingAmount=0 (status={order_resp.get('status')})")
                    if not last:
                        _time.sleep(delay)
                        continue
                    return None, None

                raw_matched = order_resp.get("size_matched") or order_resp.get("sizeMatched")
                if raw_matched is not None and float(raw_matched) == 0.0:
                    _log_retry(f"size_matched=0 (status={order_resp.get('status')})")
                    if not last:
                        _time.sleep(delay)
                        continue
                    return None, None

                if taking_amount_usdc is None and not raw_price:
                    logger.warning(
                        "Real sell bet %d: no fill confirmation in response "
                        "(status=%s, resp=%s) — accepting with estimated price %.4f",
                        copied_bet.id,
                        order_resp.get("status"),
                        order_resp,
                        fill_price,
                    )

                return fill_price, taking_amount_usdc

            except Exception as exc:
                _log_retry(f"exception: {exc}")
                if not last:
                    _time.sleep(delay)
                    continue
                return None, None

        return None, None

    def _close_bet(
        self,
        copied_bet: CopiedBet,
        current_price: float,
        session: MonitoringSession,
        db: DBSession,
        close_reason: str = "",
    ) -> float:
        """
        Close a position via the appropriate path for its mode.
        - SIMULATION / HEDGE_SIM: calls simulate_sell (virtual balance update).
        - REAL: executes a live CLOB sell, then updates records with actual fill.
        Returns pnl_usdc (0.0 on REAL sell failure — position is NOT marked closed).
        """
        if copied_bet.mode in ("SIMULATION", "HEDGE_SIM"):
            return self.simulate_sell(copied_bet, current_price, session, db, close_reason)

        # REAL mode — resolved markets (price ≥ 0.98 or ≤ 0.02) have no CLOB
        # order book; attempting a sell returns "not enough balance/allowance".
        # Close the DB record at the oracle price and let auto-redemption recover
        # the USDC on-chain (runs every REDEMPTION_CHECK_INTERVAL_SECONDS).
        # NOTE: keep this threshold tight (0.98/0.02) — a whale can legitimately
        # exit an open market at 0.95-0.97, and we must still place the CLOB sell.
        fill_price = current_price
        taking_amount_usdc: float | None = None  # actual USDC received (from takingAmount)
        if current_price >= 0.98 or current_price <= 0.02:
            logger.info(
                "REAL close bet %d at oracle price %.4f (resolved — skipping CLOB sell, redemption will settle)",
                copied_bet.id,
                current_price,
            )
        else:
            # Short-circuit: if size_shares==0 the buy was never filled — skip the API call
            if copied_bet.size_shares == 0.0:
                logger.warning(
                    "Real sell bet %d: size_shares=0 — buy was never filled, closing as neutral (full refund)",
                    copied_bet.id,
                )
                session.current_balance_usdc += copied_bet.size_usdc
                session.total_pnl_usdc = round((session.total_pnl_usdc or 0.0), 2)
                copied_bet.status = "CLOSED_NEUTRAL"
                copied_bet.pnl_usdc = 0.0
                copied_bet.resolution_price = copied_bet.price_at_entry
                copied_bet.closed_at = datetime.utcnow()
                if close_reason:
                    copied_bet.close_reason = close_reason
                db.add(copied_bet)
                db.add(session)
                logger.info(
                    "REAL unfilled-buy neutral close: bet %d refunded $%.2f | balance -> $%.2f",
                    copied_bet.id,
                    copied_bet.size_usdc,
                    session.current_balance_usdc,
                )
                return 0.0
            sell_result = self._sell_with_retry(copied_bet, current_price)
            if sell_result == (None, None):
                return 0.0
            fill_price, taking_amount_usdc = sell_result

        # Use actual USDC received (takingAmount from CLOB) when available.
        # Falls back to shares x price for oracle-priced closes (resolved markets
        # where we skip the CLOB sell and rely on on-chain redemption).
        if taking_amount_usdc is not None:
            proceeds = taking_amount_usdc
        else:
            proceeds = copied_bet.size_shares * fill_price
        pnl = proceeds - copied_bet.size_usdc

        session.current_balance_usdc = max(0.0, session.current_balance_usdc + proceeds)
        session.total_pnl_usdc = round((session.total_pnl_usdc or 0.0) + pnl, 2)

        if pnl > 0.01:
            copied_bet.status = "CLOSED_WIN"
            session.total_wins += 1
        elif pnl < -0.01:
            copied_bet.status = "CLOSED_LOSS"
            session.total_losses += 1
        else:
            copied_bet.status = "CLOSED_NEUTRAL"

        copied_bet.pnl_usdc = round(pnl, 2)
        copied_bet.resolution_price = fill_price
        copied_bet.closed_at = datetime.utcnow()

        # For REAL mode: update "we sold @ X" in the close_reason to show our
        # actual CLOB fill price, not the whale's exit price that was baked in
        # by _handle_exit before the sell was attempted.
        if close_reason and copied_bet.mode == "REAL":
            import re as _re

            actual_fill = (
                round(taking_amount_usdc / copied_bet.size_shares, 4)
                if (taking_amount_usdc and copied_bet.size_shares)
                else fill_price
            )
            close_reason = _re.sub(
                r", we sold @ [\d.]+",
                f", we sold @ {actual_fill:.3f}",
                close_reason,
            )

        if close_reason:
            copied_bet.close_reason = close_reason

        db.add(copied_bet)
        db.add(session)

        logger.info(
            "REAL SELL (resolution): bet %d %.4f shares @ %.4f = $%.2f pnl=$%.2f | balance -> $%.2f",
            copied_bet.id,
            copied_bet.size_shares,
            fill_price,
            proceeds,
            pnl,
            session.current_balance_usdc,
        )
        return round(pnl, 2)

    # ------------------------------------------------------------------
    # Multi-tranche close (single CLOB sell for combined balance)
    # ------------------------------------------------------------------

    def _close_all_tranches(
        self,
        open_positions: list,
        exit_price: float,
        session: MonitoringSession,
        db: DBSession,
        exit_reason: str = "",
    ) -> bool:
        """Close multiple open positions for the same token atomically.

        SIMULATION/HEDGE_SIM: each position is closed independently (virtual).
        REAL, single position: delegates to _close_bet (normal path).
        REAL, multiple positions: places ONE CLOB sell for the combined balance
        so that the first sell cannot consume tokens belonging to a later tranche,
        which would cause the second sell to return no_position → NEUTRAL.

        Returns True if positions were successfully closed (or skipped — e.g.,
        unfilled buys, resolved markets, simulation). Returns False if the CLOB
        sell was attempted but failed (FOK cancelled, no_position, or exception)
        so the caller can choose NOT to advance _last_seen and retry next poll.
        """
        if not open_positions:
            return True

        mode = open_positions[0].mode

        if mode in ("SIMULATION", "HEDGE_SIM"):
            for pos in open_positions:
                self.simulate_sell(pos, exit_price, session, db, exit_reason)
            return True

        if len(open_positions) == 1:
            self._close_bet(open_positions[0], exit_price, session, db, exit_reason)
            # _close_bet leaves position OPEN on FOK cancel; success sets CLOSED_*
            return open_positions[0].status != "OPEN"

        # --- REAL mode: multiple tranches, single sell ---
        import math as _math

        token_id = open_positions[0].token_id

        logger.info(
            "_close_all_tranches: REAL closing %d tranches for %s @ %.4f",
            len(open_positions),
            token_id[:16],
            exit_price,
        )
        for pos in open_positions:
            logger.info(
                "  tranche bet %d: entry=%.4f size_usdc=%.2f size_shares=%.4f",
                pos.id,
                pos.price_at_entry,
                pos.size_usdc,
                pos.size_shares,
            )

        # Separate unfilled buys (size_shares==0) — close as NEUTRAL with refund
        filled, unfilled = [], []
        for pos in open_positions:
            (unfilled if pos.size_shares == 0 else filled).append(pos)

        for pos in unfilled:
            session.current_balance_usdc += pos.size_usdc
            pos.status = "CLOSED_NEUTRAL"
            pos.pnl_usdc = 0.0
            pos.resolution_price = pos.price_at_entry
            pos.closed_at = datetime.utcnow()
            if exit_reason:
                pos.close_reason = exit_reason
            db.add(pos)
            logger.warning(
                "_close_all_tranches: bet %d refunded $%.2f (buy was never filled)",
                pos.id,
                pos.size_usdc,
            )
        db.add(session)

        if not filled:
            return True

        # For resolved markets: no CLOB sell — let redemption handle USDC
        fill_price = exit_price
        db_total_shares = sum(p.size_shares for p in filled)
        sell_shares = db_total_shares  # default

        actual = None  # on-chain balance; used later to compute fill-scale
        if exit_price < 0.98 and exit_price > 0.02:
            # Query actual on-chain balance BEFORE attempting the sell.
            # This avoids selling more than one tranche's worth on a retry.
            actual = self._get_actual_position_size(token_id)
            logger.info(
                "_close_all_tranches: Data API actual balance = %s (DB total = %.4f)",
                f"{actual:.4f}" if actual is not None else "ERROR",
                db_total_shares,
            )

            if actual is None:
                # API error — fall back to DB total (floored)
                sell_shares = _math.floor(db_total_shares * 100) / 100
                logger.warning(
                    "_close_all_tranches: Data API error — using DB total %.4f (floored: %.4f)",
                    db_total_shares,
                    sell_shares,
                )
            elif actual == 0.0:
                # Confirmed no position on chain — close all as NEUTRAL
                logger.warning(
                    "_close_all_tranches: Data API confirms 0 shares for %s — closing all as neutral",
                    token_id[:16],
                )
                for pos in filled:
                    pos.status = "CLOSED_NEUTRAL"
                    pos.pnl_usdc = 0.0
                    pos.resolution_price = pos.price_at_entry
                    pos.closed_at = datetime.utcnow()
                    if exit_reason:
                        pos.close_reason = exit_reason
                    db.add(pos)
                db.add(session)
                return True
            else:
                sell_shares = _math.floor(actual * 100) / 100

            # Single CLOB sell for total balance — retry on transient failures
            import time as _time

            _max_attempts = settings.SELL_CLOSE_RETRIES + 1
            _delay = settings.SELL_CLOSE_RETRY_DELAY_SECONDS
            _sell_ok = False
            for _attempt in range(_max_attempts):
                _last = _attempt + 1 >= _max_attempts
                try:
                    order_resp = self.place_real_sell(token_id, sell_shares, whale_price=exit_price)
                    logger.info(
                        "_close_all_tranches SELL response (attempt %d/%d): %s",
                        _attempt + 1,
                        _max_attempts,
                        order_resp,
                    )
                    _sell_status = (order_resp.get("status") or "").lower()
                    if _sell_status in ("cancelled", "canceled"):
                        if not _last:
                            logger.warning(
                                "_close_all_tranches: FOK cancelled for %s — retrying in %ds "
                                "(attempt %d/%d)",
                                token_id[:16],
                                _delay,
                                _attempt + 1,
                                _max_attempts,
                            )
                            _time.sleep(_delay)
                            continue
                        logger.warning(
                            "_close_all_tranches: FOK cancelled for %s — all %d attempts "
                            "exhausted, leaving OPEN",
                            token_id[:16],
                            _max_attempts,
                        )
                        return False
                    if order_resp.get("status") == "no_position":
                        if not _last:
                            logger.warning(
                                "_close_all_tranches: no_position for %s — retrying in %ds "
                                "(attempt %d/%d)",
                                token_id[:16],
                                _delay,
                                _attempt + 1,
                                _max_attempts,
                            )
                            _time.sleep(_delay)
                            continue
                        logger.error(
                            "_close_all_tranches: CLOB no_position selling %.4f shares of %s "
                            "— all %d attempts exhausted, leaving OPEN",
                            sell_shares,
                            token_id[:16],
                            _max_attempts,
                        )
                        return False
                    raw = (
                        order_resp.get("price")
                        or order_resp.get("avgPrice")
                        or order_resp.get("average_price")
                    )
                    if raw:
                        fill_price = float(raw)
                    _sell_ok = True
                    break
                except Exception as exc:
                    if not _last:
                        logger.warning(
                            "_close_all_tranches: sell error for %s: %s — retrying in %ds "
                            "(attempt %d/%d)",
                            token_id[:16],
                            exc,
                            _delay,
                            _attempt + 1,
                            _max_attempts,
                        )
                        _time.sleep(_delay)
                        continue
                    logger.error(
                        "_close_all_tranches: sell failed for %s after %d attempts: %s — "
                        "positions NOT closed",
                        token_id[:16],
                        _max_attempts,
                        exc,
                    )
                    return False
            if not _sell_ok:
                return False
        else:
            logger.info(
                "_close_all_tranches: resolved price %.4f — skipping CLOB sell, redemption will settle",
                exit_price,
            )

        # Distribute proceeds proportionally by invested USDC.
        #
        # When actual on-chain balance < DB total (e.g. one buy was partially/never
        # filled but still recorded with size_shares > 0), we must NOT use the full
        # size_usdc as each position's cost basis — that overstates the cost and turns
        # profitable exits into apparent losses.
        #
        # Fix: compute fill_scale = actual / db_total.  Each position's effective cost
        # is size_usdc * fill_scale; the remainder (size_usdc * (1-fill_scale)) is an
        # unfilled-buy refund returned to the session balance.  This ensures a position
        # entered at 0.870 and exited at 0.880 never shows a loss.
        total_invested = sum(p.size_usdc for p in filled)
        total_proceeds = sell_shares * fill_price

        # fill_scale: fraction of DB-recorded shares that actually existed on-chain.
        # 1.0 when actual is unknown (API error) or for resolved-price path.
        if actual is not None and db_total_shares > 0:
            fill_scale = min(actual / db_total_shares, 1.0)
        else:
            fill_scale = 1.0

        if fill_scale < 0.99:
            logger.warning(
                "_close_all_tranches: fill_scale=%.3f (actual=%.4f DB=%.4f) — "
                "unfilled portions will be refunded",
                fill_scale,
                actual if actual is not None else 0.0,
                db_total_shares,
            )

        for pos in filled:
            weight = (pos.size_usdc / total_invested) if total_invested > 0 else (1.0 / len(filled))
            pos_proceeds = total_proceeds * weight  # from the CLOB sell
            refund = pos.size_usdc * (1.0 - fill_scale)  # unfilled portion back to balance
            effective_cost = pos.size_usdc * fill_scale  # what we actually paid for real tokens
            pos_pnl = round(pos_proceeds - effective_cost, 2)

            session.current_balance_usdc = max(
                0.0, session.current_balance_usdc + pos_proceeds + refund
            )
            session.total_pnl_usdc = round((session.total_pnl_usdc or 0.0) + pos_pnl, 2)

            if pos_pnl > 0.01:
                pos.status = "CLOSED_WIN"
                session.total_wins += 1
            elif pos_pnl < -0.01:
                pos.status = "CLOSED_LOSS"
                session.total_losses += 1
            else:
                pos.status = "CLOSED_NEUTRAL"

            pos.pnl_usdc = pos_pnl
            pos.resolution_price = fill_price
            pos.closed_at = datetime.utcnow()
            if exit_reason:
                pos.close_reason = exit_reason
            db.add(pos)
            db.add(session)

            logger.info(
                "_close_all_tranches: bet %d → %s pnl=$%.2f (%.0f%% weight, scale=%.3f, refund=$%.2f) | balance -> $%.2f",
                pos.id,
                pos.status,
                pos_pnl,
                weight * 100,
                fill_scale,
                refund,
                session.current_balance_usdc,
            )

        return True

    # ------------------------------------------------------------------
    # Resolution checker
    # ------------------------------------------------------------------

    def _resolve_archived_bet_price(self, bet: "CopiedBet") -> float:
        """
        Determine a fallback close price for a bet whose market has been
        archived (Gamma API returns no data for its token).

        REAL mode: query the on-chain CTF balance.
          - balance > 0  → winning token still held → 1.0 (redemption will settle)
          - balance = 0  → lost or already redeemed → entry price (neutral P&L)
        SIM / HEDGE_SIM: outcome is unknowable; close at entry price (0 P&L).
        """
        if bet.mode == "REAL" and self._client is not None:
            try:
                # Use Data API /positions — get_balance_allowance(CONDITIONAL)
                # always returns 0 for proxy wallets so is unreliable here.
                import requests as _requests

                resp = _requests.get(
                    f"{settings.DATA_API_BASE}/positions",
                    params={"user": settings.POLY_FUNDER_ADDRESS},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                positions = (
                    data if isinstance(data, list) else data.get("data", data.get("positions", []))
                )
                pos = next((p for p in positions if str(p.get("asset", "")) == bet.token_id), None)
                if pos is not None:
                    size = float(pos.get("size", 0))
                    if size > 0:
                        logger.info(
                            "Archive resolve: bet %d token %s Data API size=%.4f "
                            "→ closing at 1.0 (redemption will settle)",
                            bet.id,
                            bet.token_id[:16],
                            size,
                        )
                        return 1.0
                logger.info(
                    "Archive resolve: bet %d token %s Data API size=0 or not found "
                    "→ closing at entry price %.4f (lost or already redeemed)",
                    bet.id,
                    bet.token_id[:16],
                    bet.price_at_entry,
                )
            except Exception as exc:
                logger.warning(
                    "Archive resolve: Data API position check failed for bet %d: %s "
                    "— using entry price",
                    bet.id,
                    exc,
                )
        return bet.price_at_entry

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
            resolution_data = asyncio.run(
                self._fetch_resolution_data([b.token_id for b in open_bets])
            )

            # Cache sessions by mode so we only query once per mode
            session_cache: dict[str, MonitoringSession | None] = {}

            now = datetime.now(UTC)
            min_hours = settings.MIN_MARKET_HOURS_TO_CLOSE

            _LIVE_SPORTS = {
                "Soccer",
                "Basketball",
                "Tennis",
                "eSports",
                "American Football",
                "Baseball",
                "Hockey",
            }

            # Whales whose bets should never be exited early via near-expiry
            # price thresholds — they hold positions to full oracle resolution.
            # Arb-mode whales are included automatically: they bet at extreme prices
            # for oracle settlement, not for a mid-market cashout.
            _NO_NEAR_EXPIRY_ALIASES = {"Crpyto", "Crpyto(lowball)"}
            _no_near_expiry_addresses: set[str] = {
                w.address
                for w in db.query(Whale)
                .filter(
                    Whale.alias.in_(_NO_NEAR_EXPIRY_ALIASES) | (Whale.arb_mode == True)  # noqa: E712
                )
                .all()
            }

            for bet, (price, end_dt, is_resolved) in zip(open_bets, resolution_data, strict=False):
                if price is None:
                    # Archive-timeout fallback: Polymarket removes resolved markets
                    # from the Gamma API index within a few hours of settlement.
                    # If Gamma has no data AND the market closed >2h ago, the market
                    # is archived and will never return price data.  Force-close to
                    # avoid positions being stuck OPEN indefinitely.
                    if bet.market_close_at is not None:
                        mc = (
                            bet.market_close_at.replace(tzinfo=UTC)
                            if bet.market_close_at.tzinfo is None
                            else bet.market_close_at
                        )
                        hours_since_close = (now - mc).total_seconds() / 3600
                        ARCHIVE_TIMEOUT_H = 2.0
                        if hours_since_close > ARCHIVE_TIMEOUT_H:
                            # Use the session that owns this bet (session_id FK).
                            # Fall back to latest-by-mode only for legacy bets without session_id.
                            if bet.session_id:
                                if bet.session_id not in session_cache:
                                    session_cache[bet.session_id] = db.get(
                                        MonitoringSession, bet.session_id
                                    )
                                session_for_archive = session_cache.get(bet.session_id)
                            else:
                                _archive_mode_key = f"archive_mode:{bet.mode}"
                                if _archive_mode_key not in session_cache:
                                    session_cache[_archive_mode_key] = (
                                        db.query(MonitoringSession)
                                        .filter_by(mode=bet.mode)
                                        .order_by(MonitoringSession.id.desc())
                                        .first()
                                    )
                                session_for_archive = session_cache.get(_archive_mode_key)
                            if session_for_archive:
                                fallback_price = self._resolve_archived_bet_price(bet)
                                logger.warning(
                                    "Archive timeout: bet %d no Gamma data %.1fh after "
                                    "market close — force-closing at %.4f",
                                    bet.id,
                                    hours_since_close,
                                    fallback_price,
                                )
                                self._close_bet(
                                    copied_bet=bet,
                                    current_price=fallback_price,
                                    session=session_for_archive,
                                    db=db,
                                    close_reason=f"Archive timeout ({hours_since_close:.1f}h since close)",
                                )
                    continue

                # Must be computed before any case that references it
                is_live_sport = bet.market_category in _LIVE_SPORTS

                # Resolve the session that OWNS this bet via session_id.
                # Fall back to latest-by-mode only for legacy bets with no session_id.
                if bet.session_id:
                    if bet.session_id not in session_cache:
                        session_cache[bet.session_id] = db.get(MonitoringSession, bet.session_id)
                    session = session_cache.get(bet.session_id)
                else:
                    fallback_key = f"mode:{bet.mode}"
                    if fallback_key not in session_cache:
                        session_cache[fallback_key] = (
                            db.query(MonitoringSession)
                            .filter_by(mode=bet.mode)
                            .order_by(MonitoringSession.id.desc())
                            .first()
                        )
                    session = session_cache.get(fallback_key)
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
                hours_remaining: float | None = None
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
                #  - 0.05 < price < 0.95         → mid-range; oracle pending; wait indefinitely for true signal
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
                            bet.id,
                            hours_since_close,
                            price,
                        )
                        self._close_bet(
                            copied_bet=bet,
                            current_price=price,
                            session=session,
                            db=db,
                            close_reason="Market ended",
                        )
                    elif price <= 0.05 and is_resolved:
                        if is_live_sport and hours_since_close < MIN_HOURS_AFTER_CLOSE:
                            logger.warning(
                                "Skipping premature NO resolution for live sport bet %d "
                                "(%.1fh after market close < %.1fh minimum) — "
                                "likely market cancellation, not game outcome",
                                bet.id,
                                hours_since_close,
                                MIN_HOURS_AFTER_CLOSE,
                            )
                        else:
                            logger.info(
                                "Force-close: bet %d market ended %.1fh ago, price=%.4f (NO resolved, oracle confirmed)",
                                bet.id,
                                hours_since_close,
                                price,
                            )
                            self._close_bet(
                                copied_bet=bet,
                                current_price=price,
                                session=session,
                                db=db,
                                close_reason="Market ended",
                            )
                    else:
                        # Oracle timeout: if price is stuck ≤0.05 for a long time without
                        # a formal is_resolved flag, accept it as a loss.
                        # Mid-range prices (0.05 < price < 0.95) are never force-closed on a
                        # timer — we only exit when we receive a true resolution signal
                        # (price ≥ 0.95 or price ≤ 0.05 + is_resolved).
                        # Live sports get a longer window because UMA settlement can take
                        # 12-24h after game end.
                        ORACLE_TIMEOUT_H = 8.0 if is_live_sport else 4.0
                        if price <= 0.05 and hours_since_close > ORACLE_TIMEOUT_H:
                            logger.info(
                                "Oracle timeout: bet %d price=%.4f ≤0.05 for %.1fh after "
                                "market close — force-closing as loss",
                                bet.id,
                                price,
                                hours_since_close,
                            )
                            self._close_bet(
                                copied_bet=bet,
                                current_price=price,
                                session=session,
                                db=db,
                                close_reason=f"Oracle timeout ({hours_since_close:.1f}h since close)",
                            )
                        elif 0.05 < price < 0.95 and bet.mode == "REAL" and not is_live_sport:
                            # REAL mode non-sport: market ended but oracle hasn't settled yet.
                            # CLOB order books remain active post-expiry while settlement is pending.
                            # Attempt to sell now rather than waiting ORACLE_STALE_H hours.
                            # _close_bet is safe here — if CLOB rejects it raises and bet stays OPEN.
                            logger.info(
                                "REAL market-ended: bet %d price=%.4f mid-range %.1fh after close "
                                "— attempting CLOB sell now (oracle pending)",
                                bet.id,
                                price,
                                hours_since_close,
                            )
                            self._close_bet(
                                copied_bet=bet,
                                current_price=price,
                                session=session,
                                db=db,
                                close_reason=f"Market ended, oracle pending ({hours_since_close:.1f}h since close)",
                            )
                        else:
                            logger.debug(
                                "Market ended %.1fh ago but not yet oracle-resolved "
                                "(price=%.4f, resolved=%s) — waiting for on-chain settlement (bet %d)",
                                hours_since_close,
                                price,
                                is_resolved,
                                bet.id,
                            )
                    continue

                # --- Case 2: Near-expiry window ---
                if hours_remaining is not None and 0 <= hours_remaining < min_hours:
                    logger.debug(
                        "Near-expiry monitoring: bet %d closes in %.2fh, price=%.4f",
                        bet.id,
                        hours_remaining,
                        price,
                    )
                    skip_near_expiry = bet.whale_address in _no_near_expiry_addresses
                    if skip_near_expiry:
                        logger.debug(
                            "Near-expiry: skipping price-based close for whale-excluded bet %d "
                            "(%.2fh left, price=%.4f) — waiting for oracle settlement",
                            bet.id,
                            hours_remaining,
                            price,
                        )
                    elif not is_live_sport and (price >= 0.80 or price <= 0.20):
                        logger.info(
                            "Near-expiry close: bet %d price=%.4f crosses 0.80/0.20 "
                            "with %.2fh remaining",
                            bet.id,
                            price,
                            hours_remaining,
                        )
                        self._close_bet(
                            copied_bet=bet,
                            current_price=price,
                            session=session,
                            db=db,
                            close_reason=f"Near-expiry exit ({hours_remaining:.1f}h left, price {price:.3f})",
                        )
                    elif is_live_sport:
                        logger.debug(
                            "Near-expiry: skipping price-based close for live sport bet %d "
                            "(%s, %.2fh left, price=%.4f) — waiting for on-chain settlement",
                            bet.id,
                            bet.market_category,
                            hours_remaining,
                            price,
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
                #
                # Skip the threshold close when we ENTERED at an already-extreme price
                # (>= 0.90 or <= 0.10) and the market hasn't confirmed resolution yet.
                # These positions are intentional high-conviction entries; the whale will
                # hold to resolution at 1.0 / 0.0 — close only on confirmed resolution
                # or a whale exit signal, not just because price is still in that zone.
                already_extreme_entry = (bet.price_at_entry >= 0.90 and price >= 0.95) or (
                    bet.price_at_entry <= 0.10 and price <= 0.05
                )
                if already_extreme_entry and not is_resolved and not market_ended:
                    continue
                if (not is_live_sport or is_resolved) and (price >= 0.95 or price <= 0.05):
                    self._close_bet(
                        copied_bet=bet,
                        current_price=price,
                        session=session,
                        db=db,
                        close_reason=f"Price reached resolution threshold ({price:.3f})",
                    )
                    logger.info(
                        "Resolved bet %d at price %.4f status=%s",
                        bet.id,
                        price,
                        bet.status,
                    )

            db.commit()

        except Exception as exc:
            logger.error("check_resolution error: %s", exc)
            db.rollback()
        finally:
            db.close()

    def check_orphan_positions(self):
        """
        Close OPEN REAL positions where the whale has already exited on-chain.

        When a CLOB sell is cancelled (FOK) or otherwise fails, the exit
        poll's last_seen timestamp is still advanced — so the exit signal
        is consumed and never re-fires.  This job is the safety net: it
        queries each whale's current on-chain positions via the Data API
        and closes our position in two cases:

        1. Whale no longer holds the token at all.
        2. Whale holds the token BUT re-bought after we opened our position
           (detected by the presence of a WhaleBet EXIT record for this token
           with timestamp > bet.opened_at in our own DB).

        Case 2 handles the scenario where the whale exited and immediately
        re-entered — their current position appears active, but the exit we
        needed to copy already fired and failed.

        Runs on the same interval as check_resolution.
        Only acts on REAL-mode bets (simulation needs no on-chain check).
        """
        import requests as _requests

        if not settings.credentials_valid():
            return

        db = SessionLocal()
        try:
            open_real_bets = (
                db.query(CopiedBet)
                .filter(CopiedBet.status == "OPEN", CopiedBet.mode == "REAL")
                .all()
            )
            if not open_real_bets:
                return

            # Group bets by whale address so we fetch each whale's positions once
            by_whale: dict[str, list] = {}
            for bet in open_real_bets:
                if bet.whale_address:
                    by_whale.setdefault(bet.whale_address, []).append(bet)

            for whale_address, bets in by_whale.items():
                # Look up the Whale row once — needed for WhaleBet JOIN
                whale_obj = db.query(Whale).filter_by(address=whale_address).first()

                try:
                    resp = _requests.get(
                        f"{settings.DATA_API_BASE}/positions",
                        params={"user": whale_address},
                        timeout=10,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    whale_positions = (
                        data
                        if isinstance(data, list)
                        else data.get("data", data.get("positions", []))
                    )
                except Exception as exc:
                    logger.warning(
                        "check_orphan_positions: failed to fetch positions for whale %s: %s",
                        whale_address[:10],
                        exc,
                    )
                    continue

                # Tokens the whale currently holds (size > 0)
                whale_held = {
                    str(p.get("asset", "")).strip()
                    for p in whale_positions
                    if float(p.get("size", 0) or 0) > 0
                }

                for bet in bets:
                    needs_close = False

                    # Skip positions opened very recently — on-chain state takes
                    # several seconds to propagate to the Data API and CLOB indexer.
                    # A confirmed buy that fires the orphan check within ~3 minutes
                    # will see 0 shares and ghost-close a valid position as NEUTRAL.
                    _age_seconds = (
                        (datetime.utcnow() - bet.opened_at).total_seconds()
                        if bet.opened_at
                        else 999
                    )
                    if _age_seconds < 60:
                        logger.debug(
                            "check_orphan_positions: skipping bet %d — only %.0fs old "
                            "(waiting for on-chain propagation)",
                            bet.id,
                            _age_seconds,
                        )
                        continue

                    if bet.token_id not in whale_held:
                        # Case 1: whale no longer holds the token at all
                        needs_close = True
                    elif whale_obj is not None:
                        # Case 2: whale holds the token — check for a recorded EXIT
                        # trade in our DB that occurred after we opened this position.
                        # If one exists, they exited (and we failed to copy it) then
                        # re-bought, so our original position is still orphaned.
                        exit_after_open = (
                            db.query(WhaleBet)
                            .filter(
                                WhaleBet.whale_id == whale_obj.id,
                                WhaleBet.token_id == bet.token_id,
                                WhaleBet.bet_type == "EXIT",
                                WhaleBet.timestamp > bet.opened_at,
                            )
                            .first()
                        )
                        if exit_after_open:
                            logger.info(
                                "check_orphan_positions: bet %d — whale re-bought token %s "
                                "(EXIT recorded at %s, bet opened at %s) — treating as orphan",
                                bet.id,
                                bet.token_id[:16],
                                exit_after_open.timestamp,
                                bet.opened_at,
                            )
                            needs_close = True

                    if not needs_close:
                        continue

                    # Verify we still hold shares before attempting a sell
                    our_shares = self._get_actual_position_size(bet.token_id)
                    if our_shares is None:
                        continue  # Data API error — skip, try next cycle

                    if bet.session_id:
                        session = db.get(MonitoringSession, bet.session_id)
                    else:
                        session = (
                            db.query(MonitoringSession)
                            .filter_by(mode=bet.mode)
                            .order_by(MonitoringSession.id.desc())
                            .first()
                        )
                    if not session:
                        continue

                    # Fetch live price for both branches — used as the whale_price
                    # hint to place_real_sell so the FOK executes near current BBO.
                    current_price = bet.price_at_entry
                    try:
                        if self._client is not None:
                            clob = self._client._get_clob_client()
                            pr = clob.get_price(bet.token_id, "BUY")
                            if isinstance(pr, dict) and pr.get("price"):
                                current_price = float(pr["price"])
                    except Exception:
                        pass

                    if our_shares == 0.0:
                        # Data API returned 0 — could be lag or a glitch; don't
                        # ghost-close without verifying via the CLOB.  Call
                        # _close_bet() with the DB-recorded size_shares: it will
                        # attempt a real sell and let the CLOB be the arbiter.
                        #
                        # Outcomes handled by _close_bet():
                        #   size_shares == 0 in DB     → CLOSED_NEUTRAL (unfilled buy)
                        #   CLOB "no_position"         → CLOSED_NEUTRAL (confirmed absent)
                        #   FOK cancelled              → leaves OPEN, retried next cycle
                        #   Sell succeeds              → CLOSED_WIN/LOSS with real P&L
                        logger.info(
                            "check_orphan_positions: bet %d — Data API shows 0 shares, "
                            "attempting CLOB sell (size_shares=%.4f) before closing neutral",
                            bet.id,
                            bet.size_shares,
                        )
                        self._close_bet(
                            copied_bet=bet,
                            current_price=current_price,
                            session=session,
                            db=db,
                            close_reason="Orphan: whale exited, no shares on-chain",
                        )
                        db.commit()
                    else:
                        # We hold shares the whale no longer holds — sell at market
                        logger.info(
                            "check_orphan_positions: bet %d — whale exited token %s, "
                            "we hold %.4f shares — closing at market",
                            bet.id,
                            bet.token_id[:16],
                            our_shares,
                        )

                        bet.size_shares = our_shares
                        self._close_bet(
                            copied_bet=bet,
                            current_price=current_price,
                            session=session,
                            db=db,
                            close_reason="Orphan: whale exited, delayed sell",
                        )
                        db.commit()

        except Exception as exc:
            logger.error("check_orphan_positions error: %s", exc)
            db.rollback()
        finally:
            db.close()

    async def _fetch_resolution_data(
        self, token_ids: list
    ) -> list[tuple[float | None, datetime | None, bool]]:
        """Fetch price, endDate and resolved flag for multiple tokens.

        Requests are throttled to at most MAX_CONCURRENT concurrent connections
        to avoid rate-limiting on the Gamma API.  Each failed request is retried
        once after a short back-off before giving up and returning (None, None, False).
        """
        MAX_CONCURRENT = 2  # keep VPN pressure low — was 4, caused ReadTimeouts
        RETRY_DELAY = 2.0  # seconds

        sem = asyncio.Semaphore(MAX_CONCURRENT)

        # Use the same proxy as the main client so all traffic goes through the
        # configured VPN proxy (not raw socket through gluetun network interface).
        proxy = settings.PROXY_URL or None

        async def _fetch_with_limit(tid: str, http: httpx.AsyncClient):
            async with sem:
                result = await self._get_resolution_data_async(tid, http)
                if result[0] is None:
                    # Single retry after a short back-off
                    await asyncio.sleep(RETRY_DELAY)
                    result = await self._get_resolution_data_async(tid, http)
                return result

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
            proxy=proxy,
        ) as http:
            return await asyncio.gather(*[_fetch_with_limit(tid, http) for tid in token_ids])

    async def _get_resolution_data_async(
        self, token_id: str, http: httpx.AsyncClient
    ) -> tuple[float | None, datetime | None, bool]:
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
                price: float | None = None
                idx = tokens.index(token_id)
                if outcomes and idx < len(outcomes):
                    with contextlib.suppress(TypeError, ValueError):
                        price = float(outcomes[idx])

                # Parse endDate (Polymarket trading cutoff) and gameStartTime.
                # For sports markets, endDate is often the betting cutoff set to
                # midnight before the game, while gameStartTime is the actual tip-off /
                # kick-off. We prefer gameStartTime when it is later than endDate so
                # that market_close_at (and thus the ledger countdown) reflects the
                # real event time rather than an arbitrary trading cutoff.
                end_dt: datetime | None = None
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
                            dt = dt.replace(tzinfo=UTC)
                        end_dt = dt
                    except (ValueError, TypeError):
                        pass

                # Override with gameStartTime when available and later than endDate
                game_start_str = market.get("gameStartTime")
                if game_start_str:
                    try:
                        gst = datetime.fromisoformat(str(game_start_str).rstrip("Z"))
                        if gst.tzinfo is None:
                            gst = gst.replace(tzinfo=UTC)
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
                    with contextlib.suppress(TypeError, ValueError, IndexError):
                        other_price_val = float(outcomes[1 - idx])

                # Primary signal: EITHER token ≥ 0.99 (oracle set price to "1").
                # Must check both: when our token WINS (price=1.0, other=0.0)
                # the old check (other >= 0.99) would return False and leave the
                # bet stuck open forever.
                price_val: float = 0.0
                if outcomes and idx < len(outcomes):
                    with contextlib.suppress(TypeError, ValueError):
                        price_val = float(outcomes[idx])
                is_resolved = other_price_val >= 0.999 or price_val >= 0.999

                # When the oracle has settled our token at 1.0 (we won) but the
                # CLOB order book is gone (price returns 0.0 due to no liquidity),
                # override the CLOB price with the oracle price so the close logic
                # correctly registers a win rather than a loss.
                if price_val >= 0.99 and price < 0.95:
                    logger.info(
                        "_get_resolution_data_async: oracle price_val=%.4f overrides "
                        "CLOB price=%.4f for winning token %s",
                        price_val,
                        price,
                        token_id[:16],
                    )
                    price = price_val

                # Secondary signal: umaResolutionStatus="resolved" means the
                # oracle has finalised on-chain even if outcomePrices hasn't
                # updated yet (e.g. still shows ["0","0"] due to API lag).
                uma_status = (market.get("umaResolutionStatus") or "").lower()
                if uma_status == "resolved" and not is_resolved:
                    logger.info(
                        "_get_resolution_data_async: umaResolutionStatus=resolved "
                        "but outcomePrices=%s (price lag) — treating as resolved for token %s",
                        outcomes,
                        token_id[:16],
                    )
                    is_resolved = True

                return price, end_dt, is_resolved
        except Exception as exc:
            logger.warning(
                "_get_resolution_data_async error for %s: %s (%s)",
                token_id,
                exc,
                type(exc).__name__,
            )
        return None, None, False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_exit_fraction(
        self,
        whale_bet: WhaleBet,
        open_positions: list,
        db: DBSession,
    ) -> float:
        """Estimate what fraction of the whale's position this exit represents.

        Uses our own open CopiedBet positions as the denominator — anchors the
        fraction to what WE actually hold rather than the whale's full history.

        Returns 1.0 (full exit) when data is insufficient to determine the fraction.
        """
        if not (whale_bet.whale_id and whale_bet.token_id and whale_bet.size_shares > 0):
            return 1.0

        our_open_shares = sum(cb.size_shares for cb in open_positions if cb.size_shares)
        if our_open_shares <= 0:
            return 1.0  # no open positions tracked — treat as full exit

        fraction = whale_bet.size_shares / our_open_shares
        return min(fraction, 1.0)

    def _handle_exit(
        self,
        whale_bet: WhaleBet,
        session: MonitoringSession,
        db: DBSession,
        live_exit_price: float | None = None,
    ) -> CopiedBet | bool | None:
        """Whale is selling out of a position.

        - Single tranche: always close it.
        - Multiple tranches: compute what fraction of the whale's total position
          this exit represents.  If >= _FULL_EXIT_THRESHOLD → close all tranches.
          Otherwise → close the oldest N tranches proportionally (FIFO), leaving
          remaining tranches open until the whale exits those entry levels too.

        Each whale BUY creates a separate CopiedBet tracked independently.
        """
        _FULL_EXIT_THRESHOLD = 0.80  # ≥80% of position sold → treat as full exit

        exiting_whale_address = whale_bet.whale.address if whale_bet.whale else None
        open_positions = self._find_all_open_positions(
            market_id=whale_bet.market_id,
            token_id=whale_bet.token_id,
            session_mode=session.mode,
            db=db,
            whale_address=exiting_whale_address,
        )

        if not open_positions:
            # We never held a position here — this EXIT is orphaned (whale sold a
            # market we either missed or skipped at entry).  Drop silently.
            logger.warning(
                "_handle_exit: no open positions for whale_bet %d "
                "(market_id=%r, token_id=%s, mode=%s, whale=%s) — orphaned exit",
                whale_bet.id,
                whale_bet.market_id,
                whale_bet.token_id,
                session.mode,
                (exiting_whale_address or "?")[:10],
            )
            return None

        exit_price = whale_bet.price
        whale_alias = (
            (whale_bet.whale.alias or whale_bet.whale.address[:10]) if whale_bet.whale else "?"
        )

        # For simulation, use the live price at exit time so the fill mirrors what
        # real mode would get from the CLOB (which fills at the current best bid,
        # not the whale's historical sell price). Fall back to whale price if unavailable
        # or if the CLOB returns 0.0 (resolved/illiquid market — invalid for an active exit).
        mode = open_positions[0].mode if open_positions else session.mode
        sim_exit_price = (
            live_exit_price
            if (
                live_exit_price is not None
                and live_exit_price > 0.0
                and mode in ("SIMULATION", "HEDGE_SIM")
            )
            else exit_price
        )

        exit_reason = (
            f"Whale exited ({whale_alias} sold @ {exit_price:.3f}, we sold @ {sim_exit_price:.3f})"
        )

        close_ok: bool
        if len(open_positions) == 1:
            # Single tranche — always close regardless of fraction
            logger.info(
                "_handle_exit: closing 1 tranche for whale %s token %s @ %.4f (sim fill @ %.4f)",
                (exiting_whale_address or "?")[:10],
                whale_bet.token_id[:16],
                exit_price,
                sim_exit_price,
            )
            close_ok = self._close_all_tranches(
                open_positions, sim_exit_price, session, db, exit_reason
            )
        else:
            # Multiple tranches — determine full vs partial exit
            exit_fraction = self._compute_exit_fraction(whale_bet, open_positions, db)
            if exit_fraction >= _FULL_EXIT_THRESHOLD:
                logger.info(
                    "_handle_exit: full exit (%.0f%%) — closing all %d tranches "
                    "for whale %s token %s @ %.4f (sim fill @ %.4f)",
                    exit_fraction * 100,
                    len(open_positions),
                    (exiting_whale_address or "?")[:10],
                    whale_bet.token_id[:16],
                    exit_price,
                    sim_exit_price,
                )
                close_ok = self._close_all_tranches(
                    open_positions, sim_exit_price, session, db, exit_reason
                )
            else:
                # Partial exit — close oldest tranches first (FIFO)
                n_to_close = max(1, round(len(open_positions) * exit_fraction))
                to_close = open_positions[:n_to_close]
                logger.info(
                    "_handle_exit: partial exit (%.0f%%) — closing %d of %d tranches "
                    "for whale %s token %s @ %.4f (sim fill @ %.4f), %d remain open",
                    exit_fraction * 100,
                    n_to_close,
                    len(open_positions),
                    (exiting_whale_address or "?")[:10],
                    whale_bet.token_id[:16],
                    exit_price,
                    sim_exit_price,
                    len(open_positions) - n_to_close,
                )
                close_ok = self._close_all_tranches(
                    to_close, sim_exit_price, session, db, exit_reason
                )

        db.commit()
        if not close_ok:
            # Sell failed (FOK cancelled / exception) — signal to caller that
            # _last_seen should NOT be advanced so the exit is retried next poll.
            return False
        return open_positions[0]

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
        risk_factor = risk_calc.calculate_risk_factor(
            whale_bet.size_usdc,
            whale_avg,
            conviction_exponent=settings.CONVICTION_EXPONENT,
        )
        suggested_add_usdc = risk_calc.calculate_bet_size(
            session_balance=session.current_balance_usdc,
            risk_factor=risk_factor,
            max_bet_pct=settings.MAX_BET_PCT,
        )

        # Upsert: accumulate into the existing signal for this position rather than
        # creating a new row for every addition.  This keeps one signal per position
        # showing the total the whale has added and how many times they've added.
        existing_signal = (
            db.query(AddToPositionSignal).filter_by(copied_bet_id=existing_open.id).first()
        )
        if existing_signal:
            existing_signal.whale_additional_usdc += whale_bet.size_usdc
            existing_signal.whale_additional_shares += whale_bet.size_shares
            existing_signal.addition_count = (existing_signal.addition_count or 1) + 1
            existing_signal.last_addition_at = whale_bet.timestamp
            existing_signal.price = whale_bet.price  # latest addition price
            existing_signal.suggested_add_usdc = suggested_add_usdc
            db.add(existing_signal)
            logger.info(
                "Updated add-to-position signal for copied_bet=%d: total=$%.2f additions=%d",
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
        item = _WatchItem(
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
        with self._drift_lock:
            self._drift_watchlist[whale_bet.id] = item
        logger.info(
            "Drift watchlist: added whale_bet=%d token=%s…  expires_in=%ds  (watchlist size=%d)",
            whale_bet.id,
            whale_bet.token_id[:12],
            settings.DRIFT_RETRY_WINDOW_SECONDS,
            len(self._drift_watchlist),
        )

    async def retry_drift_watchlist(self, db: DBSession) -> None:
        """
        Called by the fast-poll scheduler every DRIFT_RETRY_INTERVAL_SECONDS.
        For each watchlist item: fetch a fresh (uncached) price and place the bet
        if the price has come back within the drift threshold.  On success the
        existing SKIPPED CopiedBet record is updated to OPEN in-place.
        """
        with self._drift_lock:
            if not self._drift_watchlist:
                return
            # Snapshot so async work below doesn't hold the lock
            items_snapshot = list(self._drift_watchlist.items())

        now = time.monotonic()
        # Within-iteration dedup: prevents the race condition where multiple watchlist
        # items for the same token all see "no open position" before the first one's
        # DB commit is visible.
        placed_tokens_this_iter: set[str] = set()
        for whale_bet_id, item in items_snapshot:
            # 1. Expiry check
            if now > item.expires_at:
                logger.debug("Drift watchlist: whale_bet=%d expired — removing", whale_bet_id)
                with self._drift_lock:
                    self._drift_watchlist.pop(whale_bet_id, None)
                continue

            # 2. Load and validate session
            session = db.get(MonitoringSession, item.session_id)
            if not session or not session.is_active:
                logger.debug(
                    "Drift watchlist: session %d gone — removing whale_bet=%d",
                    item.session_id,
                    whale_bet_id,
                )
                with self._drift_lock:
                    self._drift_watchlist.pop(whale_bet_id, None)
                continue

            # 3. Check the whale hasn't exited this token since the buy signal was skipped.
            #    If a SELL WhaleBet exists for the same token recorded after the original
            #    buy, the whale is no longer in the position — entering now would create
            #    an instant orphan that the orphan checker must clean up at a loss.
            original_wb = db.get(WhaleBet, whale_bet_id)
            if original_wb:
                later_sell = (
                    db.query(WhaleBet)
                    .filter(
                        WhaleBet.whale_id == original_wb.whale_id,
                        WhaleBet.token_id == item.token_id,
                        WhaleBet.side == "SELL",
                        WhaleBet.timestamp > original_wb.timestamp,
                    )
                    .first()
                )
                if later_sell:
                    logger.info(
                        "Drift watchlist: whale_bet=%d aborted — whale exited token %s...%s "
                        "at %s (after buy at %s) — removing",
                        whale_bet_id,
                        item.token_id[:10],
                        item.token_id[-6:],
                        later_sell.timestamp,
                        original_wb.timestamp,
                    )
                    with self._drift_lock:
                        self._drift_watchlist.pop(whale_bet_id, None)
                    continue

            # 4. Add-to-position guard: skip if we already hold an OPEN position at a
            #    similar price for this token/whale. The drift-skipped entry would
            #    create a duplicate tranche rather than a meaningful new position.
            _SAME_PRICE_TOLERANCE = 0.03
            if original_wb:
                whale_addr = original_wb.whale.address if original_wb.whale else None
                if whale_addr:
                    _existing = (
                        db.query(CopiedBet)
                        .filter(
                            CopiedBet.whale_address == whale_addr,
                            CopiedBet.token_id == item.token_id,
                            CopiedBet.status == "OPEN",
                            CopiedBet.mode == session.mode,
                        )
                        .all()
                    )
                    if _existing:
                        _closest = min(
                            _existing, key=lambda p: abs(item.whale_price - p.price_at_entry)
                        )
                        if abs(item.whale_price - _closest.price_at_entry) <= _SAME_PRICE_TOLERANCE:
                            logger.info(
                                "Drift watchlist: whale_bet=%d — open position already exists "
                                "at %.3f (retry price %.3f within %.2f tolerance) — removing",
                                whale_bet_id,
                                _closest.price_at_entry,
                                item.whale_price,
                                _SAME_PRICE_TOLERANCE,
                            )
                            with self._drift_lock:
                                self._drift_watchlist.pop(whale_bet_id, None)
                            continue

            # 4b. Within-iteration dedup: if another watchlist item for the same token
            #     already placed a bet this loop cycle, the DB won't reflect it yet.
            #     Skip to avoid the race-condition that caused multi-position flooding.
            if item.token_id in placed_tokens_this_iter:
                logger.info(
                    "Drift watchlist: whale_bet=%d — token %s already placed this "
                    "iteration — removing to prevent duplicate",
                    whale_bet_id,
                    item.token_id[:16],
                )
                with self._drift_lock:
                    self._drift_watchlist.pop(whale_bet_id, None)
                continue

            # 4c. Per-market hard cap — cross-iteration protection against exceeding
            #     MAX_OPEN_POSITIONS_PER_MARKET even across sequential retry cycles.
            _market_id_check = (
                original_wb.market_id if original_wb else item.market_info.get("conditionId", "")
            )
            if _market_id_check:
                _market_open = (
                    db.query(CopiedBet)
                    .filter(
                        CopiedBet.market_id == _market_id_check,
                        CopiedBet.status == "OPEN",
                        CopiedBet.mode == session.mode,
                    )
                    .count()
                )
                if _market_open >= settings.MAX_OPEN_POSITIONS_PER_MARKET:
                    logger.info(
                        "Drift watchlist: whale_bet=%d — market %s already has %d open "
                        "position(s) (cap=%d) — removing",
                        whale_bet_id,
                        _market_id_check[:16],
                        _market_open,
                        settings.MAX_OPEN_POSITIONS_PER_MARKET,
                    )
                    with self._drift_lock:
                        self._drift_watchlist.pop(whale_bet_id, None)
                    continue

            # 5. Fetch a live price, bypassing the 30 s cache
            try:
                live_price = await self._client.get_best_price(item.token_id, force_refresh=True)
            except Exception as exc:
                logger.debug(
                    "Drift watchlist: price fetch failed for whale_bet=%d: %s", whale_bet_id, exc
                )
                continue

            # 6. Re-check drift only — all other guards already passed
            price_ok, price_reason = risk_calc.check_price_staleness(
                whale_price=item.whale_price,
                live_price=live_price,
                max_upward_drift=settings.MAX_UPWARD_DRIFT_PCT,
                max_downward_drift=settings.MAX_DOWNWARD_DRIFT_PCT,
            )
            if not price_ok:
                logger.debug(
                    "Drift watchlist: whale_bet=%d still drifted (%s) — retrying",
                    whale_bet_id,
                    price_reason,
                )
                continue

            with self._placement_lock:
                db.refresh(session)
                if session.current_balance_usdc < 1.0:
                    logger.info(
                        "Drift watchlist: whale_bet=%d — insufficient balance $%.2f — removing",
                        whale_bet_id,
                        session.current_balance_usdc,
                    )
                    with self._drift_lock:
                        self._drift_watchlist.pop(whale_bet_id, None)
                    continue
                # 7. Price is back in range — execute
                mode = session.mode
                try:
                    if mode in ("SIMULATION", "HEDGE_SIM"):
                        size_shares, actual_price, actual_cost = self.simulate_buy(
                            session=session,
                            market_id=item.market_info.get("conditionId", ""),
                            token_id=item.token_id,
                            question=item.market_info.get("question", ""),
                            outcome=item.market_info.get("outcome", ""),
                            price=live_price if live_price is not None else item.whale_price,
                            size_usdc=item.bet_size_usdc,
                            db=db,
                            live_price=live_price,
                        )
                        item.bet_size_usdc = (
                            actual_cost  # update cost basis to fee-inclusive amount
                        )
                    else:
                        order_resp = self.place_real_buy(
                            token_id=item.token_id,
                            size_usdc=item.bet_size_usdc,
                            price=item.whale_price,
                            neg_risk=bool(item.market_info.get("negRisk", False)),
                        )
                        size_shares = float(
                            order_resp.get(
                                "size_matched", item.bet_size_usdc / max(item.whale_price, 0.01)
                            )
                        )
                        fill_price = (
                            order_resp.get("price")
                            or order_resp.get("avgPrice")
                            or order_resp.get("average_price")
                        )
                        actual_price = (
                            float(fill_price) if fill_price else (live_price or item.whale_price)
                        )
                        # Deduct from session balance (simulate_buy handles this for SIM)
                        session.current_balance_usdc = max(
                            0.0, session.current_balance_usdc - item.bet_size_usdc
                        )
                        db.add(session)
                except Exception as exc:
                    logger.error(
                        "Drift watchlist: placement failed for whale_bet=%d: %s", whale_bet_id, exc
                    )
                    with self._drift_lock:
                        self._drift_watchlist.pop(whale_bet_id, None)
                    continue

                # 8. Upgrade the existing SKIPPED record to OPEN in-place
                copied_bet = db.get(CopiedBet, item.copied_bet_id)
                if copied_bet is None:
                    logger.warning(
                        "Drift watchlist: CopiedBet %d not found — removing", item.copied_bet_id
                    )
                    with self._drift_lock:
                        self._drift_watchlist.pop(whale_bet_id, None)
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

            placed_tokens_this_iter.add(item.token_id)
            with self._drift_lock:
                self._drift_watchlist.pop(whale_bet_id, None)
            logger.info(
                "Drift watchlist: RETRY SUCCESS whale_bet=%d — placed %s @ %.4f for $%.2f  (watchlist size=%d)",
                whale_bet_id,
                mode,
                actual_price,
                item.bet_size_usdc,
                len(self._drift_watchlist),
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
            session_id=session.id,
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
            "closed ",  # "Market closed 3.2h ago"
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
        whale_address: str | None = None,
    ) -> CopiedBet | None:
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

    def _find_all_open_positions(
        self,
        market_id: str,
        token_id: str,
        session_mode: str,
        db: DBSession,
        whale_address: str | None = None,
    ) -> list:
        """Return ALL OPEN CopiedBets for this market/token in entry order (oldest first).

        Used by _handle_exit to close every open tranche simultaneously when the
        whale exits a position they may have built up across multiple BUY trades.
        """
        q = (
            db.query(CopiedBet)
            .filter(
                CopiedBet.market_id == market_id,
                CopiedBet.token_id == token_id,
                CopiedBet.status == "OPEN",
                CopiedBet.mode == session_mode,
            )
            .order_by(CopiedBet.opened_at.asc())
        )
        if whale_address is not None:
            q = q.filter(CopiedBet.whale_address == whale_address)
        return q.all()

    def _get_actual_position_size(self, token_id: str) -> float | None:
        """
        Query the Polymarket Data API for our actual on-chain token balance.

        Used as a fallback when place_real_sell returns no_position — the CLOB
        rejects our sell if size_shares in the DB doesn't exactly match what's
        on-chain (e.g. due to fee deduction or tick-size rounding on the buy).
        Returns the actual size if a position is found, 0.0 if absent, or None
        on API error (caller should treat None as "unknown, don't retry").
        """
        import requests as _requests

        try:
            resp = _requests.get(
                f"{settings.DATA_API_BASE}/positions",
                params={"user": settings.POLY_FUNDER_ADDRESS},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            positions = (
                data if isinstance(data, list) else data.get("data", data.get("positions", []))
            )
            logger.info(
                "_get_actual_position_size: Data API returned %d position(s) for %s, "
                "looking for token %s",
                len(positions),
                settings.POLY_FUNDER_ADDRESS[:10],
                token_id[:16],
            )
            # Log first few assets to help diagnose format mismatches
            for p in positions[:5]:
                logger.debug(
                    "  Data API position: asset=%s size=%s",
                    str(p.get("asset", ""))[:20],
                    p.get("size"),
                )

            # Try exact match first, then strip/normalise for robustness
            def _matches(asset_val: str) -> bool:
                a = str(asset_val).strip()
                t = token_id.strip()
                if a == t:
                    return True
                # Handle leading-zero padding differences between decimal representations
                try:
                    return int(a) == int(t)
                except (ValueError, TypeError):
                    pass
                # Handle hex vs decimal
                try:
                    a_dec = str(int(a, 16)) if a.startswith("0x") else a
                    t_dec = str(int(t, 16)) if t.startswith("0x") else t
                    return a_dec == t_dec
                except (ValueError, TypeError):
                    return False

            pos = next(
                (p for p in positions if _matches(p.get("asset", ""))),
                None,
            )
            if pos is not None:
                size = float(pos.get("size", 0))
                logger.info(
                    "_get_actual_position_size: found %.4f shares for token %s",
                    size,
                    token_id[:16],
                )
                return size
            logger.info(
                "_get_actual_position_size: token %s not found in Data API — 0 shares",
                token_id[:16],
            )
            return 0.0
        except Exception as exc:
            logger.warning(
                "_get_actual_position_size error for %s: %s — will not retry sell",
                token_id[:16],
                exc,
            )
            return None

    def _find_opposite_outcome_position(
        self,
        market_id: str,
        token_id: str,
        session_mode: str,
        db: DBSession,
        whale_address: str,
    ) -> CopiedBet | None:
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
