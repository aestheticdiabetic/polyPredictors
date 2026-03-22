"""
Regression tests for the standard scan "no bets found" bug.

Root cause: bet_engine.py changed _find_open_position to pass whale_address=None
(search across ALL whales) instead of whale_address=whale_address (scoped to
current whale).  This caused ALL new signals from whale_B in a market where
whale_A already had an OPEN position to be SKIPPED with "Already have open
position from a different whale," dramatically suppressing bet placement.

The fix: revert to per-whale scoping in the entry check.  Exits are already
scoped by whale address so one whale's exit can never close another whale's
position — there is no cross-contamination risk.

Run with:  python tests/test_scan_bug.py
"""

import sys
import os
from datetime import datetime, timedelta, timezone

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "backend"))

# ---------------------------------------------------------------------------
# In-memory SQLite (must be patched before importing backend modules)
# ---------------------------------------------------------------------------
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

_test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

import backend.database as _db_mod

_db_mod.engine = _test_engine
_db_mod.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_test_engine)

from backend.database import (
    Base, CopiedBet, MonitoringSession, Whale, WhaleBet, AddToPositionSignal,
)

Base.metadata.create_all(bind=_test_engine)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_db():
    return _db_mod.SessionLocal()


def _clear_db():
    db = _new_db()
    db.query(AddToPositionSignal).delete()
    db.query(CopiedBet).delete()
    db.query(WhaleBet).delete()
    db.query(MonitoringSession).delete()
    db.query(Whale).delete()
    db.commit()
    db.close()


def _make_session(db, mode="SIMULATION", balance=200.0, is_active=True) -> MonitoringSession:
    s = MonitoringSession(
        mode=mode,
        is_active=is_active,
        starting_balance_usdc=balance,
        current_balance_usdc=balance,
    )
    db.add(s)
    db.commit()
    db.refresh(s)
    return s


def _make_whale(db, address="0xwhale1", alias="Whale1", avg_bet=100.0) -> Whale:
    existing = db.query(Whale).filter_by(address=address).first()
    if existing:
        return existing
    w = Whale(address=address, alias=alias, avg_bet_size_usdc=avg_bet)
    db.add(w)
    db.commit()
    db.refresh(w)
    return w


def _make_whale_bet(
    db, whale: Whale, market_id="mkt1", token_id="tok_yes",
    outcome="YES", side="BUY", price=0.6, size_usdc=100.0,
    tx_hash=None,
) -> WhaleBet:
    wb = WhaleBet(
        whale_id=whale.id,
        market_id=market_id,
        token_id=token_id,
        question=f"Will {market_id} happen?",
        side=side,
        outcome=outcome,
        price=price,
        size_usdc=size_usdc,
        size_shares=size_usdc / max(price, 0.001),
        timestamp=datetime.now(timezone.utc),
        bet_type="OPEN" if side == "BUY" else "EXIT",
        tx_hash=tx_hash,
    )
    db.add(wb)
    db.commit()
    db.refresh(wb)
    return wb


def _make_open_copied_bet(
    db, whale: Whale, session: MonitoringSession,
    market_id="mkt1", token_id="tok_yes", outcome="YES",
) -> CopiedBet:
    """Insert a pre-existing OPEN CopiedBet directly, bypassing bet_engine."""
    wb = _make_whale_bet(db, whale, market_id=market_id, token_id=token_id, outcome=outcome)
    b = CopiedBet(
        whale_bet_id=wb.id,
        whale_address=whale.address,
        mode=session.mode,
        market_id=market_id,
        token_id=token_id,
        question=f"Will {market_id} happen?",
        side="BUY",
        outcome=outcome,
        price_at_entry=0.6,
        size_usdc=10.0,
        size_shares=16.67,
        risk_factor=1.0,
        whale_bet_usdc=100.0,
        whale_avg_bet_usdc=100.0,
        status="OPEN",
        opened_at=datetime.utcnow(),
    )
    db.add(b)
    db.commit()
    db.refresh(b)
    return b


def _make_market_info(hours_from_now=48):
    return {
        "active": True,
        "closed": False,
        "resolved": False,
        "status": "open",
        "endDate": (datetime.now(timezone.utc) + timedelta(hours=hours_from_now)).isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# 1. Standard scan places bets in fresh markets (no existing positions)
# ---------------------------------------------------------------------------

def test_fresh_market_always_gets_a_bet():
    """
    A fresh market with no prior positions must always receive a SIMULATION bet.
    This is the most basic sanity check for the standard scan.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=200.0)
    whale = _make_whale(db, "0xfresh1")
    wb = _make_whale_bet(db, whale, market_id="mkt_fresh", token_id="tok_fresh_yes")

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb, session, db,
        market_info=_make_market_info(),
        live_price=0.6,
    )

    assert result is not None, "Expected a CopiedBet to be returned"
    assert result.status == "OPEN", (
        f"Fresh market bet should be OPEN, got {result.status}: {result.skip_reason}"
    )
    assert result.mode == "SIMULATION"
    db.close()


# ---------------------------------------------------------------------------
# 2. Regression: different whales can independently enter the same market
# ---------------------------------------------------------------------------

def test_second_whale_can_enter_market_already_held_by_first_whale():
    """
    REGRESSION: With the buggy code (whale_address=None in _find_open_position),
    whale_B's bet was SKIPPED with "Already have open position from a different whale"
    when whale_A already had an OPEN position in the same market.

    Correct behaviour: each whale's positions are tracked independently.
    Whale_B should receive its own OPEN CopiedBet regardless of whale_A's position.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=200.0)
    whale_a = _make_whale(db, "0xwhale_a", "WhaleA")
    whale_b = _make_whale(db, "0xwhale_b", "WhaleB")

    # Whale_A already has an OPEN position in mkt_shared
    _make_open_copied_bet(db, whale_a, session, market_id="mkt_shared", token_id="tok_yes")

    # Whale_B now signals the same market — must NOT be blocked by whale_A's position
    wb_b = _make_whale_bet(db, whale_b, market_id="mkt_shared", token_id="tok_yes")

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb_b, session, db,
        market_info=_make_market_info(),
        live_price=0.6,
    )

    assert result is not None, "Expected a CopiedBet for whale_B"
    assert result.status == "OPEN", (
        f"Whale_B should get its own OPEN position, got {result.status}: "
        f"{getattr(result, 'skip_reason', None)!r}\n"
        f"Bug: _find_open_position searched across ALL whales instead of just the current one."
    )
    assert result.whale_address == "0xwhale_b", (
        f"CopiedBet should belong to whale_B, got {result.whale_address}"
    )
    db.close()


def test_two_whales_get_independent_open_positions_in_same_market():
    """
    End-to-end: place whale_A's bet first via bet_engine, then place whale_B's.
    Both should end up with OPEN CopiedBets for the same market.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=400.0)
    whale_a = _make_whale(db, "0xindep_a", "IndepA")
    whale_b = _make_whale(db, "0xindep_b", "IndepB")

    engine = BetEngine(polymarket_client=None)
    market = _make_market_info()

    # Whale_A bets first
    wb_a = _make_whale_bet(db, whale_a, market_id="mkt_both", token_id="tok_yes", tx_hash="txA")
    result_a = engine.process_new_whale_bet(wb_a, session, db, market_info=market, live_price=0.6)

    # Whale_B bets in the same market
    wb_b = _make_whale_bet(db, whale_b, market_id="mkt_both", token_id="tok_yes", tx_hash="txB")
    result_b = engine.process_new_whale_bet(wb_b, session, db, market_info=market, live_price=0.6)

    assert result_a is not None and result_a.status == "OPEN", (
        f"Whale_A bet should be OPEN, got {getattr(result_a, 'status', None)}: "
        f"{getattr(result_a, 'skip_reason', None)}"
    )
    assert result_b is not None and result_b.status == "OPEN", (
        f"Whale_B bet should be OPEN (independent of whale_A), got "
        f"{getattr(result_b, 'status', None)}: {getattr(result_b, 'skip_reason', None)}\n"
        f"This is the regression bug — whale_address=None blocked whale_B."
    )

    open_bets = db.query(CopiedBet).filter_by(
        market_id="mkt_both", status="OPEN", mode="SIMULATION"
    ).all()
    assert len(open_bets) == 2, (
        f"Expected 2 independent OPEN positions for the same market, got {len(open_bets)}"
    )
    db.close()


# ---------------------------------------------------------------------------
# 3. Same whale adding to a position records a signal, not a new bet
# ---------------------------------------------------------------------------

def test_same_whale_same_price_creates_signal_not_new_bet():
    """
    When the SAME whale adds more capital at the SAME price (within 3 cents),
    an AddToPositionSignal is recorded and no new CopiedBet is created.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=200.0)
    whale = _make_whale(db, "0xadder", "Adder")

    # Initial position at price 0.6
    _make_open_copied_bet(db, whale, session, market_id="mkt_add", token_id="tok_yes")
    initial_bet_count = db.query(CopiedBet).count()

    # Same whale bets again at the same price (0.6) — should be AddToPositionSignal
    wb2 = _make_whale_bet(db, whale, market_id="mkt_add", token_id="tok_yes",
                          tx_hash="tx_add_2", price=0.6)

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb2, session, db,
        market_info=_make_market_info(),
        live_price=0.6,
    )

    # No new CopiedBet should be created
    final_bet_count = db.query(CopiedBet).count()
    assert final_bet_count == initial_bet_count, (
        f"No new CopiedBet should be created for same-price add. "
        f"Before: {initial_bet_count}, after: {final_bet_count}"
    )

    # An AddToPositionSignal should have been created
    signals = db.query(AddToPositionSignal).all()
    assert len(signals) == 1, f"Expected 1 AddToPositionSignal, got {len(signals)}"
    db.close()


def test_same_whale_different_price_creates_independent_tranche():
    """
    When the SAME whale buys the same token at a DIFFERENT price (> 3 cents apart),
    a new independent CopiedBet tranche is opened. This lets each entry level be
    tracked and exited independently when the whale exits that specific position.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=200.0)
    whale = _make_whale(db, "0xtranche", "Tranche")

    # Initial position at price 0.6
    _make_open_copied_bet(db, whale, session, market_id="mkt_tranche", token_id="tok_yes")
    initial_bet_count = db.query(CopiedBet).count()

    # Same whale bets at a different price (0.8 — 20 cents apart, > 3 cent tolerance)
    wb2 = _make_whale_bet(db, whale, market_id="mkt_tranche", token_id="tok_yes",
                          tx_hash="tx_tranche_2", price=0.8)

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb2, session, db,
        market_info=_make_market_info(),
        live_price=0.8,
    )

    # A new independent tranche CopiedBet should be created
    final_bet_count = db.query(CopiedBet).count()
    assert final_bet_count == initial_bet_count + 1, (
        f"A new tranche should be created for different-price entry. "
        f"Before: {initial_bet_count}, after: {final_bet_count}"
    )
    assert result is not None and result.status == "OPEN", (
        f"New tranche should be OPEN, got {result.status if result else None}"
    )
    # No AddToPositionSignal should be created
    signals = db.query(AddToPositionSignal).all()
    assert len(signals) == 0, f"Expected no signal for different-price tranche, got {len(signals)}"
    db.close()


# ---------------------------------------------------------------------------
# 4. Whale_B's exit does NOT close whale_A's position (safety check)
# ---------------------------------------------------------------------------

def test_different_whale_exit_does_not_close_other_whales_position():
    """
    Safety check: with independent positions per whale, whale_B's EXIT signal
    must not close whale_A's position. The exit handler is scoped by whale_address.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=400.0)
    whale_a = _make_whale(db, "0xexit_a", "ExitA")
    whale_b = _make_whale(db, "0xexit_b", "ExitB")

    engine = BetEngine(polymarket_client=None)
    market = _make_market_info()

    # Both whales enter the same market independently
    wb_a = _make_whale_bet(db, whale_a, market_id="mkt_exit", token_id="tok_exit_yes", tx_hash="txE_a")
    engine.process_new_whale_bet(wb_a, session, db, market_info=market, live_price=0.6)

    wb_b = _make_whale_bet(db, whale_b, market_id="mkt_exit", token_id="tok_exit_yes", tx_hash="txE_b")
    engine.process_new_whale_bet(wb_b, session, db, market_info=market, live_price=0.6)

    open_before = db.query(CopiedBet).filter_by(status="OPEN", mode="SIMULATION").count()
    assert open_before == 2, f"Expected 2 OPEN positions before exit, got {open_before}"

    # Whale_B exits — should only close whale_B's position
    wb_exit_b = WhaleBet(
        whale_id=whale_b.id,
        market_id="mkt_exit",
        token_id="tok_exit_yes",
        question="Will mkt_exit happen?",
        side="SELL",
        outcome="YES",
        price=0.75,
        size_usdc=75.0,
        size_shares=100.0,
        timestamp=datetime.now(timezone.utc),
        bet_type="EXIT",
        tx_hash="txE_b_exit",
    )
    db.add(wb_exit_b)
    db.commit()
    db.refresh(wb_exit_b)

    engine.process_new_whale_bet(wb_exit_b, session, db)

    open_after = db.query(CopiedBet).filter_by(status="OPEN", mode="SIMULATION").count()
    assert open_after == 1, (
        f"After whale_B exits, 1 position (whale_A's) should still be OPEN. "
        f"Got {open_after} OPEN positions."
    )

    # Verify it's whale_A's position that remains open
    remaining = db.query(CopiedBet).filter_by(status="OPEN", mode="SIMULATION").first()
    assert remaining is not None
    assert remaining.whale_address == "0xexit_a", (
        f"Whale_A's position should remain open. Got whale_address={remaining.whale_address}"
    )
    db.close()


# ---------------------------------------------------------------------------
# 5. Stale OPEN bets from a previous session don't block the SAME whale
# ---------------------------------------------------------------------------

def test_stale_open_bets_from_previous_session_same_price_creates_signal():
    """
    If previous SIMULATION sessions left OPEN bets, the SAME whale signalling
    the same market at the SAME price in a new session should create an
    AddToPositionSignal — not a new CopiedBet.  The position is mode-scoped,
    not session-scoped, so stale same-price entries are treated as additions.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    # Previous session (stopped)
    old_session = _make_session(db, mode="SIMULATION", balance=200.0, is_active=False)
    whale = _make_whale(db, "0xstale", "StaleWhale")
    # Stale open bet from the old session at price 0.6
    _make_open_copied_bet(db, whale, old_session, market_id="mkt_stale", token_id="tok_stale_yes")

    # New session starts
    new_session = _make_session(db, mode="SIMULATION", balance=200.0, is_active=True)

    # Same whale signals the same market at the same price (0.6)
    wb_new = _make_whale_bet(db, whale, market_id="mkt_stale", token_id="tok_stale_yes",
                             tx_hash="tx_stale_new", price=0.6)

    engine = BetEngine(polymarket_client=None)
    engine.process_new_whale_bet(
        wb_new, new_session, db,
        market_info=_make_market_info(),
        live_price=0.6,
    )

    # Should record as add-to-position — no new CopiedBet
    copied_bets = db.query(CopiedBet).filter_by(
        market_id="mkt_stale", mode="SIMULATION"
    ).all()
    assert len(copied_bets) == 1, (
        f"Same price add across sessions should create AddToPositionSignal, not new bet. "
        f"Got {len(copied_bets)} CopiedBets."
    )
    signals = db.query(AddToPositionSignal).all()
    assert len(signals) == 1, (
        f"Expected 1 AddToPositionSignal for same-price cross-session add. Got {len(signals)}"
    )
    db.close()


# ---------------------------------------------------------------------------
# 6. Stale OPEN bets from a different whale do NOT block new whale's bet
# ---------------------------------------------------------------------------

def test_stale_open_bet_from_different_whale_does_not_block_new_whale():
    """
    REGRESSION: The buggy code (whale_address=None) caused new whale_B bets to
    be SKIPPED whenever whale_A had any stale OPEN position in the same market
    from any previous session.

    Correct behaviour: whale_B should independently receive a new OPEN position.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    old_session = _make_session(db, mode="SIMULATION", balance=200.0, is_active=False)
    whale_a = _make_whale(db, "0xstale_a", "StaleA")
    whale_b = _make_whale(db, "0xstale_b", "StaleB")

    # Whale_A has a stale open bet from a previous session
    _make_open_copied_bet(db, whale_a, old_session, market_id="mkt_stale2", token_id="tok_stale2")

    # New session, whale_B signals the same market
    new_session = _make_session(db, mode="SIMULATION", balance=200.0, is_active=True)
    wb_b = _make_whale_bet(db, whale_b, market_id="mkt_stale2", token_id="tok_stale2",
                           tx_hash="tx_stale_b")

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb_b, new_session, db,
        market_info=_make_market_info(),
        live_price=0.6,
    )

    assert result is not None, "Expected a CopiedBet for whale_B"
    assert result.status == "OPEN", (
        f"Whale_B should get an OPEN position even though whale_A has a stale bet. "
        f"Got {result.status}: {getattr(result, 'skip_reason', None)!r}\n"
        f"This is the regression bug — whale_address=None blocked whale_B."
    )
    db.close()


# ---------------------------------------------------------------------------
# 7. Multiple whales scanning simultaneously — realistic multi-signal scenario
# ---------------------------------------------------------------------------

def test_three_whales_all_place_bets_in_active_markets():
    """
    Simulate a realistic scan: 3 whales each signal different markets,
    with some market overlap. All should produce OPEN bets.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=600.0)
    whale_a = _make_whale(db, "0xscan_a", "ScanA")
    whale_b = _make_whale(db, "0xscan_b", "ScanB")
    whale_c = _make_whale(db, "0xscan_c", "ScanC")

    engine = BetEngine(polymarket_client=None)
    market = _make_market_info()

    trades = [
        (whale_a, "mkt_p1", "tok_p1", "txSA1"),
        (whale_b, "mkt_p1", "tok_p1", "txSB1"),  # same market as whale_a
        (whale_c, "mkt_p2", "tok_p2", "txSC1"),
        (whale_a, "mkt_p3", "tok_p3", "txSA2"),
        (whale_b, "mkt_p2", "tok_p2", "txSB2"),  # same market as whale_c
    ]

    results = []
    for whale, mkt, tok, txh in trades:
        wb = _make_whale_bet(db, whale, market_id=mkt, token_id=tok, tx_hash=txh)
        r = engine.process_new_whale_bet(wb, session, db, market_info=market, live_price=0.6)
        results.append((whale.address, mkt, r))

    open_bets = db.query(CopiedBet).filter_by(status="OPEN", mode="SIMULATION").all()

    # All 5 trades should produce OPEN bets (each whale tracks independently)
    assert len(open_bets) == 5, (
        f"Expected 5 independent OPEN bets (one per whale×market signal), "
        f"got {len(open_bets)}. "
        f"Results: {[(a, m, r.status if r else None) for a, m, r in results]}"
    )
    db.close()


# ---------------------------------------------------------------------------
# 8. Hedge guard is still SILENT in SIMULATION mode (non-regression check)
# ---------------------------------------------------------------------------

def test_hedge_guard_does_not_fire_in_simulation_even_with_opposite_position():
    """
    In SIMULATION mode, the hedge guard must NOT fire even if the same whale
    holds an OPEN position on the opposite outcome in the same market.
    Both positions should be independently tracked.
    """
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = _make_session(db, mode="SIMULATION", balance=200.0)
    whale = _make_whale(db, "0xhg_sim", "HGSim")

    # Whale already has YES position
    _make_open_copied_bet(db, whale, session, market_id="mkt_hg", token_id="tok_hg_yes", outcome="YES")

    # Whale now bets NO on the same market (price=0.4, live matches within max_drift=0.05)
    wb_no = _make_whale_bet(
        db, whale, market_id="mkt_hg", token_id="tok_hg_no",
        outcome="NO", price=0.4, tx_hash="tx_hg_no",
    )

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb_no, session, db,
        market_info=_make_market_info(),
        live_price=0.41,  # within 0.05 drift of whale price 0.4
    )

    # Hedge guard must not fire in SIMULATION mode
    if result is not None and result.status == "SKIPPED":
        assert "hedg" not in (result.skip_reason or "").lower(), (
            f"Hedge guard fired in SIMULATION mode — it should ONLY fire in HEDGE_SIM mode. "
            f"skip_reason={result.skip_reason!r}"
        )

    # The NO bet should be OPEN (different token from YES position)
    assert result is not None and result.status == "OPEN", (
        f"In SIMULATION mode, whale betting NO when it holds YES should produce "
        f"an OPEN bet. Got {getattr(result, 'status', None)}: "
        f"{getattr(result, 'skip_reason', None)}"
    )
    db.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    print(f"\nRunning {len(tests)} tests...\n")
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {name}")
            print(f"        {e}")
            failed += 1
        except Exception as e:
            import traceback
            print(f"  ERROR {name}")
            print(f"        {type(e).__name__}: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*60}")
    print(f"  {passed}/{passed + failed} passed", end="")
    if failed:
        print(f"  *** {failed} FAILURES ***")
        sys.exit(1)
    else:
        print("  ALL OK")
