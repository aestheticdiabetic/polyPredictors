"""
Tests for check_resolution() in bet_engine.py.

Focuses on:
1. is_live_sport ordering bug — oracle-settled NO resolution must work even when
   the resolved bet is the FIRST bet processed in a check_resolution pass.
2. YES and NO resolution paths for both live sports and non-sports.
3. Premature NO guard for live sports (< 5h after close).
4. Mid-range / unresolved bets left open.
5. Resolution checker does NOT crash when is_live_sport was previously undefined.

Run with:  python tests/test_check_resolution.py
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, AsyncMock, patch

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "backend"))

# ---------------------------------------------------------------------------
# In-memory DB (same pattern as test_mode_isolation.py)
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
    Base, CopiedBet, MonitoringSession, Whale, WhaleBet,
)
Base.metadata.create_all(bind=_test_engine)

from backend.bet_engine import BetEngine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_db():
    return _db_mod.SessionLocal()


def _clear_db():
    db = _new_db()
    db.query(CopiedBet).delete()
    db.query(WhaleBet).delete()
    db.query(MonitoringSession).delete()
    db.query(Whale).delete()
    db.commit()
    db.close()


def _make_open_bet(db, mode="SIMULATION", category="Other", token_id="tok1") -> CopiedBet:
    whale = db.query(Whale).filter_by(address="0xrestest").first()
    if not whale:
        whale = Whale(address="0xrestest", alias="ResTest", avg_bet_size_usdc=100.0)
        db.add(whale)
        db.commit()
        db.refresh(whale)

    wb = WhaleBet(
        whale_id=whale.id, market_id="mkt_res", token_id=token_id,
        question="Will Z happen?", side="BUY", outcome="YES",
        price=0.6, size_usdc=100.0, size_shares=166.0,
        timestamp=datetime.utcnow(), bet_type="OPEN",
    )
    db.add(wb)
    db.commit()
    db.refresh(wb)

    session = MonitoringSession(
        mode=mode, is_active=True,
        starting_balance_usdc=200.0, current_balance_usdc=200.0,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    bet = CopiedBet(
        whale_bet_id=wb.id, whale_address=whale.address,
        mode=mode, market_id="mkt_res", token_id=token_id,
        question="Will Z happen?", side="BUY", outcome="YES",
        price_at_entry=0.6, size_usdc=10.0, size_shares=16.67,
        risk_factor=0.1, whale_bet_usdc=100.0, whale_avg_bet_usdc=100.0,
        status="OPEN", market_category=category,
        opened_at=datetime.utcnow(),
    )
    db.add(bet)
    db.commit()
    db.refresh(bet)
    return bet


def _run_resolution_with_mock(price: float, end_dt: datetime, is_resolved: bool):
    """
    Run check_resolution() with a mocked _fetch_resolution_data that returns
    the given (price, end_dt, is_resolved) for the single open bet.
    """
    engine = BetEngine(polymarket_client=None)

    async def _mock_fetch(token_ids):
        return [(price, end_dt, is_resolved)] * len(token_ids)

    # Patch SessionLocal where it's used (not just where it's defined) so that
    # this test module's in-memory engine is used regardless of pytest collection order.
    with patch("backend.bet_engine.SessionLocal", _db_mod.SessionLocal):
        with patch.object(engine, "_fetch_resolution_data", side_effect=_mock_fetch):
            engine.check_resolution()


def _past(hours: float) -> datetime:
    """Return a UTC datetime `hours` hours in the past."""
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def _future(hours: float) -> datetime:
    """Return a UTC datetime `hours` hours in the future."""
    return datetime.now(timezone.utc) + timedelta(hours=hours)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_yes_win_resolves_for_non_sports():
    """price >= 0.95 on a closed non-sports market → CLOSED_WIN."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Politics")
    bet_id = bet.id
    db.close()

    _run_resolution_with_mock(price=0.97, end_dt=_past(2), is_resolved=True)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "CLOSED_WIN", f"Expected CLOSED_WIN, got {resolved.status}"
    assert resolved.pnl_usdc is not None
    db.close()


def test_yes_win_resolves_for_live_sport():
    """price >= 0.95 on a closed live-sport market → CLOSED_WIN (no guard on YES)."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Basketball")
    bet_id = bet.id
    db.close()

    _run_resolution_with_mock(price=0.97, end_dt=_past(1), is_resolved=False)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "CLOSED_WIN", f"Expected CLOSED_WIN, got {resolved.status}"
    db.close()


def test_no_win_resolves_for_non_sports_first_bet():
    """
    Regression: oracle-settled NO win on a non-sports market when this is the
    FIRST (and only) bet in the loop — previously crashed with UnboundLocalError
    on is_live_sport because it was defined after the Case 1 block.
    """
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Politics")  # non-sports, is_live_sport=False
    bet_id = bet.id
    db.close()

    # Market ended 8h ago, price=0.02, oracle resolved (opposing token = 0.98 → 1.0)
    _run_resolution_with_mock(price=0.02, end_dt=_past(8), is_resolved=True)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "CLOSED_LOSS", (
        f"Expected CLOSED_LOSS (NO won), got {resolved.status}"
    )
    db.close()


def test_no_win_resolves_for_live_sport_after_guard_period():
    """Live sport NO win with >= 5h since close → CLOSED_LOSS (guard passed)."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Soccer")
    bet_id = bet.id
    db.close()

    _run_resolution_with_mock(price=0.02, end_dt=_past(7), is_resolved=True)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "CLOSED_LOSS", (
        f"Expected CLOSED_LOSS (7h after close, guard passed), got {resolved.status}"
    )
    db.close()


def test_no_win_live_sport_within_guard_stays_open():
    """Live sport NO win within 5h of close → stays OPEN (premature cancellation guard)."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Basketball")
    bet_id = bet.id
    db.close()

    # Only 1h after close — within the 5h guard
    _run_resolution_with_mock(price=0.02, end_dt=_past(1), is_resolved=True)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "OPEN", (
        f"Expected OPEN (guard active, only 1h after close), got {resolved.status}"
    )
    db.close()


def test_unresolved_no_price_stays_open():
    """price <= 0.05 but oracle not settled (is_resolved=False) and within oracle timeout → stays OPEN.

    Live sports have an 8h oracle timeout. We use _past(5) to stay within that window
    so the oracle-timeout fallback doesn't fire and the bet correctly waits for settlement.
    """
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Soccer")
    bet_id = bet.id
    db.close()

    _run_resolution_with_mock(price=0.02, end_dt=_past(5), is_resolved=False)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "OPEN", (
        f"Expected OPEN (oracle not settled), got {resolved.status}"
    )
    db.close()


def test_mid_range_price_stays_open():
    """Mid-range price (e.g. 0.5) on closed market → still waiting, stays OPEN."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Politics")
    bet_id = bet.id
    db.close()

    _run_resolution_with_mock(price=0.5, end_dt=_past(3), is_resolved=False)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "OPEN", f"Expected OPEN (mid-range price), got {resolved.status}"
    db.close()


def test_active_market_high_price_non_sport_resolves():
    """Non-sport with price >= 0.95 on still-open market (Case 3) → CLOSED_WIN."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Crypto")
    bet_id = bet.id
    db.close()

    # Market closes in 2h (still open), price converged to 0.96
    _run_resolution_with_mock(price=0.96, end_dt=_future(2), is_resolved=False)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "CLOSED_WIN", (
        f"Expected CLOSED_WIN (Case 3 threshold), got {resolved.status}"
    )
    db.close()


def test_active_live_sport_high_price_stays_open():
    """Live sport with high price on still-open market (Case 3) → NOT closed (live odds)."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Basketball")
    bet_id = bet.id
    db.close()

    # Market closes in 1h (still open), price 0.96 — live odds, NOT oracle settlement
    _run_resolution_with_mock(price=0.96, end_dt=_future(1), is_resolved=False)

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "OPEN", (
        f"Expected OPEN (live sport, Case 3 skipped), got {resolved.status}"
    )
    db.close()


def test_all_sports_categories_use_live_sport_guard():
    """Verify every live sport category is guarded against premature NO resolution."""
    live_sports = [
        "Soccer", "Basketball", "Tennis", "eSports",
        "American Football", "Baseball", "Hockey",
    ]
    for sport in live_sports:
        _clear_db()
        db = _new_db()
        bet = _make_open_bet(db, category=sport)
        bet_id = bet.id
        db.close()

        # 1h after close — within the 5h guard
        _run_resolution_with_mock(price=0.02, end_dt=_past(1), is_resolved=True)

        db = _new_db()
        resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
        assert resolved.status == "OPEN", (
            f"Guard NOT applied for sport={sport!r}: status={resolved.status}"
        )
        db.close()


def test_no_crash_with_none_price():
    """If price is None (API error), the bet is skipped but no exception raised."""
    _clear_db()
    db = _new_db()
    bet = _make_open_bet(db, category="Politics")
    bet_id = bet.id
    db.close()

    engine = BetEngine(polymarket_client=None)

    async def _mock_fetch(token_ids):
        return [(None, None, False)] * len(token_ids)

    with patch("backend.bet_engine.SessionLocal", _db_mod.SessionLocal):
        with patch.object(engine, "_fetch_resolution_data", side_effect=_mock_fetch):
            engine.check_resolution()  # must not raise

    db = _new_db()
    resolved = db.query(CopiedBet).filter_by(id=bet_id).first()
    assert resolved.status == "OPEN", "Bet should stay OPEN when price is None"
    db.close()


def test_multiple_bets_one_resolves():
    """
    When there are multiple bets, only the one with a decisive price resolves.
    Others stay OPEN. This also exercises that is_live_sport is correct per-bet.
    """
    _clear_db()
    db = _new_db()

    # Bet 1: Politics, market ended, price 0.97 → should close WIN
    bet1 = _make_open_bet(db, category="Politics", token_id="tok_politics")

    # Bet 2: Soccer, market ended 1h ago, price 0.02, is_resolved=True → guard keeps OPEN
    whale = db.query(Whale).filter_by(address="0xrestest").first()
    session = db.query(MonitoringSession).order_by(MonitoringSession.id.desc()).first()
    wb2 = WhaleBet(
        whale_id=whale.id, market_id="mkt_res2", token_id="tok_soccer",
        question="Soccer match?", side="BUY", outcome="YES",
        price=0.6, size_usdc=100.0, size_shares=166.0,
        timestamp=datetime.utcnow(), bet_type="OPEN",
    )
    db.add(wb2)
    db.commit()
    db.refresh(wb2)
    bet2 = CopiedBet(
        whale_bet_id=wb2.id, whale_address=whale.address,
        mode="SIMULATION", market_id="mkt_res2", token_id="tok_soccer",
        question="Soccer match?", side="BUY", outcome="YES",
        price_at_entry=0.6, size_usdc=10.0, size_shares=16.67,
        risk_factor=0.1, whale_bet_usdc=100.0, whale_avg_bet_usdc=100.0,
        status="OPEN", market_category="Soccer",
        opened_at=datetime.utcnow(),
    )
    db.add(bet2)
    db.commit()
    db.refresh(bet2)

    bet1_id, bet2_id = bet1.id, bet2.id
    db.close()

    engine = BetEngine(polymarket_client=None)

    async def _mock_fetch(token_ids):
        results = []
        for tid in token_ids:
            if tid == "tok_politics":
                results.append((0.97, _past(3), True))   # should resolve
            else:  # tok_soccer
                results.append((0.02, _past(1), True))   # guard — stays open
        return results

    with patch("backend.bet_engine.SessionLocal", _db_mod.SessionLocal):
        with patch.object(engine, "_fetch_resolution_data", side_effect=_mock_fetch):
            engine.check_resolution()

    db = _new_db()
    b1 = db.query(CopiedBet).filter_by(id=bet1_id).first()
    b2 = db.query(CopiedBet).filter_by(id=bet2_id).first()
    assert b1.status == "CLOSED_WIN", f"Politics bet should be CLOSED_WIN, got {b1.status}"
    assert b2.status == "OPEN", f"Soccer bet should stay OPEN (guard), got {b2.status}"
    db.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    print(f"\nRunning {len(tests)} resolution tests...\n")
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
