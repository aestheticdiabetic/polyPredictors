"""
Tests for SIMULATION / HEDGE_SIM data isolation.

Covers every API endpoint touched by the mode-filter changes and the
hedge-guard logic in bet_engine.py.

Sections
--------
1.  _apply_mode_filter helper (pure logic)
2.  /api/ledger         — mode isolation
3.  /api/ledger/stats   — stats isolation + correct session balance per mode
4.  /api/activity       — isolation
5.  /api/signals + /api/signals/stats — isolation via CopiedBet JOIN
6.  /api/stats/by-whale + /api/stats/by-whale-category — isolation
7.  /api/sessions/latest — mode-scoped session lookup
8.  Hedge guard (bet_engine) — fires in HEDGE_SIM, silent in SIMULATION
9.  Standard dashboard JS contract — mode=standard excludes HEDGE_SIM

Run with:  python tests/test_mode_isolation.py
"""

import sys
import os
import types
from datetime import datetime

# ---------------------------------------------------------------------------
# Path setup — allow imports from backend/
# ---------------------------------------------------------------------------
ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "backend"))

# ---------------------------------------------------------------------------
# In-memory DB — must be done BEFORE importing main.py / database.py so
# that the engine points at :memory: and not the real SQLite file.
# ---------------------------------------------------------------------------
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# StaticPool keeps a single connection alive for all sessions — required so
# that tables created on the engine are visible to subsequent sessions.
_test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# Patch database module before anything else imports it
import importlib
import backend.database as _db_mod

_db_mod.engine = _test_engine
_db_mod.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_test_engine)

# Now create tables on the in-memory engine
from backend.database import (
    Base, CopiedBet, MonitoringSession, Whale, WhaleBet,
    AddToPositionSignal, get_db,
)
Base.metadata.create_all(bind=_test_engine)

# ---------------------------------------------------------------------------
# FastAPI test client
# ---------------------------------------------------------------------------
# Stub out heavy external dependencies before importing main.py
# so the tests don't need a real Polymarket connection.
from unittest.mock import MagicMock, AsyncMock, patch

_mock_poly = MagicMock()
_mock_poly.get_wallet_balance = AsyncMock(return_value=500.0)
_mock_poly.close = AsyncMock()

_mock_monitor = MagicMock()
_mock_monitor.is_running.return_value = False
_mock_monitor.get_status.return_value = {"is_running": False}
_mock_monitor.start_monitoring = MagicMock()
_mock_monitor.stop_monitoring = MagicMock()
_mock_monitor.shutdown = MagicMock()
_mock_monitor.refresh_risk_profiles = MagicMock()
_mock_monitor.reset_last_seen = MagicMock()

# Patch at module level so main.py uses the mocks
with patch("backend.main.poly_client", _mock_poly), \
     patch("backend.main.whale_monitor", _mock_monitor), \
     patch("backend.main.init_db", lambda: None):
    import backend.main as _main_mod
    # Re-point the app's DB dependency to use the test session factory
    _main_mod.app.dependency_overrides[get_db] = lambda: _db_mod.SessionLocal()

from fastapi.testclient import TestClient
client = TestClient(_main_mod.app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_db():
    return _db_mod.SessionLocal()


def _make_whale(db, address="0xwhale1", alias="TestWhale") -> Whale:
    existing = db.query(Whale).filter_by(address=address).first()
    if existing:
        return existing
    w = Whale(address=address, alias=alias, avg_bet_size_usdc=100.0)
    db.add(w)
    db.commit()
    db.refresh(w)
    return w


def _make_whale_bet(db, whale: Whale, market_id="mkt1", token_id="tok_yes",
                    outcome="YES", side="BUY") -> WhaleBet:
    wb = WhaleBet(
        whale_id=whale.id,
        market_id=market_id,
        token_id=token_id,
        question="Will X happen?",
        side=side,
        outcome=outcome,
        price=0.6,
        size_usdc=100.0,
        size_shares=166.0,
        timestamp=datetime.utcnow(),
        bet_type="OPEN" if side == "BUY" else "EXIT",
    )
    db.add(wb)
    db.commit()
    db.refresh(wb)
    return wb


def _make_bet(db, mode: str, status: str = "OPEN", pnl: float = None,
              market_id: str = "mkt1", token_id: str = "tok_yes",
              outcome: str = "YES", whale_address: str = "0xwhale1") -> CopiedBet:
    db_w = _make_whale(db, whale_address)
    wb = _make_whale_bet(db, db_w, market_id=market_id, token_id=token_id, outcome=outcome)
    b = CopiedBet(
        whale_bet_id=wb.id,
        whale_address=whale_address,
        mode=mode,
        market_id=market_id,
        token_id=token_id,
        question="Test market",
        side="BUY",
        outcome=outcome,
        price_at_entry=0.6,
        size_usdc=10.0,
        size_shares=16.0,
        risk_factor=0.1,
        whale_bet_usdc=100.0,
        whale_avg_bet_usdc=100.0,
        status=status,
        pnl_usdc=pnl,
        opened_at=datetime.utcnow(),
    )
    db.add(b)
    db.commit()
    db.refresh(b)
    return b


def _make_session(db, mode: str, is_active: bool = False,
                  balance: float = 200.0) -> MonitoringSession:
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


def _make_signal(db, copied_bet: CopiedBet) -> AddToPositionSignal:
    db_w = db.query(Whale).filter_by(address=copied_bet.whale_address).first()
    wb = _make_whale_bet(db, db_w, market_id=copied_bet.market_id + "_sig")
    sig = AddToPositionSignal(
        whale_bet_id=wb.id,
        copied_bet_id=copied_bet.id,
        whale_additional_usdc=50.0,
        price=0.65,
        timestamp=datetime.utcnow(),
        suggested_add_usdc=5.0,
    )
    db.add(sig)
    db.commit()
    db.refresh(sig)
    return sig


def _clear_db():
    """Wipe all rows between tests."""
    db = _new_db()
    db.query(AddToPositionSignal).delete()
    db.query(CopiedBet).delete()
    db.query(WhaleBet).delete()
    db.query(MonitoringSession).delete()
    db.query(Whale).delete()
    db.commit()
    db.close()


# ---------------------------------------------------------------------------
# 1. _apply_mode_filter helper
# ---------------------------------------------------------------------------

def test_apply_mode_filter_simulation():
    """mode=SIMULATION filters to only SIMULATION rows."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "REAL")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/ledger?mode=SIMULATION")
    assert r.status_code == 200
    bets = r.json()["bets"]
    assert all(b["mode"] == "SIMULATION" for b in bets), f"Got modes: {[b['mode'] for b in bets]}"
    assert len(bets) == 1


def test_apply_mode_filter_hedge_sim():
    """mode=HEDGE_SIM returns only HEDGE_SIM rows."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/ledger?mode=HEDGE_SIM")
    assert r.status_code == 200
    bets = r.json()["bets"]
    assert all(b["mode"] == "HEDGE_SIM" for b in bets)
    assert len(bets) == 1


def test_apply_mode_filter_standard_excludes_hedge_sim():
    """mode=standard returns SIMULATION+REAL but NOT HEDGE_SIM."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "REAL")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/ledger?mode=standard")
    assert r.status_code == 200
    bets = r.json()["bets"]
    modes = {b["mode"] for b in bets}
    assert "HEDGE_SIM" not in modes, f"HEDGE_SIM leaked into standard: {modes}"
    assert len(bets) == 2


def test_apply_mode_filter_all_returns_everything():
    """mode=all returns rows from all three modes."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "REAL")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/ledger?mode=all")
    assert r.status_code == 200
    modes = {b["mode"] for b in r.json()["bets"]}
    assert modes == {"SIMULATION", "REAL", "HEDGE_SIM"}


# ---------------------------------------------------------------------------
# 2. /api/ledger — mode isolation
# ---------------------------------------------------------------------------

def test_ledger_hedge_sim_bets_not_in_simulation_view():
    """HEDGE_SIM bets must not appear when requesting mode=SIMULATION."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="OPEN")
    _make_bet(db, "HEDGE_SIM", status="OPEN")
    db.close()

    r = client.get("/api/ledger?mode=SIMULATION")
    bets = r.json()["bets"]
    assert len(bets) == 1
    assert bets[0]["mode"] == "SIMULATION"


def test_ledger_simulation_bets_not_in_hedge_sim_view():
    """SIMULATION bets must not appear when requesting mode=HEDGE_SIM."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="OPEN")
    _make_bet(db, "HEDGE_SIM", status="OPEN")
    db.close()

    r = client.get("/api/ledger?mode=HEDGE_SIM")
    bets = r.json()["bets"]
    assert len(bets) == 1
    assert bets[0]["mode"] == "HEDGE_SIM"


def test_ledger_status_filter_works_within_mode():
    """Status filters (wins/open/etc.) work correctly within a mode scope."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="OPEN")
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=5.0)
    _make_bet(db, "HEDGE_SIM", status="OPEN")
    db.close()

    r = client.get("/api/ledger?mode=SIMULATION&status=open")
    bets = r.json()["bets"]
    assert len(bets) == 1
    assert bets[0]["status"] == "OPEN"
    assert bets[0]["mode"] == "SIMULATION"


# ---------------------------------------------------------------------------
# 3. /api/ledger/stats — isolation + session balance
# ---------------------------------------------------------------------------

def test_stats_standard_excludes_hedge_sim_pnl():
    """P&L in mode=standard must not include HEDGE_SIM bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=10.0)
    _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=99.0)  # must be excluded
    db.close()

    r = client.get("/api/ledger/stats?mode=standard")
    assert r.status_code == 200
    data = r.json()
    assert data["total_pnl_usdc"] == 10.0, f"Expected 10.0, got {data['total_pnl_usdc']}"


def test_stats_hedge_sim_excludes_simulation_pnl():
    """P&L in mode=HEDGE_SIM must not include SIMULATION bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=99.0)  # must be excluded
    _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=7.0)
    db.close()

    r = client.get("/api/ledger/stats?mode=HEDGE_SIM")
    data = r.json()
    assert data["total_pnl_usdc"] == 7.0, f"Expected 7.0, got {data['total_pnl_usdc']}"


def test_stats_standard_win_rate_ignores_hedge_sim():
    """Win rate in mode=standard must be computed from SIMULATION+REAL only."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=5.0)
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=5.0)
    _make_bet(db, "HEDGE_SIM", status="CLOSED_LOSS", pnl=-50.0)  # must not drag win rate down
    db.close()

    r = client.get("/api/ledger/stats?mode=standard")
    data = r.json()
    assert data["win_rate_pct"] == 100.0, f"Expected 100.0, got {data['win_rate_pct']}"


def test_stats_balance_uses_standard_session_when_hedge_sim_active():
    """
    When a HEDGE_SIM session is active, mode=standard should NOT use its balance.
    It should fall back to SIM_STARTING_BALANCE (no standard session active).
    """
    db = _new_db()
    _clear_db()
    _make_session(db, "HEDGE_SIM", is_active=True, balance=999.0)
    db.close()

    r = client.get("/api/ledger/stats?mode=standard")
    data = r.json()
    from backend.config import settings
    assert data["session_balance"] == settings.SIM_STARTING_BALANCE, (
        f"Standard stats used HEDGE_SIM balance! Got {data['session_balance']}"
    )


def test_stats_balance_uses_hedge_sim_session_when_active():
    """mode=HEDGE_SIM stats should use the HEDGE_SIM session's balance."""
    db = _new_db()
    _clear_db()
    _make_session(db, "HEDGE_SIM", is_active=True, balance=175.0)
    db.close()

    r = client.get("/api/ledger/stats?mode=HEDGE_SIM")
    data = r.json()
    assert data["session_balance"] == 175.0, f"Expected 175.0, got {data['session_balance']}"


def test_stats_balance_uses_simulation_session_when_active():
    """mode=standard uses the active SIMULATION session's balance."""
    db = _new_db()
    _clear_db()
    _make_session(db, "SIMULATION", is_active=True, balance=185.0)
    db.close()

    r = client.get("/api/ledger/stats?mode=standard")
    data = r.json()
    assert data["session_balance"] == 185.0, f"Expected 185.0, got {data['session_balance']}"


# ---------------------------------------------------------------------------
# 4. /api/activity — isolation
# ---------------------------------------------------------------------------

def test_activity_standard_excludes_hedge_sim():
    """Activity feed with mode=standard must not show HEDGE_SIM bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/activity?mode=standard")
    assert r.status_code == 200
    items = r.json()["activity"]
    modes = {i["mode"] for i in items}
    assert "HEDGE_SIM" not in modes, f"HEDGE_SIM appeared in standard activity: {modes}"


def test_activity_hedge_sim_excludes_standard():
    """Activity feed with mode=HEDGE_SIM must not show SIMULATION/REAL bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/activity?mode=HEDGE_SIM")
    items = r.json()["activity"]
    modes = {i["mode"] for i in items}
    assert modes == {"HEDGE_SIM"}, f"Expected only HEDGE_SIM, got {modes}"


def test_activity_all_returns_all_modes():
    """Activity with no mode filter returns all modes."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION")
    _make_bet(db, "HEDGE_SIM")
    db.close()

    r = client.get("/api/activity?mode=all")
    modes = {i["mode"] for i in r.json()["activity"]}
    assert "SIMULATION" in modes and "HEDGE_SIM" in modes


# ---------------------------------------------------------------------------
# 5. /api/signals + /api/signals/stats — isolation via JOIN
# ---------------------------------------------------------------------------

def test_signals_standard_excludes_hedge_sim():
    """Signals linked to HEDGE_SIM CopiedBets must not appear in mode=standard."""
    db = _new_db()
    _clear_db()
    sim_bet = _make_bet(db, "SIMULATION")
    hedge_bet = _make_bet(db, "HEDGE_SIM")
    _make_signal(db, sim_bet)
    _make_signal(db, hedge_bet)
    db.close()

    r = client.get("/api/signals?mode=standard")
    assert r.status_code == 200
    signals = r.json()["signals"]
    # All returned signals must belong to a non-HEDGE_SIM bet
    for sig in signals:
        assert sig.get("bet_status") is not None  # signal enriched — from copied_bet
    # Pagination total should be 1, not 2
    assert r.json()["pagination"]["total"] == 1, (
        f"Expected 1 standard signal, got {r.json()['pagination']['total']}"
    )


def test_signals_hedge_sim_excludes_standard():
    """Signals linked to SIMULATION CopiedBets must not appear in mode=HEDGE_SIM."""
    db = _new_db()
    _clear_db()
    sim_bet = _make_bet(db, "SIMULATION")
    hedge_bet = _make_bet(db, "HEDGE_SIM")
    _make_signal(db, sim_bet)
    _make_signal(db, hedge_bet)
    db.close()

    r = client.get("/api/signals?mode=HEDGE_SIM")
    assert r.json()["pagination"]["total"] == 1


def test_signals_stats_standard_excludes_hedge_sim():
    """Signal stats for mode=standard must not count HEDGE_SIM signals."""
    db = _new_db()
    _clear_db()
    sim_bet = _make_bet(db, "SIMULATION")
    hedge_bet = _make_bet(db, "HEDGE_SIM")
    _make_signal(db, sim_bet)
    _make_signal(db, sim_bet)   # 2 sim signals
    _make_signal(db, hedge_bet) # 1 hedge signal — must be excluded
    db.close()

    r = client.get("/api/signals/stats?mode=standard")
    assert r.status_code == 200
    assert r.json()["total_signals"] == 2, (
        f"Expected 2, got {r.json()['total_signals']}"
    )


def test_signals_stats_hedge_sim():
    """Signal stats for mode=HEDGE_SIM counts only HEDGE_SIM signals."""
    db = _new_db()
    _clear_db()
    sim_bet = _make_bet(db, "SIMULATION")
    hedge_bet = _make_bet(db, "HEDGE_SIM")
    _make_signal(db, sim_bet)   # must be excluded
    _make_signal(db, hedge_bet)
    _make_signal(db, hedge_bet)
    db.close()

    r = client.get("/api/signals/stats?mode=HEDGE_SIM")
    assert r.json()["total_signals"] == 2


# ---------------------------------------------------------------------------
# 6. /api/stats/by-whale + /api/stats/by-whale-category — isolation
# ---------------------------------------------------------------------------

def test_by_whale_standard_excludes_hedge_sim():
    """Per-whale stats for mode=standard must not include HEDGE_SIM bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=20.0)
    _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=999.0)  # must not appear
    db.close()

    r = client.get("/api/stats/by-whale?mode=standard")
    assert r.status_code == 200
    whales = r.json()["whales"]
    # Only 1 whale with total_pnl=20, not 1019
    assert len(whales) == 1
    assert whales[0]["total_pnl_usdc"] == 20.0, (
        f"Expected 20.0, got {whales[0]['total_pnl_usdc']}"
    )


def test_by_whale_hedge_sim_excludes_standard():
    """Per-whale stats for mode=HEDGE_SIM must not include SIMULATION bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=999.0)  # must not appear
    _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=15.0)
    db.close()

    r = client.get("/api/stats/by-whale?mode=HEDGE_SIM")
    whales = r.json()["whales"]
    assert len(whales) == 1
    assert whales[0]["total_pnl_usdc"] == 15.0


def test_by_whale_category_standard_excludes_hedge_sim():
    """Category breakdown for mode=standard must not count HEDGE_SIM bets."""
    db = _new_db()
    _clear_db()
    _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=5.0)
    _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=500.0)
    db.close()

    r = client.get("/api/stats/by-whale-category?mode=standard")
    assert r.status_code == 200
    whales = r.json()["whales"]
    assert len(whales) == 1
    # Total sport P&L must only reflect the SIMULATION bet
    total = sum(s["total_pnl_usdc"] for s in whales[0]["by_sport"])
    assert total == 5.0, f"Expected 5.0 pnl, got {total}"


# ---------------------------------------------------------------------------
# 7. /api/sessions/latest — mode-scoped lookup
# ---------------------------------------------------------------------------

def test_sessions_latest_standard_skips_hedge_sim():
    """mode=standard must return most recent SIM/REAL session, not HEDGE_SIM."""
    db = _new_db()
    _clear_db()
    sim_s = _make_session(db, "SIMULATION", is_active=False, balance=190.0)
    _make_session(db, "HEDGE_SIM", is_active=False, balance=155.0)  # created last
    db.close()

    r = client.get("/api/sessions/latest?mode=standard")
    assert r.status_code == 200
    s = r.json()["session"]
    assert s is not None
    assert s["mode"] == "SIMULATION", f"Got mode={s['mode']} instead of SIMULATION"


def test_sessions_latest_hedge_sim_skips_simulation():
    """mode=HEDGE_SIM must return most recent HEDGE_SIM session."""
    db = _new_db()
    _clear_db()
    _make_session(db, "SIMULATION", is_active=False)
    hedge_s = _make_session(db, "HEDGE_SIM", is_active=False, balance=160.0)
    db.close()

    r = client.get("/api/sessions/latest?mode=HEDGE_SIM")
    s = r.json()["session"]
    assert s["mode"] == "HEDGE_SIM"
    assert s["current_balance_usdc"] == 160.0


def test_sessions_latest_all_returns_most_recent():
    """mode=all returns the single most recent session regardless of mode."""
    db = _new_db()
    _clear_db()
    _make_session(db, "SIMULATION", is_active=False)
    _make_session(db, "HEDGE_SIM", is_active=False)
    db.close()

    r = client.get("/api/sessions/latest?mode=all")
    s = r.json()["session"]
    assert s["mode"] == "HEDGE_SIM"  # last inserted → highest id


def test_sessions_latest_returns_none_when_no_matching_session():
    """mode=HEDGE_SIM returns null if no HEDGE_SIM session exists."""
    db = _new_db()
    _clear_db()
    _make_session(db, "SIMULATION", is_active=False)
    db.close()

    r = client.get("/api/sessions/latest?mode=HEDGE_SIM")
    assert r.json()["session"] is None


# ---------------------------------------------------------------------------
# 8. Hedge guard (bet_engine)
# ---------------------------------------------------------------------------

def _build_engine_fixtures(db, mode: str):
    """
    Create a Whale, WhaleBet (YES), an existing OPEN CopiedBet (YES),
    and a new WhaleBet (NO) to trigger the hedge-guard path.
    Returns (session, existing_yes_bet, no_whale_bet).
    """
    session = MonitoringSession(
        mode=mode,
        is_active=True,
        starting_balance_usdc=200.0,
        current_balance_usdc=200.0,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    whale = Whale(address="0xhedgetest", alias="HedgeTester", avg_bet_size_usdc=100.0)
    db.add(whale)
    db.commit()
    db.refresh(whale)

    # First WhaleBet — YES side already copied
    wb_yes = WhaleBet(
        whale_id=whale.id, market_id="mkt_hedge", token_id="tok_yes",
        question="Will Y happen?", side="BUY", outcome="YES",
        price=0.55, size_usdc=100.0, size_shares=181.0, timestamp=datetime.utcnow(),
        bet_type="OPEN",
    )
    db.add(wb_yes)
    db.commit()
    db.refresh(wb_yes)

    existing_yes = CopiedBet(
        whale_bet_id=wb_yes.id, whale_address=whale.address,
        mode=mode, market_id="mkt_hedge", token_id="tok_yes",
        question="Will Y happen?", side="BUY", outcome="YES",
        price_at_entry=0.55, size_usdc=10.0, size_shares=18.0,
        risk_factor=0.1, whale_bet_usdc=100.0, whale_avg_bet_usdc=100.0,
        status="OPEN", opened_at=datetime.utcnow(),
    )
    db.add(existing_yes)
    db.commit()
    db.refresh(existing_yes)

    # Second WhaleBet — NO side (the hedge signal)
    wb_no = WhaleBet(
        whale_id=whale.id, market_id="mkt_hedge", token_id="tok_no",
        question="Will Y happen?", side="BUY", outcome="NO",
        price=0.45, size_usdc=80.0, size_shares=177.0, timestamp=datetime.utcnow(),
        bet_type="OPEN",
    )
    db.add(wb_no)
    db.commit()
    db.refresh(wb_no)

    return session, existing_yes, wb_no


def test_hedge_guard_fires_in_hedge_sim_mode():
    """When HEDGE_SIM session is active and whale holds YES, a new NO signal is SKIPPED."""
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()
    session, _, wb_no = _build_engine_fixtures(db, "HEDGE_SIM")

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(wb_no, session, db)

    assert result is not None, "Expected a CopiedBet to be created (as SKIPPED)"
    assert result.status == "SKIPPED", f"Expected SKIPPED, got {result.status}"
    assert "hedg" in result.skip_reason.lower(), (
        f"Skip reason doesn't mention hedging: {result.skip_reason!r}"
    )
    db.close()


def test_hedge_guard_silent_in_simulation_mode():
    """In SIMULATION mode the hedge guard must NOT fire — both sides get copied."""
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()
    session, _, wb_no = _build_engine_fixtures(db, "SIMULATION")

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(wb_no, session, db)

    # In SIMULATION mode the hedge guard must not fire.
    # The bet may still be SKIPPED for other reasons (e.g. no market info in tests),
    # but the skip_reason must NOT mention hedging.
    if result is not None and result.status == "SKIPPED":
        assert "hedg" not in (result.skip_reason or "").lower(), (
            f"Hedge guard fired in SIMULATION mode — it should not! reason={result.skip_reason!r}"
        )
    db.close()


def test_hedge_guard_does_not_fire_for_different_whale():
    """Hedge guard must only check positions held by the SAME whale."""
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()
    # Build fixtures under "HEDGE_SIM" mode normally — YES held by 0xhedgetest
    session, existing_yes, wb_no = _build_engine_fixtures(db, "HEDGE_SIM")

    # Make the NEW bet belong to a DIFFERENT whale
    other_whale = Whale(address="0xother", alias="Other", avg_bet_size_usdc=100.0)
    db.add(other_whale)
    db.commit()
    db.refresh(other_whale)
    wb_no.whale_id = other_whale.id
    db.commit()

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(wb_no, session, db)

    # Different whale — hedge guard must not fire (may still skip for other reasons)
    if result is not None and result.status == "SKIPPED":
        assert "hedg" not in (result.skip_reason or "").lower(), (
            f"Hedge guard fired across different whales — cross-whale suppression bug! reason={result.skip_reason!r}"
        )
    db.close()


def test_hedge_guard_does_not_fire_for_closed_opposite():
    """Hedge guard must ignore CLOSED opposite positions — only OPEN ones count."""
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()
    session, existing_yes, wb_no = _build_engine_fixtures(db, "HEDGE_SIM")

    # Close the existing YES position
    existing_yes.status = "CLOSED_WIN"
    db.commit()

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(wb_no, session, db)

    # The YES bet is closed — hedge guard must not fire (may skip for other reasons)
    if result is not None and result.status == "SKIPPED":
        assert "hedg" not in (result.skip_reason or "").lower(), (
            f"Hedge guard fired on a CLOSED position — false positive! reason={result.skip_reason!r}"
        )
    db.close()


# ---------------------------------------------------------------------------
# 9. Standard dashboard JS contract — verify via API
# ---------------------------------------------------------------------------

def test_standard_mode_contract_no_hedge_sim_in_any_endpoint():
    """
    Simulate what app.js calls on page load: activity, ledger, stats, signals.
    None should contain any HEDGE_SIM data when mode=standard is used.
    """
    db = _new_db()
    _clear_db()
    sim_bet = _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=10.0)
    hedge_bet = _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=999.0)
    _make_signal(db, sim_bet)
    _make_signal(db, hedge_bet)
    _make_session(db, "SIMULATION", is_active=False, balance=190.0)
    _make_session(db, "HEDGE_SIM", is_active=True, balance=150.0)
    db.close()

    # /api/ledger
    bets = client.get("/api/ledger?mode=standard").json()["bets"]
    assert all(b["mode"] != "HEDGE_SIM" for b in bets), "Ledger leaked HEDGE_SIM"

    # /api/ledger/stats
    stats = client.get("/api/ledger/stats?mode=standard").json()
    assert stats["total_pnl_usdc"] == 10.0, f"Stats pnl leaked: {stats['total_pnl_usdc']}"
    assert stats["session_balance"] != 150.0, "Stats balance used HEDGE_SIM session!"

    # /api/activity
    activity = client.get("/api/activity?mode=standard").json()["activity"]
    assert all(a["mode"] != "HEDGE_SIM" for a in activity), "Activity leaked HEDGE_SIM"

    # /api/signals
    sigs = client.get("/api/signals?mode=standard").json()
    assert sigs["pagination"]["total"] == 1, f"Signals leaked HEDGE_SIM: total={sigs['pagination']['total']}"

    # /api/signals/stats
    sig_stats = client.get("/api/signals/stats?mode=standard").json()
    assert sig_stats["total_signals"] == 1, f"Signal stats leaked: {sig_stats['total_signals']}"

    # /api/sessions/latest
    latest = client.get("/api/sessions/latest?mode=standard").json()["session"]
    assert latest is not None
    assert latest["mode"] == "SIMULATION", f"Latest session leaked HEDGE_SIM: {latest['mode']}"


def test_hedge_sim_contract_no_standard_in_any_endpoint():
    """
    Simulate what hedge.js calls: none should contain SIMULATION data.
    """
    db = _new_db()
    _clear_db()
    sim_bet = _make_bet(db, "SIMULATION", status="CLOSED_WIN", pnl=999.0)
    hedge_bet = _make_bet(db, "HEDGE_SIM", status="CLOSED_WIN", pnl=8.0)
    _make_signal(db, sim_bet)
    _make_signal(db, hedge_bet)
    _make_session(db, "SIMULATION", is_active=False)
    _make_session(db, "HEDGE_SIM", is_active=True, balance=177.0)
    db.close()

    # /api/ledger
    bets = client.get("/api/ledger?mode=HEDGE_SIM").json()["bets"]
    assert all(b["mode"] == "HEDGE_SIM" for b in bets), "Hedge ledger shows SIMULATION bets"

    # /api/ledger/stats
    stats = client.get("/api/ledger/stats?mode=HEDGE_SIM").json()
    assert stats["total_pnl_usdc"] == 8.0, f"Hedge stats pnl leaked: {stats['total_pnl_usdc']}"
    assert stats["session_balance"] == 177.0, f"Hedge balance wrong: {stats['session_balance']}"

    # /api/activity
    activity = client.get("/api/activity?mode=HEDGE_SIM").json()["activity"]
    assert all(a["mode"] == "HEDGE_SIM" for a in activity), "Hedge activity shows SIMULATION bets"

    # /api/signals
    sigs = client.get("/api/signals?mode=HEDGE_SIM").json()
    assert sigs["pagination"]["total"] == 1

    # /api/sessions/latest
    latest = client.get("/api/sessions/latest?mode=HEDGE_SIM").json()["session"]
    assert latest["mode"] == "HEDGE_SIM"
    assert latest["current_balance_usdc"] == 177.0


# ---------------------------------------------------------------------------
# 10. HEDGE_SIM uses simulate_buy / simulate_sell — never place_real_buy/sell
# ---------------------------------------------------------------------------

def _make_market_info():
    """Minimal market_info dict that passes all guards in process_new_whale_bet."""
    from datetime import timedelta
    return {
        "active": True,
        "closed": False,
        "resolved": False,
        "status": "open",
        "endDate": (datetime.utcnow() + timedelta(hours=48)).isoformat() + "Z",
    }


def test_hedge_sim_buy_uses_simulate_not_real():
    """
    HEDGE_SIM open-position bets must call simulate_buy, not place_real_buy.
    Regression: before the fix, mode != "SIMULATION" fell through to place_real_buy,
    causing 'No module named eip712_structs' errors in HEDGE_SIM mode.
    """
    from backend.bet_engine import BetEngine
    from unittest.mock import MagicMock, patch

    db = _new_db()
    _clear_db()

    session = MonitoringSession(
        mode="HEDGE_SIM", is_active=True,
        starting_balance_usdc=200.0, current_balance_usdc=200.0,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    whale = Whale(address="0xbuyer", alias="Buyer", avg_bet_size_usdc=100.0)
    db.add(whale)
    db.commit()
    db.refresh(whale)

    wb = WhaleBet(
        whale_id=whale.id, market_id="mkt_buy", token_id="tok_buy",
        question="Will buy happen?", side="BUY", outcome="YES",
        price=0.5, size_usdc=100.0, size_shares=200.0,
        timestamp=datetime.utcnow(), bet_type="OPEN",
    )
    db.add(wb)
    db.commit()
    db.refresh(wb)

    engine = BetEngine(polymarket_client=None)

    # Patch place_real_buy to detect if it gets called (it should NOT be)
    with patch.object(engine, "place_real_buy", side_effect=AssertionError(
        "place_real_buy was called in HEDGE_SIM mode — regression!"
    )):
        result = engine.process_new_whale_bet(
            wb, session, db,
            market_info=_make_market_info(),
            live_price=0.5,
        )

    # The bet must be OPEN (simulated), not failed/skipped due to real order attempt
    assert result is not None, "No CopiedBet returned"
    assert result.status == "OPEN", (
        f"Expected OPEN (simulated), got {result.status}: {result.skip_reason}"
    )
    assert result.mode == "HEDGE_SIM"
    # Balance should have been deducted by simulate_buy
    db.refresh(session)
    assert session.current_balance_usdc < 200.0, (
        "Balance unchanged — simulate_buy was not called"
    )
    db.close()


def test_hedge_sim_exit_uses_simulate_sell_not_real():
    """
    HEDGE_SIM exit (whale SELL) must call simulate_sell, not place_real_sell.
    Regression: _handle_exit only checked mode == 'SIMULATION'; HEDGE_SIM fell
    through to place_real_sell.
    """
    from backend.bet_engine import BetEngine
    from unittest.mock import patch

    db = _new_db()
    _clear_db()

    session = MonitoringSession(
        mode="HEDGE_SIM", is_active=True,
        starting_balance_usdc=200.0, current_balance_usdc=190.0,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    whale = Whale(address="0xseller", alias="Seller", avg_bet_size_usdc=100.0)
    db.add(whale)
    db.commit()
    db.refresh(whale)

    # Simulate an already-open position we copied
    wb_open = WhaleBet(
        whale_id=whale.id, market_id="mkt_exit", token_id="tok_exit",
        question="Exit test?", side="BUY", outcome="YES",
        price=0.5, size_usdc=100.0, size_shares=200.0,
        timestamp=datetime.utcnow(), bet_type="OPEN",
    )
    db.add(wb_open)
    db.commit()
    db.refresh(wb_open)

    open_pos = CopiedBet(
        whale_bet_id=wb_open.id, whale_address=whale.address,
        mode="HEDGE_SIM", market_id="mkt_exit", token_id="tok_exit",
        question="Exit test?", side="BUY", outcome="YES",
        price_at_entry=0.5, size_usdc=10.0, size_shares=20.0,
        risk_factor=0.1, whale_bet_usdc=100.0, whale_avg_bet_usdc=100.0,
        status="OPEN", opened_at=datetime.utcnow(),
    )
    db.add(open_pos)
    db.commit()
    db.refresh(open_pos)

    # Whale now exits (SELL)
    wb_exit = WhaleBet(
        whale_id=whale.id, market_id="mkt_exit", token_id="tok_exit",
        question="Exit test?", side="SELL", outcome="YES",
        price=0.75, size_usdc=150.0, size_shares=200.0,
        timestamp=datetime.utcnow(), bet_type="EXIT",
    )
    db.add(wb_exit)
    db.commit()
    db.refresh(wb_exit)

    engine = BetEngine(polymarket_client=None)

    with patch.object(engine, "place_real_sell", side_effect=AssertionError(
        "place_real_sell was called in HEDGE_SIM mode — regression!"
    )):
        result = engine.process_new_whale_bet(wb_exit, session, db)

    # Position must have been closed via simulate_sell
    db.refresh(open_pos)
    assert open_pos.status in ("CLOSED_WIN", "CLOSED_LOSS", "CLOSED_NEUTRAL"), (
        f"Position was not closed — simulate_sell was not called. Status={open_pos.status}"
    )
    db.close()


def test_hedge_sim_open_bet_recorded_with_correct_mode():
    """CopiedBets created in HEDGE_SIM mode must have mode='HEDGE_SIM', not 'SIMULATION'."""
    from backend.bet_engine import BetEngine

    db = _new_db()
    _clear_db()

    session = MonitoringSession(
        mode="HEDGE_SIM", is_active=True,
        starting_balance_usdc=200.0, current_balance_usdc=200.0,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    whale = Whale(address="0xmodecheck", alias="ModeCheck", avg_bet_size_usdc=100.0)
    db.add(whale)
    db.commit()
    db.refresh(whale)

    wb = WhaleBet(
        whale_id=whale.id, market_id="mkt_mode", token_id="tok_mode",
        question="Mode recorded?", side="BUY", outcome="YES",
        price=0.6, size_usdc=100.0, size_shares=166.0,
        timestamp=datetime.utcnow(), bet_type="OPEN",
    )
    db.add(wb)
    db.commit()
    db.refresh(wb)

    engine = BetEngine(polymarket_client=None)
    result = engine.process_new_whale_bet(
        wb, session, db,
        market_info=_make_market_info(),
        live_price=0.6,
    )

    assert result is not None
    assert result.mode == "HEDGE_SIM", (
        f"CopiedBet mode recorded as '{result.mode}' instead of 'HEDGE_SIM'"
    )
    db.close()


def test_background_exit_hedge_sim_uses_simulate_sell():
    """
    whale_monitor._handle_exit_trades must call simulate_sell for HEDGE_SIM positions.
    Regression: the background exit poller only checked mode == 'SIMULATION'.
    """
    from backend.bet_engine import BetEngine
    from backend.whale_monitor import WhaleMonitor
    from unittest.mock import patch, AsyncMock, MagicMock
    import asyncio

    db = _new_db()
    _clear_db()

    session = MonitoringSession(
        mode="HEDGE_SIM", is_active=True,
        starting_balance_usdc=200.0, current_balance_usdc=190.0,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    whale = Whale(address="0xbgexit", alias="BgExit", avg_bet_size_usdc=100.0)
    db.add(whale)
    db.commit()
    db.refresh(whale)

    # Existing open HEDGE_SIM position
    wb_open = WhaleBet(
        whale_id=whale.id, market_id="mkt_bg", token_id="tok_bg",
        question="BG exit test?", side="BUY", outcome="YES",
        price=0.5, size_usdc=100.0, size_shares=200.0,
        timestamp=datetime.utcnow(), bet_type="OPEN",
        tx_hash="txopen_bg",
    )
    db.add(wb_open)
    db.commit()
    db.refresh(wb_open)

    open_pos = CopiedBet(
        whale_bet_id=wb_open.id, whale_address=whale.address,
        mode="HEDGE_SIM", market_id="mkt_bg", token_id="tok_bg",
        question="BG exit test?", side="BUY", outcome="YES",
        price_at_entry=0.5, size_usdc=10.0, size_shares=20.0,
        risk_factor=0.1, whale_bet_usdc=100.0, whale_avg_bet_usdc=100.0,
        status="OPEN", opened_at=datetime.utcnow(),
    )
    db.add(open_pos)
    db.commit()
    db.refresh(open_pos)
    db.close()

    # Raw trade dict simulating a SELL from the whale
    sell_trade = {
        "transactionHash": "txexit_bg",
        "side": "SELL",
        "outcome": "YES",
        "price": "0.75",
        "usdcSize": "15.0",
        "shares": "20.0",
        "asset": "tok_bg",
        "conditionId": "mkt_bg",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    engine = BetEngine(polymarket_client=None)
    mock_client = MagicMock()
    mock_client.get_user_activity = AsyncMock(return_value=[sell_trade])

    monitor = WhaleMonitor(bet_engine=engine, polymarket_client=mock_client)
    monitor._resolution_scheduler.shutdown(wait=False)

    with patch("backend.whale_monitor.SessionLocal", _db_mod.SessionLocal), \
         patch("backend.bet_engine.SessionLocal", _db_mod.SessionLocal), \
         patch.object(engine, "place_real_sell", side_effect=AssertionError(
             "place_real_sell called for HEDGE_SIM background exit — regression!"
         )):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(
                monitor._handle_exit_trades("0xbgexit", [sell_trade])
            )
        finally:
            loop.close()

    # Verify position was closed via simulate_sell
    check_db = _db_mod.SessionLocal()
    try:
        pos = check_db.query(CopiedBet).filter_by(id=open_pos.id).first()
        assert pos.status in ("CLOSED_WIN", "CLOSED_LOSS", "CLOSED_NEUTRAL"), (
            f"Background exit did not close HEDGE_SIM position — status={pos.status}"
        )
    finally:
        check_db.close()


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
