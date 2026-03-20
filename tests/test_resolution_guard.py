"""
Tests for:
  1. Premature NO-resolution guard in bet_engine.py (Case 1)
  2. Market categorizer correctness (categorizer.py)

Run with:  python tests/test_resolution_guard.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

# ---------------------------------------------------------------------------
# 1. Resolution guard — pure logic replica of the Case 1 branch
# ---------------------------------------------------------------------------

LIVE_SPORTS = {
    "Soccer", "Basketball", "Tennis", "eSports",
    "American Football", "Baseball", "Hockey",
}

MIN_HOURS_AFTER_CLOSE = 5.0  # must match bet_engine.py


def _case1_action(category: str, hours_since_close: float, price: float, is_resolved: bool) -> str:
    """
    Mirrors the Case 1 branch of check_resolution().
    Returns 'close_yes' | 'close_no' | 'skip_premature' | 'wait'
    """
    is_live_sport = category in LIVE_SPORTS
    if price >= 0.95:
        return "close_yes"
    if price <= 0.05 and is_resolved:
        if is_live_sport and hours_since_close < MIN_HOURS_AFTER_CLOSE:
            return "skip_premature"
        return "close_no"
    return "wait"


# --- Historical bug reproductions ---

def test_bucks_jazz_0_08h_skipped():
    """Bug: Bucks/Jazz 229.5 closed 5 min after game start."""
    assert _case1_action("Basketball", 0.08, 0.0, True) == "skip_premature"

def test_bucks_jazz_228_5_0_10h_skipped():
    """Bug: Bucks/Jazz 228.5 closed 6 min after game start."""
    assert _case1_action("Basketball", 0.10, 0.0, True) == "skip_premature"

def test_cavaliers_bulls_1_1h_skipped():
    """Bug: Cavaliers/Bulls 240.5 closed 1h 6m after game start."""
    assert _case1_action("Basketball", 1.1, 0.0, True) == "skip_premature"

def test_rangers_blue_jackets_2_44h_skipped():
    """Bug: Rangers/Blue Jackets 5.5 closed 2h 26m after game start (NHL)."""
    assert _case1_action("Hockey", 2.44, 0.0, True) == "skip_premature"

# --- Threshold boundary ---

def test_just_under_threshold_skipped():
    """4.99h < 5.0h → still guarded."""
    assert _case1_action("Hockey", 4.99, 0.0, True) == "skip_premature"

def test_exactly_at_threshold_closes():
    """Exactly 5.0h: guard no longer applies."""
    assert _case1_action("Basketball", 5.0, 0.0, True) == "close_no"

def test_well_past_threshold_closes():
    """7.27h: confirmed-legitimate oracle settlement (observed in prod data)."""
    assert _case1_action("Soccer", 7.27, 0.0, True) == "close_no"

# --- YES wins are always immediate ---

def test_yes_win_closes_immediately_basketball():
    assert _case1_action("Basketball", 0.01, 0.97, True) == "close_yes"

def test_yes_win_closes_immediately_hockey():
    assert _case1_action("Hockey", 0.05, 0.96, False) == "close_yes"

# --- Non-sports are never guarded ---

def test_politics_no_early_closes():
    assert _case1_action("Politics", 1.0, 0.0, True) == "close_no"

def test_crypto_no_early_closes():
    assert _case1_action("Crypto", 0.5, 0.0, True) == "close_no"

def test_other_no_early_closes():
    """'Other' is not in LIVE_SPORTS → no guard."""
    assert _case1_action("Other", 0.1, 0.0, True) == "close_no"

# --- Unresolved oracle always waits ---

def test_no_without_oracle_confirmation_waits():
    assert _case1_action("Basketball", 10.0, 0.0, False) == "wait"

def test_mid_range_price_waits():
    assert _case1_action("Basketball", 8.0, 0.5, True) == "wait"

# --- All live sport categories are guarded ---

def test_all_live_sport_categories_guarded():
    for sport in LIVE_SPORTS:
        result = _case1_action(sport, 2.0, 0.0, True)
        assert result == "skip_premature", f"Guard missing for {sport!r}"


# ---------------------------------------------------------------------------
# 2. Categorizer correctness
# ---------------------------------------------------------------------------

from categorizer import classify

def test_rangers_blue_jackets_is_hockey():
    """NY Rangers vs Columbus Blue Jackets = NHL Hockey."""
    sport, _ = classify("Rangers vs. Blue Jackets: O/U 5.5")
    assert sport == "Hockey", f"got {sport!r}"

def test_rangers_fc_aberdeen_is_soccer():
    """Rangers FC vs Aberdeen FC = Scottish Premiership Soccer, NOT Hockey."""
    sport, _ = classify("Rangers FC vs. Aberdeen FC: O/U 3.5")
    assert sport == "Soccer", f"got {sport!r}"

def test_red_bulls_is_soccer_not_basketball():
    """New York Red Bulls = MLS Soccer; 'BULLS' substring must not match NBA."""
    sport, _ = classify("Charlotte FC vs. New York Red Bulls: O/U 2.5")
    assert sport == "Soccer", f"got {sport!r}"

def test_sabres_sharks_is_hockey():
    sport, _ = classify("Sabres vs. Sharks: O/U 5.5")
    assert sport == "Hockey", f"got {sport!r}"

def test_utah_golden_knights_is_hockey():
    """Utah HC vs Vegas Golden Knights = NHL."""
    sport, _ = classify("Utah vs. Golden Knights")
    assert sport == "Hockey", f"got {sport!r}"

def test_bucks_jazz_is_basketball():
    sport, _ = classify("Bucks vs. Jazz: O/U 229.5")
    assert sport == "Basketball", f"got {sport!r}"

def test_cavaliers_bulls_is_basketball():
    """'BULLS' here is a pure NBA team, not Red Bulls."""
    sport, _ = classify("Cavaliers vs. Bulls: O/U 240.5")
    assert sport == "Basketball", f"got {sport!r}"

def test_ou_bet_type():
    _, bet_type = classify("Rangers vs. Blue Jackets: O/U 5.5")
    assert bet_type == "Over/Under", f"got {bet_type!r}"

def test_spread_bet_type():
    _, bet_type = classify("Spread: Sabres (-1.5)")
    assert bet_type == "Spread", f"got {bet_type!r}"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {name}  {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {name}  {e}")
            failed += 1

    print(f"\n{passed}/{passed + failed} passed", end="")
    if failed:
        print(f"  *** {failed} FAILURES ***")
        sys.exit(1)
    else:
        print("  OK")
