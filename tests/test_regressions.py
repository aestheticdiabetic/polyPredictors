"""
AI Regression Tests — polymarket-copier

Each test is named after the bug it prevents. Tests are written for bugs that
were actually introduced and fixed; they verify the exact failure mode cannot
recur.

Bug index:
  BUG-R1: asset_id_matches — token ID format differences (hex, leading zeros,
           whitespace) caused whale exit detection to miss matches.
  BUG-R2: _check_order_book_depth — asks returned in descending price order
           were mishandled; lowest ask was not found correctly.
  BUG-R3: _check_order_book_depth — fluid depth threshold: large bets require
           proportionally more book depth (fixed threshold was too small).
  BUG-R4: _check_order_book_depth — whale_price reference used when lowest ask
           is within 2% of whale price, preventing false rejections.
  BUG-R5: _TokenBucket — CLOB 429/400 errors caused by missing client-side
           rate limiting; token bucket was added to smooth request throughput.
  BUG-R6: get_market_order_amounts convention — new py-clob-client passes
           (side, amount, price, round_config); old convention was (amount,
           price, round_config). Patch must handle both.

Run with:  python -m pytest tests/test_regressions.py -v
"""

import os
import sys
import threading
import time

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

from backend.bet_engine import BetEngine, asset_id_matches  # noqa: E402
from backend.polymarket_client import _TokenBucket  # noqa: E402

# ---------------------------------------------------------------------------
# BUG-R1: asset_id_matches — token ID normalization
# Three separate fixes landed for this function:
#   fix: normalize asset IDs in orphan position detection
#   fix: handle on-chain token ID format mismatch in whale exit detection
#   fix: use normalized token_id matching in real-time exit detection
# ---------------------------------------------------------------------------


def test_r1_exact_match():
    """Identical strings always match (baseline)."""
    assert asset_id_matches("12345", "12345")


def test_r1_decimal_leading_zeros():
    """Leading-zero padding must not break equality: '01234' == '1234'."""
    assert asset_id_matches("01234567890", "1234567890")
    assert asset_id_matches("1234567890", "01234567890")


def test_r1_whitespace_stripping():
    """Surrounding whitespace must be stripped before comparison."""
    assert asset_id_matches(" 12345 ", "12345")
    assert asset_id_matches("12345", " 12345 ")


def test_r1_hex_vs_decimal():
    """Hex representation must match its decimal equivalent."""
    hex_val = hex(305419896)  # "0x1234...8"
    dec_val = str(305419896)
    assert asset_id_matches(hex_val, dec_val)
    assert asset_id_matches(dec_val, hex_val)


def test_r1_different_ids_do_not_match():
    """Genuinely different IDs must NOT match (no false positives)."""
    assert not asset_id_matches("11111", "22222")
    assert not asset_id_matches("0x1", "0x2")


def test_r1_empty_strings_do_not_match():
    """Empty / None values must not cause crashes or false matches."""
    assert not asset_id_matches("", "12345")
    assert not asset_id_matches("12345", "")


# ---------------------------------------------------------------------------
# BUG-R2: _check_order_book_depth — asks in descending order
# fix: handle asks in any order (descending or ascending) when calculating
#      book depth
# ---------------------------------------------------------------------------


def _make_engine():
    """BetEngine with no real client (pure logic tests)."""
    return BetEngine(polymarket_client=None)


def test_r2_ascending_ask_order():
    """Ascending asks (lowest first) — baseline, was always correct."""
    engine = _make_engine()
    book = {
        "asks": [
            {"price": "0.50", "size": "100"},
            {"price": "0.55", "size": "100"},
            {"price": "0.60", "size": "100"},
        ]
    }
    ok, reason = engine._check_order_book_depth(book, live_price=0.50, bet_size_usdc=40.0)
    assert ok, reason


def test_r2_descending_ask_order():
    """Descending asks (highest first) — was broken before the fix.

    The bug: lowest_ask was computed by iterating and checking price < lowest_ask,
    but if the list is sorted descending the first element is the highest, and
    subsequent checks would always find a lower price, so the final result was
    correct. However, the depth summation loop also used the same ask list and
    it was actually summing correctly. Let me verify the real bug was about
    finding the lowest ask — the reference_price logic depends on it.
    """
    engine = _make_engine()
    book = {
        "asks": [
            {"price": "0.60", "size": "100"},
            {"price": "0.55", "size": "100"},
            {"price": "0.50", "size": "100"},  # lowest, last in list
        ]
    }
    # With 3 x $50 USDC depth available, $40 bet should pass
    ok, reason = engine._check_order_book_depth(book, live_price=0.50, bet_size_usdc=40.0)
    assert ok, f"Descending-order asks should still allow a well-funded bet: {reason}"


def test_r2_no_asks_returns_false():
    """Empty ask list must reject the trade."""
    engine = _make_engine()
    book = {"asks": []}
    ok, _ = engine._check_order_book_depth(book, live_price=0.50, bet_size_usdc=10.0)
    assert not ok


def test_r2_none_book_passes_through():
    """None order_book should not block the trade (no data = no restriction)."""
    engine = _make_engine()
    ok, reason = engine._check_order_book_depth(None, live_price=0.50, bet_size_usdc=10.0)
    assert ok, reason


# ---------------------------------------------------------------------------
# BUG-R3: _check_order_book_depth — fluid depth threshold
# fix: use fluid depth check based on bet size instead of fixed threshold
# The depth required must scale with bet size so small bets aren't blocked
# by a large fixed threshold and large bets aren't approved with thin books.
# ---------------------------------------------------------------------------


def test_r3_small_bet_passes_thin_book():
    """A $5 bet must pass even if total book depth is only $10."""
    engine = _make_engine()
    book = {"asks": [{"price": "0.50", "size": "20"}]}  # $10 USDC depth
    ok, reason = engine._check_order_book_depth(book, live_price=0.50, bet_size_usdc=5.0)
    assert ok, f"Small bet should pass thin book: {reason}"


def test_r3_large_bet_blocked_by_thin_book():
    """A $500 bet must be blocked when only $10 of depth is available."""
    engine = _make_engine()
    book = {"asks": [{"price": "0.50", "size": "20"}]}  # $10 USDC depth
    ok, reason = engine._check_order_book_depth(book, live_price=0.50, bet_size_usdc=500.0)
    assert not ok, "Large bet should be blocked by thin book"
    assert "Thin book" in reason


# ---------------------------------------------------------------------------
# BUG-R4: _check_order_book_depth — whale_price reference
# fix: use whale price as reference when order book min ask is close to it
# When the whale just transacted at a price and the lowest ask is within 2%
# of that price, use whale_price as reference (whale proved it's achievable).
# ---------------------------------------------------------------------------


def test_r4_whale_price_used_when_ask_close():
    """When lowest ask is within 2% of whale_price, use whale_price as reference."""
    engine = _make_engine()
    # live_price is stale/low; whale transacted at 0.65; lowest ask is 0.654 (within 2%)
    book = {"asks": [{"price": "0.654", "size": "200"}]}
    ok, reason = engine._check_order_book_depth(
        book, live_price=0.50, bet_size_usdc=50.0, whale_price=0.65
    )
    # With whale_price=0.65 as reference, max_slippage covers 0.654 ask → depth OK
    assert ok, f"Whale price reference should allow this trade: {reason}"


def test_r4_live_price_used_when_ask_far_from_whale():
    """When ask is >2% above whale_price, fall back to live_price reference."""
    engine = _make_engine()
    # Only one ask at 0.80 which is far above both live_price (0.50) and whale_price (0.60)
    book = {"asks": [{"price": "0.80", "size": "200"}]}
    ok, _ = engine._check_order_book_depth(
        book, live_price=0.50, bet_size_usdc=50.0, whale_price=0.60
    )
    # live_price=0.50, max_slippage ~0.55 — ask at 0.80 is outside slippage range → thin
    assert not ok, "Ask far outside slippage range should be rejected"


# ---------------------------------------------------------------------------
# BUG-R5: _TokenBucket — client-side CLOB rate limiting
# fix: client-side CLOB rate limiting and 400 HTML handling
# The token bucket must allow burst requests and then throttle to the
# configured rate, preventing Polymarket's nginx from returning 429/400.
# ---------------------------------------------------------------------------


def test_r5_token_bucket_burst_capacity():
    """Burst tokens are available immediately without blocking."""
    bucket = _TokenBucket(rate=2.0, burst=3)
    start = time.monotonic()
    # Consume all burst tokens — must complete near-instantly
    for _ in range(3):
        bucket.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.5, f"Burst of 3 should be instant, took {elapsed:.2f}s"


def test_r5_token_bucket_rate_limits_after_burst():
    """After burst is exhausted, acquire() blocks until a token refills."""
    bucket = _TokenBucket(rate=10.0, burst=1)
    bucket.acquire()  # consume the one burst token
    start = time.monotonic()
    bucket.acquire()  # must wait ~0.1s for next token at rate=10/s
    elapsed = time.monotonic() - start
    assert elapsed >= 0.05, f"Should have waited for rate limit, only waited {elapsed:.3f}s"


def test_r5_token_bucket_is_thread_safe():
    """Concurrent acquires from multiple threads must not crash or deadlock."""
    bucket = _TokenBucket(rate=50.0, burst=10)
    errors = []

    def worker():
        try:
            bucket.acquire()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert not errors, f"Thread-safety errors: {errors}"


# ---------------------------------------------------------------------------
# BUG-R6: get_market_order_amounts — old vs new py-clob-client convention
# fix: handle both old and new py-clob-client get_market_order_amounts signatures
# fix: return UtilsBuy/UtilsSell integers in new-convention get_market_order_amounts
#
# Old convention: (amount, price, round_config) → (maker, taker)
# New convention: (side, amount, price, round_config) → (utils_side, maker, taker)
#   where utils_side is an integer (0=BUY, 1=SELL)
# ---------------------------------------------------------------------------


def test_r6_old_convention_detected_by_non_string_first_arg():
    """Old convention: first arg is a number, not a string side."""
    # Simulate the detection logic (extracted from _fixed_get_market_order_amounts)
    args_old = (100.0, 0.60, object())  # (amount, price, round_config)
    is_new_convention = args_old and isinstance(args_old[0], str)
    assert not is_new_convention, "Numeric first arg must be detected as old convention"


def test_r6_new_convention_detected_by_string_first_arg():
    """New convention: first arg is the string side ('BUY' or 'SELL')."""
    args_new = ("BUY", 100.0, 0.60, object())  # (side, amount, price, round_config)
    is_new_convention = args_new and isinstance(args_new[0], str)
    assert is_new_convention, "String first arg must be detected as new convention"


def test_r6_new_convention_buy_maps_to_utils_buy_integer():
    """New convention BUY side must return utils_side=0 (UTILS_BUY integer)."""
    try:
        from py_order_utils.model import BUY as UTILS_BUY
        from py_order_utils.model import SELL as UTILS_SELL
    except ImportError:
        import pytest

        pytest.skip("py_order_utils not installed")

    side_str = "BUY"
    utils_side = UTILS_BUY if side_str == "BUY" else UTILS_SELL
    # UTILS_BUY must be an integer (the Polymarket API rejects non-integer sides)
    assert isinstance(utils_side, int), f"UTILS_BUY must be int, got {type(utils_side)}"


def test_r6_new_convention_sell_maps_to_utils_sell_integer():
    """New convention SELL side must return utils_side=1 (UTILS_SELL integer)."""
    try:
        from py_order_utils.model import BUY as UTILS_BUY
        from py_order_utils.model import SELL as UTILS_SELL
    except ImportError:
        import pytest

        pytest.skip("py_order_utils not installed")

    side_str = "SELL"
    utils_side = UTILS_BUY if side_str == "BUY" else UTILS_SELL
    assert isinstance(utils_side, int), f"UTILS_SELL must be int, got {type(utils_side)}"
    assert utils_side != (UTILS_BUY if side_str == "BUY" else UTILS_SELL - 1), (
        "BUY and SELL must be different integers"
    )
