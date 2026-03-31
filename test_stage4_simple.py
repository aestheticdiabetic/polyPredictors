#!/usr/bin/env python3
"""
Stage 4 Implementation Verification — Simple Check
This verifies that all Stage 4 components are in place and functional.
"""

import sys
from collections import deque

print("\n" + "=" * 60)
print("STAGE 4 - SLIPPAGE TRACKING IMPLEMENTATION CHECK")
print("=" * 60)

try:
    # 1. Check TokenSlippageRecord model exists
    print("\n[1/5] Checking TokenSlippageRecord model...")
    from backend.database import TokenSlippageRecord

    assert hasattr(TokenSlippageRecord, "__tablename__")
    assert TokenSlippageRecord.__tablename__ == "token_slippage_records"
    print("    [OK] TokenSlippageRecord model defined")

    # 2. Check BetEngine has slippage infrastructure
    print("\n[2/5] Checking BetEngine slippage infrastructure...")
    from backend.bet_engine import BetEngine

    engine = BetEngine()
    assert hasattr(engine, "_slippage_register")
    assert hasattr(engine, "_record_slippage")
    assert hasattr(engine, "_get_avg_slippage")
    assert hasattr(engine, "_seed_slippage_register")
    print("    [OK] _slippage_register field exists")
    print("    [OK] _record_slippage method exists")
    print("    [OK] _get_avg_slippage method exists")
    print("    [OK] _seed_slippage_register method exists")

    # 3. Check config has slippage settings
    print("\n[3/5] Checking configuration...")
    from backend.config import settings

    assert hasattr(settings, "SLIPPAGE_TRACKING_ENABLED")
    assert hasattr(settings, "MAX_HISTORICAL_SLIPPAGE_PCT")
    assert settings.MAX_HISTORICAL_SLIPPAGE_PCT == 0.05  # default
    print(f"    [OK] SLIPPAGE_TRACKING_ENABLED = {settings.SLIPPAGE_TRACKING_ENABLED}")
    print(f"    [OK] MAX_HISTORICAL_SLIPPAGE_PCT = {settings.MAX_HISTORICAL_SLIPPAGE_PCT}")

    # 4. Check in-memory register behavior
    print("\n[4/5] Checking in-memory register behavior...")
    register = engine._slippage_register["test_token"]
    assert isinstance(register, deque)
    assert register.maxlen == 20  # Should be maxlen=20

    # Simulate recording slippage values
    for i in range(25):
        register.append(float(i))

    assert len(register) == 20  # Should keep only last 20
    print("    [OK] In-memory deque works correctly (maxlen=20)")
    print("    [OK] Added 25 values, kept last 20")

    # 5. Check that get_avg_slippage works
    print("\n[5/5] Checking average slippage calculation...")
    avg = engine._get_avg_slippage("test_token")
    assert avg is not None
    # Last 20 values should be 5..24, so average is (5+24)/2 = 14.5
    expected = sum(range(5, 25)) / 20
    assert abs(avg - expected) < 0.01
    print(f"    [OK] Average slippage calculation works: {avg:.2f}%")

    print("\n" + "=" * 60)
    print("[OK] ALL CHECKS PASSED - STAGE 4 IMPLEMENTATION COMPLETE")
    print("=" * 60)
    print("\nStage 4 components verified:")
    print("  - TokenSlippageRecord model")
    print("  - Slippage recording infrastructure")
    print("  - In-memory register with 20-record rolling window")
    print("  - Average slippage calculation")
    print("  - Configuration support")
    print("\nNext steps:")
    print("  1. Enable SLIPPAGE_TRACKING_ENABLED=true in .env")
    print("  2. Run bot normally to accumulate slippage data")
    print("  3. Check database for token_slippage_records")
    print("  4. Monitor entry gate for 'High historical slippage' skips")
    sys.exit(0)

except AssertionError as e:
    print(f"\n[FAIL] Assertion failed: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

except Exception as e:
    print(f"\n[FAIL] Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)
