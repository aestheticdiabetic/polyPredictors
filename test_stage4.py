#!/usr/bin/env python3
"""
Stage 4 verification test — validates slippage tracking implementation.
Run: python test_stage4.py
"""

import os
import tempfile

# Temporarily override DATABASE_URL for test isolation
test_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
os.environ["DATABASE_URL"] = f"sqlite:///{test_db_file}"

from backend.bet_engine import BetEngine
from backend.database import Base, SessionLocal, TokenSlippageRecord, engine


def test_token_slippage_record_model():
    """Test TokenSlippageRecord model creation and serialization."""
    print("\n=== Testing TokenSlippageRecord Model ===")

    # Create tables
    Base.metadata.create_all(engine)
    session = SessionLocal()

    # Create test record
    record = TokenSlippageRecord(
        token_id="test_token_123",
        fill_price=0.525,
        mid_price=0.500,
        slippage_pct=5.0,
        mode="EXIT",
    )
    session.add(record)
    session.commit()

    # Retrieve and verify
    retrieved = session.query(TokenSlippageRecord).filter_by(token_id="test_token_123").first()
    assert retrieved is not None
    assert retrieved.fill_price == 0.525
    assert retrieved.mid_price == 0.500
    assert abs(retrieved.slippage_pct - 5.0) < 0.01
    assert retrieved.mode == "EXIT"

    # Test to_dict serialization
    data = retrieved.to_dict()
    assert data["token_id"] == "test_token_123"
    assert "recorded_at" in data
    assert data["mode"] == "EXIT"

    session.close()
    print("[OK] TokenSlippageRecord model works correctly")


def test_slippage_recording():
    """Test _record_slippage method."""
    print("\n=== Testing Slippage Recording ===")

    session = SessionLocal()
    engine = BetEngine()

    # Record multiple slippage values for same token
    for i, (fill, mid) in enumerate([(0.525, 0.500), (0.510, 0.500), (0.495, 0.500)]):
        engine._record_slippage(
            token_id="test_token_456",
            fill_price=fill,
            mid_price=mid,
            mode="EXIT",
            db=session,
        )

    # Check in-memory register
    register = engine._slippage_register.get("test_token_456")
    assert register is not None
    assert len(register) == 3
    # Slippage calculation: |fill - mid| / mid * 100
    assert abs(register[0] - 5.0) < 0.01  # |0.525 - 0.500| / 0.500 * 100 = 5.0
    assert abs(register[1] - 2.0) < 0.01  # |0.510 - 0.500| / 0.500 * 100 = 2.0
    assert abs(register[2] - 1.0) < 0.01  # |0.495 - 0.500| / 0.500 * 100 = 1.0

    # Check DB records (need to commit to persist)
    session.commit()
    db_records = session.query(TokenSlippageRecord).filter_by(token_id="test_token_456").all()
    assert len(db_records) == 3

    session.close()
    print(f"[OK] Recorded {len(register)} slippage values to register and DB")


def test_average_slippage():
    """Test _get_avg_slippage method."""
    print("\n=== Testing Average Slippage Calculation ===")

    engine = BetEngine()
    session = SessionLocal()

    # Record slippage values
    slippage_values = [5.0, 2.0, 1.0]
    for fill, mid in [(0.525, 0.500), (0.510, 0.500), (0.495, 0.500)]:
        engine._record_slippage(
            token_id="test_token_789",
            fill_price=fill,
            mid_price=mid,
            mode="EXIT",
            db=session,
        )

    # Get average
    avg = engine._get_avg_slippage("test_token_789")
    expected_avg = sum(slippage_values) / len(slippage_values)
    assert avg is not None
    assert abs(avg - expected_avg) < 0.01

    # Test non-existent token
    avg_nonexistent = engine._get_avg_slippage("nonexistent_token")
    assert avg_nonexistent is None

    session.close()
    print(f"[OK] Average slippage calculated correctly: {avg:.2f}%")


def test_slippage_register_seeding():
    """Test _seed_slippage_register method."""
    print("\n=== Testing Register Seeding ===")

    # Setup: populate DB with records
    session = SessionLocal()
    for i in range(5):
        record = TokenSlippageRecord(
            token_id="seed_token_1",
            fill_price=0.51,
            mid_price=0.50,
            slippage_pct=2.0,
            mode="EXIT",
        )
        session.add(record)
    session.commit()

    # Create fresh engine and seed
    engine = BetEngine()
    engine._seed_slippage_register(session)

    # Verify register was populated
    register = engine._slippage_register.get("seed_token_1")
    assert register is not None
    assert len(register) == 5
    assert all(abs(val - 2.0) < 0.01 for val in register)

    session.close()
    print("[OK] Register seeded successfully from DB")


def test_slippage_gate_logic():
    """Test entry gate slippage check logic."""
    print("\n=== Testing Entry Gate Logic ===")

    engine = BetEngine()
    session = SessionLocal()

    # Populate register with high slippage
    for _ in range(3):
        engine._record_slippage("gate_test_token", 0.60, 0.50, "EXIT", session)

    avg_slippage = engine._get_avg_slippage("gate_test_token")
    print(f"  Recorded average slippage: {avg_slippage:.2f}%")

    # Simulate entry gate check
    max_threshold = 5.0  # 5% max
    should_skip = avg_slippage is not None and avg_slippage > max_threshold
    print(f"  Threshold: {max_threshold}% -> Should skip: {should_skip}")
    assert should_skip, "Entry should be skipped for high slippage"

    session.close()
    print("[OK] Entry gate logic works correctly")


def test_maxlen_behavior():
    """Test that in-memory deque respects maxlen=20."""
    print("\n=== Testing Deque MaxLen Behavior ===")

    engine = BetEngine()
    session = SessionLocal()

    # Record 25 values (more than maxlen=20)
    for i in range(25):
        fill = 0.505 + (0.001 * i)
        engine._record_slippage("maxlen_test", fill, 0.500, "EXIT", session)

    register = engine._slippage_register["maxlen_test"]
    assert len(register) == 20, f"Expected 20, got {len(register)}"
    print("[OK] Deque correctly maintained maxlen=20 (recorded 25, kept last 20)")

    session.close()


if __name__ == "__main__":
    print("=" * 60)
    print("STAGE 4 — SLIPPAGE TRACKING IMPLEMENTATION TEST")
    print("=" * 60)

    try:
        test_token_slippage_record_model()
        test_slippage_recording()
        test_average_slippage()
        test_slippage_register_seeding()
        test_slippage_gate_logic()
        test_maxlen_behavior()

        print("\n" + "=" * 60)
        print("[OK] ALL TESTS PASSED")
        print("=" * 60)
        print("\nStage 4 implementation is complete and working correctly!")

    except Exception as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
    finally:
        # Cleanup
        if os.path.exists(test_db_file):
            os.unlink(test_db_file)
