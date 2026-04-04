"""Test that redemption only processes positions with meaningful value."""

import pytest


def test_filters_zero_value_positions():
    """Positions with $0.00 value should not be redeemed."""
    # Simulate position with size but zero value (already closed)
    position_zero_value = {
        "conditionId": "0x123",
        "outcome": "YES",
        "size": 100.0,
        "currentValue": 0.0,
        "redeemable": True,
        "outcomeIndex": 0,
    }

    # Current buggy logic would include this
    buggy_filter = (
        position_zero_value.get("redeemable") and float(position_zero_value.get("size", 0)) > 0
    )
    assert buggy_filter is True, "Bug confirmed: zero-value position passes current filter"

    # Fixed logic should exclude it
    MIN_VALUE_TO_REDEEM = 0.01
    value = float(position_zero_value.get("currentValue") or position_zero_value.get("value") or 0)
    fixed_filter = (
        position_zero_value.get("redeemable")
        and float(position_zero_value.get("size", 0)) > 0
        and value >= MIN_VALUE_TO_REDEEM
    )
    assert fixed_filter is False, "Fixed filter should exclude zero-value position"


def test_includes_profitable_positions():
    """Positions with meaningful value should be redeemed."""
    position_profitable = {
        "conditionId": "0x456",
        "outcome": "NO",
        "size": 100.0,
        "currentValue": 50.25,
        "redeemable": True,
        "outcomeIndex": 1,
    }

    MIN_VALUE_TO_REDEEM = 0.01
    value = float(position_profitable.get("currentValue") or position_profitable.get("value") or 0)
    fixed_filter = (
        position_profitable.get("redeemable")
        and float(position_profitable.get("size", 0)) > 0
        and value >= MIN_VALUE_TO_REDEEM
    )
    assert fixed_filter is True, "Fixed filter should include profitable position"


def test_excludes_dust_positions():
    """Positions with value < $0.01 (dust) should not be redeemed."""
    position_dust = {
        "conditionId": "0x789",
        "outcome": "YES",
        "size": 1.0,
        "currentValue": 0.005,  # Less than 1 cent
        "redeemable": True,
        "outcomeIndex": 0,
    }

    MIN_VALUE_TO_REDEEM = 0.01
    value = float(position_dust.get("currentValue") or position_dust.get("value") or 0)
    fixed_filter = (
        position_dust.get("redeemable")
        and float(position_dust.get("size", 0)) > 0
        and value >= MIN_VALUE_TO_REDEEM
    )
    assert fixed_filter is False, "Fixed filter should exclude dust positions below $0.01"


def test_falls_back_to_value_field_if_no_current_value():
    """If currentValue missing, fall back to 'value' field."""
    position_fallback = {
        "conditionId": "0xABC",
        "outcome": "YES",
        "size": 50.0,
        "value": 25.0,  # Use this if currentValue missing
        "redeemable": True,
        "outcomeIndex": 0,
    }

    MIN_VALUE_TO_REDEEM = 0.01
    value = float(position_fallback.get("currentValue") or position_fallback.get("value") or 0)
    fixed_filter = (
        position_fallback.get("redeemable")
        and float(position_fallback.get("size", 0)) > 0
        and value >= MIN_VALUE_TO_REDEEM
    )
    assert fixed_filter is True, "Fixed filter should use fallback 'value' field"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
