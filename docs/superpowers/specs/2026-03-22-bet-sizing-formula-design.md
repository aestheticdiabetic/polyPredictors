# Bet Sizing Formula — Design Spec
**Date:** 2026-03-22
**Status:** Approved

---

## Context

The polymarket-copier bot follows whale traders on Polymarket. When a whale places a bet, we compute a copy-bet size and place it. The current formula has two problems:

1. **SIM mode inflates sub-$1 bets to $1.00**, distorting performance tracking. A low-conviction whale bet appears identically sized to a high-conviction bet in the ledger.
2. **SIM and REAL mode diverge** on sub-$1 bets: REAL skips them, SIM inflates them. This makes SIM a poor predictor of live performance.
3. **The conviction multiplier is linear**, which undersells strong signals (2x conviction → 2x bet, but a 2x bet probably signals more than 2x the edge).

Starting capital is $50 USDC with the expectation of 24/7 operation and continuous wallet growth.

---

## Goals

- Preserve the whale's edge signal faithfully — don't distort bet sizing at either end of the conviction range
- SIM and REAL modes behave identically — same formula, same floors, same logic
- Never skip or inflate a bet due to a sub-$1 calculation — the formula must naturally produce ≥ $1 at all times with a $50+ wallet
- Amplify high-conviction signals more than the current linear formula does

---

## Formula

### Step 1 — Conviction ratio

```python
conviction = whale_bet_usdc / whale_avg_bet_usdc
```

### Step 2 — Non-linear risk factor (change to `calculate_risk_factor`)

```python
if conviction >= 1.0:
    # Convex curve above average: rewards high conviction disproportionately
    risk_factor = min(conviction ** conviction_exponent, MAX_RISK_FACTOR)
else:
    # Linear below average: gently penalises low conviction, no harsh cliff
    risk_factor = max(conviction, MIN_RISK_FACTOR)
```

`CONVICTION_EXPONENT = 1.5` (configurable via env var). At this value:

| Conviction | Old risk_factor | New risk_factor | Change |
|---|---|---|---|
| 0.5x (weak) | 0.50x | 0.50x | unchanged |
| 1.0x (average) | 1.00x | 1.00x | unchanged |
| 1.5x (moderate) | 1.50x | 1.84x | +22% |
| 2.0x (strong) | 2.00x | 2.83x | +41% |
| 3.0x (very strong) | 3.00x | 3.00x | capped |

### Step 3 — Bet size (change to `calculate_bet_size`)

```python
base_bet  = session_balance * max_bet_pct        # e.g., $50 × 5% = $2.50
bet_size  = base_bet * risk_factor
hard_cap  = base_bet * MAX_RISK_FACTOR           # e.g., $2.50 × 3.0 = $7.50

result = min(bet_size, hard_cap)                 # apply ceiling first
result = max(settings.MIN_BET_USDC, result)      # floor at $1 (CLOB minimum)
result = min(result, session_balance)            # never exceed full balance
```

The `hard_cap` is unchanged from current behavior. The `max(MIN_BET_USDC, ...)` floor is also unchanged. With default settings ($50 wallet, 5% MAX_BET_PCT, 0.5 MIN_RISK_FACTOR), the natural minimum output is $50 × 5% × 0.5 = **$1.25**, which exceeds $1.00, so the floor is almost never reached.

---

## Mode Unification

Remove the SIM/REAL divergence in `process_new_whale_bet()` (lines 272–289 in `bet_engine.py`):

**Current (bad):**
```python
if bet_size_usdc < settings.MIN_BET_USDC:
    if mode == "REAL":
        return self._create_skipped_bet(...)   # REAL: skip
    # Simulation: clamp up so ledger always shows a valid size
    bet_size_usdc = settings.MIN_BET_USDC
```

**New (remove entirely):**
```python
# Block removed — formula guarantees bet_size >= MIN_BET_USDC via max() in calculate_bet_size
```

Similarly, remove the dead-code check in `retry_drift_watchlist()` (line 2083):
```python
# Remove this block — impossible to reach with formula-level floor in place
if mode == "REAL" and item.bet_size_usdc < settings.MIN_BET_USDC:
    ...
```

---

## New `calculate_risk_factor` Signature

```python
def calculate_risk_factor(
    self,
    whale_bet_usdc: float,
    whale_avg_bet_usdc: float,
    conviction_exponent: float = 1.0,   # new param; defaults to 1.0 (current linear behavior)
) -> float:
```

The default of `1.0` ensures backward compatibility if the method is called without the new argument. Call sites that should use the non-linear formula must explicitly pass `settings.CONVICTION_EXPONENT`:

**Call sites to update (pass `conviction_exponent=settings.CONVICTION_EXPONENT`):**

| Location | Line | Function |
|---|---|---|
| `bet_engine.py` | ~203 | `process_new_whale_bet()` — post-calibration risk factor |
| `bet_engine.py` | ~1946 | `_calc_add_to_position_signal()` — add-to-position sizing |

**Call site to leave unchanged (uses default 1.0):**

| Location | Line | Function |
|---|---|---|
| `bet_engine.py` | ~125 | `refresh_risk_profiles()` — display/storage only, not used for betting |

---

## Calibration Period — Intentionally Unchanged

The calibration path in `process_new_whale_bet()` (lines 190–199, `placed_count < _CALIBRATION_THRESHOLD`) bypasses `calculate_risk_factor` entirely and uses `settings.MIN_BET_USDC` directly. **This is intentionally left unchanged.** The current whale has >15 bets so calibration is not relevant to the immediate use case.

---

## New Config Parameter

Add to `backend/config.py`:
```python
CONVICTION_EXPONENT: float = float(os.getenv("CONVICTION_EXPONENT", "1.5"))
```

All existing parameters (`MAX_BET_PCT`, `MIN_BET_USDC`, `MIN_RISK_FACTOR`, `MAX_RISK_FACTOR`) are preserved and unchanged.

---

## Files to Modify

| File | Change |
|---|---|
| `backend/config.py` | Add `CONVICTION_EXPONENT` setting (default 1.5) |
| `backend/risk_calculator.py` | `calculate_risk_factor()` — add `conviction_exponent` param, apply non-linear curve for conviction ≥ 1.0 |
| `backend/bet_engine.py` | (1) Remove sub-$1 skip/clamp block in `process_new_whale_bet()` lines 272–289. (2) Remove dead check in `retry_drift_watchlist()` line 2083. (3) Pass `conviction_exponent=settings.CONVICTION_EXPONENT` at call sites ~203 and ~1946. |

---

## Verification

1. **Unit test `calculate_risk_factor`**: at conviction 1.5 expect 1.84 (±0.01), at 2.0 expect 2.83, at 3.0 expect 3.0 (capped). At conviction 0.5 expect 0.5 (unchanged). At conviction 1.0 expect 1.0 (pivot point).
2. **Regression test**: Pass `conviction_exponent=1.0` and verify output matches old linear formula exactly.
3. **Integration**: Run bot in SIM mode against the monitored whale. Confirm no `"clamping to $1"` log lines appear. Confirm no `"Below $1 minimum"` skips appear. Confirm bet sizes scale non-linearly with whale conviction.
