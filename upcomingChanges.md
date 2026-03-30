# Polymarket Copy-Bot: Upcoming Changes

## Context

The bot currently enters trades via 2s HTTP polling, has no order book awareness before entry, exits purely reactively when the whale exits, does not track per-whale win rates (Whale.win_count is always 0), and has no filtering for thin markets or wash trades. These 9 features address the core latency, risk, and edge-decay problems that limit performance. All features default to disabled — existing behavior is fully preserved unless explicitly enabled.

---

## Dependency Order

```
Stage 1 (data foundation + auth prewarm)
    +---> Stage 2 (entry gate filters)
    |         +---> Stage 4 (slippage tracking)
    +---> Stage 3 (independent exits)
    +---> Stage 5 (CLOB WebSocket entries)   [independent]
    +---> Stage 6 (adaptive sizing)          [needs Stage 1 data to mature]
```

---

## Stage 1 — Data Quality Foundation + Auth Prewarm

**Features:** Fix Whale.win_count (5a), CLOB session pre-warm (6-code-portion)

**Why first:** Every later stage depends on accurate per-whale performance data. win_count has always been 0 — useless for adaptive sizing. Pre-warming auth is a free 3-line win.

### Files to Modify

**`backend/database.py`** — `_migrate()` function
- Add migration entry: `("whales", "win_rate_window_json", "TEXT")` — stores JSON list of last N outcomes (1=win, 0=loss)

**`backend/bet_engine.py`** — `simulate_sell()` (~L987) and REAL close path in `_close_bet()` (~L1301)
- After pnl is known, load the Whale record and update win_count, total_bets_tracked, and win_rate_window_json (JSON deque capped at WHALE_PERF_WINDOW)
- Wrap in try/except so a DB error never blocks a close

**`backend/main.py`** — `lifespan()` (~L103)
- After existing startup tasks, add: call `poly_client._get_clob_client()` if REAL credentials are present — initializes the cached CLOB singleton before the first trade

**`backend/config.py`**
- Add: `WHALE_PERF_WINDOW: int = int(os.getenv("WHALE_PERF_WINDOW", "50"))`

### New DB Schema
```
whales.win_rate_window_json  TEXT  nullable — JSON list of 0/1, max WHALE_PERF_WINDOW entries
```

### Verification
1. Run SIMULATION, close several positions
2. Query `SELECT address, win_count, win_rate_window_json FROM whales` — confirm non-zero counts
3. Confirm Whale.to_dict() win_rate_pct is non-zero
4. Confirm zero change to entry/exit behavior

### Regression Risk: Very Low

---

## Stage 2 — Pre-Entry Risk Filters

**Features:** Orderbook depth check (2), Wash trade filter (7)

**Why together:** Both are pure entry gates implemented as new guard clauses inside `process_new_whale_bet()`. Neither requires WebSocket infrastructure.

### Files to Modify

**`backend/whale_monitor.py`** — `check_whale_activity()` (~L416)
- Extend the existing `asyncio.gather()` call to also fetch `client.get_order_book(token_id)`
- Pass `order_book` dict as new kwarg into `process_new_whale_bet()`

**`backend/whale_chain_monitor.py`** — `_dispatch_entry()` (~L732)
- Extend the existing `asyncio.gather()` call to also fetch `client.get_order_book(token_id)`
- Pass to `_sync_open_position()` → `process_new_whale_bet()`

**`backend/bet_engine.py`** — `process_new_whale_bet()` (~L79)
- Add `order_book: dict | None = None` kwarg (backward-compatible default)
- Insert two new guard clauses between the drift check and the placement block:

  **Guard 1 — Orderbook depth** (if `ORDER_BOOK_CHECK_ENABLED`):
  - New method `_check_order_book_depth(order_book, live_price, bet_size_usdc) -> (bool, str)`
  - Sum ask-side USDC up to price `live_price * (1 + MAX_BOOK_SLIPPAGE_PCT)`
  - Fail if sum < `MIN_BOOK_DEPTH_USDC`

  **Guard 2 — Wash trade** (if `WASH_TRADE_DETECTION_ENABLED`):
  - New method `_check_wash_trade(whale_bet, db) -> (bool, str)`
  - Query WhaleBet for same whale+token with side=SELL within last `WASH_TRADE_WINDOW_MINUTES`
  - If a prior sell exists in window → skip with reason "Wash trade suspected"

**`backend/config.py`**
```
ORDER_BOOK_CHECK_ENABLED=false
MIN_BOOK_DEPTH_USDC=50.0
MAX_BOOK_SLIPPAGE_PCT=0.03
WASH_TRADE_DETECTION_ENABLED=false
WASH_TRADE_WINDOW_MINUTES=30
```

### Verification
1. Enable `ORDER_BOOK_CHECK_ENABLED=true`, `MIN_BOOK_DEPTH_USDC=999999` → every entry skips with "Thin book" reason. Check CopiedBet.skip_reason in DB.
2. Reset to realistic value, confirm entries proceed normally
3. For wash trade: manually insert a WhaleBet SELL record for whale+token within window → confirm subsequent BUY is skipped
4. Disable both flags → identical behavior to pre-Stage 2

### Regression Risk: Low
- New kwarg with default None is fully backward-compatible
- One extra concurrent HTTP call per gather adds ~50-100ms detection latency (acceptable vs 2s polling baseline)

---

## Stage 3 — Independent Exit Logic

**Features:** Harvest threshold (3a), Near-close harvest (8), Soft exit confirmation (3b)

**Why together:** All three intercept the exit path and share the concept of "exit independent of whale action." They use the same close machinery.

### Files to Modify

**`backend/bet_engine.py`**
- New method `check_and_harvest_positions(db, session)`:
  - Fetches all OPEN CopiedBets for the session
  - For each: gets current price (from `_resolution_cache` or `get_best_price()`)
  - **Feature 3a**: if `current_price > price_at_entry * HARVEST_MULTIPLIER` → close via `_close_all_tranches()`, close_reason="Harvest threshold"
  - **Feature 8**: if market closes within `HARVEST_NEAR_CLOSE_HOURS` AND `current_price > price_at_entry * HARVEST_NEAR_CLOSE_MIN_MULTIPLIER` → close, close_reason="Near-close harvest"

- New in-memory field: `_pending_exits: dict[int, PendingExit]` keyed by copied_bet_id
  - PendingExit dataclass: `copied_bet_id, whale_exit_price, entered_at_monotonic, session_id`

- Modify `_handle_exit()` (~L2629) — **Feature 3b** soft exit filter:
  - Before calling close: if `EXIT_CONFIRMATION_ENABLED` and `current_price > price_at_entry * (1 + EXIT_CONFIRMATION_BUFFER)` → add to `_pending_exits`, defer close
  - New method `_check_pending_exits()`: runs on resolution scheduler cycle; closes if timeout exceeded or price dropped below buffer

**`backend/whale_monitor.py`** — resolution scheduler job registration
- Register `bet_engine.check_and_harvest_positions` to run every `RESOLUTION_CHECK_INTERVAL_SECONDS`

**`backend/config.py`**
```
HARVEST_ENABLED=false
HARVEST_MULTIPLIER=1.8
EXIT_CONFIRMATION_ENABLED=false
EXIT_CONFIRMATION_BUFFER=0.05
EXIT_CONFIRMATION_TIMEOUT_SECONDS=300
HARVEST_NEAR_CLOSE_HOURS=2.0
HARVEST_NEAR_CLOSE_MIN_MULTIPLIER=1.3
```

### Notes
- `_pending_exits` is in-memory only — on restart, next resolution check re-evaluates. No DB persistence needed.
- Harvest close uses same `_close_all_tranches()` path as normal whale exits — no new close code.
- Wrap `check_and_harvest_positions` body in try/except so scheduler exception doesn't affect other jobs.

### Verification
1. Set `HARVEST_MULTIPLIER=1.001`, `HARVEST_ENABLED=true` in SIMULATION → all profitable positions close on next cycle. Check close_reason.
2. Set `HARVEST_NEAR_CLOSE_HOURS=999`, `HARVEST_NEAR_CLOSE_MIN_MULTIPLIER=1.001` → all profitable positions near-close-harvest.
3. Disable all flags → positions only close on whale exit (unchanged behavior).

### Regression Risk: Medium
- New scheduler job; wrap in try/except to isolate failures
- All logic gated behind feature flags — zero code path change when disabled

---

## Stage 4 — Thin Book / Slippage Tracking

**Features:** Per-token historical slippage register (9)

**Why here:** Depends on Stage 2 (order_book already passed to process_new_whale_bet). Uses mid_price from live_price at entry/exit time.

### Files to Modify

**`backend/database.py`**
- New model `TokenSlippageRecord`:
  ```
  id, token_id (indexed), recorded_at, fill_price, mid_price, slippage_pct, mode
  ```
  New table — handled by `Base.metadata.create_all()`, no _migrate() entry needed.

**`backend/bet_engine.py`**
- New field: `self._slippage_register: dict[str, deque] = defaultdict(lambda: deque(maxlen=20))`
- New method `_record_slippage(token_id, fill_price, mid_price, mode, db)`: compute slippage_pct, append to in-memory register, write TokenSlippageRecord to DB
- New method `_get_avg_slippage(token_id) -> float | None`: mean of register[token_id]
- New method `_seed_slippage_register(db)`: on startup, load last 20 records per token from DB
- Extend `simulate_sell()` and REAL close path: call `_record_slippage()` after fill
- Extend `process_new_whale_bet()`: if `SLIPPAGE_TRACKING_ENABLED`, check `_get_avg_slippage()` vs `MAX_HISTORICAL_SLIPPAGE_PCT` → skip if over threshold

**`backend/main.py`** — `lifespan()`
- Add: `bet_engine._seed_slippage_register(next(get_db()))` (wrapped in try/except)

**`backend/config.py`**
```
SLIPPAGE_TRACKING_ENABLED=false
MAX_HISTORICAL_SLIPPAGE_PCT=0.05
```

### Verification
1. Run 10+ bets in SIMULATION → query `token_slippage` table, confirm rows exist
2. Set `MAX_HISTORICAL_SLIPPAGE_PCT=0.0` → all tokens with any history are skipped
3. Restart bot → confirm register is re-seeded from DB and behavior matches pre-restart

### Regression Risk: Low
- Recording is additive; entry gate is flag-gated

---

## Stage 5 — WebSocket Entry Signal Detection

**Features:** CLOB WebSocket entries (1)

**Why last among logic features:** Highest infrastructure risk. All entry-gate logic (Stage 2), exit logic (Stage 3), and slippage tracking (Stage 4) should be validated before this changes the detection path.

### New File: `backend/clob_ws_monitor.py`

New class `ClobWsEntryMonitor` modeled directly on `WhaleChainMonitor`:
- Same constructor/`_run()`/backoff/reconnect pattern as `whale_chain_monitor.py`
- Same `_db_write_lock` + `run_in_executor` Phase 1/Phase 2 dispatch pattern
- Same `_maybe_refresh_whale_map()` (30s TTL)
- WebSocket endpoint: `wss://ws-subscriptions-clob.polymarket.com`
- Subscription: subscribe to trades channel, filter by tracked whale addresses as maker or taker
- `_dispatch_entry()`: identical structure to `WhaleChainMonitor._dispatch_entry()` — Phase 1 asyncio.gather (market, price, fee, order_book), Phase 2 executor `_sync_open_position()`

**Deduplication:** CLOB WS entries arrive before HTTP polling catches them. The existing UNIQUE INDEX on `whale_bets.tx_hash` already prevents duplicate processing — IntegrityError is caught and silently dropped in `_save_whale_bet()`. No additional dedup work needed.

**`backend/whale_monitor.py`** — constructor
- If `CLOB_WS_ENABLED`: instantiate `ClobWsEntryMonitor`

**`backend/main.py`** — `lifespan()`
- Add: `whale_monitor.start_clob_ws_task()` alongside existing chain monitor task

**`backend/config.py`**
```
CLOB_WS_ENABLED=false
CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com
```

### Notes
- HTTP polling continues running as fallback when WS is active — no change to existing poller
- WS provides ~100ms detection vs 2-10s polling; most of remaining latency is network round-trip and order signing

### Verification
1. Enable `CLOB_WS_ENABLED=true` in SIMULATION → confirm "CLOB WS: connected" in logs
2. Wait for whale trade → confirm entry logged with WS source tag; HTTP poller hits IntegrityError and skips duplicate
3. Kill WS connection → confirm reconnect with exponential backoff
4. Disable flag → identical behavior to pre-Stage 5

### Regression Risk: Medium-High (but isolated)
- Existing HTTP polling path is completely unchanged
- Risk is isolated to new infrastructure code — copy WhaleChainMonitor pattern precisely

---

## Stage 6 — Adaptive Whale Sizing

**Features:** Adaptive sizing based on rolling win rate (5b)

**Why last:** Requires Stage 1 data to have been populating win_rate_window_json for several cycles before multipliers are meaningful.

### Files to Modify

**`backend/risk_calculator.py`** — `calculate_risk_factor()` (~L24)
- Add new param `whale_performance_multiplier: float = 1.0`
- Multiply raw value by this before clamping to `[_MIN_RISK_FACTOR, _MAX_RISK_FACTOR]`

**`backend/bet_engine.py`** — `process_new_whale_bet()` risk calculation block (~L292)
- New method `_compute_whale_performance_multiplier(whale) -> float`:
  - Parse `win_rate_window_json`; return 1.0 if < WHALE_PERF_WINDOW/2 samples
  - Linear interpolation: win_rate=0.5→1.0x, win_rate=1.0→MAX_MULTIPLIER, win_rate=0.0→MIN_MULTIPLIER
- Pass result to `calculate_risk_factor()` as `whale_performance_multiplier`

**`backend/config.py`**
```
ADAPTIVE_SIZING_ENABLED=false
WHALE_PERF_MIN_MULTIPLIER=0.5
WHALE_PERF_MAX_MULTIPLIER=1.5
```

### Verification
1. Manually set `win_rate_window_json` to 25x `1` (100% win) for a whale; enable `ADAPTIVE_SIZING_ENABLED=true`, `WHALE_PERF_MAX_MULTIPLIER=1.5` → confirm bet sizes are ~1.5x expected
2. Set to all zeros (0% win) → confirm ~0.5x sizing
3. Disable flag → identical sizing to pre-Stage 6

### Regression Risk: Low
- New param has default=1.0 (no effect when adaptive sizing disabled)
- Existing clamp in `calculate_risk_factor()` bounds any extreme multiplier output

---

## Critical Files Summary

| File | Stages |
|------|--------|
| `backend/bet_engine.py` | All stages — central modification target |
| `backend/database.py` | Stage 1 (win_rate_window_json column), Stage 4 (TokenSlippageRecord table) |
| `backend/config.py` | All stages — new env vars each stage |
| `backend/whale_monitor.py` | Stage 2 (order_book gather), Stage 3 (harvest scheduler job) |
| `backend/whale_chain_monitor.py` | Stage 2 (order_book gather extension) |
| `backend/main.py` | Stage 1 (auth prewarm), Stage 4 (slippage seed), Stage 5 (CLOB WS task) |
| `backend/risk_calculator.py` | Stage 6 (performance multiplier param) |
| `backend/clob_ws_monitor.py` | Stage 5 (new file — mirrors whale_chain_monitor.py) |

---

## General Implementation Rules

- All new features default to `false` in config — opt-in only
- Never modify `place_real_buy()` / `place_real_sell()` — all gates go in `process_new_whale_bet()` before the placement call
- All new scheduler jobs wrapped in try/except — never let a harvest/slippage check crash the resolution scheduler
- Reuse `_close_all_tranches()` for all harvest closes — no new close code paths
- New `order_book` kwarg on `process_new_whale_bet()` defaults to None — all callers without the kwarg continue working unchanged

---

## Research Notes: What Top-Performing Copy Bots Do (Worth Noting)

These patterns came up consistently in research and are worth keeping in mind as implementation matures:

### 1. Exponential Decay on Win-Rate History
The flat rolling window in Stage 6 (last 50 bets equally weighted) is a reasonable start but top bots weight recent outcomes more heavily. A whale who was 70% over 6 months but 40% in the last 2 weeks should be treated as closer to 40%. Consider adding an exponential decay option to `_compute_whale_performance_multiplier()` as a Stage 6 refinement: `weight[i] = decay_factor ^ i` applied to the window before averaging.

### 2. Partial Exit Laddering
Rather than all-or-nothing harvest exits (Stage 3), the most profitable approach is laddering out: sell 50% at the harvest threshold to lock in profit, hold 50% for potential resolution at 1.0. This requires no structural change to the close machinery — just add a `harvest_fraction` config param (default 1.0) to `check_and_harvest_positions()`. At 1.0 it's full close (current plan); at 0.5 it closes half the shares.

### 4. Maker vs Taker Detection for Signal Quality
A whale placing a limit order (maker) is exhibiting patience and likely entered early. A whale using a market order (taker) is urgent and may already be chasing price. Polymarket CLOB events distinguish maker from taker. In `whale_chain_monitor._dispatch_entry()`, the event already includes maker/taker fields. Tagging `WhaleBet` with `is_taker: bool` and slightly discounting taker signals (or skipping entirely above a drift threshold) could filter out late reactive whales.

### 5. Pre-Signed Limit Order Cache (Advanced)
The highest-performing bots pre-sign limit orders at several price points for every token they're watching, so order submission is near-instant. Polymarket's CLOB supports GTC limit orders. On whale signal, instead of a market order, the bot could place a pre-signed limit at `whale_price + tolerance` — already signed and ready. This reduces the signing latency from ~30ms to ~0ms. Complex to implement correctly (need to cancel stale pre-signs); worth noting for a future performance pass after Stage 5 validates the WS detection path.
