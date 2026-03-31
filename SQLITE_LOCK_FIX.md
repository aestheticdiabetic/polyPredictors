# SQLite Database Lock Fix

## Problem

The app experiences `sqlite3.OperationalError: database is locked` errors because multiple background tasks write to SQLite simultaneously:

- `whale_chain_monitor` (WebSocket events)
- `whale_monitor.poll_whales()` (every 2-5s)
- `bet_engine` schedulers (check_resolution, harvest, etc.)
- `stop_loss` scheduler
- Various other background jobs

While WAL mode + 180s timeout provides tolerance, it's not a proper solution. Writes still race and can exceed timeouts under peak load. Exceptions in background tasks are silently swallowed.

## Root Cause

SQLite only allows **one writer at a time**. When multiple threads/tasks try to write:

1. First writer acquires SQLite's write lock
2. Other writers receive SQLITE_BUSY
3. SQLite retries for `busy_timeout` (5s) + connection `timeout` (180s)
4. If lock isn't released in time → OperationalError
5. If error is in background task → silently caught and lost

Example timeline:
```
T=0ms:   poll_whales starts flush()
         │ acquires SQLite write lock
         │ holds for ~500ms

T=100ms: chain_monitor event triggers _dispatch_entry()
         │ tries to flush()
         │ SQLITE_BUSY (poll_whales still writing)
         │ retries for 5000ms...

T=200ms: check_resolution timer fires, calls db.commit()
         │ SQLITE_BUSY (poll_whales + chain_monitor competing)
         │ retries for 5000ms...

T=600ms: poll_whales finally commits, releases lock
         │ but check_resolution may have exceeded timeout
```

## Solution: Global Write Lock

**File**: `backend/db_writer.py` (already created)

Implements a module-level `threading.Lock` that serializes ALL database writes across the entire application:

```python
from backend.db_writer import synchronized_commit, synchronized_flush

# Instead of: db.commit()
synchronized_commit(db)

# Instead of: db.flush()
synchronized_flush(db)
```

Features:
- Global `threading.Lock` ensures only one write at a time
- Exponential backoff retry (100ms → 200ms → 400ms → max 5s)
- Proper error logging instead of silent failures
- Works with both sync and async code

## Implementation Roadmap

### Phase 1: Fix Background Task Error Handling (30 min)

**File**: `backend/main.py`

Wrap all scheduler jobs to catch and log exceptions instead of silently swallowing:

```python
def safe_scheduler_job(job_name: str, func, *args, **kwargs):
    """Wrap scheduler job to catch exceptions and log them."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        log.error(f"{job_name} failed: {e}", exc_info=True)
        # Re-raise so scheduler is aware of failure
        raise

# Then:
scheduler.add_job(
    safe_scheduler_job,
    args=("check_resolution", self._bet_engine.check_resolution),
    trigger="interval",
    seconds=60,
)
```

### Phase 2: Migrate High-Frequency Writes (1-2 hours)

Priority order (by write frequency):

1. **whale_monitor.poll_whales()** (2-5s interval, 15 writes/cycle)
   - Location: `whale_monitor.py:check_whale_activity()`
   - Change: Replace `db.commit()` → `synchronized_commit(db)`

2. **bet_engine check_and_harvest_positions()** (60s interval, 3-5 writes)
   - Location: `bet_engine.py:307, 333, 406`
   - Change: Replace `db.commit()` → `synchronized_commit(db)`

3. **whale_chain_monitor._sync_open_position()** (per event, 3 writes)
   - Location: `whale_chain_monitor.py:890`
   - **ALREADY PROTECTED** by `async with self._db_write_lock` but now also uses global lock

4. **whale_chain_monitor._backfill()** (per reconnect, ~20 writes)
   - Location: `whale_chain_monitor.py:520+`
   - Change: Add `synchronized_commit()` wrapper

5. **stop_loss operations** (30min + trigger, 1-2 writes)
   - Location: `stop_loss.py:62, 143`
   - Change: Replace `db.commit()` → `synchronized_commit(db)`

### Phase 3: Reduce Lock Hold Time (2-4 hours)

Once writes are serialized, optimize to minimize lock duration:

1. **Batch queries before writes**
   ```python
   # WRONG: Lock held during query
   with lock:
       data = db.query(...).all()
       for row in data:
           row.status = "DONE"
       db.commit()

   # RIGHT: Query outside lock, write inside
   data = db.query(...).all()
   with lock:
       for row in data:
           row.status = "DONE"
       db.commit()
   ```

2. **Move API calls outside transaction**
   ```python
   # WRONG: API call during write (causes timeout)
   db.add(whale_bet)
   price = await client.get_price(token_id)  # slow!
   db.commit()

   # RIGHT: Fetch before transaction
   price = await client.get_price(token_id)
   db.add(whale_bet)
   db.commit()
   ```

3. **Use `.expire_on_commit(False)` for large batches**
   ```python
   # Avoid lazy-loading after commit
   whales = db.query(Whale).options(
       joinedload(Whale.bets)
   ).all()

   with synchronized_lock:
       for whale in whales:
           whale.status = "updated"
       db.commit()
   ```

## Migration Checklist

### Step 1: Import and Test

```bash
# Verify db_writer.py works
pytest backend/test_db_writer.py -v
```

### Step 2: Add to whale_monitor.py

```python
# At top of file
from backend.db_writer import synchronized_commit

# In check_whale_activity() - Line ~547
# Change from: db.commit()
# Change to:
synchronized_commit(db)
```

### Step 3: Add to bet_engine.py

```python
# At top of file
from backend.db_writer import synchronized_commit, synchronized_flush

# In check_and_harvest_positions() - Lines 307, 333, 406
# Change from: db.commit()
# Change to:
synchronized_commit(db)

# In _update_whale_stats() - Lines 154-155
# Change from: db.flush()
# Change to:
synchronized_flush(db)
```

### Step 4: Improve whale_chain_monitor.py

The async lock is still useful to prevent concurrent same-block events from racing on HTTP calls. Keep it AND add global lock:

```python
# In _sync_open_position() - Around line 890
from backend.db_writer import synchronized_commit

# Existing code:
async with self._db_write_lock:
    await loop.run_in_executor(
        None,
        self._sync_open_position,
        ...
    )

# Inside _sync_open_position():
def _sync_open_position(self, ...):
    # ... existing code ...
    db.add(whale_bet)
    synchronized_flush(db)

    # db.commit() in called function:
    synchronized_commit(db)
```

### Step 5: Add to stop_loss.py

```python
# At top of file
from backend.db_writer import synchronized_commit

# In snapshot_async() - Lines 62-63
# Change from: db.commit()
# Change to:
synchronized_commit(db)

# In _stop_session() - Line 143
# Change from: db.commit()
# Change to:
synchronized_commit(db)
```

### Step 6: Improve Error Handling in main.py

```python
# Add to main.py
import logging
from functools import wraps

log = logging.getLogger(__name__)

def safe_scheduler_job(name: str):
    """Decorator that catches scheduler job exceptions."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.error(
                    f"Scheduler job '{name}' failed: {e}",
                    exc_info=True
                )
                # Re-raise so scheduler logs it too
                raise
        return wrapper
    return decorator

# Usage:
@safe_scheduler_job("check_resolution")
def check_resolution():
    self._bet_engine.check_resolution()

scheduler.add_job(
    check_resolution,
    trigger="interval",
    seconds=60,
)
```

## Testing

### Unit Tests

```python
# test_db_writer.py
def test_synchronized_commit_retries_on_lock():
    """Verify retry logic works."""
    db = SessionLocal()

    # Simulate lock contention
    # ... test code ...

    # Should NOT raise after retry
    synchronized_commit(db)

def test_synchronized_flush_retries_on_lock():
    """Verify flush retry logic works."""
    db = SessionLocal()
    db.add(Whale(address="0x123..."))

    # Should NOT raise after retry
    synchronized_flush(db)
```

### Integration Tests

Run existing tests with high concurrency to verify no lock timeouts:

```bash
# Run with verbose output to see lock contention
PYTHONUNBUFFERED=1 pytest -v --log-cli-level=DEBUG -x
```

Monitor logs for:
- No `OperationalError: database is locked` errors
- Retry messages logged and retries succeed

## Configuration Changes

Update `.env` if needed:

```diff
# Keep aggressive timeout as safety net, but shouldn't be needed now:
# SQLITE_TIMEOUT=180  (existing)

# Optional: reduce if lock contention is fixed
# SQLITE_TIMEOUT=30
```

Update `database.py` connection pragmas:

```python
# These are still good to keep:
PRAGMA journal_mode=WAL              # Already set
PRAGMA synchronous=NORMAL            # Already set
PRAGMA wal_autocheckpoint=10000      # Already set

# busy_timeout can be reduced since we now serialize writes:
# cursor.execute("PRAGMA busy_timeout=1000")  # Down from 5000
```

## Expected Results

**Before fix:**
```
sqlite3.OperationalError: database is locked (every 1-2 hours under load)
Task exception was never retrieved (data loss)
Operations timeout or fail silently
```

**After fix:**
```
No lock contention errors
All writes complete successfully
Background tasks log successes/failures properly
Lock wait times < 100ms (serialized, no retry needed)
```

## Rollback Plan

If issues arise, rollback is safe:

```bash
git revert <commit_hash>

# Or manually:
# 1. Revert imports to use db.commit() directly
# 2. Delete backend/db_writer.py
# 3. Restart app
```

No database migrations needed — the lock is purely application-level.

## Alternative: PostgreSQL Migration

If SQLite lock contention persists even after this fix, consider migrating to PostgreSQL for true concurrent writes.

See: `POSTGRESQL_MIGRATION.md` (to be created if needed)

Key benefits:
- True MVCC (Multi-Version Concurrency Control)
- Parallel writers without serialization
- Better for high-frequency writes (30-50/sec)
- Built-in replication + backups

Downside:
- Requires VPS/cloud PostgreSQL service
- More operational overhead

## Summary

| Aspect | Before | After |
|--------|--------|-------|
| Lock strategy | WAL mode + timeout | WAL + global write lock |
| Serialization | Implicit (SQLite) | Explicit (app-level) |
| Error handling | Silent failures | Logged with retries |
| Lock hold time | Variable (up to 180s) | Bounded + serialized |
| Data loss | Possible | No |
| Debugging | Hard (timeouts) | Easy (logs) |

---

## Next Steps

1. ✅ Created `backend/db_writer.py`
2. ⬜ Test db_writer module
3. ⬜ Migrate whale_monitor.py
4. ⬜ Migrate bet_engine.py
5. ⬜ Migrate whale_chain_monitor.py
6. ⬜ Migrate stop_loss.py
7. ⬜ Improve error handling in main.py
8. ⬜ Run integration tests under load
9. ⬜ Monitor logs for 24 hours
10. ⬜ Document in project wiki
