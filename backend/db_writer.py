"""
Centralized database write retry logic.

SQLite WAL mode serializes concurrent writers natively — a threading.Lock is
NOT needed and is actively harmful: if a session holds the SQLite WAL write
lock between a flush and its commit, any other thread that holds the threading
lock and waits for the SQLite lock will block the first thread from acquiring
the threading lock to commit.  That produces a deadlock that lasts for the
entire busy_timeout duration per retry.

Architecture:
- SQLite WAL mode (set in database.py) allows concurrent readers and
  serializes writers without application-level locking.
- PRAGMA busy_timeout (set in database.py) makes SQLite wait and retry
  internally before raising OperationalError.
- This module adds an application-level retry layer as a last-resort safety
  net for cases where SQLite returns SQLITE_BUSY despite the busy handler.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from typing import TypeVar

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

T = TypeVar("T")

# Configuration
_MAX_RETRIES = 3
_INITIAL_BACKOFF_MS = 500
_MAX_BACKOFF_MS = 5000


def synchronized_flush(db: Session) -> None:
    """
    Flush changes to database with retries.

    Args:
        db: SQLAlchemy session

    Raises:
        OperationalError: If retry exhausted after max attempts
    """
    _execute_with_retries(lambda: db.flush(), operation="flush", db=db)


def synchronized_commit(db: Session) -> None:
    """
    Commit transaction with retries.

    Args:
        db: SQLAlchemy session

    Raises:
        OperationalError: If retry exhausted after max attempts
    """
    _execute_with_retries(lambda: db.commit(), operation="commit", db=db)


def synchronized_add_and_flush(db: Session, obj: object) -> None:
    """
    Add object and flush in single synchronized operation.

    Args:
        db: SQLAlchemy session
        obj: ORM object to add

    Raises:
        OperationalError: If retry exhausted after max attempts
    """

    def _add_and_flush():
        db.add(obj)
        db.flush()

    _execute_with_retries(_add_and_flush, operation="add_and_flush", db=db)


async def synchronized_commit_async(
    db: Session,
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """
    Async wrapper for commit with retries.

    Args:
        db: SQLAlchemy session
        loop: Event loop (if None, uses get_event_loop)

    Raises:
        OperationalError: If retry exhausted after max attempts
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    await loop.run_in_executor(None, lambda: synchronized_commit(db))


def _execute_with_retries(
    func: Callable[[], T],
    operation: str = "db_write",
    db: Session | None = None,
) -> T | None:
    """
    Execute database operation with exponential backoff retry.

    No threading lock is held here.  SQLite WAL mode + PRAGMA busy_timeout
    serializes writers at the SQLite level without application-level locking.
    Holding a threading lock across a SQLite busy-wait causes deadlocks when
    two sessions each hold a resource the other needs.

    Args:
        func: Callable that performs the DB operation
        operation: String name for logging
        db: Optional SQLAlchemy session for rollback on retry

    Returns:
        Return value of operation if successful

    Raises:
        OperationalError: If all retries exhausted
    """
    backoff_ms = _INITIAL_BACKOFF_MS

    for attempt in range(1, _MAX_RETRIES + 1):
        # Snapshot pending-new objects BEFORE the commit/flush call.
        # SQLAlchemy's autoflush moves objects from db.new → db.identity_map
        # during db.commit(), so capturing db.new only after a failure misses
        # objects that were auto-flushed.  After rollback those objects become
        # detached and are never re-added, causing "not persistent" errors on
        # the next db.refresh() call.
        pending_new = list(db.new) if db is not None else []

        try:
            return func()
        except OperationalError as e:
            if "database is locked" not in str(e):
                # Not a lock contention error — re-raise immediately
                raise

            if attempt == _MAX_RETRIES:
                log.error(
                    f"{operation} failed after {_MAX_RETRIES} retries (lock contention). "
                    f"Last error: {e}"
                )
                raise

            # Log retry
            log.warning(
                f"{operation} hit database lock (attempt {attempt}/{_MAX_RETRIES}). "
                f"Retrying in {backoff_ms}ms..."
            )

            # Reset session state before retry (SQLAlchemy requires explicit rollback
            # after a failed commit/flush before any subsequent operations can proceed).
            # Re-add the pending_new objects captured before autoflush so they are
            # included in the next commit attempt.
            if db is not None:
                db.rollback()
                for obj in pending_new:
                    db.add(obj)

            # Exponential backoff
            time.sleep(backoff_ms / 1000.0)
            backoff_ms = min(backoff_ms * 2, _MAX_BACKOFF_MS)

    return None
