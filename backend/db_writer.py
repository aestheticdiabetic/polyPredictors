"""
Centralized database write serialization.

All database write operations (flush, commit) go through this module to prevent
SQLite lock contention when multiple background tasks (chain monitor, pollers,
schedulers) compete for the single SQLite writer.

Architecture:
- Global threading.Lock ensures only one thread writes to SQLite at a time
- Wrapper functions add retry logic with exponential backoff
- All background tasks (scheduler threads, async executors) coordinate here
"""

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import TypeVar

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

T = TypeVar("T")

# Global lock for all database writes
_db_write_lock = threading.Lock()

# Configuration
_MAX_RETRIES = 3
_INITIAL_BACKOFF_MS = 100
_MAX_BACKOFF_MS = 5000


def synchronized_flush(db: Session) -> None:
    """
    Flush changes to database with retries.

    Acquires global write lock before flushing to prevent concurrent
    writes from multiple background tasks.

    Args:
        db: SQLAlchemy session

    Raises:
        OperationalError: If retry exhausted after max attempts
    """
    _execute_with_retries(lambda: db.flush(), operation="flush")


def synchronized_commit(db: Session) -> None:
    """
    Commit transaction with retries.

    Acquires global write lock before committing to prevent concurrent
    writes from multiple background tasks.

    Args:
        db: SQLAlchemy session

    Raises:
        OperationalError: If retry exhausted after max attempts
    """
    _execute_with_retries(lambda: db.commit(), operation="commit")


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

    _execute_with_retries(_add_and_flush, operation="add_and_flush")


async def synchronized_commit_async(
    db: Session,
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """
    Async wrapper for commit with retries.

    Suitable for use in async contexts (e.g., run_in_executor).

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
) -> T | None:
    """
    Execute database operation with exponential backoff retry.

    Args:
        func: Callable that performs the DB operation
        operation: String name for logging

    Returns:
        Return value of operation if successful

    Raises:
        OperationalError: If all retries exhausted
    """
    backoff_ms = _INITIAL_BACKOFF_MS

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with _db_write_lock:
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

            # Exponential backoff with jitter
            time.sleep(backoff_ms / 1000.0)
            backoff_ms = min(backoff_ms * 2, _MAX_BACKOFF_MS)

    return None


def with_write_lock(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator that wraps a function to serialize database writes.

    Usage:
        @with_write_lock
        def my_db_operation(db: Session):
            db.add(obj)
            db.commit()

    Args:
        func: Function that performs DB writes

    Returns:
        Wrapped function that acquires global write lock
    """

    def wrapper(*args, **kwargs) -> T:
        with _db_write_lock:
            return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper
