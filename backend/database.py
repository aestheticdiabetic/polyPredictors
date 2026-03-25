"""
SQLAlchemy database setup and all ORM models.
"""

from datetime import datetime
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    event,
)
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker

from backend.config import settings

# Ensure data directory exists
Path(settings.DATABASE_URL.replace("sqlite:///", "")).parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    settings.DATABASE_URL,
    # timeout=30: SQLite busy-wait — retry for up to 30s when another thread
    # holds the write lock instead of immediately raising OperationalError.
    # Needed because the chain monitor (2s interval) + activity API (5s) +
    # resolution checker all write from separate APScheduler threads.
    connect_args={"check_same_thread": False, "timeout": 60},
    echo=False,
)


# Enable WAL mode for better concurrent access
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    # WAL (Write-Ahead Logging) mode: readers never block writers and writers
    # never block readers — far better throughput for the 3+ concurrent threads
    # that write here (chain monitor, activity poller, resolution checker).
    # Safe on Linux native filesystems (VPS deployment). Docker on Windows NTFS
    # bind mounts may not support WAL due to incomplete mmap; if that's the
    # deployment target revert to DELETE mode.
    cursor.execute("PRAGMA journal_mode=WAL")
    # NORMAL: flush after each WAL checkpoint rather than every write.
    # Safe with WAL — durability is maintained by the WAL file itself.
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Whale(Base):
    __tablename__ = "whales"

    id = Column(Integer, primary_key=True, index=True)
    address = Column(String(64), unique=True, nullable=False, index=True)
    alias = Column(String(100), nullable=False, default="")
    is_active = Column(Boolean, default=True, nullable=False)

    # Risk profiling
    avg_bet_size_usdc = Column(Float, default=0.0, nullable=False)
    risk_profile_calculated_at = Column(DateTime, nullable=True)

    # Stats
    total_bets_tracked = Column(Integer, default=0, nullable=False)
    win_count = Column(Integer, default=0, nullable=False)

    # Category filters — JSON: {"disabled_sports": [...], "disabled_bet_types": [...]}
    # null means follow all categories (opt-out model)
    category_filters = Column(Text, nullable=True)

    # Arb mode — when True, bet sizing uses price-extremity formula instead of
    # conviction (whale_bet / whale_avg). Designed for arbitrage whales whose bet
    # size reflects market liquidity, not confidence level.
    arb_mode = Column(Boolean, default=False, nullable=False)

    # Follow add-to-position signals — when True, place a real/simulated bet each
    # time the whale adds capital to a market we already hold, sized to suggested_add_usdc.
    follow_add_signals = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    bets = relationship("WhaleBet", back_populates="whale", cascade="all, delete-orphan")

    def to_dict(self) -> dict:
        win_rate = (
            (self.win_count / self.total_bets_tracked * 100) if self.total_bets_tracked > 0 else 0
        )
        return {
            "id": self.id,
            "address": self.address,
            "alias": self.alias,
            "is_active": self.is_active,
            "avg_bet_size_usdc": round(self.avg_bet_size_usdc, 2),
            "risk_profile_calculated_at": self.risk_profile_calculated_at.isoformat()
            if self.risk_profile_calculated_at
            else None,
            "total_bets_tracked": self.total_bets_tracked,
            "win_count": self.win_count,
            "win_rate_pct": round(win_rate, 1),
            "category_filters": self.category_filters,
            "arb_mode": self.arb_mode,
            "follow_add_signals": self.follow_add_signals,
            "created_at": self.created_at.isoformat(),
        }


class WhaleBet(Base):
    __tablename__ = "whale_bets"

    id = Column(Integer, primary_key=True, index=True)
    whale_id = Column(
        Integer, ForeignKey("whales.id", ondelete="CASCADE"), nullable=False, index=True
    )
    market_id = Column(String(128), nullable=False, index=True)  # condition_id
    token_id = Column(String(128), nullable=False)
    question = Column(Text, nullable=False, default="")
    side = Column(String(10), nullable=False)  # BUY / SELL
    outcome = Column(String(10), nullable=False)  # YES / NO
    price = Column(Float, nullable=False)  # 0.0 - 1.0
    size_usdc = Column(Float, nullable=False)
    size_shares = Column(Float, nullable=False, default=0.0)
    timestamp = Column(DateTime, nullable=False, index=True)
    tx_hash = Column(String(128), nullable=True)
    bet_type = Column(String(10), nullable=False, default="OPEN")  # OPEN / EXIT

    # Relationships
    whale = relationship("Whale", back_populates="bets")
    copied_bet = relationship("CopiedBet", back_populates="whale_bet", uselist=False)
    add_to_position_signal = relationship(
        "AddToPositionSignal", back_populates="whale_bet", uselist=False
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "whale_id": self.whale_id,
            "whale_address": self.whale.address if self.whale else None,
            "whale_alias": self.whale.alias if self.whale else None,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "question": self.question,
            "side": self.side,
            "outcome": self.outcome,
            "price": self.price,
            "size_usdc": round(self.size_usdc, 2),
            "size_shares": round(self.size_shares, 4),
            "timestamp": self.timestamp.isoformat(),
            "tx_hash": self.tx_hash,
            "bet_type": self.bet_type,
        }


class CopiedBet(Base):
    __tablename__ = "copied_bets"

    id = Column(Integer, primary_key=True, index=True)
    whale_bet_id = Column(Integer, ForeignKey("whale_bets.id"), nullable=False, index=True)
    whale_address = Column(String(64), nullable=False)

    session_id = Column(Integer, ForeignKey("sessions.id"), nullable=True, index=True)
    mode = Column(String(20), nullable=False)  # SIMULATION / REAL

    # Market details
    market_id = Column(String(128), nullable=False, index=True)
    token_id = Column(String(128), nullable=False)
    question = Column(Text, nullable=False, default="")
    side = Column(String(10), nullable=False)
    outcome = Column(String(10), nullable=False)

    # Execution details
    price_at_entry = Column(Float, nullable=False)
    size_usdc = Column(Float, nullable=False)
    size_shares = Column(Float, nullable=False, default=0.0)

    # Risk info
    risk_factor = Column(Float, nullable=False, default=1.0)
    whale_bet_usdc = Column(Float, nullable=False)
    whale_avg_bet_usdc = Column(Float, nullable=False, default=0.0)

    # Status
    status = Column(String(20), nullable=False, default="PENDING")
    # PENDING / OPEN / CLOSED_WIN / CLOSED_LOSS / CLOSED_NEUTRAL / SKIPPED
    skip_reason = Column(Text, nullable=True)
    close_reason = Column(Text, nullable=True)  # why a position was closed early

    # Resolution
    pnl_usdc = Column(Float, nullable=True)
    gas_fees_usdc = Column(Float, nullable=True)  # gas cost allocated to this position
    resolution_price = Column(Float, nullable=True)

    # Categorisation
    market_category = Column(String(50), nullable=True)  # Soccer | Basketball | Tennis | ...
    bet_type = Column(String(50), nullable=True)  # Over/Under | Spread | Moneyline | ...

    # Timestamps
    opened_at = Column(DateTime, nullable=True)
    closed_at = Column(DateTime, nullable=True)
    market_close_at = Column(DateTime, nullable=True)  # market end date from Polymarket

    # When placed by following an add-to-position signal, points to that signal. NULL otherwise.
    followed_signal_id = Column(Integer, ForeignKey("add_to_position_signals.id"), nullable=True)

    # Relationships
    whale_bet = relationship("WhaleBet", back_populates="copied_bet")
    session = relationship("MonitoringSession", foreign_keys=[session_id])
    add_to_position_signals = relationship(
        "AddToPositionSignal",
        back_populates="copied_bet",
        cascade="all, delete-orphan",
        foreign_keys="AddToPositionSignal.copied_bet_id",
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "whale_bet_id": self.whale_bet_id,
            "whale_address": self.whale_address,
            "mode": self.mode,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "question": self.question,
            "side": self.side,
            "outcome": self.outcome,
            "price_at_entry": round(self.price_at_entry, 4),
            "size_usdc": round(self.size_usdc, 2),
            "size_shares": round(self.size_shares, 4),
            "risk_factor": round(self.risk_factor, 3),
            "whale_bet_usdc": round(self.whale_bet_usdc, 2),
            "whale_avg_bet_usdc": round(self.whale_avg_bet_usdc, 2),
            "status": self.status,
            "skip_reason": self.skip_reason,
            "close_reason": self.close_reason,
            "pnl_usdc": round(self.pnl_usdc, 2) if self.pnl_usdc is not None else None,
            "gas_fees_usdc": round(self.gas_fees_usdc, 4)
            if self.gas_fees_usdc is not None
            else None,
            "net_pnl_usdc": round(self.pnl_usdc - (self.gas_fees_usdc or 0.0), 2)
            if self.pnl_usdc is not None
            else None,
            "resolution_price": round(self.resolution_price, 4)
            if self.resolution_price is not None
            else None,
            "market_category": self.market_category,
            "bet_type": self.bet_type,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "market_close_at": self.market_close_at.isoformat() if self.market_close_at else None,
            "followed_signal_id": self.followed_signal_id,
        }


class AddToPositionSignal(Base):
    __tablename__ = "add_to_position_signals"

    id = Column(Integer, primary_key=True, index=True)
    whale_bet_id = Column(Integer, ForeignKey("whale_bets.id"), nullable=False, index=True)
    copied_bet_id = Column(Integer, ForeignKey("copied_bets.id"), nullable=False, index=True)

    whale_additional_usdc = Column(Float, nullable=False)  # cumulative total across all additions
    whale_additional_shares = Column(Float, nullable=False, default=0.0)  # cumulative
    price = Column(Float, nullable=False)  # price of the most recent addition
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)  # first addition

    # Scaled bet size we would add under our own sizing rules (risk-factor
    # adjusted to our session balance), distinct from the whale's raw addition.
    suggested_add_usdc = Column(Float, nullable=True)

    hypothetical_pnl_usdc = Column(Float, nullable=True)
    note = Column(Text, nullable=True)

    # Grouped addition tracking — one signal row per position, updated on each addition
    addition_count = Column(Integer, nullable=False, default=1)
    last_addition_at = Column(DateTime, nullable=True)

    # Relationships
    whale_bet = relationship("WhaleBet", back_populates="add_to_position_signal")
    copied_bet = relationship(
        "CopiedBet",
        back_populates="add_to_position_signals",
        foreign_keys="AddToPositionSignal.copied_bet_id",
    )
    followed_bets = relationship("CopiedBet", foreign_keys="CopiedBet.followed_signal_id")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "whale_bet_id": self.whale_bet_id,
            "copied_bet_id": self.copied_bet_id,
            "whale_additional_usdc": round(self.whale_additional_usdc, 2),
            "whale_additional_shares": round(self.whale_additional_shares, 4),
            "price": round(self.price, 4),
            "timestamp": self.timestamp.isoformat(),
            "suggested_add_usdc": round(self.suggested_add_usdc, 2)
            if self.suggested_add_usdc is not None
            else None,
            "hypothetical_pnl_usdc": round(self.hypothetical_pnl_usdc, 2)
            if self.hypothetical_pnl_usdc is not None
            else None,
            "note": self.note,
            "addition_count": self.addition_count if self.addition_count is not None else 1,
            "last_addition_at": self.last_addition_at.isoformat()
            if self.last_addition_at
            else None,
        }


class MonitoringSession(Base):
    """Tracks each monitoring session (renamed to avoid conflict with sqlalchemy Session)."""

    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True, index=True)
    mode = Column(String(20), nullable=False)  # SIMULATION / REAL
    runtime_hours = Column(Float, nullable=True)  # null = manual stop

    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    stopped_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)

    starting_balance_usdc = Column(Float, nullable=False, default=200.0)
    current_balance_usdc = Column(Float, nullable=False, default=200.0)

    total_bets_placed = Column(Integer, default=0, nullable=False)
    total_wins = Column(Integer, default=0, nullable=False)
    total_losses = Column(Integer, default=0, nullable=False)
    total_pnl_usdc = Column(Float, default=0.0, nullable=False)
    total_gas_fees_usdc = Column(Float, default=0.0, nullable=False)

    def to_dict(self) -> dict:
        win_rate = (
            (self.total_wins / self.total_bets_placed * 100) if self.total_bets_placed > 0 else 0
        )
        duration_seconds = None
        if self.started_at:
            end = self.stopped_at or datetime.utcnow()
            duration_seconds = int((end - self.started_at).total_seconds())
        return {
            "id": self.id,
            "mode": self.mode,
            "runtime_hours": self.runtime_hours,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "stopped_at": self.stopped_at.isoformat() if self.stopped_at else None,
            "is_active": self.is_active,
            "starting_balance_usdc": round(self.starting_balance_usdc, 2),
            "current_balance_usdc": round(self.current_balance_usdc, 2),
            "total_bets_placed": self.total_bets_placed,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
            "win_rate_pct": round(win_rate, 1),
            "total_pnl_usdc": round(self.total_pnl_usdc, 2),
            "total_gas_fees_usdc": round(self.total_gas_fees_usdc or 0.0, 4),
            "net_pnl_usdc": round(
                (self.total_pnl_usdc or 0.0) - (self.total_gas_fees_usdc or 0.0), 2
            ),
            "duration_seconds": duration_seconds,
        }


class AppState(Base):
    """Single-row KV store for application configuration."""

    __tablename__ = "app_state"

    key = Column(String(64), primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_db():
    """FastAPI dependency: yields a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables, run lightweight migrations, and seed defaults."""
    Base.metadata.create_all(bind=engine)
    _migrate()
    _seed_defaults()


def _migrate():
    """Apply additive schema migrations (add missing columns)."""
    migrations = [
        ("copied_bets", "market_close_at", "DATETIME"),
        ("copied_bets", "close_reason", "TEXT"),
        ("copied_bets", "market_category", "VARCHAR(50)"),
        ("copied_bets", "bet_type", "VARCHAR(50)"),
        ("copied_bets", "session_id", "INTEGER REFERENCES sessions(id)"),
        ("whales", "category_filters", "TEXT"),
        ("whales", "arb_mode", "BOOLEAN NOT NULL DEFAULT 0"),
        ("add_to_position_signals", "suggested_add_usdc", "FLOAT"),
        ("add_to_position_signals", "addition_count", "INTEGER DEFAULT 1"),
        ("add_to_position_signals", "last_addition_at", "DATETIME"),
        ("sessions", "total_gas_fees_usdc", "FLOAT DEFAULT 0.0"),
        ("copied_bets", "gas_fees_usdc", "FLOAT"),
        ("whales", "follow_add_signals", "BOOLEAN NOT NULL DEFAULT 0"),
        ("copied_bets", "followed_signal_id", "INTEGER REFERENCES add_to_position_signals(id)"),
    ]
    sa = __import__("sqlalchemy")
    with engine.connect() as conn:
        for table, column, col_type in migrations:
            try:
                conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                conn.commit()
            except Exception:
                # Column already exists — safe to ignore
                pass

        # UNIQUE index on tx_hash — prevents duplicate inserts from concurrent scanners.
        # Partial index (WHERE tx_hash IS NOT NULL) avoids conflicts on rows without a hash.
        try:
            conn.execute(
                sa.text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_whale_bets_tx_hash "
                    "ON whale_bets (tx_hash) WHERE tx_hash IS NOT NULL"
                )
            )
            conn.commit()
        except Exception:
            pass

        # Compound indices on copied_bets — eliminate full-table scans in the hot
        # placement path (process_new_whale_bet queries by whale+token+status and
        # whale+mode+status on every bet).
        for idx_sql in (
            "CREATE INDEX IF NOT EXISTS ix_cb_whale_token_status "
            "ON copied_bets (whale_address, token_id, status)",
            "CREATE INDEX IF NOT EXISTS ix_cb_whale_mode_status "
            "ON copied_bets (whale_address, mode, status)",
            # Partial index — resolution checker scans only status='OPEN' rows.
            "CREATE INDEX IF NOT EXISTS ix_cb_status_open "
            "ON copied_bets (status) WHERE status = 'OPEN'",
            # FK indices on add_to_position_signals — fast lookups by copied_bet_id.
            "CREATE INDEX IF NOT EXISTS ix_atps_whale_bet_id "
            "ON add_to_position_signals (whale_bet_id)",
            "CREATE INDEX IF NOT EXISTS ix_atps_copied_bet_id "
            "ON add_to_position_signals (copied_bet_id)",
        ):
            try:
                conn.execute(sa.text(idx_sql))
                conn.commit()
            except Exception:
                pass

        # Back-fill session_id for existing bets: assign each bet to the session
        # with matching mode that started most recently at or before the bet's opened_at.
        try:
            conn.execute(
                sa.text("""
                UPDATE copied_bets
                SET session_id = (
                    SELECT s.id FROM sessions s
                    WHERE s.mode = copied_bets.mode
                      AND s.started_at <= COALESCE(copied_bets.opened_at, s.started_at)
                    ORDER BY s.started_at DESC
                    LIMIT 1
                )
                WHERE session_id IS NULL
            """)
            )
            conn.commit()
        except Exception:
            pass


def _seed_defaults():
    """Insert default whale addresses if the table is empty."""
    from backend.config import settings  # avoid circular at module level

    db = SessionLocal()
    try:
        existing = db.query(Whale).count()
        if existing == 0:
            for w in settings.DEFAULT_WHALES:
                whale = Whale(address=w["address"], alias=w["alias"])
                db.add(whale)
            db.commit()
    finally:
        db.close()
