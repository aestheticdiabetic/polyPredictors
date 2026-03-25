"""
FastAPI application entry point.
All API routes are defined here.
"""

import json
import logging
import logging.handlers
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy.orm import Session as DBSession

from backend.bet_engine import BetEngine
from backend.config import settings
from backend.database import (
    AddToPositionSignal,
    CopiedBet,
    MonitoringSession,
    SessionLocal,
    Whale,
    get_db,
    init_db,
)
from backend.polymarket_client import PolymarketClient
from backend.whale_monitor import WhaleMonitor

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_root = Path(__file__).parent.parent
_LOGS_DIR = _root / "logs"
_LOGS_DIR.mkdir(exist_ok=True)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Console handler — INFO and above
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_fmt)

# Rotating file handler — DEBUG and above (verbose), 5 MB per file, keep 10
_file_handler = logging.handlers.RotatingFileHandler(
    _LOGS_DIR / "app.log",
    maxBytes=5 * 1024 * 1024,
    backupCount=10,
    encoding="utf-8",
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_fmt)

logging.root.setLevel(logging.DEBUG)
logging.root.addHandler(_console_handler)
logging.root.addHandler(_file_handler)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database — initialised here so tables exist before any singleton queries them
# ---------------------------------------------------------------------------
init_db()
logger.info("Database initialised")

# ---------------------------------------------------------------------------
# Application-level singletons
# ---------------------------------------------------------------------------
poly_client = PolymarketClient()
bet_engine = BetEngine(polymarket_client=poly_client)
whale_monitor = WhaleMonitor(bet_engine=bet_engine, polymarket_client=poly_client)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Clean up on shutdown."""
    yield
    # Graceful shutdown
    whale_monitor.shutdown()
    await poly_client.close()
    logger.info("Application shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Polymarket Whale Copier", version="1.0.0", lifespan=lifespan)

# Static files
app.mount(
    "/static",
    StaticFiles(directory=str(_root / "frontend" / "static")),
    name="static",
)
templates = Jinja2Templates(directory=str(_root / "frontend" / "templates"))


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------
class SessionStartRequest(BaseModel):
    mode: str = "SIMULATION"  # SIMULATION | REAL
    runtime_hours: float | None = None  # None = manual stop


class WhaleAddRequest(BaseModel):
    address: str
    alias: str = ""


# ---------------------------------------------------------------------------
# UI Route
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ---------------------------------------------------------------------------
# Wallet
# ---------------------------------------------------------------------------
@app.get("/api/wallet/balance")
async def get_wallet_balance():
    """Return the live USDC balance for the configured funder wallet."""
    if not settings.credentials_valid():
        raise HTTPException(status_code=400, detail="Wallet credentials not configured")
    balance = await poly_client.get_wallet_balance()
    if balance is None:
        raise HTTPException(status_code=502, detail="Failed to fetch wallet balance")
    return {"balance": round(balance, 2)}


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
@app.get("/api/status")
async def get_status(db: DBSession = Depends(get_db)):
    """Return current session status and basic stats."""
    active_session = db.query(MonitoringSession).filter_by(is_active=True).first()
    monitor_status = whale_monitor.get_status()

    return {
        "session": active_session.to_dict() if active_session else None,
        "monitor": monitor_status,
        "credentials_configured": settings.credentials_valid(),
    }


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------
@app.post("/api/session/start")
async def start_session(body: SessionStartRequest, db: DBSession = Depends(get_db)):
    """Start a new monitoring session.

    Multiple modes (e.g. SIMULATION + HEDGE_SIM) can run simultaneously for
    side-by-side comparison. Starting a mode that is already active is rejected;
    starting a second mode while another is running is allowed.
    """

    # Reject if the same mode is already running (different modes are fine)
    existing = db.query(MonitoringSession).filter_by(mode=body.mode, is_active=True).first()
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"A {body.mode} session is already running",
        )

    # Validate REAL mode requirements
    if body.mode == "REAL" and not settings.credentials_valid():
        raise HTTPException(
            status_code=400,
            detail="REAL mode requires Polymarket credentials in .env",
        )

    # Close any lingering stale sessions with the same mode
    db.query(MonitoringSession).filter(
        MonitoringSession.mode == body.mode,
        MonitoringSession.is_active == True,  # noqa: E712
    ).update({"is_active": False, "stopped_at": datetime.utcnow()})
    db.commit()

    starting_balance = (
        (await poly_client.get_wallet_balance() or 0.0)
        if body.mode == "REAL"
        else settings.SIM_STARTING_BALANCE
    )

    session = MonitoringSession(
        mode=body.mode,
        runtime_hours=body.runtime_hours,
        starting_balance_usdc=starting_balance,
        current_balance_usdc=starting_balance,
    )
    db.add(session)
    db.commit()
    db.refresh(session)

    # Start the monitor if it isn't already polling (it will pick up all active sessions)
    whale_monitor.start_monitoring()

    # Auto-stop this mode after runtime_hours if specified
    if body.runtime_hours:
        from datetime import timedelta

        from apscheduler.triggers.date import DateTrigger

        stop_at = datetime.utcnow() + timedelta(hours=body.runtime_hours)
        try:
            whale_monitor._scheduler.add_job(
                func=_auto_stop_session,
                trigger=DateTrigger(run_date=stop_at),
                id=f"auto_stop_{body.mode}",
                replace_existing=True,
                kwargs={"mode": body.mode},
            )
        except Exception as exc:
            logger.warning("Could not schedule auto-stop: %s", exc)

    logger.info("Session %d started in %s mode", session.id, body.mode)
    return {"session": session.to_dict(), "message": "Session started"}


@app.post("/api/session/stop")
async def stop_session(
    mode: str | None = Query(None),
    db: DBSession = Depends(get_db),
):
    """Stop monitoring session(s).

    If `mode` is provided, only sessions of that mode are stopped (e.g. HEDGE_SIM
    while SIMULATION keeps running). Omit `mode` to stop all active sessions.
    The whale monitor is shut down only when no active sessions remain.
    """
    query = db.query(MonitoringSession).filter_by(is_active=True)
    if mode:
        query = query.filter(MonitoringSession.mode == mode)

    sessions_to_stop = query.all()
    stopped = None
    for s in sessions_to_stop:
        s.is_active = False
        s.stopped_at = datetime.utcnow()
        stopped = s
    db.commit()

    # Only stop the poller when no active sessions remain
    remaining = db.query(MonitoringSession).filter_by(is_active=True).count()
    if remaining == 0:
        whale_monitor.stop_monitoring()

    return {
        "session": stopped.to_dict() if stopped else None,
        "message": "Session stopped" if stopped else "No active session to stop",
    }


def _auto_stop_session(mode: str = None):
    """Called by scheduler when runtime_hours expires for a session."""
    db = SessionLocal()
    remaining = 0
    try:
        query = db.query(MonitoringSession).filter_by(is_active=True)
        if mode:
            query = query.filter(MonitoringSession.mode == mode)
        for session in query.all():
            session.is_active = False
            session.stopped_at = datetime.utcnow()
        db.commit()
        remaining = db.query(MonitoringSession).filter_by(is_active=True).count()
    finally:
        db.close()
    if remaining == 0:
        whale_monitor.stop_monitoring()
    logger.info("Auto-stopped %s session after runtime expiry", mode or "all")


# ---------------------------------------------------------------------------
# Whale management
# ---------------------------------------------------------------------------
@app.get("/api/whales")
async def list_whales(db: DBSession = Depends(get_db)):
    """Return all tracked whales with stats."""
    whales = db.query(Whale).order_by(Whale.created_at.desc()).all()
    return {"whales": [w.to_dict() for w in whales]}


@app.post("/api/whales")
async def add_whale(body: WhaleAddRequest, db: DBSession = Depends(get_db)):
    """Add a new whale to track."""
    address = body.address.strip().lower()
    if not address:
        raise HTTPException(status_code=400, detail="Address is required")

    existing = db.query(Whale).filter_by(address=address).first()
    if existing:
        raise HTTPException(status_code=409, detail="Whale already tracked")

    alias = body.alias.strip() or f"Whale_{address[:6]}"
    whale = Whale(address=address, alias=alias)
    db.add(whale)
    db.commit()
    db.refresh(whale)

    # Add to monitor's last_seen (starts from now)
    whale_monitor.reset_last_seen(address)

    return {"whale": whale.to_dict(), "message": "Whale added"}


@app.delete("/api/whales/{address}")
async def remove_whale(address: str, db: DBSession = Depends(get_db)):
    """Remove a whale from tracking."""
    address = address.strip().lower()
    whale = db.query(Whale).filter_by(address=address).first()
    if not whale:
        raise HTTPException(status_code=404, detail="Whale not found")

    db.delete(whale)
    db.commit()
    whale_monitor.reset_last_seen(address)
    return {"message": "Whale removed"}


@app.patch("/api/whales/{address}/toggle")
async def toggle_whale(address: str, db: DBSession = Depends(get_db)):
    """Toggle a whale's active status."""
    address = address.strip().lower()
    whale = db.query(Whale).filter_by(address=address).first()
    if not whale:
        raise HTTPException(status_code=404, detail="Whale not found")

    whale.is_active = not whale.is_active
    db.commit()
    return {"whale": whale.to_dict()}


@app.patch("/api/whales/{address}/arb-mode")
async def toggle_arb_mode(address: str, db: DBSession = Depends(get_db)):
    """Toggle a whale's arb mode (price-extremity sizing vs conviction sizing)."""
    address = address.strip().lower()
    whale = db.query(Whale).filter_by(address=address).first()
    if not whale:
        raise HTTPException(status_code=404, detail="Whale not found")

    whale.arb_mode = not whale.arb_mode
    db.commit()
    return {"whale": whale.to_dict()}


@app.get("/api/whales/{address}/history")
async def whale_history(address: str, limit: int = Query(50, ge=1, le=200)):
    """Fetch a whale's recent activity from Polymarket API."""
    try:
        trades = await poly_client.get_user_activity(address, limit=limit)
        positions = await poly_client.get_user_positions(address)
        return {"trades": trades, "positions": positions, "address": address}
    except Exception as exc:
        logger.error("whale_history error: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/whales/refresh-profiles")
async def refresh_profiles():
    """Manually trigger whale risk profile refresh."""
    try:
        # Run in a thread to avoid blocking event loop
        import asyncio

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, whale_monitor.refresh_risk_profiles)
        return {"message": "Risk profiles refreshed"}
    except Exception as exc:
        logger.error("refresh_profiles error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Bet Ledger
# ---------------------------------------------------------------------------
def _apply_mode_filter(query, mode: str):
    """
    Apply a mode filter to a CopiedBet query.
    - 'standard' → SIMULATION or REAL only (excludes HEDGE_SIM)
    - 'SIMULATION' / 'REAL' / 'HEDGE_SIM' → exact match
    - 'all' or anything else → no filter
    """
    m = mode.upper()
    if m in ("SIMULATION", "REAL", "HEDGE_SIM"):
        return query.filter(CopiedBet.mode == m)
    if m == "STANDARD":
        return query.filter(CopiedBet.mode.in_(["SIMULATION", "REAL"]))
    return query


def _apply_session_mode_filter(query, mode: str):
    """Same as _apply_mode_filter but for MonitoringSession rows."""
    m = mode.upper()
    if m in ("SIMULATION", "REAL", "HEDGE_SIM"):
        return query.filter(MonitoringSession.mode == m)
    if m == "STANDARD":
        return query.filter(MonitoringSession.mode.in_(["SIMULATION", "REAL"]))
    return query


@app.get("/api/sessions/latest")
async def get_latest_session(mode: str = Query("all"), db: DBSession = Depends(get_db)):
    """Return the most recent session (active or stopped), optionally filtered by mode."""
    query = db.query(MonitoringSession)
    query = _apply_session_mode_filter(query, mode)
    session = query.order_by(MonitoringSession.id.desc()).first()
    if not session:
        return {"session": None}

    d = session.to_dict()

    # Override the stored total_pnl_usdc, total_wins, total_losses, and total_bets_placed
    # with values computed directly from the bet records for this session.
    # The stored fields accumulate via in-memory writes and can drift from the true
    # values if a background job incorrectly attributes a bet to the wrong session.
    from sqlalchemy import func as _func

    rows = (
        db.query(CopiedBet.status, _func.count(CopiedBet.id), _func.sum(CopiedBet.pnl_usdc))
        .filter(CopiedBet.session_id == session.id)
        .group_by(CopiedBet.status)
        .all()
    )
    wins = losses = placed = 0
    pnl_total = 0.0
    for status, cnt, pnl_sum in rows:
        if status != "SKIPPED":
            placed += cnt
        if status == "CLOSED_WIN":
            wins += cnt
        elif status == "CLOSED_LOSS":
            losses += cnt
        if pnl_sum is not None:
            pnl_total += pnl_sum
    win_rate = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0.0
    d["total_bets_placed"] = placed
    d["total_wins"] = wins
    d["total_losses"] = losses
    d["total_pnl_usdc"] = round(pnl_total, 2)
    d["win_rate_pct"] = win_rate

    return {"session": d}


@app.get("/api/ledger")
async def get_ledger(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    mode: str = Query("all"),
    status: str = Query("all"),
    sort: str = Query("default"),  # "default" = newest first; "close_asc" = soonest close first
    since: str | None = Query(None),  # ISO datetime — filter opened_at >= since
    until: str | None = Query(None),  # ISO datetime — filter opened_at <= until
    db: DBSession = Depends(get_db),
):
    """Paginated bet ledger with optional filters."""
    query = db.query(CopiedBet)
    query = _apply_mode_filter(query, mode)

    if status.upper() != "ALL":
        status_map = {
            "open": "OPEN",
            "wins": "CLOSED_WIN",
            "losses": "CLOSED_LOSS",
            "skipped": "SKIPPED",
            "neutral": "CLOSED_NEUTRAL",
        }
        db_status = status_map.get(status.lower(), status.upper())
        query = query.filter(CopiedBet.status == db_status)

    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00")).replace(tzinfo=None)
            query = query.filter(CopiedBet.opened_at >= since_dt)
        except ValueError:
            pass
    if until:
        try:
            until_dt = datetime.fromisoformat(until.replace("Z", "+00:00")).replace(tzinfo=None)
            query = query.filter(CopiedBet.opened_at <= until_dt)
        except ValueError:
            pass

    total = query.count()

    # Sort: soonest market close first (nulls last), otherwise newest bet first
    from sqlalchemy import nulls_last

    if sort == "close_asc":
        order = nulls_last(CopiedBet.market_close_at.asc())
    else:
        order = CopiedBet.id.desc()

    bets = query.order_by(order).offset((page - 1) * limit).limit(limit).all()

    # Build alias lookup for the addresses on this page
    addresses = {b.whale_address for b in bets}
    alias_map = {
        w.address: w.alias for w in db.query(Whale).filter(Whale.address.in_(addresses)).all()
    }

    result = []
    for b in bets:
        d = b.to_dict()
        d["whale_alias"] = alias_map.get(b.whale_address, b.whale_address[:10])
        result.append(d)

    return {
        "bets": result,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": max(1, (total + limit - 1) // limit),
        },
    }


@app.get("/api/ledger/stats")
async def ledger_stats(
    mode: str = Query("all"),
    db: DBSession = Depends(get_db),
):
    """Aggregate stats across all (or mode-filtered) bets."""
    # Determine the active (or most recent) session first so we can scope bets to it
    m = mode.upper()
    if m == "STANDARD":
        session_q = db.query(MonitoringSession).filter(
            MonitoringSession.mode.in_(["SIMULATION", "REAL"])
        )
    elif m in ("SIMULATION", "REAL", "HEDGE_SIM"):
        session_q = db.query(MonitoringSession).filter(MonitoringSession.mode == m)
    else:
        session_q = db.query(MonitoringSession)

    active = (
        session_q.filter(MonitoringSession.is_active == True).first()  # noqa: E712
        or session_q.order_by(MonitoringSession.id.desc()).first()
    )

    query = db.query(CopiedBet)
    query = _apply_mode_filter(query, mode)
    bets = query.all()

    total_bets = len(bets)
    placed = [b for b in bets if b.status != "SKIPPED"]
    wins = [b for b in bets if b.status == "CLOSED_WIN"]
    losses = [b for b in bets if b.status == "CLOSED_LOSS"]
    closed_pnl = [b.pnl_usdc for b in bets if b.pnl_usdc is not None]
    total_pnl = round(sum(closed_pnl), 2)
    decided = [b for b in bets if b.status in ("CLOSED_WIN", "CLOSED_LOSS")]
    decided_pnl = [b.pnl_usdc for b in decided if b.pnl_usdc is not None]
    avg_pnl = round(sum(decided_pnl) / len(decided_pnl), 2) if decided_pnl else 0.0
    win_rate = round(len(wins) / len(decided) * 100, 1) if decided else 0.0

    # Capital currently at risk (sum of size_usdc for all OPEN bets)
    open_bets = [b for b in bets if b.status == "OPEN"]
    capital_at_risk = round(sum(b.size_usdc for b in open_bets if b.mode == "REAL"), 2)
    sim_capital_at_risk = round(
        sum(b.size_usdc for b in open_bets if b.mode in ("SIMULATION", "HEDGE_SIM")), 2
    )

    if active:
        balance = active.current_balance_usdc
    elif m == "REAL":
        balance = 0.0  # No REAL session ever started — show 0, not sim default
    else:
        balance = settings.SIM_STARTING_BALANCE

    return {
        "total_bets": total_bets,
        "placed": len(placed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": win_rate,
        "total_pnl_usdc": total_pnl,
        "avg_pnl_per_bet": avg_pnl,
        "session_balance": round(balance, 2),
        "capital_at_risk": capital_at_risk,
        "sim_capital_at_risk": sim_capital_at_risk,
    }


# ---------------------------------------------------------------------------
# Per-whale analysis
# ---------------------------------------------------------------------------
@app.get("/api/stats/by-whale")
async def stats_by_whale(mode: str = Query("all"), db: DBSession = Depends(get_db)):
    """Aggregate copied_bets performance broken down by whale."""
    from collections import defaultdict

    query = db.query(CopiedBet).filter(CopiedBet.status != "SKIPPED")
    query = _apply_mode_filter(query, mode)
    bets = query.all()

    # Build alias lookup from Whale table
    whales = db.query(Whale).all()
    alias_map = {w.address: (w.alias or w.address) for w in whales}

    groups: dict = defaultdict(
        lambda: {
            "followed": 0,
            "open": 0,
            "wins": 0,
            "losses": 0,
            "neutral": 0,
            "closed": 0,
            "pnl_values": [],
        }
    )

    for b in bets:
        g = groups[b.whale_address]
        g["followed"] += 1
        if b.status == "OPEN":
            g["open"] += 1
        elif b.status == "CLOSED_WIN":
            g["wins"] += 1
            g["closed"] += 1
            if b.pnl_usdc is not None:
                g["pnl_values"].append(b.pnl_usdc)
        elif b.status == "CLOSED_LOSS":
            g["losses"] += 1
            g["closed"] += 1
            if b.pnl_usdc is not None:
                g["pnl_values"].append(b.pnl_usdc)
        elif b.status == "CLOSED_NEUTRAL":
            g["neutral"] += 1
            g["closed"] += 1
            if b.pnl_usdc is not None:
                g["pnl_values"].append(b.pnl_usdc)

    result = []
    for address, g in groups.items():
        total_pnl = round(sum(g["pnl_values"]), 2)
        decided = g["wins"] + g["losses"]
        avg_pnl = round(total_pnl / decided, 2) if decided > 0 else 0.0
        win_rate = round(g["wins"] / decided * 100, 1) if decided > 0 else None
        best = round(max(g["pnl_values"]), 2) if g["pnl_values"] else None
        worst = round(min(g["pnl_values"]), 2) if g["pnl_values"] else None
        result.append(
            {
                "whale_address": address,
                "whale_alias": alias_map.get(address, address),
                "followed": g["followed"],
                "open": g["open"],
                "wins": g["wins"],
                "losses": g["losses"],
                "neutral": g["neutral"],
                "closed": g["closed"],
                "win_rate_pct": win_rate,
                "total_pnl_usdc": total_pnl,
                "avg_pnl_usdc": avg_pnl,
                "best_bet_usdc": best,
                "worst_bet_usdc": worst,
            }
        )

    # --- Add-to-position signal stats per whale ----------------------------
    # Fetch all signals whose linked CopiedBet passes the mode filter.
    sig_query = (
        db.query(AddToPositionSignal)
        .join(CopiedBet, AddToPositionSignal.copied_bet_id == CopiedBet.id)
        .filter(CopiedBet.status != "SKIPPED")
    )
    sig_query = _apply_mode_filter(sig_query, mode)
    all_signals = sig_query.all()

    sig_groups: dict = defaultdict(lambda: {"resolved": 0, "profitable": 0, "pnl": 0.0})
    for sig in all_signals:
        bet: CopiedBet = sig.copied_bet
        if not bet or bet.status in ("OPEN", "PENDING", "SKIPPED", "CLOSED_NEUTRAL"):
            continue
        investment = (
            sig.suggested_add_usdc
            if sig.suggested_add_usdc is not None
            else sig.whale_additional_usdc
        )
        if bet.resolution_price is None:
            continue
        hypo_pnl = investment / max(sig.price, 0.001) * bet.resolution_price - investment
        sg = sig_groups[bet.whale_address]
        sg["resolved"] += 1
        sg["pnl"] += hypo_pnl
        if hypo_pnl > 0.01:
            sg["profitable"] += 1

    for entry in result:
        sg = sig_groups.get(entry["whale_address"])
        if sg and sg["resolved"] > 0:
            entry["signal_resolved"] = sg["resolved"]
            entry["signal_win_rate_pct"] = round(sg["profitable"] / sg["resolved"] * 100, 1)
            entry["signal_total_pnl_usdc"] = round(sg["pnl"], 2)
        else:
            entry["signal_resolved"] = 0
            entry["signal_win_rate_pct"] = None
            entry["signal_total_pnl_usdc"] = None

    # Sort by total P&L descending
    result.sort(key=lambda x: x["total_pnl_usdc"], reverse=True)
    return {"whales": result}


# ---------------------------------------------------------------------------
# Per-whale category breakdown
# ---------------------------------------------------------------------------
@app.get("/api/stats/by-whale-category")
async def stats_by_whale_category(mode: str = Query("all"), db: DBSession = Depends(get_db)):
    """
    Aggregate closed CopiedBets broken down by whale × sport × bet_type.
    Returns per-whale sport and bet-type breakdowns for the analysis page.
    """
    import json as _json
    from collections import defaultdict

    def _empty_bucket():
        return {"wins": 0, "losses": 0, "neutral": 0, "closed": 0, "pnl_values": []}

    query = db.query(CopiedBet).filter(
        CopiedBet.status.in_(["CLOSED_WIN", "CLOSED_LOSS", "CLOSED_NEUTRAL"])
    )
    query = _apply_mode_filter(query, mode)
    closed_bets = query.all()

    whales = db.query(Whale).all()
    alias_map = {w.address: (w.alias or w.address) for w in whales}
    filters_map = {w.address: w.category_filters for w in whales}
    arb_mode_map = {w.address: bool(w.arb_mode) for w in whales}

    # {address: {"by_sport": {sport: bucket}, "by_bet_type": {bet_type: bucket}}}
    data: dict = defaultdict(
        lambda: {
            "by_sport": defaultdict(_empty_bucket),
            "by_bet_type": defaultdict(_empty_bucket),
        }
    )

    for b in closed_bets:
        sport = b.market_category or "Other"
        bt = b.bet_type or "Other"
        for bucket_key, bucket_val in [("by_sport", sport), ("by_bet_type", bt)]:
            g = data[b.whale_address][bucket_key][bucket_val]
            g["closed"] += 1
            if b.status == "CLOSED_WIN":
                g["wins"] += 1
            elif b.status == "CLOSED_LOSS":
                g["losses"] += 1
            else:
                g["neutral"] += 1
            if b.pnl_usdc is not None:
                g["pnl_values"].append(b.pnl_usdc)

    def _summarise(buckets: dict) -> list:
        result = []
        for label, g in sorted(buckets.items()):
            total_pnl = round(sum(g["pnl_values"]), 2) if g["pnl_values"] else 0.0
            decided = g["wins"] + g["losses"]
            win_rate = round(g["wins"] / decided * 100, 1) if decided > 0 else None
            result.append(
                {
                    "label": label,
                    "wins": g["wins"],
                    "losses": g["losses"],
                    "neutral": g["neutral"],
                    "closed": g["closed"],
                    "win_rate_pct": win_rate,
                    "total_pnl_usdc": total_pnl,
                }
            )
        result.sort(key=lambda x: x["total_pnl_usdc"], reverse=True)
        return result

    output = []
    for address, d in data.items():
        raw_filters = filters_map.get(address)
        try:
            parsed_filters = _json.loads(raw_filters) if raw_filters else {}
        except Exception:
            parsed_filters = {}
        output.append(
            {
                "whale_address": address,
                "whale_alias": alias_map.get(address, address),
                "category_filters": parsed_filters,
                "arb_mode": arb_mode_map.get(address, False),
                "by_sport": _summarise(d["by_sport"]),
                "by_bet_type": _summarise(d["by_bet_type"]),
            }
        )

    output.sort(key=lambda x: sum(s["total_pnl_usdc"] for s in x["by_sport"]), reverse=True)
    return {"whales": output}


# ---------------------------------------------------------------------------
# Whale category filter management
# ---------------------------------------------------------------------------


class CategoryFiltersRequest(BaseModel):
    # Opt-out: skip bets in these sports/bet-types
    disabled_sports: list[str] = []
    disabled_bet_types: list[str] = []
    # Opt-in: if non-empty, ONLY copy bets from these sports (overrides disabled_sports)
    allowed_sports: list[str] = []
    # Price threshold: only copy if whale's entry price is within [min, max]
    min_entry_price: float | None = None
    max_entry_price: float | None = None
    # Whale bet size filter: only copy if whale's bet size is within [min, max]
    min_whale_bet_usdc: float | None = None
    max_whale_bet_usdc: float | None = None
    # Keyword filters on the market question (case-insensitive)
    required_keywords: list[str] = []  # ALL must be present
    excluded_keywords: list[str] = []  # NONE may be present


@app.get("/api/whales/{address}/categories")
async def get_whale_categories(address: str, db: DBSession = Depends(get_db)):
    """Return the current category filter config for a whale."""
    import json as _json

    whale = db.query(Whale).filter_by(address=address).first()
    if not whale:
        raise HTTPException(status_code=404, detail="Whale not found")
    try:
        filters = _json.loads(whale.category_filters) if whale.category_filters else {}
    except Exception:
        filters = {}
    return {
        "address": address,
        "disabled_sports": filters.get("disabled_sports", []),
        "disabled_bet_types": filters.get("disabled_bet_types", []),
        "allowed_sports": filters.get("allowed_sports", []),
        "min_entry_price": filters.get("min_entry_price"),
        "max_entry_price": filters.get("max_entry_price"),
        "min_whale_bet_usdc": filters.get("min_whale_bet_usdc"),
        "max_whale_bet_usdc": filters.get("max_whale_bet_usdc"),
        "required_keywords": filters.get("required_keywords", []),
        "excluded_keywords": filters.get("excluded_keywords", []),
    }


@app.patch("/api/whales/{address}/categories")
async def update_whale_categories(
    address: str,
    body: CategoryFiltersRequest,
    db: DBSession = Depends(get_db),
):
    """Update per-whale copy filters (category, price, size, keywords)."""
    import json as _json

    whale = db.query(Whale).filter_by(address=address).first()
    if not whale:
        raise HTTPException(status_code=404, detail="Whale not found")
    payload = {
        "disabled_sports": body.disabled_sports,
        "disabled_bet_types": body.disabled_bet_types,
        "allowed_sports": body.allowed_sports,
        "min_entry_price": body.min_entry_price,
        "max_entry_price": body.max_entry_price,
        "min_whale_bet_usdc": body.min_whale_bet_usdc,
        "max_whale_bet_usdc": body.max_whale_bet_usdc,
        "required_keywords": body.required_keywords,
        "excluded_keywords": body.excluded_keywords,
    }
    whale.category_filters = _json.dumps(payload)
    db.add(whale)
    db.commit()
    return {"address": address, **payload}


# ---------------------------------------------------------------------------
# Whale analysis page
# ---------------------------------------------------------------------------
@app.get("/whales", response_class=HTMLResponse)
async def whale_analysis_page(request: Request):
    return templates.TemplateResponse("whales.html", {"request": request})


@app.get("/hedge", response_class=HTMLResponse)
async def hedge_dashboard(request: Request):
    return templates.TemplateResponse("hedge.html", {"request": request})


@app.get("/hedge/whales", response_class=HTMLResponse)
async def hedge_whales_page(request: Request):
    return templates.TemplateResponse("hedge_whales.html", {"request": request})


# ---------------------------------------------------------------------------
# Activity feed
# ---------------------------------------------------------------------------
@app.get("/api/activity")
async def get_activity(mode: str = Query("all"), db: DBSession = Depends(get_db)):
    """
    Returns the last 20 CopiedBets for the live feed, sorted by ID descending.
    Includes max_id so the frontend can skip re-renders when nothing changed.
    """
    query = db.query(CopiedBet)
    query = _apply_mode_filter(query, mode)
    bets = query.order_by(CopiedBet.id.desc()).limit(20).all()

    # Build a whale alias lookup from the addresses present in this page only
    addresses = {bet.whale_address for bet in bets}
    alias_map = {
        w.address: w.alias for w in db.query(Whale).filter(Whale.address.in_(addresses)).all()
    }

    items = []
    for bet in bets:
        d = bet.to_dict()
        d["whale_alias"] = alias_map.get(bet.whale_address, bet.whale_address[:10])
        items.append(d)

    max_id = bets[0].id if bets else 0
    return {"activity": items, "max_id": max_id}


# ---------------------------------------------------------------------------
# Leaderboard discovery
# ---------------------------------------------------------------------------
@app.get("/api/leaderboard")
async def get_leaderboard(
    time_period: str = Query("ALL"),
    limit: int = Query(50, ge=1, le=100),
    db: DBSession = Depends(get_db),
):
    """Fetch Polymarket leaderboard for whale discovery."""
    try:
        entries = await poly_client.get_leaderboard(time_period=time_period, limit=limit)
    except Exception as exc:
        logger.error("get_leaderboard error: %s", exc)
        raise HTTPException(status_code=502, detail="Leaderboard API unavailable")

    tracked_addresses = {w.address for w in db.query(Whale).all()}

    results = []
    skipped_low_volume = 0
    for entry in entries:
        # v1 API uses "proxyWallet"; keep fallbacks for any future field renames
        address = (
            entry.get("proxyWallet")
            or entry.get("proxyWalletAddress")
            or entry.get("address")
            or ""
        ).lower()

        pnl = entry.get("pnl") or 0
        # v1 API uses "vol" for volume
        volume = float(entry.get("vol") or entry.get("volume") or 0)

        # Filter: Polymarket does not expose a prediction count in any public API.
        # Volume is the best available proxy — skip traders below the threshold.
        if volume < settings.MIN_WHALE_VOLUME_USDC:
            skipped_low_volume += 1
            continue

        alias = (
            entry.get("userName")
            or entry.get("name")
            or entry.get("pseudonym")
            or f"Whale_{address[:6]}"
        )

        results.append(
            {
                "address": address,
                "alias": alias,
                "pnl_usdc": round(float(pnl), 2) if pnl else 0.0,
                "volume_usdc": round(volume, 2),
                "already_tracked": address in tracked_addresses,
            }
        )

    if skipped_low_volume:
        logger.debug(
            "Leaderboard: filtered out %d traders below $%.0f volume threshold",
            skipped_low_volume,
            settings.MIN_WHALE_VOLUME_USDC,
        )

    # Write a timestamped discover log so every leaderboard call is reviewable
    _write_discover_log(
        time_period=time_period,
        limit=limit,
        raw_entries=entries,
        results=results,
        tracked_addresses=list(tracked_addresses),
    )

    return {"leaderboard": results}


def _write_discover_log(
    time_period: str,
    limit: int,
    raw_entries: list,
    results: list,
    tracked_addresses: list,
) -> None:
    """Write a timestamped JSON snapshot of a leaderboard fetch to logs/."""
    try:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        log_path = _LOGS_DIR / f"discover_{ts}.json"
        payload = {
            "timestamp_utc": datetime.utcnow().isoformat(),
            "params": {"time_period": time_period, "limit": limit},
            "tracked_addresses": tracked_addresses,
            "results": results,
            "raw_api_response": raw_entries,
        }
        log_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        logger.debug("Discover log written: %s", log_path.name)
    except Exception as exc:
        logger.warning("Failed to write discover log: %s", exc)


# ---------------------------------------------------------------------------
# Add-to-position signals
# ---------------------------------------------------------------------------


def _signal_to_dict(sig: AddToPositionSignal) -> dict:
    """
    Enrich a signal dict with its linked CopiedBet's outcome so the frontend
    can show hypothetical P&L and market context without extra round-trips.

    Hypothetical P&L logic:
        additional_shares  = whale_additional_usdc / signal.price
        hypothetical_pnl   = additional_shares * bet.resolution_price - whale_additional_usdc
    This answers: "what would I have made if I had also added to the position?"
    """
    d = sig.to_dict()
    bet: CopiedBet = sig.copied_bet

    if bet:
        d["bet_status"] = bet.status
        d["bet_question"] = bet.question or bet.market_id
        d["entry_price"] = round(bet.price_at_entry, 4)
        d["whale_address"] = bet.whale_address
        d["whale_alias"] = bet.whale_address  # overridden below if alias available
        # Resolve alias from the whale_bet → whale relationship
        if bet.whale_bet and bet.whale_bet.whale:
            alias = bet.whale_bet.whale.alias
            d["whale_alias"] = alias if alias else bet.whale_address

        # Compute hypothetical P&L for resolved bets.
        # Use suggested_add_usdc (our scaled bet size) if available, otherwise
        # fall back to the whale's raw addition for legacy records.
        if bet.resolution_price is not None and bet.status not in ("OPEN", "PENDING", "SKIPPED"):
            investment = (
                sig.suggested_add_usdc
                if sig.suggested_add_usdc is not None
                else sig.whale_additional_usdc
            )
            additional_shares = investment / max(sig.price, 0.001)
            proceeds = additional_shares * bet.resolution_price
            hypo_pnl = round(proceeds - investment, 2)
            d["hypothetical_pnl_usdc"] = hypo_pnl
        else:
            d["hypothetical_pnl_usdc"] = None  # still open — unknown outcome
    else:
        d["bet_status"] = "UNKNOWN"
        d["bet_question"] = ""
        d["entry_price"] = None

    return d


@app.get("/api/signals/stats")
async def get_signals_stats(mode: str = Query("all"), db: DBSession = Depends(get_db)):
    """
    Aggregate statistics for all add-to-position signals.
    Used to power the "Should I Follow?" dashboard widget.
    """
    query = db.query(AddToPositionSignal)
    m = mode.upper()
    if m in ("SIMULATION", "REAL", "HEDGE_SIM"):
        query = query.join(CopiedBet, AddToPositionSignal.copied_bet_id == CopiedBet.id).filter(
            CopiedBet.mode == m
        )
    elif m == "STANDARD":
        query = query.join(CopiedBet, AddToPositionSignal.copied_bet_id == CopiedBet.id).filter(
            CopiedBet.mode.in_(["SIMULATION", "REAL"])
        )
    signals = query.all()

    total = len(signals)
    open_count = 0
    resolved_count = 0
    profitable = 0
    losing = 0
    total_invested = 0.0
    total_pnl = 0.0

    for sig in signals:
        investment = (
            sig.suggested_add_usdc
            if sig.suggested_add_usdc is not None
            else sig.whale_additional_usdc
        )
        total_invested += investment
        bet: CopiedBet = sig.copied_bet

        if not bet or bet.status in ("OPEN", "PENDING"):
            open_count += 1
            continue

        if bet.status in ("SKIPPED", "CLOSED_NEUTRAL"):
            continue  # voided/skipped bets have no outcome; exclude from stats

        resolved_count += 1
        if bet.resolution_price is not None:
            additional_shares = investment / max(sig.price, 0.001)
            hypo_pnl = additional_shares * bet.resolution_price - investment
            total_pnl += hypo_pnl
            if hypo_pnl > 0.01:
                profitable += 1
            elif hypo_pnl < -0.01:
                losing += 1

    win_rate = round(profitable / resolved_count * 100, 1) if resolved_count > 0 else None
    avg_pnl = round(total_pnl / resolved_count, 2) if resolved_count > 0 else None

    # Recommendation signal: positive if win_rate > 55% and avg_pnl > 0
    if resolved_count == 0:
        verdict = "insufficient_data"
    elif win_rate >= 60 and avg_pnl > 0:
        verdict = "follow"
    elif win_rate <= 40 or avg_pnl < 0:
        verdict = "avoid"
    else:
        verdict = "neutral"

    return {
        "total_signals": total,
        "open_signals": open_count,
        "resolved_signals": resolved_count,
        "profitable": profitable,
        "losing": losing,
        "win_rate_pct": win_rate,
        "total_invested": round(total_invested, 2),
        "total_pnl_usdc": round(total_pnl, 2),
        "avg_pnl_per_signal": avg_pnl,
        "verdict": verdict,
    }


@app.get("/api/signals")
async def get_signals(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=5000),
    mode: str = Query("all"),
    db: DBSession = Depends(get_db),
):
    """Return add-to-position signals with computed hypothetical P&L."""
    base = db.query(AddToPositionSignal)
    m = mode.upper()
    if m in ("SIMULATION", "REAL", "HEDGE_SIM"):
        base = base.join(CopiedBet, AddToPositionSignal.copied_bet_id == CopiedBet.id).filter(
            CopiedBet.mode == m
        )
    elif m == "STANDARD":
        base = base.join(CopiedBet, AddToPositionSignal.copied_bet_id == CopiedBet.id).filter(
            CopiedBet.mode.in_(["SIMULATION", "REAL"])
        )
    total = base.count()
    signals = (
        base.order_by(AddToPositionSignal.id.desc()).offset((page - 1) * limit).limit(limit).all()
    )
    return {
        "signals": [_signal_to_dict(s) for s in signals],
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": max(1, (total + limit - 1) // limit),
        },
    }
