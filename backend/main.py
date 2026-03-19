"""
FastAPI application entry point.
All API routes are defined here.
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from sqlalchemy.orm import Session as DBSession

from backend.config import settings
from backend.database import (
    AddToPositionSignal,
    CopiedBet,
    MonitoringSession,
    SessionLocal,
    Whale,
    WhaleBet,
    get_db,
    init_db,
)
from backend.bet_engine import BetEngine
from backend.polymarket_client import PolymarketClient
from backend.whale_monitor import WhaleMonitor

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

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
    """Initialise the database on startup, clean up on shutdown."""
    init_db()
    logger.info("Database initialised")
    yield
    # Graceful shutdown
    whale_monitor.stop_monitoring()
    await poly_client.close()
    logger.info("Application shutdown complete")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
_root = Path(__file__).parent.parent
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
    runtime_hours: Optional[float] = None  # None = manual stop


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
    """Start a new monitoring session."""

    # Prevent starting if already running
    if whale_monitor.is_running():
        raise HTTPException(status_code=400, detail="A monitoring session is already running")

    # Validate REAL mode requirements
    if body.mode == "REAL" and not settings.credentials_valid():
        raise HTTPException(
            status_code=400,
            detail="REAL mode requires Polymarket credentials in .env",
        )

    # Close any lingering active sessions
    db.query(MonitoringSession).filter_by(is_active=True).update(
        {"is_active": False, "stopped_at": datetime.utcnow()}
    )
    db.commit()

    starting_balance = (
        settings.SIM_STARTING_BALANCE
        if body.mode == "SIMULATION"
        else (await poly_client.get_wallet_balance() or 0.0)
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

    whale_monitor.start_monitoring(session.id)

    # Auto-stop if runtime_hours specified
    if body.runtime_hours:
        from apscheduler.triggers.date import DateTrigger
        from datetime import timedelta

        stop_at = datetime.utcnow() + timedelta(hours=body.runtime_hours)
        try:
            whale_monitor._scheduler.add_job(
                func=_auto_stop_session,
                trigger=DateTrigger(run_date=stop_at),
                id="auto_stop",
                replace_existing=True,
            )
        except Exception as exc:
            logger.warning("Could not schedule auto-stop: %s", exc)

    logger.info("Session %d started in %s mode", session.id, body.mode)
    return {"session": session.to_dict(), "message": "Session started"}


@app.post("/api/session/stop")
async def stop_session(db: DBSession = Depends(get_db)):
    """Stop the current monitoring session."""
    active_session = db.query(MonitoringSession).filter_by(is_active=True).first()

    whale_monitor.stop_monitoring()

    if active_session:
        active_session.is_active = False
        active_session.stopped_at = datetime.utcnow()
        db.commit()
        return {"session": active_session.to_dict(), "message": "Session stopped"}

    return {"session": None, "message": "No active session to stop"}


def _auto_stop_session():
    """Called by scheduler when runtime_hours expires."""
    db = SessionLocal()
    try:
        session = db.query(MonitoringSession).filter_by(is_active=True).first()
        if session:
            session.is_active = False
            session.stopped_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()
    whale_monitor.stop_monitoring()
    logger.info("Auto-stopped session after runtime expiry")


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
@app.get("/api/ledger")
async def get_ledger(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    mode: str = Query("all"),
    status: str = Query("all"),
    db: DBSession = Depends(get_db),
):
    """Paginated bet ledger with optional filters."""
    query = db.query(CopiedBet)

    if mode.upper() in ("SIMULATION", "REAL"):
        query = query.filter(CopiedBet.mode == mode.upper())

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

    total = query.count()
    bets = (
        query.order_by(CopiedBet.id.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    return {
        "bets": [b.to_dict() for b in bets],
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
    query = db.query(CopiedBet)
    if mode.upper() in ("SIMULATION", "REAL"):
        query = query.filter(CopiedBet.mode == mode.upper())

    bets = query.all()

    total_bets = len(bets)
    placed = [b for b in bets if b.status != "SKIPPED"]
    wins = [b for b in bets if b.status == "CLOSED_WIN"]
    losses = [b for b in bets if b.status == "CLOSED_LOSS"]
    closed_pnl = [b.pnl_usdc for b in bets if b.pnl_usdc is not None]
    total_pnl = round(sum(closed_pnl), 2)
    avg_pnl = round(total_pnl / len(closed_pnl), 2) if closed_pnl else 0.0
    win_rate = round(len(wins) / len(placed) * 100, 1) if placed else 0.0

    # Current session balance
    active = db.query(MonitoringSession).filter_by(is_active=True).first()
    balance = active.current_balance_usdc if active else settings.SIM_STARTING_BALANCE

    return {
        "total_bets": total_bets,
        "placed": len(placed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": win_rate,
        "total_pnl_usdc": total_pnl,
        "avg_pnl_per_bet": avg_pnl,
        "session_balance": round(balance, 2),
    }


# ---------------------------------------------------------------------------
# Activity feed
# ---------------------------------------------------------------------------
@app.get("/api/activity")
async def get_activity(db: DBSession = Depends(get_db)):
    """
    Returns the last 20 CopiedBets combined for the live feed,
    sorted by ID descending (most recent first).
    """
    bets = (
        db.query(CopiedBet)
        .order_by(CopiedBet.id.desc())
        .limit(20)
        .all()
    )
    items = []
    for bet in bets:
        d = bet.to_dict()
        # Attach whale alias by looking up whale address
        whale = db.query(Whale).filter_by(address=bet.whale_address).first()
        d["whale_alias"] = whale.alias if whale else bet.whale_address[:10]
        items.append(d)
    return {"activity": items}


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
        entries = await poly_client.get_leaderboard(
            time_period=time_period, limit=limit
        )
    except Exception as exc:
        logger.error("get_leaderboard error: %s", exc)
        raise HTTPException(status_code=502, detail="Leaderboard API unavailable")

    tracked_addresses = {
        w.address for w in db.query(Whale).all()
    }

    results = []
    for entry in entries:
        address = (
            entry.get("proxyWalletAddress")
            or entry.get("proxy_wallet_address")
            or entry.get("address")
            or ""
        ).lower()

        pnl = entry.get("pnl") or entry.get("profit") or 0
        volume = entry.get("volume") or entry.get("usdcVolume") or 0

        results.append({
            "address": address,
            "alias": entry.get("name") or entry.get("pseudonym") or f"Whale_{address[:6]}",
            "pnl_usdc": round(float(pnl), 2) if pnl else 0.0,
            "volume_usdc": round(float(volume), 2) if volume else 0.0,
            "already_tracked": address in tracked_addresses,
        })

    return {"leaderboard": results}


# ---------------------------------------------------------------------------
# Add-to-position signals
# ---------------------------------------------------------------------------
@app.get("/api/signals")
async def get_signals(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    db: DBSession = Depends(get_db),
):
    """Return add-to-position signals."""
    total = db.query(AddToPositionSignal).count()
    signals = (
        db.query(AddToPositionSignal)
        .order_by(AddToPositionSignal.id.desc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )
    return {
        "signals": [s.to_dict() for s in signals],
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": max(1, (total + limit - 1) // limit),
        },
    }
