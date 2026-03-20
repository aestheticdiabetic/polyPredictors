# Polymarket Copier

A web app that monitors high-volume Polymarket traders ("whales") and automatically copies their bets — either in simulation or with real USDC.

## How It Works

1. **Discover Whales** — Browse Polymarket leaderboard traders filtered by minimum trading volume and add them to your watchlist.
2. **Monitor** — A background scheduler polls tracked whale wallets on a configurable interval for new trades.
3. **Copy Bets** — When a whale places a bet, the engine evaluates it against configurable risk rules (price drift, market expiry, position sizing) and either simulates or executes the copy trade via the Polymarket CLOB API.
4. **Track P&L** — All positions are tracked in a local SQLite database. A resolution checker periodically closes settled markets and records profit/loss.

## Features

- **Simulation mode** — Paper-trade with a virtual balance before going live; no credentials needed
- **Real mode** — Live order execution via Polymarket's CLOB API with your wallet credentials
- **Whale discovery** — Leaderboard browser with volume filtering to surface high-activity traders
- **Whale analysis page** — Breakdown of whale activity by sport/category and bet type
- **Risk controls** — Max bet % of balance, max price drift, min hours-to-close, max trade age
- **Add-to-position signals** — Tracks repeated whale buys on the same market
- **Web UI** — Single-page dashboard built with vanilla JS + Jinja2 templates

## Stack

| Layer | Technology |
|---|---|
| Backend | Python 3, FastAPI, APScheduler |
| Database | SQLite via SQLAlchemy |
| Polymarket API | `py-clob-client`, Polymarket Data/Gamma APIs |
| Frontend | Vanilla JS, Jinja2 templates |
| Server | Uvicorn |

## Setup

```bash
# 1. Clone and create a virtual environment
python -m venv venv
source venv/Scripts/activate   # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — simulation mode works without credentials
```

### Environment Variables (`.env`)

| Variable | Default | Description |
|---|---|---|
| `POLY_PRIVATE_KEY` | — | Wallet private key (real mode only) |
| `POLY_API_KEY` | — | Polymarket API key (real mode only) |
| `POLY_API_SECRET` | — | Polymarket API secret (real mode only) |
| `POLY_API_PASSPHRASE` | — | Polymarket API passphrase (real mode only) |
| `POLY_FUNDER_ADDRESS` | — | Wallet address (real mode only) |
| `SIM_STARTING_BALANCE` | `200.0` | Virtual balance for simulation (USDC) |
| `MAX_BET_PCT` | `0.05` | Max fraction of balance per copied bet |
| `POLLING_INTERVAL_SECONDS` | `30` | How often to poll whale activity |
| `RESOLUTION_CHECK_INTERVAL_SECONDS` | `60` | How often to check for settled markets |
| `MIN_MARKET_HOURS_TO_CLOSE` | `1.0` | Skip markets closing within this window |
| `MAX_TRADE_AGE_HOURS` | `24.0` | Ignore whale trades older than this |
| `MAX_PRICE_DRIFT_PCT` | `0.05` | Max price movement allowed since whale's trade |
| `MIN_WHALE_VOLUME_USDC` | `1000000` | Min leaderboard volume to surface in Discover |

## Running

```bash
python run.py
# Dashboard available at http://localhost:8000
```

## VPN Tunnelling (Canadian IP)

Polymarket requires a Canadian IP. Rather than enabling a VPN system-wide, you can route **only this app's traffic** through a Canadian VPN connection using Docker + [gluetun](https://github.com/qdm12/gluetun).

### Prerequisites

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) for Windows (free for personal use, WSL2 backend required — default on Windows 11)
2. Obtain a Canadian OpenVPN config file (`.ovpn`) from your VPN provider

### Setup

**1. Add your `.ovpn` file**

Place your OpenVPN config at `vpn/canada.ovpn` (or any name — update `docker-compose.vpn.yml` if you use a different name).

> If the `.ovpn` file contains `block-outside-dns`, remove that line — it is a Windows-only directive not supported inside the Linux container.

**2. Add VPN credentials to `.env`**

```env
# ProtonVPN: find these at account.proton.me → Account → OpenVPN/IKEv2 credentials
OPENVPN_USER=your_openvpn_username
OPENVPN_PASSWORD=your_openvpn_password

# Enable the proxy
PROXY_URL=http://localhost:8888
```

**3. Start with VPN**

```bat
start-with-vpn.bat
```

This script starts the gluetun container, waits until the VPN tunnel is healthy, then launches the app. Your browser and all other applications remain on your normal connection.

### Manual start

```bash
# Start VPN container
docker compose -f docker-compose.vpn.yml up -d

# Check it's healthy and confirm Canadian IP
docker inspect --format={{.State.Health.Status}} polymarket-vpn
curl -x http://localhost:8888 https://api.ipify.org?format=json

# Then run the app normally
python run.py

# Stop VPN when done
docker compose -f docker-compose.vpn.yml down
```

### Without VPN

Comment out `PROXY_URL` in `.env` and run `python run.py` directly (e.g. if your system VPN is already active).

### Environment variables added for VPN

| Variable | Description |
|---|---|
| `PROXY_URL` | HTTP proxy URL for routing app traffic through VPN (e.g. `http://localhost:8888`) |
| `OPENVPN_USER` | OpenVPN username (passed to gluetun container) |
| `OPENVPN_PASSWORD` | OpenVPN password (passed to gluetun container) |

## Project Structure

```
polymarket-copier/
├── backend/
│   ├── main.py              # FastAPI app and all API routes
│   ├── whale_monitor.py     # Background scheduler — polls whale activity
│   ├── bet_engine.py        # Bet evaluation, risk checks, order execution
│   ├── polymarket_client.py # Polymarket API client wrapper
│   ├── categorizer.py       # Sport/bet-type classification for markets
│   ├── database.py          # SQLAlchemy models and DB helpers
│   └── config.py            # Settings loaded from .env
├── frontend/
│   ├── templates/index.html # Jinja2 dashboard template
│   └── static/              # CSS and JS
├── data/                    # SQLite database (gitignored)
├── logs/                    # Rotating log files (gitignored)
├── requirements.txt
└── run.py                   # Entrypoint
```

## Disclaimer

This tool is for educational and personal use. Prediction market trading carries significant financial risk. Use simulation mode to evaluate performance before enabling real trading.
