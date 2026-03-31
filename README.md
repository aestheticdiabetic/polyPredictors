# Polymarket Copier

A web app that monitors high-volume Polymarket traders ("whales") and automatically copies their bets — either in simulation or with real USDC.

## How It Works

1. **Discover Whales** — Browse Polymarket leaderboard traders filtered by minimum trading volume and add them to your watchlist.
2. **Monitor** — A background scheduler polls tracked whale wallets on a configurable interval for new trades. Optionally, whale exits are detected via on-chain Polygon event logs instead of API polling.
3. **Copy Bets** — When a whale places a bet, the engine evaluates it against configurable risk rules (asymmetric price drift guards, entry price ceiling, market expiry, position sizing) and either simulates or executes the copy trade via the Polymarket CLOB API.
4. **Track P&L** — All positions are tracked in a local SQLite database. A resolution checker periodically closes settled markets and records profit/loss. In real mode, resolved positions are automatically redeemed on-chain.

## Features

- **Simulation mode** — Paper-trade with a virtual balance; no credentials needed. Optional fee simulation to project real-mode returns.
- **Real mode** — Live order execution via Polymarket's CLOB API with your wallet credentials
- **Conviction-based sizing** — Bet size scales with the whale's relative bet size vs their historical average, using a configurable conviction exponent
- **Arb mode** — Alternative sizing model that divides the balance across expected concurrent positions and scales by extremity of the market price
- **Asymmetric drift guards** — Tight cap on upward drift (overpaying vs. whale), wider allowance for downward drift (getting in cheaper)
- **Drift retry watchlist** — Near-miss bets are retried on a short interval for a configurable window instead of being dropped immediately
- **Background price pre-fetching** — Keeps prices fresh for all open-position tokens so drift checks are near-instant when a whale bet fires
- **On-chain exit detection** — Optional Polygon `eth_getLogs` monitor on the CTF Exchange contracts detects whale sells with ~4s latency, replacing activity-API polling
- **CLOB WebSocket entry detection** — Optional CLOB WebSocket monitor (`wss://ws-subscriptions-clob.polymarket.com`) detects whale buys with ~100ms latency vs 2–4s HTTP polling; runs alongside polling as fallback
- **Order book depth check** — Optional pre-entry gate rejects bets when ask-side USDC depth is insufficient relative to the intended bet size (fluid threshold scales with bet size)
- **Wash trade filter** — Optional gate skips bets where the same whale sold the same token within a configurable rolling window
- **Per-token slippage tracking** — Records fill vs mid-price slippage at every close; optional entry gate blocks tokens with historically high average slippage
- **Harvest exits** — Optional scheduler closes positions independently of whale action when a configurable profit multiplier is reached
- **Near-close harvest** — Optional secondary harvest that locks in profit when a market is within a configurable number of hours of closing
- **Soft exit confirmation** — Optional delay before following a whale sell: if price stays above a buffer threshold for the confirmation window, the position is held
- **Whale win rate tracking** — Tracks per-whale rolling win/loss outcomes; used by adaptive sizing to scale bet sizes up or down based on recent performance
- **CLOB auth pre-warm** — CLOB session is initialized at startup to eliminate first-trade auth latency in REAL mode
- **Auto-redemption** — Resolved positions are automatically redeemed on-chain (REAL mode only)
- **Whale discovery** — Leaderboard browser with volume filtering to surface high-activity traders
- **Whale analysis page** — Breakdown of whale activity by sport/category and bet type
- **Add-to-position signals** — Tracks repeated whale buys on the same market
- **Web UI** — Single-page dashboard built with vanilla JS + Jinja2 templates

## Stack

| Layer | Technology |
|---|---|
| Backend | Python 3, FastAPI, APScheduler |
| Database | SQLite via SQLAlchemy |
| Polymarket API | `py-clob-client`, Polymarket Data/Gamma/CLOB APIs |
| On-chain | `web3.py`, Polygon RPC (redemption + chain exit monitoring) |
| Frontend | Vanilla JS, Jinja2 templates |
| Server | Uvicorn |
| Deployment | Docker, Hetzner VPS |

## Project Structure

```
polymarket-copier/
├── backend/
│   ├── main.py                # FastAPI app and all API routes
│   ├── whale_monitor.py       # Background scheduler — polls whale activity; owns harvest scheduler
│   ├── whale_chain_monitor.py # On-chain exit detection via Polygon eth_getLogs
│   ├── clob_ws_monitor.py     # CLOB WebSocket entry detection (~100ms latency)
│   ├── bet_engine.py          # Bet evaluation, risk checks, order execution, slippage tracking
│   ├── risk_calculator.py     # Conviction-based bet sizing logic (whale perf multiplier hook)
│   ├── redemption.py          # Auto-redemption of resolved positions on-chain
│   ├── polymarket_client.py   # Polymarket API client wrapper
│   ├── categorizer.py         # Sport/bet-type classification for markets
│   ├── database.py            # SQLAlchemy models and DB helpers
│   └── config.py              # Settings loaded from .env
├── frontend/
│   ├── templates/index.html   # Jinja2 dashboard template
│   └── static/                # CSS and JS
├── tests/
│   └── test_regressions.py   # Regression tests covering known past bugs
├── data/                      # SQLite database (gitignored)
├── logs/                      # Rotating log files (gitignored)
├── docker-compose.yml         # Production deployment (port 8081)
├── docker-compose.vpn.yml     # Local VPN proxy via gluetun
├── Dockerfile
├── requirements.txt
└── run.py                     # Entrypoint
```

## Deployment (VPS + Docker)

The primary deployment runs in Docker on a Hetzner VPS. The container runs on port 8081 (bound to `127.0.0.1`) and is accessed via SSH tunnel.

See [VPS-COMMANDS.md](VPS-COMMANDS.md) for the full reference of SSH, Docker, deploy, database, and maintenance commands.

**Quick deploy after code change:**

```powershell
# On home machine
git push
```

```bash
# On VPS
git pull && docker compose up -d --build
```

**Access the dashboard via SSH tunnel** (see VPS-COMMANDS.md for the tunnel command).

## Local Setup

```bash
# 1. Create and activate a virtual environment
python -m venv venv
source venv/Scripts/activate   # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — simulation mode works without credentials

# 4. Run
python run.py
# Dashboard at http://localhost:8000
```

## Environment Variables (`.env`)

### Polymarket Credentials (REAL mode only)

| Variable | Description |
|---|---|
| `POLY_PRIVATE_KEY` | Wallet private key |
| `POLY_API_KEY` | Polymarket API key |
| `POLY_API_SECRET` | Polymarket API secret |
| `POLY_API_PASSPHRASE` | Polymarket API passphrase |
| `POLY_FUNDER_ADDRESS` | Wallet address |

### Simulation

| Variable | Default | Description |
|---|---|---|
| `SIM_STARTING_BALANCE` | `200.0` | Virtual balance in USDC |
| `SIM_APPLY_FEES` | `false` | Deduct taker fees in simulation to mirror real-mode P&L |
| `SIM_ASSUMED_FEE_BPS` | `0` | Assumed taker fee (basis points) when `SIM_APPLY_FEES=true` |

### Bet Sizing

| Variable | Default | Description |
|---|---|---|
| `MAX_BET_PCT` | `0.05` | Max fraction of balance per copied bet |
| `BET_SIZE_SCALAR` | `1.0` | Global scalar applied to all computed bet sizes (`< 1.0` to reduce) |
| `MIN_BET_USDC` | `1.0` | Minimum USDC to place on any single bet |
| `CONVICTION_EXPONENT` | `1.5` | Exponent on conviction ratio ≥ 1.0 — rewards high-conviction bets convexly |
| `ARB_MIDPOINT` | `0.25` | Price at which the arb extremity multiplier equals 1.0 |
| `ARB_CONCURRENT_POSITIONS` | `6` | Expected concurrent positions for arb-mode whales |

### Risk & Entry Filters

| Variable | Default | Description |
|---|---|---|
| `MAX_PRICE_DRIFT_PCT` | `0.05` | Legacy symmetric drift cap (still used for sell-side checks) |
| `MAX_UPWARD_DRIFT_PCT` | `0.02` | Max upward drift before skipping (price > whale entry) |
| `MAX_DOWNWARD_DRIFT_PCT` | `0.10` | Max downward drift allowed (price < whale entry — favourable) |
| `REAL_MAX_ENTRY_PRICE` | `0.80` | REAL mode: skip bets above this price (insufficient upside) |
| `REAL_MIN_NET_UPSIDE` | `0.05` | REAL mode: minimum `(1 - price) - fee` required to enter |
| `MAX_OPEN_POSITIONS_PER_MARKET` | `2` | Maximum open tranches per market (drift watchlist retry path) |
| `MIN_MARKET_HOURS_TO_CLOSE` | `0.0` | Skip markets closing within this many hours |
| `MAX_TRADE_AGE_HOURS` | `24.0` | Ignore whale trades older than this |

### Sell Handling

| Variable | Default | Description |
|---|---|---|
| `SELL_MAX_FOK_RETRIES` | `3` | FOK retry attempts before declaring a sell exhausted |
| `SELL_ACCEPT_DEGRADED_FILL` | `false` | After retry exhaustion, accept best bid regardless of drift |
| `SELL_CLOSE_RETRIES` | `4` | Transient-failure retries before waiting for the next poll cycle |
| `SELL_CLOSE_RETRY_DELAY_SECONDS` | `1` | Seconds between sell retry attempts |

### Rate Limiting

| Variable | Default | Description |
|---|---|---|
| `CLOB_MAX_RATE_PER_SEC` | `5.0` | Client-side CLOB request rate cap (req/s). Lower = safer; Polymarket's nginx threshold is ~10–20 req/s |

### Polling & Scheduling

| Variable | Default | Description |
|---|---|---|
| `POLLING_INTERVAL_SECONDS` | `2` | How often to poll whale activity |
| `RESOLUTION_CHECK_INTERVAL_SECONDS` | `60` | How often to check for settled markets |
| `PRICE_PREFETCH_INTERVAL_SECONDS` | `2` | Background price pre-fetch interval (`0` to disable) |
| `DRIFT_RETRY_INTERVAL_SECONDS` | `5` | How often to re-check near-miss drift skips |
| `DRIFT_RETRY_WINDOW_SECONDS` | `120` | How long to retry a drift-skipped bet before giving up |

### Order Book & Entry Filters (Stage 2)

| Variable | Default | Description |
|---|---|---|
| `ORDER_BOOK_CHECK_ENABLED` | `false` | Reject bets when ask-side depth is below the fluid threshold |
| `MIN_BOOK_DEPTH_USDC` | `50.0` | Minimum required USDC depth (scaled by bet size at runtime) |
| `MAX_BOOK_SLIPPAGE_PCT` | `0.03` | How far above reference price to sum depth |
| `WASH_TRADE_DETECTION_ENABLED` | `false` | Skip bets where the same whale sold the same token recently |
| `WASH_TRADE_WINDOW_MINUTES` | `30` | Look-back window for detecting wash trades |

### Harvest & Independent Exits (Stage 3)

| Variable | Default | Description |
|---|---|---|
| `HARVEST_ENABLED` | `false` | Close positions independently when the profit multiplier is reached |
| `HARVEST_MULTIPLIER` | `1.8` | Close when `current_price > entry_price × multiplier` |
| `HARVEST_NEAR_CLOSE_HOURS` | `2.0` | Additional near-close harvest: hours before market close to trigger |
| `HARVEST_NEAR_CLOSE_MIN_MULTIPLIER` | `1.3` | Minimum profit multiplier required for near-close harvest |
| `EXIT_CONFIRMATION_ENABLED` | `false` | Delay whale-triggered sell if price is above the buffer threshold |
| `EXIT_CONFIRMATION_BUFFER` | `0.05` | Price must be > `entry × (1 + buffer)` to trigger confirmation delay |
| `EXIT_CONFIRMATION_TIMEOUT_SECONDS` | `300` | Seconds to wait before closing anyway if still above buffer |

### Slippage Tracking (Stage 4)

| Variable | Default | Description |
|---|---|---|
| `SLIPPAGE_TRACKING_ENABLED` | `false` | Record fill vs mid-price slippage and use as entry gate |
| `MAX_HISTORICAL_SLIPPAGE_PCT` | `0.05` | Skip tokens whose rolling average slippage exceeds this |

### CLOB WebSocket (Stage 5)

| Variable | Default | Description |
|---|---|---|
| `CLOB_WS_ENABLED` | `false` | Enable CLOB WebSocket entry detection (~100ms vs 2–4s polling) |
| `CLOB_WS_URL` | `wss://ws-subscriptions-clob.polymarket.com` | CLOB WebSocket endpoint |

### Whale Performance & Adaptive Sizing (Stages 1 & 6)

| Variable | Default | Description |
|---|---|---|
| `WHALE_PERF_WINDOW` | `50` | Rolling window of outcomes used to compute per-whale win rate |
| `ADAPTIVE_SIZING_ENABLED` | `false` | Scale bet sizes by whale's recent win rate (requires populated win_rate data) |
| `WHALE_PERF_MIN_MULTIPLIER` | `0.5` | Minimum size multiplier for a whale with 0% recent win rate |
| `WHALE_PERF_MAX_MULTIPLIER` | `1.5` | Maximum size multiplier for a whale with 100% recent win rate |

### On-Chain (Polygon)

| Variable | Default | Description |
|---|---|---|
| `POLYGON_RPC_URL` | `https://polygon-rpc.com` | Polygon RPC for redemption and chain monitoring |
| `CHAIN_EXIT_ENABLED` | `false` | Replace API exit polling with on-chain `OrderFilled` event monitoring |
| `CHAIN_EXIT_POLL_INTERVAL_SECONDS` | `2` | Poll interval for on-chain exit monitor |
| `CHAIN_EXIT_LOOKBACK_BLOCKS` | `150` | Blocks to look back on startup (~5 min at 2s/block) |
| `REDEMPTION_CHECK_INTERVAL_SECONDS` | `300` | How often to check and redeem resolved positions (REAL mode only) |
| `MATIC_USD_PRICE` | `0.35` | MATIC/USD rate for converting gas fees to USDC in P&L |

### Discovery

| Variable | Default | Description |
|---|---|---|
| `MIN_WHALE_VOLUME_USDC` | `1000000` | Min leaderboard volume to appear in Discover Whales |

### VPN / Proxy

| Variable | Default | Description |
|---|---|---|
| `PROXY_URL` | — | HTTP proxy URL for routing app traffic through VPN (e.g. `http://localhost:8888`) |
| `OPENVPN_USER` | — | OpenVPN username (local gluetun container only) |
| `OPENVPN_PASSWORD` | — | OpenVPN password (local gluetun container only) |

## VPN

Polymarket requires a non-US/geo-restricted IP. The VPS is located in Helsinki (Hetzner) which satisfies this — no additional VPN configuration is needed in production.

For local development, you can route app traffic through a VPN proxy using gluetun:

```bash
# Start VPN container (uses docker-compose.vpn.yml + vpn/canada.ovpn)
docker compose -f docker-compose.vpn.yml up -d

# Confirm Canadian IP
curl -x http://localhost:8888 https://api.ipify.org?format=json

# Run the app (PROXY_URL must be set in .env)
python run.py

# Stop VPN when done
docker compose -f docker-compose.vpn.yml down
```

Add `PROXY_URL=http://localhost:8888` to `.env`. Comment it out to run without the proxy (e.g. if a system VPN is already active).

## Performance & Limitations

### Typical Response Times

| Event | Latency | Notes |
|---|---|---|
| Whale buy detected (CLOB WebSocket) | ~100 ms | `CLOB_WS_ENABLED=true`; network round-trip + order signing dominate after this |
| Whale buy detected (HTTP polling) | 2–4 s | Default; polling interval (2 s) + processing; runs as fallback alongside WS |
| Whale sell detected (on-chain) | ~4 s | `eth_getLogs` monitor at 2 s poll, ~2-block finality |
| Whale sell detected (API polling) | 2–10 s | Fallback when `CHAIN_EXIT_ENABLED=false` |
| Drift check | < 100 ms | Background pre-fetch keeps prices fresh |
| Order book depth check | ~50–100 ms | One extra concurrent HTTP call per entry evaluation |
| Order placed (buy or sell) | 1–3 s | CLOB API round-trip; token-bucket rate limiter adds ≤ 0.2 s at 5 req/s |
| Harvest / slippage check cycle | 60 s | Runs on resolution scheduler interval |
| Sell retry exhaustion | up to ~12 s | `SELL_CLOSE_RETRIES=4` × `SELL_CLOSE_RETRY_DELAY_SECONDS=3` |
| Market resolution check | 60 s | Configurable; triggers auto-redemption in REAL mode |
| Rate-limit back-off (sell) | +15 s | Applied automatically on nginx 400 HTML responses |

**End-to-end copy latency** from whale fill → your fill:
- **With CLOB WebSocket** (`CLOB_WS_ENABLED=true`): ~1–3 s (detection ~100 ms + CLOB order round-trip)
- **HTTP polling only** (default): 3–8 s under normal API conditions

On-chain exit monitoring reduces sell detection to ~4 s regardless of API polling.

### Strengths

- **Ultra-low entry latency** — CLOB WebSocket detection (~100ms) cuts copy lag from 2–4s to sub-second; HTTP polling continues as a fallback
- **Low-latency sell detection** — on-chain `OrderFilled` monitor fires in ~4 s, well ahead of most API-polling copiers
- **Drift guards protect entry quality** — asymmetric thresholds skip bets where the price has moved unfavourably while still allowing fills at a better price than the whale paid
- **Near-miss recovery** — drift retry watchlist gives skipped bets a second chance over a configurable window rather than dropping them permanently
- **Order book depth protection** — fluid depth check scaled to bet size prevents entries into thin markets where slippage would negate any edge
- **Per-token slippage tracking** — records historical fill vs mid-price slippage and can gate future entries on tokens with consistently poor fills
- **Conviction scaling** — bet size rewards high-conviction whale trades convexly, reducing exposure on low-confidence signals
- **Whale win-rate tracking** — rolling window of per-whale outcomes (last N closes) powers adaptive sizing; bad recent performers are down-sized automatically
- **Independent profit harvesting** — harvest and near-close harvest exits lock in profit without waiting for the whale to sell; soft exit confirmation delays following a whale sell if the position is currently winning
- **Wash trade detection** — skips bets that look like a whale cycling the same position for volume rather than edge
- **Client-side rate limiting** — token bucket prevents IP throttling before it occurs; graceful 400 HTML detection skips or backs off rather than hammering the API
- **Auto-redemption** — resolved positions are redeemed on-chain automatically; no manual claiming required
- **Simulation parity** — optional fee simulation makes sim-mode P&L directly comparable to real-mode returns

### Weaknesses & Known Limitations

- **Residual copy lag** — even with WebSocket detection, CLOB order signing and network round-trip add ~1–3 s; fast-resolving events may have moved past the drift cap by then
- **Market order slippage** — orders execute at the best available ask/bid, not the whale's fill price; the order book depth check mitigates but does not eliminate this on larger sizes
- **Single exchange** — only Polymarket CLOB; no cross-venue arbitrage or hedging
- **Whale quality partially volume-based** — discovery is still filtered by raw trading volume; win-rate adaptive sizing requires several closed positions before it meaningfully differentiates good from bad whales
- **Harvest timing is approximate** — harvest checks run on the resolution scheduler interval (default 60 s), so actual close may lag the threshold crossing by up to that interval
- **SQLite concurrency** — a single SQLite file is fine for one bot instance but will not scale to multiple concurrent writers
- **No position netting** — multiple tranches in the same market accumulate separately; the bot does not consolidate or net positions across whales
- **Win-rate cold start** — adaptive sizing (Stage 6) is meaningless until a whale has ≥ WHALE_PERF_WINDOW/2 recorded closes; newly tracked whales get 1.0× sizing regardless of past performance

## Disclaimer

This tool is for educational and personal use. Prediction market trading carries significant financial risk. Use simulation mode to evaluate performance before enabling real trading.
