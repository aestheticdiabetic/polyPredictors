"""
Application configuration - loads from .env file.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root
_root = Path(__file__).parent.parent
load_dotenv(_root / ".env")


class Settings:
    # Polymarket credentials
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_API_KEY: str = os.getenv("POLY_API_KEY", "")
    POLY_API_SECRET: str = os.getenv("POLY_API_SECRET", "")
    POLY_API_PASSPHRASE: str = os.getenv("POLY_API_PASSPHRASE", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")

    # Simulation settings
    SIM_STARTING_BALANCE: float = float(os.getenv("SIM_STARTING_BALANCE", "200.0"))

    # If True, simulation deducts taker fees from buy cost to mirror real mode P&L.
    # Default False preserves existing fee-free behaviour. Enable to see realistic
    # projections of what real mode would have returned.
    SIM_APPLY_FEES: bool = os.getenv("SIM_APPLY_FEES", "false").lower() == "true"

    # Assumed taker fee (bps) for simulation when SIM_APPLY_FEES=True.
    # Most Polymarket markets have 0% fee; set to 1000 to stress-test profitability.
    SIM_ASSUMED_FEE_BPS: int = int(os.getenv("SIM_ASSUMED_FEE_BPS", "0"))
    MAX_BET_PCT: float = float(os.getenv("MAX_BET_PCT", "0.05"))

    # Global scalar applied to every computed bet size (both standard and arb modes).
    # Set < 1.0 to reduce all bet sizes proportionally without touching the underlying
    # risk/conviction logic. Default 1.0 = no change. Increase back to 1.0 when
    # capital grows sufficiently.
    BET_SIZE_SCALAR: float = float(os.getenv("BET_SIZE_SCALAR", "1.0"))

    # Monitoring settings
    POLLING_INTERVAL_SECONDS: int = int(os.getenv("POLLING_INTERVAL_SECONDS", "5"))
    MIN_MARKET_HOURS_TO_CLOSE: float = float(os.getenv("MIN_MARKET_HOURS_TO_CLOSE", "0.0"))

    # How often (in seconds) the permanent resolution checker runs.
    # Lower = more likely to catch prices at 0.05 before they snap to 0.0.
    # Default 60s is a reasonable balance; set lower (e.g. 30) for tighter stops.
    RESOLUTION_CHECK_INTERVAL_SECONDS: int = int(
        os.getenv("RESOLUTION_CHECK_INTERVAL_SECONDS", "60")
    )

    # Ignore whale trades older than this many hours.  Prevents the initial backlog
    # of historical trades (fetched when a whale is first added) from being evaluated
    # against markets that have since closed.
    MAX_TRADE_AGE_HOURS: float = float(os.getenv("MAX_TRADE_AGE_HOURS", "24.0"))

    # Maximum allowable price drift (in probability points) between when the whale
    # placed their bet and when we copy it.  E.g. 0.05 means: if the whale bet at
    # 0.20 and the current price is now above 0.25 or below 0.15, skip the bet.
    # Still used for the exit-drift guard in polymarket_client (sell-side check).
    MAX_PRICE_DRIFT_PCT: float = float(os.getenv("MAX_PRICE_DRIFT_PCT", "0.05"))

    # Asymmetric entry-drift thresholds (applied to all session modes).
    # Upward drift (price > whale entry) means we'd overpay relative to the whale;
    # we cap this tightly at 2 probability points.
    # Downward drift (price < whale entry) is actually favourable — we get in cheaper
    # — so we allow up to 10 probability points before treating it as a stale signal.
    MAX_UPWARD_DRIFT_PCT: float = float(os.getenv("MAX_UPWARD_DRIFT_PCT", "0.02"))
    MAX_DOWNWARD_DRIFT_PCT: float = float(os.getenv("MAX_DOWNWARD_DRIFT_PCT", "0.10"))

    # REAL mode only: maximum entry price allowed. Bets above this price have too
    # little remaining upside to cover the taker fee. Default 0.80.
    REAL_MAX_ENTRY_PRICE: float = float(os.getenv("REAL_MAX_ENTRY_PRICE", "0.80"))

    # REAL mode only: minimum net upside required after fees before entering a bet.
    # Net upside = (1 - entry_price) - (fee_bps / 10000). Default 0.05 = 5%.
    REAL_MIN_NET_UPSIDE: float = float(os.getenv("REAL_MIN_NET_UPSIDE", "0.05"))

    # Minimum USDC to place on any single bet.  Set lower (e.g. 0.02) when following
    # a whale that places many small tracker bets on long-shot outcomes.
    MIN_BET_USDC: float = float(os.getenv("MIN_BET_USDC", "1.0"))

    # Maximum open positions (tranches) per market enforced by the drift watchlist
    # retry loop.  Prevents the race condition where multiple watchlist items for the
    # same token all fire before the first fill is committed to the database.
    # Does NOT restrict new tranches opened via the normal process_new_whale_bet path —
    # those are governed by the add-to-position guard (same whale, same price ±0.03).
    MAX_OPEN_POSITIONS_PER_MARKET: int = int(os.getenv("MAX_OPEN_POSITIONS_PER_MARKET", "2"))

    # Arb mode: price at which the extremity multiplier equals 1.0 (neutral).
    # extremity = abs(price - 0.5); rf = extremity / ARB_MIDPOINT, clamped [0.5, 3.0].
    # At 0.25 (default): price 0.25 or 0.75 → rf=1.0 (base bet); price 0.10/0.90 → rf=1.6.
    ARB_MIDPOINT: float = float(os.getenv("ARB_MIDPOINT", "0.25"))

    # Arb mode: expected number of concurrent open positions for the whale.
    # The wallet is divided across this many positions and scaled by the whale's
    # relative bet size.  bet = (balance / N) * (whale_bet / whale_avg).
    # Set to match how many simultaneous positions your arb whale typically holds.
    ARB_CONCURRENT_POSITIONS: int = int(os.getenv("ARB_CONCURRENT_POSITIONS", "6"))

    # Exponent applied to conviction ratios >= 1.0 when computing the risk factor.
    # 1.0 = linear (original behaviour). 1.5 = convex curve that rewards high
    # conviction bets disproportionately (e.g. 2x avg → 2.83x factor vs 2.0x before).
    CONVICTION_EXPONENT: float = float(os.getenv("CONVICTION_EXPONENT", "1.5"))

    # Background price pre-fetching — keeps prices fresh for all tokens in open positions
    # so that when a new whale trade fires the cached price is at most this many seconds old.
    # Set to 0 to disable. Lower = fresher prices, more CLOB API calls.
    PRICE_PREFETCH_INTERVAL_SECONDS: int = int(os.getenv("PRICE_PREFETCH_INTERVAL_SECONDS", "3"))

    # Drift-skip retry watchlist — for near-expiry markets that were skipped due to
    # price drift, re-check the price this often (seconds) and for this long (seconds).
    # Lower DRIFT_RETRY_INTERVAL = more responsive; higher DRIFT_RETRY_WINDOW = more
    # time for the price to come back within range before giving up.
    DRIFT_RETRY_INTERVAL_SECONDS: int = int(os.getenv("DRIFT_RETRY_INTERVAL_SECONDS", "5"))
    DRIFT_RETRY_WINDOW_SECONDS: int = int(os.getenv("DRIFT_RETRY_WINDOW_SECONDS", "120"))

    # How often (seconds) to check for redeemable positions and redeem them on-chain.
    # Only active when REAL credentials are configured. Default 5 minutes.
    REDEMPTION_CHECK_INTERVAL_SECONDS: int = int(
        os.getenv("REDEMPTION_CHECK_INTERVAL_SECONDS", "300")
    )

    # Polygon RPC URL used for on-chain redemption transactions.
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

    # On-chain exit monitoring via Polygon eth_getLogs (CTF Exchange OrderFilled events).
    # When true, replaces activity-API exit polling with block-native detection.
    # Requires a reliable POLYGON_RPC_URL — default public RPC may have rate limits.
    CHAIN_EXIT_ENABLED: bool = os.getenv("CHAIN_EXIT_ENABLED", "false").lower() == "true"

    # Poll interval for on-chain exit monitoring. Polygon blocks confirm every ~2s,
    # so 2s gives ~4s worst-case detection latency (poll wait + block time).
    CHAIN_EXIT_POLL_INTERVAL_SECONDS: int = int(os.getenv("CHAIN_EXIT_POLL_INTERVAL_SECONDS", "2"))

    # Blocks to look back on first start (Polygon ~2s/block → 150 blocks ≈ 5 min).
    CHAIN_EXIT_LOOKBACK_BLOCKS: int = int(os.getenv("CHAIN_EXIT_LOOKBACK_BLOCKS", "150"))

    # Minimum USDC trading volume a leaderboard trader must have to appear in the
    # Discover Whales modal.  The Polymarket API does not expose a prediction count,
    # so volume is used as the practical proxy for high-activity traders.
    # A trader averaging $5 per prediction needs $100,000 vol for ~20,000 predictions.
    MIN_WHALE_VOLUME_USDC: float = float(os.getenv("MIN_WHALE_VOLUME_USDC", "1000000"))

    # Maximum number of FOK retry attempts before declaring a sell exhausted.
    # Default 3. Increase if your markets are thin and orders frequently cancel.
    SELL_MAX_FOK_RETRIES: int = int(os.getenv("SELL_MAX_FOK_RETRIES", "3"))

    # After FOK retry exhaustion, accept a fill at the current best bid regardless
    # of drift from the whale's exit price. Prevents positions staying open forever
    # when the market is moving fast. Default False = leave OPEN for next poll.
    SELL_ACCEPT_DEGRADED_FILL: bool = (
        os.getenv("SELL_ACCEPT_DEGRADED_FILL", "false").lower() == "true"
    )

    # MATIC/USD conversion rate used to convert Polygon gas fees to USDC for P&L tracking.
    # Update when MATIC price drifts significantly. Gas costs are small (< $0.05/tx)
    # so a stale rate has minimal impact on overall P&L accuracy.
    MATIC_USD_PRICE: float = float(os.getenv("MATIC_USD_PRICE", "0.35"))

    # Optional: HTTP proxy URL for routing traffic through VPN (e.g. gluetun)
    PROXY_URL: str = os.getenv("PROXY_URL", "")

    # API endpoints
    DATA_API_BASE: str = "https://data-api.polymarket.com"
    GAMMA_API_BASE: str = "https://gamma-api.polymarket.com"
    CLOB_HOST: str = "https://clob.polymarket.com"

    # Database
    DATABASE_URL: str = f"sqlite:///{_root}/data/polymarket_copier.db"

    # No default whales — add real ones via the UI Discover button or POST /api/whales
    DEFAULT_WHALES: list = []  # noqa: RUF012

    def credentials_valid(self) -> bool:
        """Check if all REAL mode credentials are present."""
        return all(
            [
                self.POLY_PRIVATE_KEY,
                self.POLY_API_KEY,
                self.POLY_API_SECRET,
                self.POLY_API_PASSPHRASE,
                self.POLY_FUNDER_ADDRESS,
            ]
        )


settings = Settings()

# Propagate proxy to environment so requests-based libs (py-clob-client) also use it
if settings.PROXY_URL:
    for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.setdefault(_k, settings.PROXY_URL)
    os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")
    os.environ.setdefault("no_proxy", "localhost,127.0.0.1")
