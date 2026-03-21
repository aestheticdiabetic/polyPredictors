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
    MAX_BET_PCT: float = float(os.getenv("MAX_BET_PCT", "0.05"))

    # Monitoring settings
    POLLING_INTERVAL_SECONDS: int = int(os.getenv("POLLING_INTERVAL_SECONDS", "30"))
    MIN_MARKET_HOURS_TO_CLOSE: float = float(os.getenv("MIN_MARKET_HOURS_TO_CLOSE", "1.0"))

    # How often (in seconds) the permanent resolution checker runs.
    # Lower = more likely to catch prices at 0.05 before they snap to 0.0.
    # Default 60s is a reasonable balance; set lower (e.g. 30) for tighter stops.
    RESOLUTION_CHECK_INTERVAL_SECONDS: int = int(os.getenv("RESOLUTION_CHECK_INTERVAL_SECONDS", "60"))

    # Ignore whale trades older than this many hours.  Prevents the initial backlog
    # of historical trades (fetched when a whale is first added) from being evaluated
    # against markets that have since closed.
    MAX_TRADE_AGE_HOURS: float = float(os.getenv("MAX_TRADE_AGE_HOURS", "24.0"))

    # Maximum allowable price drift (in probability points) between when the whale
    # placed their bet and when we copy it.  E.g. 0.05 means: if the whale bet at
    # 0.20 and the current price is now above 0.25 or below 0.15, skip the bet.
    MAX_PRICE_DRIFT_PCT: float = float(os.getenv("MAX_PRICE_DRIFT_PCT", "0.05"))

    # Minimum USDC to place on any single bet.  Set lower (e.g. 0.02) when following
    # a whale that places many small tracker bets on long-shot outcomes.
    MIN_BET_USDC: float = float(os.getenv("MIN_BET_USDC", "1.0"))

    # Drift-skip retry watchlist — for near-expiry markets that were skipped due to
    # price drift, re-check the price this often (seconds) and for this long (seconds).
    # Lower DRIFT_RETRY_INTERVAL = more responsive; higher DRIFT_RETRY_WINDOW = more
    # time for the price to come back within range before giving up.
    DRIFT_RETRY_INTERVAL_SECONDS: int = int(os.getenv("DRIFT_RETRY_INTERVAL_SECONDS", "5"))
    DRIFT_RETRY_WINDOW_SECONDS: int = int(os.getenv("DRIFT_RETRY_WINDOW_SECONDS", "120"))

    # Minimum USDC trading volume a leaderboard trader must have to appear in the
    # Discover Whales modal.  The Polymarket API does not expose a prediction count,
    # so volume is used as the practical proxy for high-activity traders.
    # A trader averaging $5 per prediction needs $100,000 vol for ~20,000 predictions.
    MIN_WHALE_VOLUME_USDC: float = float(os.getenv("MIN_WHALE_VOLUME_USDC", "1000000"))

    # Optional: HTTP proxy URL for routing traffic through VPN (e.g. gluetun)
    PROXY_URL: str = os.getenv("PROXY_URL", "")

    # API endpoints
    DATA_API_BASE: str = "https://data-api.polymarket.com"
    GAMMA_API_BASE: str = "https://gamma-api.polymarket.com"
    CLOB_HOST: str = "https://clob.polymarket.com"

    # Database
    DATABASE_URL: str = f"sqlite:///{_root}/data/polymarket_copier.db"

    # No default whales — add real ones via the UI Discover button or POST /api/whales
    DEFAULT_WHALES: list = []

    def credentials_valid(self) -> bool:
        """Check if all REAL mode credentials are present."""
        return all([
            self.POLY_PRIVATE_KEY,
            self.POLY_API_KEY,
            self.POLY_API_SECRET,
            self.POLY_API_PASSPHRASE,
            self.POLY_FUNDER_ADDRESS,
        ])


settings = Settings()

# Propagate proxy to environment so requests-based libs (py-clob-client) also use it
if settings.PROXY_URL:
    for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.setdefault(_k, settings.PROXY_URL)
    os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")
    os.environ.setdefault("no_proxy", "localhost,127.0.0.1")
