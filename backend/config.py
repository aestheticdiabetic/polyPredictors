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

    # API endpoints
    DATA_API_BASE: str = "https://data-api.polymarket.com"
    GAMMA_API_BASE: str = "https://gamma-api.polymarket.com"
    CLOB_HOST: str = "https://clob.polymarket.com"

    # Database
    DATABASE_URL: str = f"sqlite:///{_root}/data/polymarket_copier.db"

    # Pre-loaded placeholder whale addresses (real ones come from leaderboard)
    DEFAULT_WHALES: list = [
        {"address": "0x8fB9E03Aef7b4dCA123F05BDbFB3e82aA39CF5", "alias": "TopWhale_1"},
        {"address": "0x1D5B3D9f3B6c7E2d4F8A9C0B7e6d5A3c2B1f0E", "alias": "TopWhale_2"},
    ]

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
