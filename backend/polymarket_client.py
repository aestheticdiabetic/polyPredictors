"""
Polymarket API client - wraps Data API, Gamma API, and CLOB API.
All methods are async using httpx.
"""

import logging
import time
from typing import Any, Optional

import httpx

from backend.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(15.0, connect=5.0)
_MARKET_CACHE_TTL = 3600  # seconds


class PolymarketClient:
    """Async client for all Polymarket public and private APIs."""

    def __init__(self):
        self._http = httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True)
        # {condition_id: (data, expires_at)} — 1-hour TTL market metadata cache
        self._market_cache: dict = {}

    async def close(self):
        await self._http.aclose()

    # ------------------------------------------------------------------
    # Data API  (https://data-api.polymarket.com)
    # ------------------------------------------------------------------

    async def get_user_activity(
        self,
        address: str,
        limit: int = 100,
    ) -> list[dict]:
        """
        Fetch recent TRADE activity for a wallet address.
        Returns a list of trade dicts sorted by timestamp DESC.
        """
        url = f"{settings.DATA_API_BASE}/activity"
        params = {
            "user": address,
            "type": "TRADE",
            "limit": limit,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            # API may return list directly or wrapped in a key
            if isinstance(data, list):
                return data
            return data.get("data", data.get("activities", []))
        except httpx.HTTPStatusError as exc:
            logger.warning("get_user_activity HTTP error for %s: %s", address, exc)
            return []
        except Exception as exc:
            logger.error("get_user_activity error for %s: %s", address, exc)
            return []

    async def get_user_positions(self, address: str) -> list[dict]:
        """Fetch open positions for a wallet address."""
        url = f"{settings.DATA_API_BASE}/positions"
        params = {"user": address}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("data", data.get("positions", []))
        except Exception as exc:
            logger.error("get_user_positions error for %s: %s", address, exc)
            return []

    async def get_leaderboard(
        self,
        time_period: str = "ALL",
        order_by: str = "PNL",
        limit: int = 50,
    ) -> list[dict]:
        """
        Fetch leaderboard from Data API v1.
        Returns list of trader dicts with proxyWallet, pnl, vol, userName, etc.
        """
        url = f"{settings.DATA_API_BASE}/v1/leaderboard"
        params = {
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
        }
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("data", data.get("leaderboard", []))
        except Exception as exc:
            logger.error("get_leaderboard error: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Gamma API  (https://gamma-api.polymarket.com)
    # ------------------------------------------------------------------

    async def get_market(
        self, condition_id: str, token_id: str = None
    ) -> Optional[dict]:
        """
        Fetch market details from Gamma API.

        Strategy:
          1. Try GET /markets/{condition_id}  (fast, cached)
          2. If that returns 404 or 422 (common for certain condition_id formats),
             fall back to GET /markets?clob_token_ids={token_id} when token_id
             is supplied.  This endpoint is known-good for all active markets.

        Both successful and failed lookups are cached to avoid hammering the API:
          - Success  → cached for _MARKET_CACHE_TTL  (1 hour)
          - Failure  → cached for 1800 s             (30 minutes)
        """
        _NEGATIVE_TTL = 1800  # 30 min negative cache
        now = time.monotonic()

        # Check cache (stores None for known-bad condition_ids too)
        cached = self._market_cache.get(condition_id)
        if cached and cached[1] > now:
            result = cached[0]
            if result is not None:
                return result
            # Negative cache hit — skip straight to token fallback if available
        else:
            # Attempt condition_id lookup
            url = f"{settings.GAMMA_API_BASE}/markets/{condition_id}"
            try:
                resp = await self._http.get(url)
                resp.raise_for_status()
                data = resp.json()
                self._market_cache[condition_id] = (data, now + _MARKET_CACHE_TTL)
                return data
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status in (404, 422):
                    logger.debug(
                        "get_market %d for condition_id %s — will try token fallback",
                        status, condition_id,
                    )
                else:
                    logger.warning("get_market HTTP error for %s: %s", condition_id, exc)
                # Cache the failure so we don't retry for 30 minutes
                self._market_cache[condition_id] = (None, now + _NEGATIVE_TTL)
            except Exception as exc:
                logger.error("get_market error for %s: %s", condition_id, exc)
                return None

        # Fallback: look up by CLOB token_id
        if not token_id:
            return None

        cache_key = f"token:{token_id}"
        cached = self._market_cache.get(cache_key)
        if cached and cached[1] > now:
            return cached[0]

        url = f"{settings.GAMMA_API_BASE}/markets"
        try:
            resp = await self._http.get(url, params={"clob_token_ids": token_id})
            resp.raise_for_status()
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            market = markets[0] if markets else None
            self._market_cache[cache_key] = (market, now + _MARKET_CACHE_TTL)
            if market:
                logger.debug(
                    "get_market resolved via token fallback for condition_id %s",
                    condition_id,
                )
            return market
        except Exception as exc:
            logger.debug("get_market token fallback error for %s: %s", token_id, exc)
            self._market_cache[cache_key] = (None, now + _NEGATIVE_TTL)
            return None

    async def get_last_trade_price(self, token_id: str) -> Optional[float]:
        """
        Retrieve the last trade price for a token from Gamma API market data.
        Falls back to CLOB price if not available.
        """
        # Search for a market containing this token_id
        url = f"{settings.GAMMA_API_BASE}/markets"
        params = {"clob_token_ids": token_id}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            for market in markets:
                tokens = market.get("clobTokenIds", [])
                outcomes = market.get("outcomePrices", [])
                if token_id in tokens and outcomes:
                    idx = tokens.index(token_id)
                    if idx < len(outcomes):
                        return float(outcomes[idx])
            return None
        except Exception as exc:
            logger.debug("get_last_trade_price error for %s: %s", token_id, exc)
            return None

    async def resolve_proxy_wallet(self, address: str) -> Optional[dict]:
        """
        Resolve a wallet address to its public profile via Gamma API.
        Returns dict with name, pseudonym, proxyWallet, etc.
        """
        url = f"{settings.GAMMA_API_BASE}/public-profile"
        params = {"address": address}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("resolve_proxy_wallet error for %s: %s", address, exc)
            return None

    # ------------------------------------------------------------------
    # CLOB API  (https://clob.polymarket.com)
    # ------------------------------------------------------------------

    async def get_market_price(
        self, token_id: str, side: str = "BUY"
    ) -> Optional[float]:
        """
        Get current best price for a token from the CLOB.
        side: 'BUY' or 'SELL'
        """
        url = f"{settings.CLOB_HOST}/price"
        params = {"token_id": token_id, "side": side}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            price_str = data.get("price")
            if price_str is not None:
                return float(price_str)
            return None
        except Exception as exc:
            logger.debug("get_market_price error for %s: %s", token_id, exc)
            return None

    async def get_order_book(self, token_id: str) -> Optional[dict]:
        """Fetch the full order book for a token."""
        url = f"{settings.CLOB_HOST}/book"
        params = {"token_id": token_id}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("get_order_book error for %s: %s", token_id, exc)
            return None

    async def get_best_price(self, token_id: str) -> Optional[float]:
        """
        Get the best available price by checking both CLOB and Gamma.
        Returns None if no price can be determined.
        """
        price = await self.get_market_price(token_id, "BUY")
        if price is not None:
            return price
        price = await self.get_last_trade_price(token_id)
        return price

    # ------------------------------------------------------------------
    # Authenticated CLOB  (py-clob-client)
    # ------------------------------------------------------------------

    def _get_clob_client(self):
        """
        Create an authenticated py-clob-client instance.
        Only valid when REAL credentials are configured.
        """
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=settings.POLY_API_KEY,
                api_secret=settings.POLY_API_SECRET,
                api_passphrase=settings.POLY_API_PASSPHRASE,
            )
            return ClobClient(
                host=settings.CLOB_HOST,
                chain_id=137,  # Polygon
                private_key=settings.POLY_PRIVATE_KEY,
                creds=creds,
                funder=settings.POLY_FUNDER_ADDRESS,
            )
        except ImportError:
            logger.error("py-clob-client is not installed")
            raise
        except Exception as exc:
            logger.error("Failed to create ClobClient: %s", exc)
            raise

    def place_market_buy(self, token_id: str, amount_usdc: float) -> dict:
        """
        Place a real market buy order via py-clob-client.
        amount_usdc: USDC to spend.
        Returns order response dict.
        """
        client = self._get_clob_client()
        try:
            from py_clob_client.clob_types import MarketOrderArgs

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usdc,
            )
            signed_order = client.create_market_order(order_args)
            resp = client.post_order(signed_order)
            return resp if isinstance(resp, dict) else {"status": "ok", "response": str(resp)}
        except Exception as exc:
            logger.error("place_market_buy error: %s", exc)
            raise

    def place_market_sell(self, token_id: str, size_shares: float) -> dict:
        """
        Place a real market sell order via py-clob-client.
        size_shares: number of shares to sell.
        Returns order response dict.
        """
        client = self._get_clob_client()
        try:
            from py_clob_client.clob_types import MarketOrderArgs

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=size_shares,
            )
            signed_order = client.create_market_order(order_args, is_selling=True)
            resp = client.post_order(signed_order)
            return resp if isinstance(resp, dict) else {"status": "ok", "response": str(resp)}
        except Exception as exc:
            logger.error("place_market_sell error: %s", exc)
            raise

    async def get_wallet_balance(self) -> Optional[float]:
        """Fetch USDC balance for the configured funder address."""
        if not settings.POLY_FUNDER_ADDRESS:
            return None
        url = f"{settings.CLOB_HOST}/balance"
        params = {"address": settings.POLY_FUNDER_ADDRESS}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            balance = data.get("balance") or data.get("usdc_balance")
            return float(balance) if balance is not None else None
        except Exception as exc:
            logger.debug("get_wallet_balance error: %s", exc)
            return None
