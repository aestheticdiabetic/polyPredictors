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
_PRICE_CACHE_TTL = 30      # seconds — live prices refresh every 30 s
_PRICE_NEGATIVE_TTL = 300  # seconds — 404 (no order book) cached for 5 min


class PolymarketClient:
    """Async client for all Polymarket public and private APIs."""

    def __init__(self):
        self._http = httpx.AsyncClient(
            timeout=_TIMEOUT,
            follow_redirects=True,
            proxy=settings.PROXY_URL or None,
        )
        # {condition_id / "token:{token_id}": (data, expires_at)} — 1-hour TTL market metadata cache
        self._market_cache: dict = {}
        # {"price:{token_id}:{side}": (price_or_None, expires_at)} — short-lived price cache
        self._price_cache: dict = {}
        # Cached authenticated CLOB client — rebuilt only if credentials change
        self._clob_client = None
        # {token_id: taker_fee_bps} — cached taker fees fetched from CLOB /markets
        self._taker_fee_cache: dict = {}

    async def close(self):
        await self._http.aclose()

    async def reset_http_client(self):
        """Close and recreate the shared AsyncClient.

        poll_whales/poll_exits_only each create a new asyncio event loop per
        tick.  httpx connections are bound to the loop they were created in,
        so reusing self._http across loops causes 'TCPTransport closed' errors
        even on successful 200 responses (the body read fails on the stale
        transport).  Resetting at the start of each loop invocation fixes this.
        """
        try:
            await self._http.aclose()
        except Exception:
            pass
        self._http = httpx.AsyncClient(
            timeout=_TIMEOUT,
            follow_redirects=True,
            proxy=settings.PROXY_URL or None,
        )

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
        # Use synchronous requests (via asyncio.to_thread) rather than the shared
        # httpx.AsyncClient.  The VPN tunnel drops the body read mid-response even
        # after a clean 200 OK — an asyncio transport-level failure that cannot be
        # retried on the same AsyncClient.  Synchronous requests uses plain OS
        # sockets with no asyncio transport layer, which reconnect automatically.
        import asyncio as _asyncio
        import requests as _requests

        MAX_ATTEMPTS = 3
        for attempt in range(MAX_ATTEMPTS):
            try:
                def _fetch():
                    r = _requests.get(url, params=params, timeout=15)
                    r.raise_for_status()
                    return r.json()

                data = await _asyncio.to_thread(_fetch)
                if isinstance(data, list):
                    return data
                return data.get("data", data.get("activities", []))
            except Exception as exc:
                if attempt < MAX_ATTEMPTS - 1:
                    logger.warning(
                        "get_user_activity attempt %d/%d failed for %s: %s — retrying",
                        attempt + 1, MAX_ATTEMPTS, address[:10], exc,
                    )
                    await _asyncio.sleep(0.5 * (attempt + 1))
                    continue
                logger.error("get_user_activity error for %s after %d attempts: %s",
                             address[:10], MAX_ATTEMPTS, exc)
                return []
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
        except httpx.HTTPStatusError as exc:
            # Definitive HTTP error (404 = token not found) — cache negatively so we
            # don't hammer the API on every poll for a dead/unknown token.
            logger.debug("get_market token fallback HTTP error for %s: %s", token_id, exc)
            self._market_cache[cache_key] = (None, now + _NEGATIVE_TTL)
            return None
        except Exception as exc:
            # Transient network error — don't cache so the next poll can retry.
            logger.debug("get_market token fallback transient error for %s: %s", token_id, exc)
            return None

    async def get_last_trade_price(self, token_id: str) -> Optional[float]:
        """
        Retrieve the last trade price for a token from Gamma API market data.
        Uses _market_cache (keyed "token:{token_id}") to avoid re-fetching data
        that get_market already cached; falls back to a fresh request only when
        the cache has no entry yet.
        """
        now = time.monotonic()
        cache_key = f"token:{token_id}"

        # Check the shared market cache first (populated by get_market token fallback)
        cached = self._market_cache.get(cache_key)
        market = None
        if cached and cached[1] > now:
            market = cached[0]
        else:
            # Fetch from Gamma and populate the cache for future callers
            url = f"{settings.GAMMA_API_BASE}/markets"
            try:
                resp = await self._http.get(url, params={"clob_token_ids": token_id})
                resp.raise_for_status()
                data = resp.json()
                markets = data if isinstance(data, list) else data.get("markets", [])
                market = markets[0] if markets else None
                self._market_cache[cache_key] = (market, now + _MARKET_CACHE_TTL)
            except Exception as exc:
                logger.debug("get_last_trade_price error for %s: %s", token_id, exc)
                return None

        if not market:
            return None

        tokens = market.get("clobTokenIds", [])
        outcomes = market.get("outcomePrices", [])
        if token_id in tokens and outcomes:
            idx = tokens.index(token_id)
            if idx < len(outcomes):
                return float(outcomes[idx])
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
        self, token_id: str, side: str = "BUY", force_refresh: bool = False
    ) -> Optional[float]:
        """
        Get current best price for a token from the CLOB.
        side: 'BUY' or 'SELL'
        Caches successful prices for 30 s; caches 404 (no order book) for 5 min
        so closed/resolved markets don't get hammered on every trade processed.
        force_refresh: bypass cache and fetch a live price (used by drift retry loop).
        """
        cache_key = f"price:{token_id}:{side}"
        now = time.monotonic()
        if not force_refresh:
            cached = self._price_cache.get(cache_key)
            if cached and cached[1] > now:
                return cached[0]

        url = f"{settings.CLOB_HOST}/price"
        params = {"token_id": token_id, "side": side}
        try:
            resp = await self._http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            price_str = data.get("price")
            price = float(price_str) if price_str is not None else None
            self._price_cache[cache_key] = (price, now + _PRICE_CACHE_TTL)
            return price
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                # No active order book — market likely resolved or thin.
                # Cache the miss so we don't hammer the endpoint for every trade.
                self._price_cache[cache_key] = (None, now + _PRICE_NEGATIVE_TTL)
            else:
                logger.debug("get_market_price HTTP error for %s: %s", token_id, exc)
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

    async def get_best_price(self, token_id: str, force_refresh: bool = False) -> Optional[float]:
        """
        Get the best available price by checking both CLOB and Gamma.
        Returns None if no price can be determined.
        force_refresh: bypass the 30 s price cache (used by drift retry loop).
        """
        price = await self.get_market_price(token_id, "BUY", force_refresh=force_refresh)
        if price is not None:
            return price
        price = await self.get_last_trade_price(token_id)
        return price

    # ------------------------------------------------------------------
    # Authenticated CLOB  (py-clob-client)
    # ------------------------------------------------------------------

    def _get_clob_client(self):
        """
        Return a cached authenticated py-clob-client instance.
        The client is built once and reused for all orders within a session,
        avoiding repeated crypto-credential setup on every buy/sell call.
        Only valid when REAL credentials are configured.
        """
        if self._clob_client is not None:
            return self._clob_client
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=settings.POLY_API_KEY,
                api_secret=settings.POLY_API_SECRET,
                api_passphrase=settings.POLY_API_PASSPHRASE,
            )
            self._clob_client = ClobClient(
                host=settings.CLOB_HOST,
                chain_id=137,  # Polygon
                key=settings.POLY_PRIVATE_KEY,
                creds=creds,
                signature_type=2,   # Polymarket proxy wallet (funder != signer EOA)
                funder=settings.POLY_FUNDER_ADDRESS,
            )
            # Patch the order builder's get_market_order_amounts to round the taker
            # (shares) to round_config.size (2dp) rather than round_config.amount
            # (4–6dp depending on tick size). The Polymarket API rejects market buy
            # orders whose taker amount has more than 2 decimal places.
            import types
            from py_clob_client.order_builder.helpers import (
                round_down, round_normal, round_up, decimal_places, to_token_decimals,
            )

            def _fixed_get_market_order_amounts(self_builder, amount, price, round_config):
                raw_price = round_normal(price, round_config.price)
                # API enforces: taker (shares) max 2dp, maker (USDC) max 4dp.
                # Compute shares first, then derive USDC so maker = taker * price
                # is consistent with what the API validates.
                raw_taker_amt = round_down(amount / raw_price, round_config.size)
                raw_maker_amt = raw_taker_amt * raw_price
                if decimal_places(raw_maker_amt) > round_config.amount:
                    raw_maker_amt = round_up(raw_maker_amt, round_config.amount + 4)
                    if decimal_places(raw_maker_amt) > round_config.amount:
                        raw_maker_amt = round_down(raw_maker_amt, round_config.amount)
                # API minimum order is $1 — rounding (e.g. 1.01 shares × $0.99)
                # can push maker below $1; bump taker by one unit and recompute.
                if raw_maker_amt < 1.0:
                    unit = 10 ** (-round_config.size)
                    raw_taker_amt = round_normal(raw_taker_amt + unit, round_config.size)
                    raw_maker_amt = raw_taker_amt * raw_price
                    if decimal_places(raw_maker_amt) > round_config.amount:
                        raw_maker_amt = round_up(raw_maker_amt, round_config.amount + 4)
                        if decimal_places(raw_maker_amt) > round_config.amount:
                            raw_maker_amt = round_down(raw_maker_amt, round_config.amount)
                return to_token_decimals(raw_maker_amt), to_token_decimals(raw_taker_amt)

            self._clob_client.builder.get_market_order_amounts = types.MethodType(
                _fixed_get_market_order_amounts, self._clob_client.builder
            )
            return self._clob_client
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

        Fee handling: the CLOB /markets endpoint is unreliable for fee data.
        We attempt with the cached fee (default 0), and if the API rejects with
        an "invalid fee rate" error we parse the required fee from the message,
        cache it, and retry once.
        """
        import re
        from py_clob_client.clob_types import MarketOrderArgs

        client = self._get_clob_client()

        def _attempt(fee_bps: int) -> dict:
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=round(amount_usdc, 2),
                fee_rate_bps=fee_bps,
            )
            signed_order = client.create_market_order(order_args)
            resp = client.post_order(signed_order)
            return resp if isinstance(resp, dict) else {"status": "ok", "response": str(resp)}

        fee = self._taker_fee_cache.get(token_id, 0)
        try:
            return _attempt(fee)
        except Exception as exc:
            # Parse required fee from error message, e.g.:
            # "invalid fee rate (0), current market's taker fee: 1000"
            match = re.search(r"taker fee[:\s]+(\d+)", str(exc), re.IGNORECASE)
            if match:
                required_fee = int(match.group(1))
                if required_fee != fee:
                    logger.warning(
                        "place_market_buy: fee mismatch for %s — retrying with fee_rate_bps=%d",
                        token_id[:16], required_fee,
                    )
                    self._taker_fee_cache[token_id] = required_fee
                    try:
                        return _attempt(required_fee)
                    except Exception as retry_exc:
                        logger.error("place_market_buy error: %s", retry_exc)
                        raise
            logger.error("place_market_buy error: %s", exc)
            raise

    def place_market_sell(self, token_id: str, size_shares: float) -> dict:
        """
        Place a real market sell order via py-clob-client.
        size_shares: number of shares to sell (from DB record).

        create_market_order only supports BUY, so we use create_order with
        side="SELL" and the current best bid price to get immediate fill.

        No balance pre-check: get_balance_allowance(CONDITIONAL) is unreliable
        for proxy wallets and returns 0 even for valid positions. Instead we
        attempt the sell directly; the CLOB will reject with "not enough
        balance/allowance" if tokens are genuinely absent, which the caller
        catches and maps to no_position.
        """
        import re
        from py_clob_client.clob_types import OrderArgs

        client = self._get_clob_client()

        # Fetch price and tick size
        price_resp = client.get_price(token_id, "BUY")
        price = float(price_resp.get("price", 0)) if isinstance(price_resp, dict) else 0.0
        if not price:
            raise ValueError(f"Cannot determine sell price for token {token_id[:16]}")
        tick_size = float(client.get_tick_size(token_id))
        tick_dp = len(str(tick_size).rstrip("0").split(".")[-1]) if "." in str(tick_size) else 0
        price = round(max(tick_size, min(price, 1.0 - tick_size)), tick_dp)

        def _attempt(fee_bps: int) -> dict:
            # Floor to 2dp to match the CLOB's own round_down applied when filling
            # the original buy.  Using round() can produce 2.46 when we only hold
            # 2.45, causing a spurious "not enough balance" rejection.
            import math as _math
            floored = _math.floor(size_shares * 100) / 100
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=floored,
                side="SELL",
                fee_rate_bps=fee_bps,
            )
            signed_order = client.create_order(order_args)
            resp = client.post_order(signed_order)
            return resp if isinstance(resp, dict) else {"status": "ok", "response": str(resp)}

        fee = self._taker_fee_cache.get(token_id, 0)
        try:
            return _attempt(fee)
        except Exception as exc:
            exc_str = str(exc)
            # Fee mismatch — parse required fee and retry once
            match = re.search(r"taker fee[:\s]+(\d+)", exc_str, re.IGNORECASE)
            if match:
                required_fee = int(match.group(1))
                if required_fee != fee:
                    logger.warning(
                        "place_market_sell: fee mismatch for %s — retrying with fee_rate_bps=%d",
                        token_id[:16], required_fee,
                    )
                    self._taker_fee_cache[token_id] = required_fee
                    try:
                        return _attempt(required_fee)
                    except Exception as retry_exc:
                        logger.error("place_market_sell error: %s", retry_exc)
                        raise
            # No tokens held — CLOB rejects with "not enough balance/allowance"
            if "not enough balance" in exc_str.lower() or "allowance" in exc_str.lower():
                logger.warning(
                    "place_market_sell: no balance/allowance for token %s — marking as no-position",
                    token_id[:16],
                )
                return {"status": "no_position", "size_matched": 0}
            logger.error("place_market_sell error: %s", exc)
            raise

    async def get_wallet_balance(self) -> Optional[float]:
        """Fetch USDC balance for the configured funder address via authenticated CLOB API."""
        if not settings.credentials_valid():
            return None
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
            client = self._get_clob_client()
            resp = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2)
            )
            # Response: {"balance": "34970873", ...} — raw value in micro-USDC (6 decimals)
            raw = resp.get("balance") if isinstance(resp, dict) else None
            return round(float(raw) / 1_000_000, 2) if raw is not None else None
        except Exception as exc:
            logger.debug("get_wallet_balance error: %s", exc)
            return None
