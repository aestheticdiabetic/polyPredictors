"""
Polymarket API client - wraps Data API, Gamma API, and CLOB API.
All methods are async using httpx.
"""

import logging
import time

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
        max_pages: int = 10,
    ) -> list[dict]:
        """
        Fetch recent TRADE activity for a wallet address.
        Returns a list of trade dicts sorted by timestamp DESC.
        Paginates automatically until fewer than `limit` results are returned
        or `max_pages` pages have been fetched (default 10 → up to 1000 trades).
        """
        url = f"{settings.DATA_API_BASE}/activity"
        # Use synchronous requests (via asyncio.to_thread) rather than the shared
        # httpx.AsyncClient.  The VPN tunnel drops the body read mid-response even
        # after a clean 200 OK — an asyncio transport-level failure that cannot be
        # retried on the same AsyncClient.  Synchronous requests uses plain OS
        # sockets with no asyncio transport layer, which reconnect automatically.
        import asyncio as _asyncio

        import requests as _requests

        MAX_ATTEMPTS = 3
        all_trades: list[dict] = []

        for page in range(max_pages):
            params = {
                "user": address,
                "type": "TRADE",
                "limit": limit,
                "offset": page * limit,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            }
            page_trades: list[dict] = []
            for attempt in range(MAX_ATTEMPTS):
                try:
                    def _fetch(p=params):
                        r = _requests.get(url, params=p, timeout=15)
                        r.raise_for_status()
                        return r.json()

                    data = await _asyncio.to_thread(_fetch)
                    if isinstance(data, list):
                        page_trades = data
                    else:
                        page_trades = data.get("data", data.get("activities", []))
                    break
                except Exception as exc:
                    if attempt < MAX_ATTEMPTS - 1:
                        logger.warning(
                            "get_user_activity page %d attempt %d/%d failed for %s: %s — retrying",
                            page + 1, attempt + 1, MAX_ATTEMPTS, address[:10], exc,
                        )
                        await _asyncio.sleep(0.5 * (attempt + 1))
                    else:
                        logger.error(
                            "get_user_activity page %d error for %s after %d attempts: %s",
                            page + 1, address[:10], MAX_ATTEMPTS, exc,
                        )

            all_trades.extend(page_trades)

            # Stop paginating if this page was a partial page (end of data)
            if len(page_trades) < limit:
                break

        return all_trades

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
    ) -> dict | None:
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

    async def get_last_trade_price(self, token_id: str) -> float | None:
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

    async def resolve_proxy_wallet(self, address: str) -> dict | None:
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
    ) -> float | None:
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

    async def get_order_book(self, token_id: str) -> dict | None:
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

    async def get_best_price(
        self, token_id: str, force_refresh: bool = False, side: str = "BUY"
    ) -> float | None:
        """
        Get the best available price by checking both CLOB and Gamma.
        Returns None if no price can be determined.
        side: 'BUY' for entry (ASK price), 'SELL' for exit (BID price — what we receive).
        force_refresh: bypass the 30 s price cache (used by drift retry loop).
        """
        price = await self.get_market_price(token_id, side, force_refresh=force_refresh)
        # CLOB returned 0.0 — could be a stale cache entry or a transient empty book.
        # Retry once with a forced fresh fetch before accepting the zero.
        if price == 0.0 and not force_refresh:
            price = await self.get_market_price(token_id, side, force_refresh=True)
        if price is not None:
            return price
        price = await self.get_last_trade_price(token_id)
        return price

    async def get_taker_fee_async(self, token_id: str) -> int:
        """
        Return the taker fee in basis points for a token, using the cache where
        possible (populated by buy retries) and falling back to the Gamma market
        data (which includes a feeRateBps field).  Returns 1000 on any error so
        the fee-viability gate stays conservative.
        """
        if token_id in self._taker_fee_cache:
            return self._taker_fee_cache[token_id]

        # Re-use already-cached market data to avoid an extra round-trip.
        # get_market stores Gamma results under key "token:{token_id}".
        cache_key = f"token:{token_id}"
        cached = self._market_cache.get(cache_key)
        market = cached[0] if cached else None

        if market is None:
            # Market data not yet cached — fetch it now.
            try:
                market = await self.get_market(token_id=token_id)
            except Exception as exc:
                logger.debug("get_taker_fee_async market fetch failed for %s: %s", token_id, exc)

        if market and isinstance(market, dict):
            raw = market.get("feeRateBps")
            if raw is not None:
                try:
                    fee_bps = int(raw)
                    self._taker_fee_cache[token_id] = fee_bps
                    logger.debug("get_taker_fee_async: token %s → %d bps (from Gamma)", token_id[:16], fee_bps)
                    return fee_bps
                except (ValueError, TypeError):
                    pass

        # Fall back to conservative default so the viability gate doesn't under-estimate fees.
        logger.debug("get_taker_fee_async: no feeRateBps for %s — using 1000 bps default", token_id[:16])
        return 1000

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
                decimal_places,
                round_down,
                round_normal,
                round_up,
                to_token_decimals,
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

        # Default to 1000 bps (10%) — the standard Polymarket taker fee.
        # Starting at 0 caused every first buy to fail and retry (4-6s delay),
        # allowing significant price movement between intent and fill.
        fee = self._taker_fee_cache.get(token_id, 1000)
        try:
            return _attempt(fee)
        except Exception as exc:
            # Parse required fee from error message, e.g.:
            # "invalid fee rate (1000), current market's taker fee: 0"
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

    def place_market_sell(self, token_id: str, size_shares: float, whale_price: float = None) -> dict:
        """
        Place a real market sell order via py-clob-client.
        size_shares: number of shares to sell (from DB record).
        whale_price: the price at which the whale exited (used as the drift anchor).

        create_market_order only supports BUY, so we use create_order with
        side="SELL" + OrderType.FOK so the order fills immediately or is
        cancelled (never left as a live GTC order in the book).

        If a FOK is cancelled (price ticked between fetch and submit), we
        re-fetch the best bid and retry up to MAX_FOK_RETRIES times as long
        as the new price is within MAX_PRICE_DRIFT_PCT of the whale's exit price.
        Once drift exceeds that threshold we return the cancelled response so
        the caller can leave the position OPEN for the next exit poll.
        """
        import math as _math
        import re

        from py_clob_client.client import OrderType
        from py_clob_client.clob_types import OrderArgs

        client = self._get_clob_client()

        # tick size is stable; fetch once up-front
        tick_size = float(client.get_tick_size(token_id))
        tick_dp = len(str(tick_size).rstrip("0").split(".")[-1]) if "." in str(tick_size) else 0
        floored_shares = _math.floor(size_shares * 100) / 100

        def _fetch_bid() -> float:
            price_resp = client.get_price(token_id, "BUY")
            raw = float(price_resp.get("price", 0)) if isinstance(price_resp, dict) else 0.0
            if not raw:
                raise ValueError(f"Cannot determine sell price for token {token_id[:16]}")
            return round(max(tick_size, min(raw, 1.0 - tick_size)), tick_dp)

        def _submit(price: float, fee_bps: int) -> dict:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=floored_shares,
                side="SELL",
                fee_rate_bps=fee_bps,
            )
            signed_order = client.create_order(order_args)
            # FOK: fill immediately in full or cancel — never leaves a live order.
            resp = client.post_order(signed_order, OrderType.FOK)
            return resp if isinstance(resp, dict) else {"status": "ok", "response": str(resp)}

        def _submit_with_fee_retry(price: float) -> dict:
            """Submit at price, retrying once on fee-mismatch error."""
            current_fee = self._taker_fee_cache.get(token_id, 0)
            try:
                return _submit(price, current_fee)
            except Exception as exc:
                exc_str = str(exc)
                match = re.search(r"taker fee[:\s]+(\d+)", exc_str, re.IGNORECASE)
                if match:
                    required_fee = int(match.group(1))
                    if required_fee != current_fee:
                        logger.warning(
                            "place_market_sell: fee mismatch for %s — retrying with fee_rate_bps=%d",
                            token_id[:16], required_fee,
                        )
                        self._taker_fee_cache[token_id] = required_fee
                        try:
                            return _submit(price, required_fee)
                        except Exception as retry_exc:
                            logger.error("place_market_sell error: %s", retry_exc)
                            raise
                if "not enough balance" in exc_str.lower() or "allowance" in exc_str.lower():
                    logger.warning(
                        "place_market_sell: no balance/allowance for token %s — marking as no-position",
                        token_id[:16],
                    )
                    return {"status": "no_position", "size_matched": 0}
                logger.error("place_market_sell error: %s", exc)
                raise

        max_fok_retries = settings.SELL_MAX_FOK_RETRIES
        initial_price = _fetch_bid()
        # Drift anchor: whale's exit price when provided, else first bid we fetched.
        # Using the whale's price means we retry as long as the market is still
        # near where the whale sold — if the market has moved far from that level
        # something unusual is happening and we back off.
        drift_anchor = float(whale_price) if whale_price is not None else initial_price
        resp = {"status": "cancelled"}

        for attempt in range(max_fok_retries):
            if attempt == 0:
                current_price = initial_price
            else:
                current_price = _fetch_bid()
                drift = abs(current_price - drift_anchor)
                if drift > settings.MAX_PRICE_DRIFT_PCT:
                    logger.warning(
                        "place_market_sell: price drifted from whale exit %.4f to %.4f "
                        "(drift=%.4f exceeds limit=%.4f) — aborting retries",
                        drift_anchor, current_price, drift, settings.MAX_PRICE_DRIFT_PCT,
                    )
                    # If degraded-fill mode is on, skip the drift guard and fall through
                    # to the degraded fill below rather than returning immediately.
                    if not settings.SELL_ACCEPT_DEGRADED_FILL:
                        return {"status": "cancelled", "price": current_price}
                    break

            resp = _submit_with_fee_retry(current_price)
            sell_status = (resp.get("status") or "").lower()
            if sell_status not in ("cancelled", "canceled"):
                return resp  # filled (or no_position / error)

            logger.warning(
                "place_market_sell: FOK cancelled at %.4f (attempt %d/%d)%s",
                current_price, attempt + 1, max_fok_retries,
                " — retrying at new market price" if attempt < max_fok_retries - 1 else " — all retries exhausted",
            )

        # All standard retries exhausted. If SELL_ACCEPT_DEGRADED_FILL is enabled,
        # make one final attempt at the current best bid with no drift check.
        # This prevents positions getting permanently stuck open.
        if settings.SELL_ACCEPT_DEGRADED_FILL:
            try:
                degraded_price = _fetch_bid()
                logger.warning(
                    "place_market_sell: all retries exhausted — degraded fill attempt at %.4f "
                    "(drift from anchor %.4f = %.4f, drift guard bypassed)",
                    degraded_price, drift_anchor, abs(degraded_price - drift_anchor),
                )
                resp = _submit_with_fee_retry(degraded_price)
                sell_status = (resp.get("status") or "").lower()
                if sell_status not in ("cancelled", "canceled"):
                    return resp
                logger.warning("place_market_sell: degraded fill also cancelled at %.4f", degraded_price)
            except Exception as exc:
                logger.error("place_market_sell: degraded fill error: %s", exc)

        return resp

    async def get_wallet_balance(self) -> float | None:
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
