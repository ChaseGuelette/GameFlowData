"""
Kalshi API Client
=================
Low-level HTTP client for the Kalshi prediction markets API.

Auth: RSA-PSS SHA256 signature with API key header.
Rate limits: 20 reads/sec (basic tier), enforced via built-in delay.

Private key loaded from:
  - KALSHI_PRIVATE_KEY_PATH (local file path)
  - KALSHI_PRIVATE_KEY_B64 (base64-encoded, for Railway)

Graceful no-op when credentials are missing (dry-run / mock mode).

Usage:
    client = KalshiClient()
    if client.is_authenticated:
        markets = client.list_markets(series_ticker="KXNBA")
"""

import base64
import logging
import os
import time
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def _load_private_key():
    """Load RSA private key from env var (path or base64).

    Returns:
        RSA private key object, or None if unavailable.
    """
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
    except ImportError:
        logger.warning("cryptography package not installed — Kalshi auth unavailable")
        return None

    # Try file path first
    key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
    if key_path:
        try:
            with open(key_path, "rb") as f:
                return load_pem_private_key(f.read(), password=None)
        except Exception as e:
            logger.error(f"Failed to load private key from {key_path}: {e}")
            return None

    # Try base64-encoded key (Railway)
    key_b64 = os.getenv("KALSHI_PRIVATE_KEY_B64")
    if key_b64:
        try:
            key_bytes = base64.b64decode(key_b64)
            return load_pem_private_key(key_bytes, password=None)
        except Exception as e:
            logger.error(f"Failed to decode base64 private key: {e}")
            return None

    return None


def _sign_request(private_key, timestamp_ms: str, method: str, path: str) -> str:
    """Create RSA-PSS SHA256 signature for Kalshi API auth.

    Args:
        private_key: RSA private key object.
        timestamp_ms: Unix timestamp in milliseconds as string.
        method: HTTP method (GET, POST, etc.).
        path: API path (e.g., /trade-api/v2/markets).

    Returns:
        Base64-encoded signature string.
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    message = f"{timestamp_ms}{method}{path}".encode()
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,  # Kalshi requires DIGEST_LENGTH, not MAX_LENGTH
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


class KalshiClient:
    """HTTP client for the Kalshi prediction markets API."""

    def __init__(self, delay: float = 0.10, max_retries: int = 3):
        self.delay = delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self._last_request_time: float | None = None

        # Load credentials
        self.api_key = os.getenv("KALSHI_API_KEY")
        self._private_key = _load_private_key()
        self.is_authenticated = bool(self.api_key and self._private_key)

        if self.is_authenticated:
            logger.info("Kalshi client initialized with API credentials")
        else:
            logger.warning(
                "Kalshi client initialized WITHOUT credentials — "
                "only mock/dry-run mode available"
            )

    def _rate_limit(self) -> None:
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def _auth_headers(self, method: str, path: str) -> dict:
        """Generate auth headers for a request."""
        ts_ms = str(int(datetime.now(UTC).timestamp() * 1000))
        sig = _sign_request(self._private_key, ts_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, params: dict | None = None) -> dict | None:
        """Make an authenticated API request with retries.

        Args:
            method: HTTP method.
            path: API path (appended to base URL).
            params: Query parameters.

        Returns:
            JSON response dict, or None on failure.
        """
        if not self.is_authenticated:
            logger.warning("Cannot make API request — no credentials configured")
            return None

        url = f"{KALSHI_BASE_URL}{path}"
        sign_path = f"/trade-api/v2{path}"

        for attempt in range(self.max_retries):
            self._rate_limit()
            try:
                headers = self._auth_headers(method.upper(), sign_path)
                response = self.session.request(
                    method, url, headers=headers, params=params, timeout=15,
                )

                if response.status_code == 429:
                    wait = (2 ** attempt) * 5
                    logger.warning(f"Rate limited (429). Waiting {wait}s before retry.")
                    time.sleep(wait)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed ({response.status_code}). "
                        "Check KALSHI_API_KEY and private key."
                    )
                    return None

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on {path} (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.HTTPError as e:
                if response.status_code >= 500:
                    wait = (2 ** attempt) * 2
                    logger.warning(f"Server error {response.status_code}. Waiting {wait}s.")
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP error on {path}: {e}")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on {path}: {e}")

            if attempt < self.max_retries - 1:
                wait = (2 ** attempt) * self.delay
                time.sleep(wait)

        logger.error(f"Failed after {self.max_retries} attempts: {path}")
        return None

    def _request_with_body(
        self, method: str, path: str, body: dict | None = None
    ) -> dict | None:
        """Make an authenticated API request with a JSON body (POST/PUT/DELETE).

        Args:
            method: HTTP method (POST, PUT, DELETE).
            path: API path (appended to base URL).
            body: JSON body dict.

        Returns:
            JSON response dict, or None on failure.
        """
        if not self.is_authenticated:
            logger.warning("Cannot make API request — no credentials configured")
            return None

        url = f"{KALSHI_BASE_URL}{path}"
        sign_path = f"/trade-api/v2{path}"

        for attempt in range(self.max_retries):
            self._rate_limit()
            try:
                headers = self._auth_headers(method.upper(), sign_path)
                response = self.session.request(
                    method, url, headers=headers, json=body, timeout=15,
                )

                if response.status_code == 429:
                    wait = (2 ** attempt) * 5
                    logger.warning(f"Rate limited (429). Waiting {wait}s before retry.")
                    time.sleep(wait)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed ({response.status_code}). "
                        "Check KALSHI_API_KEY and private key."
                    )
                    return None

                response.raise_for_status()
                # DELETE may return 204 No Content
                if response.status_code == 204:
                    return {}
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on {path} (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.HTTPError as e:
                if response.status_code >= 500:
                    wait = (2 ** attempt) * 2
                    logger.warning(f"Server error {response.status_code}. Waiting {wait}s.")
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP error on {path}: {e} — {response.text}")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on {path}: {e}")

            if attempt < self.max_retries - 1:
                wait = (2 ** attempt) * self.delay
                time.sleep(wait)

        logger.error(f"Failed after {self.max_retries} attempts: {path}")
        return None

    # ------------------------------------------------------------------
    # Portfolio / Trading endpoints
    # ------------------------------------------------------------------

    def get_balance(self) -> dict | None:
        """Get portfolio balance.

        Returns:
            Dict with 'balance' and 'portfolio_value' in cents,
            or None on failure.
        """
        return self._request("GET", "/portfolio/balance")

    def get_positions(
        self,
        ticker: str | None = None,
        settlement_status: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> list[dict]:
        """Get portfolio positions.

        Args:
            ticker: Filter by market ticker.
            settlement_status: Filter by status ("open" or "settled").
            limit: Max results per page.
            cursor: Pagination cursor.

        Returns:
            List of position dicts.
        """
        params: dict = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if settlement_status:
            params["settlement_status"] = settlement_status
        if cursor:
            params["cursor"] = cursor

        result = self._request("GET", "/portfolio/positions", params=params)
        if result is None:
            return []
        return result.get("market_positions", [])

    def create_order(
        self,
        ticker: str,
        action: str = "buy",
        side: str = "yes",
        order_type: str = "market",
        count: int = 1,
        yes_price: int | None = None,
    ) -> dict | None:
        """Place an order on a market.

        Args:
            ticker: Market ticker.
            action: "buy" or "sell".
            side: "yes" or "no".
            order_type: "market" or "limit".
            count: Number of contracts.
            yes_price: Limit price in cents (required for limit orders).

        Returns:
            Order response dict with order_id, status, fill details,
            or None on failure.
        """
        if count <= 0:
            logger.error(f"Invalid contract count: {count}")
            return None

        body: dict = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "type": order_type,
            "count": count,
        }
        if yes_price is not None:
            body["yes_price"] = yes_price

        result = self._request_with_body("POST", "/portfolio/orders", body=body)
        if result:
            order = result.get("order", result)
            logger.info(
                f"Order placed: {ticker} {action} {count}x {side} "
                f"(type={order_type}) → status={order.get('status')}"
            )
        return result

    def cancel_order(self, order_id: str) -> dict | None:
        """Cancel an open order.

        Args:
            order_id: The order ID to cancel.

        Returns:
            Response dict or None on failure.
        """
        return self._request_with_body("DELETE", f"/portfolio/orders/{order_id}")

    def get_fills(
        self,
        ticker: str | None = None,
        order_id: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> list[dict]:
        """Get fill history.

        Args:
            ticker: Filter by market ticker.
            order_id: Filter by order ID.
            limit: Max results.
            cursor: Pagination cursor.

        Returns:
            List of fill dicts.
        """
        params: dict = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if order_id:
            params["order_id"] = order_id
        if cursor:
            params["cursor"] = cursor

        result = self._request("GET", "/portfolio/fills", params=params)
        if result is None:
            return []
        return result.get("fills", [])

    # ------------------------------------------------------------------
    # Market endpoints
    # ------------------------------------------------------------------

    def list_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str = "open",
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict | None:
        """List markets with optional filters.

        Args:
            series_ticker: Filter by series (e.g., "KXNBA").
            event_ticker: Filter by event.
            status: Market status filter ("open", "closed", "settled").
            limit: Max results per page (max 200).
            cursor: Pagination cursor.

        Returns:
            API response dict with "markets" and "cursor" keys.
        """
        params = {"status": status, "limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if cursor:
            params["cursor"] = cursor

        return self._request("GET", "/markets", params=params)

    def get_market(self, ticker: str) -> dict | None:
        """Get details for a single market.

        Args:
            ticker: Market ticker (e.g., "KXNBA-26MAR25-LEBRON-PTS-T29.5").

        Returns:
            API response dict with "market" key.
        """
        return self._request("GET", f"/markets/{ticker}")

    def get_orderbook(self, ticker: str, depth: int = 10) -> dict | None:
        """Get order book for a market.

        Args:
            ticker: Market ticker.
            depth: Number of price levels (default 10).

        Returns:
            API response dict with "orderbook" key.
        """
        return self._request("GET", f"/markets/{ticker}/orderbook", params={"depth": depth})

    def list_all_markets(
        self,
        series_ticker: str | None = None,
        status: str = "open",
    ) -> list[dict]:
        """Paginate through all markets for a series.

        Returns:
            Full list of market dicts.
        """
        all_markets = []
        cursor = None

        while True:
            result = self.list_markets(
                series_ticker=series_ticker,
                status=status,
                cursor=cursor,
            )
            if result is None:
                break

            markets = result.get("markets", [])
            all_markets.extend(markets)

            cursor = result.get("cursor")
            if not cursor or not markets:
                break

        logger.info(f"Fetched {len(all_markets)} markets (series={series_ticker})")
        return all_markets

    def list_events(
        self,
        series_ticker: str | None = None,
        status: str = "open",
        limit: int = 200,
        cursor: str | None = None,
    ) -> dict | None:
        """List events with optional filters.

        The /events endpoint returns series_ticker (unlike /markets).
        Use this for discovery of non-sports and other categories.

        Args:
            series_ticker: Filter by series (e.g., "KXBTC").
            status: Event status filter ("open", "closed", "settled").
            limit: Max results per page (max 200).
            cursor: Pagination cursor.

        Returns:
            API response dict with "events" and "cursor" keys.
        """
        params = {"status": status, "limit": limit}
        if series_ticker:
            params["series_ticker"] = series_ticker
        if cursor:
            params["cursor"] = cursor

        return self._request("GET", "/events", params=params)

    def list_all_events(
        self,
        series_ticker: str | None = None,
        status: str = "open",
    ) -> list[dict]:
        """Paginate through all events for a series.

        Returns:
            Full list of event dicts (each with markets nested inside).
        """
        all_events = []
        cursor = None

        while True:
            result = self.list_events(
                series_ticker=series_ticker,
                status=status,
                cursor=cursor,
            )
            if result is None:
                break

            events = result.get("events", [])
            all_events.extend(events)

            cursor = result.get("cursor")
            if not cursor or not events:
                break

        logger.info(f"Fetched {len(all_events)} events (series={series_ticker})")
        return all_events
