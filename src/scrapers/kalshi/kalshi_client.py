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
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


class KalshiClient:
    """HTTP client for the Kalshi prediction markets API."""

    def __init__(self, delay: float = 0.05, max_retries: int = 3):
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

        for attempt in range(self.max_retries):
            self._rate_limit()
            try:
                headers = self._auth_headers(method.upper(), path)
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
