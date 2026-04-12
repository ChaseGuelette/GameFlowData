"""
Polymarket API Client
=====================
Low-level HTTP client for the Polymarket prediction markets APIs.

No authentication required for read-only endpoints.
Rate limits: 4000/10s (Gamma API), 9000/10s (CLOB API).
Internal delay: 50ms between requests (conservative).

APIs:
  - Gamma API: https://gamma-api.polymarket.com — event/market discovery
  - CLOB API:  https://clob.polymarket.com     — order books + prices

Graceful no-op when API is unreachable.

Usage:
    client = PolymarketClient()
    tags = client.get_sport_tags()
    events = client.get_events(tag_id=100029)  # NBA
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"


class PolymarketClient:
    """HTTP client for the Polymarket prediction markets APIs."""

    def __init__(self, delay: float = 0.05, max_retries: int = 3):
        """Initialize the client.

        Args:
            delay: Minimum delay between requests in seconds (default 50ms).
            max_retries: Maximum retry attempts on transient errors.
        """
        self.delay = delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._last_request_time: float | None = None

    def _rate_limit(self) -> None:
        """Enforce minimum delay between requests."""
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, base_url: str, path: str, params: dict | None = None) -> dict | list | None:
        """Make a GET request with retries and exponential backoff.

        Args:
            base_url: API base URL (Gamma or CLOB).
            path: Path to append.
            params: Query parameters.

        Returns:
            Parsed JSON response (dict or list), or None on failure.
        """
        url = f"{base_url}{path}"

        for attempt in range(self.max_retries):
            self._rate_limit()
            response = None
            try:
                response = self.session.get(url, params=params, timeout=15)

                if response.status_code == 429:
                    wait = (2 ** attempt) * 5
                    logger.warning(f"Rate limited (429) on {path}. Waiting {wait}s.")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on {path} (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.HTTPError as e:
                status = response.status_code if response is not None else 0
                if status >= 500:
                    wait = (2 ** attempt) * 2
                    logger.warning(f"Server error {status} on {path}. Waiting {wait}s.")
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP error on {path}: {e}")
                return None
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error on {path} (attempt {attempt + 1}): {e}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on {path}: {e}")
                return None

            if attempt < self.max_retries - 1:
                wait = (2 ** attempt) * self.delay
                time.sleep(wait)

        logger.error(f"Failed after {self.max_retries} attempts: {path}")
        return None

    def _post(self, base_url: str, path: str, body: list | dict) -> dict | list | None:
        """Make a POST request with retries.

        Args:
            base_url: API base URL.
            path: Path to append.
            body: JSON body.

        Returns:
            Parsed JSON response, or None on failure.
        """
        url = f"{base_url}{path}"

        for attempt in range(self.max_retries):
            self._rate_limit()
            response = None
            try:
                response = self.session.post(url, json=body, timeout=15)

                if response.status_code == 429:
                    wait = (2 ** attempt) * 5
                    logger.warning(f"Rate limited (429) on {path}. Waiting {wait}s.")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on POST {path} (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.HTTPError as e:
                status = response.status_code if response is not None else 0
                if status >= 500:
                    wait = (2 ** attempt) * 2
                    logger.warning(f"Server error {status} on POST {path}. Waiting {wait}s.")
                    time.sleep(wait)
                    continue
                logger.error(f"HTTP error on POST {path}: {e}")
                return None
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error on POST {path} (attempt {attempt + 1}): {e}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed on POST {path}: {e}")
                return None

            if attempt < self.max_retries - 1:
                wait = (2 ** attempt) * self.delay
                time.sleep(wait)

        logger.error(f"Failed after {self.max_retries} attempts: POST {path}")
        return None

    # ------------------------------------------------------------------
    # Gamma API — Market/Event Discovery
    # ------------------------------------------------------------------

    def get_sport_tags(self) -> list[dict]:
        """Get all sport tags with their IDs and metadata.

        Returns:
            List of tag dicts with 'id', 'label', 'slug' fields, or [] on failure.
        """
        result = self._get(GAMMA_BASE_URL, "/sports")
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return result.get("sports", [])

    def get_events(
        self,
        tag_id: int | None = None,
        active: bool = True,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Get events, optionally filtered by sport tag.

        Automatically paginates to fetch all matching events.

        Args:
            tag_id: Sport tag ID to filter by (e.g., 100029 for NBA).
            active: Include active events.
            closed: Include closed events.
            limit: Page size (max 100).
            offset: Starting offset for pagination.

        Returns:
            List of event dicts, or [] on failure.
        """
        params: dict = {"limit": limit, "active": str(active).lower(), "closed": str(closed).lower()}
        if tag_id is not None:
            params["tag_id"] = tag_id

        all_events: list[dict] = []
        current_offset = offset

        while True:
            params["offset"] = current_offset
            result = self._get(GAMMA_BASE_URL, "/events", params=params)
            if result is None:
                break

            events = result if isinstance(result, list) else result.get("events", [])
            if not events:
                break

            all_events.extend(events)

            # Pagination: if we got a full page, fetch next
            if len(events) < limit:
                break
            current_offset += limit

        logger.info(f"Fetched {len(all_events)} events (tag_id={tag_id})")
        return all_events

    def get_markets(
        self,
        condition_id: str | None = None,
        slug: str | None = None,
    ) -> list[dict]:
        """Get markets by condition ID or slug.

        Args:
            condition_id: Polymarket condition ID.
            slug: Market slug.

        Returns:
            List of market dicts, or [] on failure.
        """
        params: dict = {}
        if condition_id:
            params["condition_id"] = condition_id
        if slug:
            params["slug"] = slug

        result = self._get(GAMMA_BASE_URL, "/markets", params=params)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return result.get("markets", [])

    def search(self, query: str, limit: int = 50) -> list[dict]:
        """Search for markets/events by text.

        Args:
            query: Search query string.
            limit: Max results.

        Returns:
            List of result dicts, or [] on failure.
        """
        params = {"query": query, "limit": limit}
        result = self._get(GAMMA_BASE_URL, "/public-search", params=params)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return result.get("results", [])

    # ------------------------------------------------------------------
    # CLOB API — Order Books + Prices
    # ------------------------------------------------------------------

    def get_orderbook(self, token_id: str) -> dict | None:
        """Get order book for a single token.

        Args:
            token_id: CLOB token ID (YES or NO token).

        Returns:
            Dict with 'bids' and 'asks' lists, or None on failure.
        """
        result = self._get(CLOB_BASE_URL, "/book", params={"token_id": token_id})
        if isinstance(result, dict):
            return result
        return None

    def get_midpoint(self, token_id: str) -> float | None:
        """Get midpoint price for a single token.

        Args:
            token_id: CLOB token ID.

        Returns:
            Midpoint price (0.0 to 1.0), or None on failure.
        """
        result = self._get(CLOB_BASE_URL, "/midpoint", params={"token_id": token_id})
        if not isinstance(result, dict):
            return None
        try:
            mid = result.get("mid")
            return float(mid) if mid is not None else None
        except (ValueError, TypeError):
            return None

    def get_batch_midpoints(self, token_ids: list[str]) -> dict[str, float]:
        """Fetch midpoints for multiple tokens in a single request.

        Args:
            token_ids: List of CLOB token IDs (up to 500 per call).

        Returns:
            Dict mapping token_id -> midpoint price (0.0 to 1.0).
        """
        if not token_ids:
            return {}

        # CLOB API supports up to 500 per batch
        results: dict[str, float] = {}
        batch_size = 500

        for i in range(0, len(token_ids), batch_size):
            batch = token_ids[i:i + batch_size]
            data = self._post(CLOB_BASE_URL, "/midpoints", body={"token_ids": batch})
            if data is None:
                continue
            if isinstance(data, dict):
                for tid, mid in data.items():
                    try:
                        results[tid] = float(mid)
                    except (ValueError, TypeError):
                        pass
            elif isinstance(data, list):
                for item in data:
                    tid = item.get("token_id") or item.get("tokenId")
                    mid = item.get("mid") or item.get("midpoint")
                    if tid and mid is not None:
                        try:
                            results[tid] = float(mid)
                        except (ValueError, TypeError):
                            pass

        return results

    def get_batch_orderbooks(self, token_ids: list[str]) -> dict[str, dict]:
        """Fetch order books for multiple tokens in a single request.

        Args:
            token_ids: List of CLOB token IDs (up to 500 per call).

        Returns:
            Dict mapping token_id -> orderbook dict with 'bids'/'asks'.
        """
        if not token_ids:
            return {}

        results: dict[str, dict] = {}
        batch_size = 500

        for i in range(0, len(token_ids), batch_size):
            batch = token_ids[i:i + batch_size]
            data = self._post(CLOB_BASE_URL, "/books", body={"token_ids": batch})
            if data is None:
                continue
            if isinstance(data, list):
                for item in data:
                    tid = item.get("token_id") or item.get("tokenId")
                    if tid:
                        results[tid] = item
            elif isinstance(data, dict):
                results.update(data)

        return results
