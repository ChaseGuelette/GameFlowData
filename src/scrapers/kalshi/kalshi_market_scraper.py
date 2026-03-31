"""
Kalshi Market Scraper
=====================
Discovers player prop markets on Kalshi, parses tickers, links to our
player database, and stores snapshots in kalshi_markets.

Supports:
  --dry-run   Parse and print markets without DB writes
  --mock      Generate synthetic markets (no API credentials needed)
  --sport     Target sport (nba, mlb)

Usage:
    python -m src.scrapers.kalshi.kalshi_market_scraper --mock --dry-run
    python -m src.scrapers.kalshi.kalshi_market_scraper --sport nba
    python -m src.scrapers.kalshi.kalshi_market_scraper --sport mlb --dry-run
"""

import argparse
import logging
import re
import sys
import unicodedata
from datetime import UTC, date, datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.scrapers.kalshi.kalshi_utils import (
    KALSHI_SERIES_MAP,
    KALSHI_STAT_MAP,
    kalshi_mid_to_prob,
    kalshi_price_to_prob,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiMarketScraper")

# Fuzzy match threshold for player name linking
FUZZY_THRESHOLD = 0.85


# ---------------------------------------------------------------------------
# Player Name Normalization (shared with mlb_linker / nba_linker)
# ---------------------------------------------------------------------------


def normalize_player(name: str) -> str | None:
    """Normalize player name for matching.

    Strips accents, removes suffixes (Jr, III, II), punctuation.
    """
    if pd.isna(name) or not name:
        return None
    name = str(name)
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    name = name.lower().strip()
    for old, new in [(".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", ""), (" iv", "")]:
        name = name.replace(old, new)
    return " ".join(name.split()) or None


# ---------------------------------------------------------------------------
# Ticker Parsing
# ---------------------------------------------------------------------------

# Ticker format: KXNBA-26MAR25-LEBRON-PTS-T29.5
# or: KXNBA-26MAR25-LEBRON-JAMES-PTS-O29.5
TICKER_PATTERN = re.compile(
    r"^(?P<series>KX\w+)-"
    r"(?P<date>\d{1,2}[A-Z]{3}\d{2})-"
    r"(?P<player>.+?)-"
    r"(?P<stat>[A-Z0-9]+)-"
    r"[TO](?P<line>[\d.]+)$"
)

# Title fallback: "Will LeBron James score 30+ points?"
TITLE_PATTERN = re.compile(
    r"Will\s+(.+?)\s+(?:score|record|throw|have|get|hit)\s+(\d+\.?\d*)\+?\s*(?:or more\s+)?(\w+)",
    re.IGNORECASE,
)

# Title stat word → Kalshi stat key
TITLE_STAT_MAP = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "threes": "3PM",
    "three-pointers": "3PM",
    "strikeouts": "K",
    "hits": "HITS",
    "home runs": "HR",
    "rbis": "RBI",
    "runs": "RUNS",
    "total bases": "TB",
}


def parse_ticker(ticker: str) -> dict | None:
    """Parse a Kalshi market ticker into components.

    Args:
        ticker: Market ticker string (e.g., "KXNBA-26MAR25-LEBRON-PTS-T29.5").

    Returns:
        Dict with series, date_str, player_name, stat_key, line, or None if unparseable.
    """
    m = TICKER_PATTERN.match(ticker)
    if not m:
        return None

    player_raw = m.group("player").replace("-", " ").title()
    stat_key = m.group("stat")
    line = float(m.group("line"))

    return {
        "series": m.group("series"),
        "date_str": m.group("date"),
        "player_name": player_raw,
        "stat_key": stat_key,
        "line": line,
    }


def parse_market(market: dict) -> dict | None:
    """Parse a Kalshi market dict into our standard format.

    Tries ticker parsing first, falls back to title regex.

    Args:
        market: Raw market dict from Kalshi API.

    Returns:
        Parsed market dict with player_name, stat_type, line, etc.
    """
    ticker = market.get("ticker", "")

    # Try ticker-based parsing first
    parsed = parse_ticker(ticker)
    if parsed:
        stat_type = KALSHI_STAT_MAP.get(parsed["stat_key"])
        if stat_type:
            return {
                "ticker": ticker,
                "event_ticker": market.get("event_ticker", ""),
                "series_ticker": parsed["series"],
                "player_name": parsed["player_name"],
                "stat_type": stat_type,
                "line": parsed["line"],
                "market_title": market.get("title", ""),
                "yes_price": market.get("yes_price", 0),
                "no_price": market.get("no_price", 0),
                "yes_bid": market.get("yes_bid", 0),
                "yes_ask": market.get("yes_ask", 0),
                "volume": market.get("volume", 0),
                "open_interest": market.get("open_interest", 0),
                "close_time": market.get("close_time"),
                "market_status": market.get("status", "open"),
            }

    # Fallback: parse from title
    title = market.get("title", "")
    m = TITLE_PATTERN.match(title)
    if m:
        player_name = m.group(1).strip()
        line = float(m.group(2))
        stat_word = m.group(3).lower()
        stat_key = TITLE_STAT_MAP.get(stat_word)
        if stat_key:
            stat_type = KALSHI_STAT_MAP.get(stat_key)
            if stat_type:
                # Infer series from sport context
                series = ""
                for prefix in KALSHI_SERIES_MAP.values():
                    if ticker.startswith(prefix):
                        series = prefix
                        break

                return {
                    "ticker": ticker,
                    "event_ticker": market.get("event_ticker", ""),
                    "series_ticker": series,
                    "player_name": player_name,
                    "stat_type": stat_type,
                    "line": line,
                    "market_title": title,
                    "yes_price": market.get("yes_price", 0),
                    "no_price": market.get("no_price", 0),
                    "yes_bid": market.get("yes_bid", 0),
                    "yes_ask": market.get("yes_ask", 0),
                    "volume": market.get("volume", 0),
                    "open_interest": market.get("open_interest", 0),
                    "close_time": market.get("close_time"),
                    "market_status": market.get("status", "open"),
                }

    return None


# ---------------------------------------------------------------------------
# Player Linking
# ---------------------------------------------------------------------------


def build_player_cache(engine, sport: str) -> dict[str, int]:
    """Build normalized_name → player_id lookup from database.

    Args:
        engine: SQLAlchemy engine.
        sport: "nba" or "mlb".

    Returns:
        Dict mapping normalized player names to player IDs.
    """
    if sport == "nba":
        query = "SELECT player_id, player_name FROM players WHERE player_name IS NOT NULL"
    else:
        query = "SELECT player_id, full_name AS player_name FROM mlb_players WHERE full_name IS NOT NULL"

    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()

    cache: dict[str, int] = {}
    for player_id, player_name in rows:
        norm = normalize_player(player_name)
        if norm:
            cache[norm] = player_id

    logger.info(f"Built player cache: {len(cache)} {sport.upper()} players")
    return cache


def link_player(
    player_name: str,
    player_cache: dict[str, int],
    fuzzy_cache: dict[str, int | None] | None = None,
) -> int | None:
    """Link a player name to a player_id using exact + fuzzy matching.

    Args:
        player_name: Raw player name from Kalshi.
        player_cache: Normalized name → player_id lookup.
        fuzzy_cache: Optional cache of previous fuzzy results.

    Returns:
        player_id or None if no match found.
    """
    norm = normalize_player(player_name)
    if not norm:
        return None

    # Exact match
    if norm in player_cache:
        return player_cache[norm]

    # Check fuzzy cache
    if fuzzy_cache is not None and norm in fuzzy_cache:
        return fuzzy_cache[norm]

    # Fuzzy match
    best_score = 0.0
    best_id = None
    norm_parts = norm.split()
    norm_last = norm_parts[-1] if norm_parts else ""

    for cached_name, pid in player_cache.items():
        score = SequenceMatcher(None, norm, cached_name).ratio()

        # Boost score when last names match
        cached_parts = cached_name.split()
        cached_last = cached_parts[-1] if cached_parts else ""
        if norm_last and cached_last and norm_last == cached_last:
            score += 0.15

        if score > best_score:
            best_score = score
            best_id = pid

    result = best_id if best_score >= FUZZY_THRESHOLD else None

    if fuzzy_cache is not None:
        fuzzy_cache[norm] = result

    if result:
        logger.debug(f"Fuzzy matched '{player_name}' → player_id={result} (score={best_score:.2f})")
    else:
        logger.debug(f"No match for '{player_name}' (best score={best_score:.2f})")

    return result


# ---------------------------------------------------------------------------
# Mock Data Generation
# ---------------------------------------------------------------------------

MOCK_MARKETS_NBA = [
    {"ticker": "KXNBA-31MAR26-LEBRON-JAMES-PTS-T29.5", "event_ticker": "KXNBA-31MAR26-LEBRON-JAMES",
     "title": "Will LeBron James score 30+ points?", "yes_price": 35, "no_price": 65,
     "yes_bid": 33, "yes_ask": 37, "volume": 1250, "open_interest": 890, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
    {"ticker": "KXNBA-31MAR26-LUKA-DONCIC-AST-T8.5", "event_ticker": "KXNBA-31MAR26-LUKA-DONCIC",
     "title": "Will Luka Doncic record 9+ assists?", "yes_price": 42, "no_price": 58,
     "yes_bid": 40, "yes_ask": 44, "volume": 980, "open_interest": 650, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
    {"ticker": "KXNBA-31MAR26-JAYSON-TATUM-PTS-T28.5", "event_ticker": "KXNBA-31MAR26-JAYSON-TATUM",
     "title": "Will Jayson Tatum score 29+ points?", "yes_price": 48, "no_price": 52,
     "yes_bid": 46, "yes_ask": 50, "volume": 2100, "open_interest": 1400, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
    {"ticker": "KXNBA-31MAR26-NIKOLA-JOKIC-REB-T12.5", "event_ticker": "KXNBA-31MAR26-NIKOLA-JOKIC",
     "title": "Will Nikola Jokic record 13+ rebounds?", "yes_price": 55, "no_price": 45,
     "yes_bid": 53, "yes_ask": 57, "volume": 1800, "open_interest": 1100, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
    {"ticker": "KXNBA-31MAR26-STEPHEN-CURRY-3PM-T4.5", "event_ticker": "KXNBA-31MAR26-STEPHEN-CURRY",
     "title": "Will Stephen Curry hit 5+ three-pointers?", "yes_price": 38, "no_price": 62,
     "yes_bid": 36, "yes_ask": 40, "volume": 3200, "open_interest": 2100, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
]

MOCK_MARKETS_MLB = [
    {"ticker": "KXMLB-31MAR26-GERRIT-COLE-K-T7.5", "event_ticker": "KXMLB-31MAR26-GERRIT-COLE",
     "title": "Will Gerrit Cole throw 8+ strikeouts?", "yes_price": 40, "no_price": 60,
     "yes_bid": 38, "yes_ask": 42, "volume": 750, "open_interest": 500, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
    {"ticker": "KXMLB-31MAR26-SHOHEI-OHTANI-HR-T0.5", "event_ticker": "KXMLB-31MAR26-SHOHEI-OHTANI",
     "title": "Will Shohei Ohtani hit 1+ home runs?", "yes_price": 22, "no_price": 78,
     "yes_bid": 20, "yes_ask": 24, "volume": 4500, "open_interest": 3000, "status": "open",
     "close_time": "2026-03-31T23:30:00Z"},
]


def generate_mock_markets(sport: str) -> list[dict]:
    """Generate synthetic market data for testing."""
    if sport == "nba":
        return MOCK_MARKETS_NBA
    elif sport == "mlb":
        return MOCK_MARKETS_MLB
    return []


# ---------------------------------------------------------------------------
# DB Storage
# ---------------------------------------------------------------------------


def store_markets(engine, parsed_markets: list[dict], snapshot_time: datetime) -> int:
    """Batch insert parsed markets into kalshi_markets.

    Uses ON CONFLICT to update existing snapshots for the same ticker + snapshot time.

    Returns:
        Number of rows upserted.
    """
    if not parsed_markets:
        return 0

    stmt = text("""
        INSERT INTO kalshi_markets (
            ticker, event_ticker, series_ticker, sport, player_name, stat_type, line,
            market_title, player_id, yes_price, no_price, yes_bid, yes_ask,
            bid_ask_spread, volume, open_interest, close_time, market_status, snapshot_time
        ) VALUES (
            :ticker, :event_ticker, :series_ticker, :sport, :player_name, :stat_type, :line,
            :market_title, :player_id, :yes_price, :no_price, :yes_bid, :yes_ask,
            :bid_ask_spread, :volume, :open_interest, :close_time, :market_status, :snapshot_time
        )
        ON CONFLICT (ticker, snapshot_time)
        DO UPDATE SET
            yes_price = EXCLUDED.yes_price,
            no_price = EXCLUDED.no_price,
            yes_bid = EXCLUDED.yes_bid,
            yes_ask = EXCLUDED.yes_ask,
            bid_ask_spread = EXCLUDED.bid_ask_spread,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            market_status = EXCLUDED.market_status,
            player_id = EXCLUDED.player_id
    """)

    count = 0
    with engine.begin() as conn:
        for m in parsed_markets:
            spread = (m.get("yes_ask", 0) or 0) - (m.get("yes_bid", 0) or 0)
            params = {
                "ticker": m["ticker"],
                "event_ticker": m.get("event_ticker", ""),
                "series_ticker": m.get("series_ticker", ""),
                "sport": m.get("sport", ""),
                "player_name": m["player_name"],
                "stat_type": m["stat_type"],
                "line": m["line"],
                "market_title": m.get("market_title", ""),
                "player_id": m.get("player_id"),
                "yes_price": m.get("yes_price", 0),
                "no_price": m.get("no_price", 0),
                "yes_bid": m.get("yes_bid", 0),
                "yes_ask": m.get("yes_ask", 0),
                "bid_ask_spread": spread,
                "volume": m.get("volume", 0),
                "open_interest": m.get("open_interest", 0),
                "close_time": m.get("close_time"),
                "market_status": m.get("market_status", "open"),
                "snapshot_time": snapshot_time,
            }
            conn.execute(stmt, params)
            count += 1

    return count


# ---------------------------------------------------------------------------
# Main Scrape Flow
# ---------------------------------------------------------------------------


def scrape_and_store(
    sport: str = "nba",
    dry_run: bool = False,
    mock: bool = False,
) -> dict:
    """Full scrape cycle: discover → parse → link → store.

    Args:
        sport: Target sport ("nba" or "mlb").
        dry_run: Print parsed markets without DB writes.
        mock: Use synthetic data instead of API.

    Returns:
        Summary dict with counts.
    """
    snapshot_time = datetime.now(UTC)
    stats = {"raw": 0, "parsed": 0, "linked": 0, "stored": 0}

    # Step 1: Discover markets
    if mock:
        raw_markets = generate_mock_markets(sport)
        logger.info(f"Generated {len(raw_markets)} mock {sport.upper()} markets")
    else:
        client = KalshiClient()
        if not client.is_authenticated:
            logger.warning("No Kalshi credentials — use --mock for testing")
            return stats

        series_ticker = KALSHI_SERIES_MAP.get(sport)
        if not series_ticker:
            logger.error(f"Unknown sport: {sport}")
            return stats

        raw_markets = client.list_all_markets(series_ticker=series_ticker)

    stats["raw"] = len(raw_markets)

    # Step 2: Parse markets
    parsed_markets = []
    for market in raw_markets:
        parsed = parse_market(market)
        if parsed:
            parsed["sport"] = sport
            parsed_markets.append(parsed)

    stats["parsed"] = len(parsed_markets)
    logger.info(f"Parsed {len(parsed_markets)}/{len(raw_markets)} markets")

    # Step 3: Link players
    if not dry_run:
        engine = get_engine()
    else:
        engine = None

    if engine and not dry_run:
        player_cache = build_player_cache(engine, sport)
        fuzzy_cache: dict[str, int | None] = {}

        for m in parsed_markets:
            pid = link_player(m["player_name"], player_cache, fuzzy_cache)
            if pid:
                m["player_id"] = pid
                stats["linked"] += 1
    else:
        # In dry-run, still try to link if DB is available
        try:
            engine = get_engine()
            player_cache = build_player_cache(engine, sport)
            fuzzy_cache = {}
            for m in parsed_markets:
                pid = link_player(m["player_name"], player_cache, fuzzy_cache)
                if pid:
                    m["player_id"] = pid
                    stats["linked"] += 1
        except Exception:
            logger.info("DB unavailable for player linking in dry-run mode")

    logger.info(f"Linked {stats['linked']}/{len(parsed_markets)} players")

    # Step 4: Print or store
    if dry_run:
        logger.info("=== DRY RUN — parsed markets ===")
        for m in parsed_markets:
            yes_p = kalshi_price_to_prob(m.get("yes_price", 0))
            mid_p = kalshi_mid_to_prob(m.get("yes_bid", 0), m.get("yes_ask", 0))
            logger.info(
                f"  {m['player_name']:25s} | {m['stat_type']:10s} | "
                f"line={m['line']:5.1f} | YES={m.get('yes_price', 0):3d}c | "
                f"mid={mid_p:.1%} | vol={m.get('volume', 0):5d} | "
                f"pid={'linked' if m.get('player_id') else 'UNLINKED'}"
            )
    else:
        stored = store_markets(engine, parsed_markets, snapshot_time)
        stats["stored"] = stored
        logger.info(f"Stored {stored} markets in kalshi_markets")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Kalshi prediction markets for player props",
    )
    parser.add_argument(
        "--sport", type=str, default="nba", choices=["nba", "mlb"],
        help="Target sport (default: nba)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print parsed markets without DB writes",
    )
    parser.add_argument(
        "--mock", action="store_true",
        help="Use synthetic data instead of API",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Kalshi Market Scraper")
    logger.info(f"  Sport: {args.sport.upper()}")
    logger.info(f"  Mock: {args.mock}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("=" * 60)

    stats = scrape_and_store(
        sport=args.sport,
        dry_run=args.dry_run,
        mock=args.mock,
    )

    logger.info("=" * 60)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"  Raw markets: {stats['raw']}")
    logger.info(f"  Parsed: {stats['parsed']}")
    logger.info(f"  Linked: {stats['linked']}")
    logger.info(f"  Stored: {stats['stored']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
