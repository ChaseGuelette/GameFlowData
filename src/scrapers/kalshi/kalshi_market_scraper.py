"""
Kalshi Market Scraper
=====================
Discovers player prop markets on Kalshi, parses tickers, links to our
player database, and stores snapshots in kalshi_markets.

Market structure (discovered Apr 2026):
  - Player props live in per-stat series: KXNBAPTS, KXNBAREB, KXNBAAST,
    KXNBA3PT, KXNBABLK, KXNBASTL (NBA) and KXMLBTB, KXMLBHR (MLB).
  - Ticker format: KX{STAT}-{YYMONDD}{MATCHUP}-{TEAM}{PLAYER}{JERSEY}-{LINE}
    Example: KXNBAPTS-26APR01BOSMIA-BOSJTATUM0-25
  - Prices are in dollars (0.00-1.00), not cents.
  - The old KXNBA/KXMLB series contain only championship futures.

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
import time
import unicodedata
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.arbitrage.team_normalizer import extract_teams_from_question, normalize_team
from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_client import KalshiClient
from src.scrapers.kalshi.kalshi_utils import (
    KALSHI_GAME_SERIES,
    KALSHI_NON_SPORTS_SERIES,
    KALSHI_PROP_SERIES,
    kalshi_mid_to_prob,
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
# Ticker Parsing (new format discovered Apr 2026)
# ---------------------------------------------------------------------------

# New ticker format: KX{STAT}-{YYMONDD}{MATCHUP}-{TEAM}{PLAYER}{JERSEY}-{LINE}
# Examples:
#   KXNBAPTS-26APR01BOSMIA-BOSJTATUM0-25       (Jayson Tatum 25+ points)
#   KXNBAREB-26APR01SASGSW-SASVWEMBANYAMA1-8    (Victor Wembanyama 8+ rebounds)
#   KXMLBTB-26APR011610NYYSEA-NYYPGOLDSCHMIDT48-5  (Paul Goldschmidt 5+ total bases)
#   KXMLBHR-26APR011610NYYSEA-NYYPGOLDSCHMIDT48-2  (Paul Goldschmidt 2+ home runs)
#
# The matchup part may include a time prefix for MLB: 1610 = 4:10 PM
# Player segment: {3-letter team}{FirstInitial/Name}{LastName}{JerseyNumber}
# Line: integer (the "+" threshold)

# We parse from the market title instead of the ticker for player names,
# since the title is human-readable: "Jayson Tatum: 25+ points"
TITLE_PLAYER_LINE_PATTERN = re.compile(
    r"^(.+?):\s*(\d+)\+",
)


def parse_market(market: dict, series_ticker: str, stat_type: str) -> dict | None:
    """Parse a Kalshi market dict into our standard format.

    Uses the market title for player name and line extraction, since titles
    are human-readable (e.g., "Jayson Tatum: 25+ points").

    Args:
        market: Raw market dict from Kalshi API.
        series_ticker: The series this market belongs to (e.g., "KXNBAPTS").
        stat_type: Our internal stat type (e.g., "pts").

    Returns:
        Parsed market dict with player_name, stat_type, line, prices, etc.
    """
    ticker = market.get("ticker", "")
    title = market.get("title", "")

    # Parse player name and line from title: "Jayson Tatum: 25+ points"
    m = TITLE_PLAYER_LINE_PATTERN.match(title)
    if not m:
        return None

    player_name = m.group(1).strip()
    line = float(m.group(2))

    # Kalshi API returns prices in dollars as strings (e.g., "0.4600")
    # Convert to cents (0 - 100) for internal consistency with edge calculator
    yes_bid_dollars = float(market.get("yes_bid_dollars") or 0)
    yes_ask_dollars = float(market.get("yes_ask_dollars") or 0)
    last_price_dollars = float(market.get("last_price_dollars") or 0)

    yes_bid_cents = round(yes_bid_dollars * 100)
    yes_ask_cents = round(yes_ask_dollars * 100)
    yes_price_cents = round(last_price_dollars * 100) if last_price_dollars else round((yes_bid_dollars + yes_ask_dollars) / 2 * 100)

    try:
        volume = int(float(market.get("volume_fp") or market.get("volume") or 0))
    except (ValueError, TypeError):
        volume = 0
    try:
        open_interest = int(float(market.get("open_interest_fp") or market.get("open_interest") or 0))
    except (ValueError, TypeError):
        open_interest = 0

    return {
        "ticker": ticker,
        "event_ticker": market.get("event_ticker", ""),
        "series_ticker": series_ticker,
        "player_name": player_name,
        "stat_type": stat_type,
        "line": line,
        "market_title": title,
        "yes_price": yes_price_cents,
        "no_price": 100 - yes_price_cents,
        "yes_bid": yes_bid_cents,
        "yes_ask": yes_ask_cents,
        "volume": volume,
        "open_interest": open_interest,
        "close_time": market.get("close_time") or market.get("expected_expiration_time"),
        # Normalize Kalshi "active" status to "open" for consistency with edge calculator
        "market_status": "open" if market.get("status") in ("active", "open") else market.get("status", "open"),
    }


# ---------------------------------------------------------------------------
# Game-Level Market Parsing
# ---------------------------------------------------------------------------


def parse_game_market_kalshi(
    market: dict,
    series_ticker: str,
    market_type: str,
    sport: str,
) -> dict | None:
    """Parse a Kalshi game-level market (moneyline, NRFI, total, future).

    Extracts team names from the market title using team_normalizer.

    Args:
        market: Raw market dict from Kalshi API.
        series_ticker: The series this market belongs to.
        market_type: Market type ('moneyline', 'nrfi', 'total', 'season_future').
        sport: Sport key ('mlb', 'nba').

    Returns:
        Parsed market dict with team1/team2 instead of player_name/stat_type,
        or None if parsing fails or market is not active.
    """
    ticker = market.get("ticker", "")
    title = market.get("title", "")

    if not title:
        return None

    # Extract prices (same logic as parse_market)
    yes_bid_dollars = float(market.get("yes_bid_dollars") or 0)
    yes_ask_dollars = float(market.get("yes_ask_dollars") or 0)
    last_price_dollars = float(market.get("last_price_dollars") or 0)

    yes_bid_cents = round(yes_bid_dollars * 100)
    yes_ask_cents = round(yes_ask_dollars * 100)
    yes_price_cents = (
        round(last_price_dollars * 100)
        if last_price_dollars
        else round((yes_bid_dollars + yes_ask_dollars) / 2 * 100)
    )

    try:
        volume = int(float(market.get("volume_fp") or market.get("volume") or 0))
    except (ValueError, TypeError):
        volume = 0
    try:
        open_interest = int(float(market.get("open_interest_fp") or market.get("open_interest") or 0))
    except (ValueError, TypeError):
        open_interest = 0

    # Try to extract teams from the title
    team1 = None
    team2 = None
    line = None

    if market_type == "season_future":
        # For futures, team1 = the subject team (often in the title)
        t = normalize_team(title.split(":")[0].strip(), sport=sport)
        if t:
            team1 = t
    else:
        # For game-level markets, extract two teams
        result = extract_teams_from_question(title, sport)
        if result:
            team1, team2 = result
        else:
            # Fall back: try to extract canonical abbrs from ticker
            # Ticker format often contains team codes: KXMLBNRFI-26APR16NYYSEA -> NYY, SEA
            import re
            ticker_upper = ticker.upper()
            # Find a block of 6 uppercase letters after the date part
            m = re.search(r"-\d{2}[A-Z]{3}\d{2}([A-Z]{3})([A-Z]{3})", ticker_upper)
            if m:
                t1 = normalize_team(m.group(1), sport=sport)
                t2 = normalize_team(m.group(2), sport=sport)
                if t1 and t2:
                    team1, team2 = t1, t2

    return {
        "ticker": ticker,
        "event_ticker": market.get("event_ticker", ""),
        "series_ticker": series_ticker,
        "market_type": market_type,
        "sport": sport,
        "market_title": title,
        "team1": team1,
        "team2": team2,
        "line": line,
        "player_name": None,
        "stat_type": None,
        "yes_price": yes_price_cents,
        "no_price": 100 - yes_price_cents,
        "yes_bid": yes_bid_cents,
        "yes_ask": yes_ask_cents,
        "volume": volume,
        "open_interest": open_interest,
        "close_time": market.get("close_time") or market.get("expected_expiration_time"),
        "market_status": "open" if market.get("status") in ("active", "open") else market.get("status", "open"),
    }


# ---------------------------------------------------------------------------
# Player Linking
# ---------------------------------------------------------------------------


def build_player_cache(engine, sport: str) -> dict[str, int]:
    """Build normalized_name -> player_id lookup from database."""
    if sport == "nba":
        query = "SELECT player_id, player_name FROM players WHERE player_name IS NOT NULL"
    else:
        query = "SELECT player_id, player_name FROM mlb_players WHERE player_name IS NOT NULL"

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
    """Link a player name to a player_id using exact + fuzzy matching."""
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
        logger.debug(f"Fuzzy matched '{player_name}' -> player_id={result} (score={best_score:.2f})")
    else:
        logger.debug(f"No match for '{player_name}' (best score={best_score:.2f})")

    return result


# ---------------------------------------------------------------------------
# Mock Data Generation
# ---------------------------------------------------------------------------

MOCK_MARKETS_NBA = [
    {
        "ticker": "KXNBAPTS-26APR01BOSMIA-BOSJTATUM0-25",
        "event_ticker": "KXNBAPTS-26APR01BOSMIA",
        "title": "Jayson Tatum: 25+ points",
        "yes_bid_dollars": 0.46, "yes_ask_dollars": 0.50,
        "last_price_dollars": 0.48,
        "volume_fp": 2100, "open_interest_fp": 1400,
        "status": "active", "close_time": "2026-04-01T23:30:00Z",
    },
    {
        "ticker": "KXNBAREB-26APR01DENUTA-DENNJOKIC15-12",
        "event_ticker": "KXNBAREB-26APR01DENUTA",
        "title": "Nikola Jokic: 12+ rebounds",
        "yes_bid_dollars": 0.53, "yes_ask_dollars": 0.57,
        "last_price_dollars": 0.55,
        "volume_fp": 1800, "open_interest_fp": 1100,
        "status": "active", "close_time": "2026-04-01T23:30:00Z",
    },
    {
        "ticker": "KXNBAAST-26APR01DENUTA-DENNJOKIC15-8",
        "event_ticker": "KXNBAAST-26APR01DENUTA",
        "title": "Nikola Jokic: 8+ assists",
        "yes_bid_dollars": 0.40, "yes_ask_dollars": 0.44,
        "last_price_dollars": 0.42,
        "volume_fp": 980, "open_interest_fp": 650,
        "status": "active", "close_time": "2026-04-01T23:30:00Z",
    },
    {
        "ticker": "KXNBA3PT-26APR01BOSMIA-BOSJTATUM0-3",
        "event_ticker": "KXNBA3PT-26APR01BOSMIA",
        "title": "Jayson Tatum: 3+ threes",
        "yes_bid_dollars": 0.36, "yes_ask_dollars": 0.40,
        "last_price_dollars": 0.38,
        "volume_fp": 3200, "open_interest_fp": 2100,
        "status": "active", "close_time": "2026-04-01T23:30:00Z",
    },
]

MOCK_MARKETS_MLB = [
    {
        "ticker": "KXMLBTB-26APR011610NYYSEA-NYYAJUDGE99-4",
        "event_ticker": "KXMLBTB-26APR011610NYYSEA",
        "title": "Aaron Judge: 4+ total bases?",
        "yes_bid_dollars": 0.18, "yes_ask_dollars": 0.22,
        "last_price_dollars": 0.20,
        "volume_fp": 750, "open_interest_fp": 500,
        "status": "active", "close_time": "2026-04-01T23:30:00Z",
    },
    {
        "ticker": "KXMLBHR-26APR011610NYYSEA-NYYAJUDGE99-1",
        "event_ticker": "KXMLBHR-26APR011610NYYSEA",
        "title": "Aaron Judge: 1+ home runs?",
        "yes_bid_dollars": 0.20, "yes_ask_dollars": 0.24,
        "last_price_dollars": 0.22,
        "volume_fp": 4500, "open_interest_fp": 3000,
        "status": "active", "close_time": "2026-04-01T23:30:00Z",
    },
]


def generate_mock_markets(sport: str) -> dict[str, list[dict]]:
    """Generate synthetic market data for testing.

    Returns:
        Dict mapping series_ticker -> list of market dicts.
    """
    if sport == "nba":
        return {
            "KXNBAPTS": [m for m in MOCK_MARKETS_NBA if "PTS" in m["ticker"]],
            "KXNBAREB": [m for m in MOCK_MARKETS_NBA if "REB" in m["ticker"]],
            "KXNBAAST": [m for m in MOCK_MARKETS_NBA if "AST" in m["ticker"]],
            "KXNBA3PT": [m for m in MOCK_MARKETS_NBA if "3PT" in m["ticker"]],
        }
    elif sport == "mlb":
        return {
            "KXMLBTB": [m for m in MOCK_MARKETS_MLB if "TB" in m["ticker"]],
            "KXMLBHR": [m for m in MOCK_MARKETS_MLB if "HR" in m["ticker"]],
        }
    return {}


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
            ticker, event_ticker, series_ticker, sport, market_type,
            player_name, stat_type, line, team1, team2,
            market_title, player_id, yes_price, no_price, yes_bid, yes_ask,
            bid_ask_spread, volume, open_interest, close_time, market_status, snapshot_time
        ) VALUES (
            :ticker, :event_ticker, :series_ticker, :sport, :market_type,
            :player_name, :stat_type, :line, :team1, :team2,
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
            player_id = EXCLUDED.player_id,
            team1 = EXCLUDED.team1,
            team2 = EXCLUDED.team2,
            market_type = EXCLUDED.market_type
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
                "market_type": m.get("market_type", "player_prop"),
                "player_name": m.get("player_name"),
                "stat_type": m.get("stat_type"),
                "line": m.get("line"),
                "team1": m.get("team1"),
                "team2": m.get("team2"),
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
    """Full scrape cycle: discover -> parse -> link -> store.

    Scrapes all per-stat series for the sport (e.g., KXNBAPTS, KXNBAREB, ...),
    parses player names from titles, links to DB players, and stores.

    Args:
        sport: Target sport ("nba" or "mlb").
        dry_run: Print parsed markets without DB writes.
        mock: Use synthetic data instead of API.

    Returns:
        Summary dict with counts.
    """
    snapshot_time = datetime.now(UTC)
    stats = {"raw": 0, "parsed": 0, "linked": 0, "stored": 0}

    prop_series = KALSHI_PROP_SERIES.get(sport, {})

    # Game-level series for this sport (may be empty if not yet discovered)
    game_series = {
        ticker: info
        for ticker, info in KALSHI_GAME_SERIES.items()
        if info.get("sport") == sport
    }

    if not prop_series and not game_series:
        logger.error(f"No prop or game series configured for sport: {sport}")
        return stats

    # Step 1: Discover markets from all stat series
    all_raw_markets: dict[str, list[dict]] = {}

    if mock:
        all_raw_markets = generate_mock_markets(sport)
        total_raw = sum(len(ms) for ms in all_raw_markets.values())
        logger.info(f"Generated {total_raw} mock {sport.upper()} markets")
    else:
        client = KalshiClient()
        if not client.is_authenticated:
            logger.warning("No Kalshi credentials -- use --mock for testing")
            return stats

        all_series_to_fetch = list(prop_series.keys()) + list(game_series.keys())
        for i, series_ticker in enumerate(all_series_to_fetch):
            if i > 0:
                time.sleep(1.0)  # pause between series to avoid rate limiting
            raw = client.list_all_markets(series_ticker=series_ticker)
            if raw:
                all_raw_markets[series_ticker] = raw
                logger.info(f"  {series_ticker}: {len(raw)} markets")

    total_raw = sum(len(ms) for ms in all_raw_markets.values())
    stats["raw"] = total_raw

    # Step 2: Parse markets (player props + game-level)
    parsed_markets = []
    for series_ticker, markets in all_raw_markets.items():
        if series_ticker in prop_series:
            # Player prop series
            stat_type = prop_series[series_ticker]
            for market in markets:
                if market.get("status") not in ("active", "open"):
                    continue
                parsed = parse_market(market, series_ticker, stat_type)
                if parsed:
                    parsed["sport"] = sport
                    parsed["market_type"] = "player_prop"
                    parsed_markets.append(parsed)

        elif series_ticker in game_series:
            # Game-level series
            info = game_series[series_ticker]
            mtype = info.get("market_type", "moneyline")
            for market in markets:
                if market.get("status") not in ("active", "open"):
                    continue
                parsed = parse_game_market_kalshi(market, series_ticker, mtype, sport)
                if parsed:
                    parsed_markets.append(parsed)

    stats["parsed"] = len(parsed_markets)
    logger.info(f"Parsed {len(parsed_markets)}/{total_raw} markets")

    # Step 3: Link players
    engine = None
    if not dry_run:
        engine = get_engine()

    try:
        link_engine = engine or get_engine()
        player_cache = build_player_cache(link_engine, sport)
        fuzzy_cache: dict[str, int | None] = {}

        for m in parsed_markets:
            pid = link_player(m["player_name"], player_cache, fuzzy_cache)
            if pid:
                m["player_id"] = pid
                stats["linked"] += 1
    except Exception:
        logger.info("DB unavailable for player linking")

    logger.info(f"Linked {stats['linked']}/{len(parsed_markets)} players")

    # Step 4: Print or store
    if dry_run:
        logger.info("=== DRY RUN -- parsed markets ===")
        for m in parsed_markets:
            mid_p = kalshi_mid_to_prob(m.get("yes_bid", 0), m.get("yes_ask", 0))
            if m.get("player_name") is not None:
                # Player prop market
                line_val = m.get("line") or 0
                logger.info(
                    f"  {m['player_name']:25s} | {m['stat_type']:20s} | "
                    f"line={line_val:5.1f} | YES={m.get('yes_price', 0):3d}c | "
                    f"mid={mid_p:.1%} | vol={m.get('volume', 0):5d} | "
                    f"pid={'linked' if m.get('player_id') else 'UNLINKED'}"
                )
            else:
                # Game-level market
                t1 = m.get("team1") or "?"
                t2 = m.get("team2") or "?"
                mtype = m.get("market_type", "game")
                logger.info(
                    f"  [{mtype:12s}] {t1:4s} vs {t2:4s} | "
                    f"YES={m.get('yes_price', 0):3d}c | mid={mid_p:.1%} | "
                    f"vol={m.get('volume', 0):5d} | {m.get('market_title', '')[:50]}"
                )
    else:
        stored = store_markets(engine, parsed_markets, snapshot_time)
        stats["stored"] = stored
        logger.info(f"Stored {stored} markets in kalshi_markets")

    return stats


# ---------------------------------------------------------------------------
# Non-Sports Scrape
# ---------------------------------------------------------------------------


def scrape_non_sports_and_store(dry_run: bool = False) -> dict:
    """Scrape all Kalshi non-sports markets and store with sport=NULL.

    Fetches economics (KXGDP, KXFED, KXCPI) and crypto (KXBTC, KXETH, etc.)
    series. Markets are stored with sport=NULL so they're picked up by the
    non-sports arb matcher in market_matcher.py.

    No player/team linking needed — just title + prices.

    Args:
        dry_run: Print parsed markets without DB writes.

    Returns:
        Summary dict with counts.
    """
    snapshot_time = datetime.now(UTC)
    stats: dict = {"raw": 0, "parsed": 0, "stored": 0, "by_series": {}}

    if not KALSHI_NON_SPORTS_SERIES:
        logger.info("No non-sports series configured in KALSHI_NON_SPORTS_SERIES")
        return stats

    client = KalshiClient()
    if not client.is_authenticated:
        logger.warning("No Kalshi credentials — non-sports scrape skipped")
        return stats

    parsed_markets: list[dict] = []

    for series_ticker, info in KALSHI_NON_SPORTS_SERIES.items():
        category = info.get("category", "other")
        desc = info.get("description", series_ticker)

        time.sleep(0.5)  # gentle pacing between series
        raw = client.list_all_markets(series_ticker=series_ticker)
        series_count = 0

        for market in raw:
            if market.get("status") not in ("active", "open"):
                continue

            ticker = market.get("ticker", "")
            title = market.get("title", "")
            if not ticker or not title:
                continue

            yes_bid_dollars = float(market.get("yes_bid_dollars") or 0)
            yes_ask_dollars = float(market.get("yes_ask_dollars") or 0)
            last_price_dollars = float(market.get("last_price_dollars") or 0)

            yes_bid_cents = round(yes_bid_dollars * 100)
            yes_ask_cents = round(yes_ask_dollars * 100)
            yes_price_cents = (
                round(last_price_dollars * 100)
                if last_price_dollars
                else round((yes_bid_dollars + yes_ask_dollars) / 2 * 100)
            )
            if yes_price_cents <= 0 and yes_bid_cents <= 0:
                continue  # No price data — skip

            try:
                volume = int(float(market.get("volume_fp") or market.get("volume") or 0))
            except (ValueError, TypeError):
                volume = 0

            parsed_markets.append({
                "ticker": ticker,
                "event_ticker": market.get("event_ticker", ""),
                "series_ticker": series_ticker,
                "sport": None,               # NULL = non-sports
                "market_type": "binary",
                "category": category,
                "player_name": None,
                "stat_type": None,
                "line": None,
                "team1": None,
                "team2": None,
                "market_title": title,
                "player_id": None,
                "yes_price": yes_price_cents,
                "no_price": 100 - yes_price_cents,
                "yes_bid": yes_bid_cents,
                "yes_ask": yes_ask_cents,
                "volume": volume,
                "open_interest": 0,
                "close_time": market.get("close_time") or market.get("expected_expiration_time"),
                "market_status": "open",
            })
            series_count += 1

        if series_count:
            logger.info(f"  {series_ticker} ({desc}): {series_count} markets")
        stats["by_series"][series_ticker] = series_count

    stats["raw"] = sum(len(client.list_all_markets(s)) for s in [])  # already counted above
    stats["parsed"] = len(parsed_markets)

    if dry_run:
        logger.info("=== DRY RUN — non-sports markets ===")
        for m in parsed_markets:
            mid_p = kalshi_mid_to_prob(m.get("yes_bid", 0), m.get("yes_ask", 0))
            logger.info(
                f"  [{m.get('category','?'):12s}] {m.get('series_ticker',''):10s} | "
                f"YES={m.get('yes_price', 0):3d}c | mid={mid_p:.1%} | "
                f"vol={m.get('volume', 0):6d} | {m.get('market_title', '')[:60]}"
            )
    else:
        engine = get_engine()
        stored = store_markets(engine, parsed_markets, snapshot_time)
        stats["stored"] = stored
        logger.info(f"Stored {stored} non-sports Kalshi markets")

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
