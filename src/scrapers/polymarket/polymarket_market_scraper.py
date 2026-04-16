"""
Polymarket Market Scraper
=========================
Discovers prediction markets on Polymarket, parses questions,
and stores snapshots in polymarket_markets.

Two modes:
  sport  — Fetch events for a specific sport tag (e.g. mlb, nba).
  all    — Fetch ALL active events across every category (sports + politics +
           crypto + economics + culture + etc.). This is the primary mode for
           broad arbitrage scanning.

Steps:
  1. Discover — Fetch active events (filtered by tag, or all)
  2. Parse    — Classify market type and category, extract game/player info
  3. Price    — Batch fetch midpoints + orderbooks via CLOB API
  4. Store    — Upsert into polymarket_markets

Usage:
    python -m src.scrapers.polymarket.polymarket_market_scraper --all --dry-run
    python -m src.scrapers.polymarket.polymarket_market_scraper --sport mlb
    python -m src.scrapers.polymarket.polymarket_market_scraper --sport nba --dry-run
"""

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.scrapers.kalshi.kalshi_market_scraper import build_player_cache, link_player
from src.scrapers.polymarket.polymarket_client import PolymarketClient
from src.scrapers.polymarket.polymarket_utils import (
    POLYMARKET_SPORT_TAGS,
    detect_category,
    detect_market_type,
    parse_game_market,
    parse_player_prop,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PolymarketMarketScraper")


# ---------------------------------------------------------------------------
# Market Parsing
# ---------------------------------------------------------------------------


def _parse_event_markets(event: dict, category: str, sport: str | None) -> list[dict]:
    """Parse all markets from a Polymarket event dict.

    Args:
        event: Raw event dict from Gamma API.
        category: Detected category ('sports', 'politics', 'crypto', etc.).
        sport: Detected sport key ('nba', 'mlb', etc.) or None.

    Returns:
        List of parsed market dicts.
    """
    parsed = []
    markets = event.get("markets", [])
    event_slug = event.get("slug", "")
    end_date = event.get("endDate") or event.get("end_date")
    is_sports = category == "sports"

    for mkt in markets:
        question = mkt.get("question", "") or mkt.get("title", "")
        if not question:
            continue

        description = mkt.get("description", "") or ""
        market_type = detect_market_type(question, description, is_sports=is_sports)

        # Extract token IDs for CLOB pricing
        # clobTokenIds comes back as a JSON string from the Gamma API, not a parsed list
        raw_tokens = mkt.get("tokens") or mkt.get("clobTokenIds") or []
        if isinstance(raw_tokens, str):
            try:
                raw_tokens = json.loads(raw_tokens)
            except (ValueError, json.JSONDecodeError):
                raw_tokens = []
        tokens = raw_tokens
        token_id_yes = None
        token_id_no = None

        if isinstance(tokens, list):
            for tok in tokens:
                if isinstance(tok, dict):
                    outcome = (tok.get("outcome") or "").upper()
                    if outcome == "YES":
                        token_id_yes = tok.get("token_id") or tok.get("tokenId")
                    elif outcome == "NO":
                        token_id_no = tok.get("token_id") or tok.get("tokenId")
                elif isinstance(tok, str):
                    if token_id_yes is None:
                        token_id_yes = tok
                    elif token_id_no is None:
                        token_id_no = tok

        condition_id = mkt.get("conditionId") or mkt.get("condition_id") or mkt.get("id", "")

        # Prices: Gamma API returns outcomePrices as a JSON string ["yes_price", "no_price"]
        # on a 0-1 scale. Convert to cents (0-100).
        yes_price = None
        no_price = None
        yes_ask = None
        raw_prices = mkt.get("outcomePrices")
        if raw_prices:
            try:
                prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                if len(prices) >= 2:
                    yes_price = round(float(prices[0]) * 100, 2)
                    no_price = round(float(prices[1]) * 100, 2)
            except (ValueError, TypeError, json.JSONDecodeError):
                pass
        # bestAsk gives the ask for YES token (0-1 scale)
        best_ask_raw = mkt.get("bestAsk")
        if best_ask_raw is not None:
            try:
                yes_ask = round(float(best_ask_raw) * 100, 2)
            except (ValueError, TypeError):
                pass
        # Derive yes_bid from spread if available
        yes_bid = None
        spread_raw = mkt.get("spread")
        if yes_ask is not None and spread_raw is not None:
            try:
                yes_bid = round(yes_ask - float(spread_raw) * 100, 2)
            except (ValueError, TypeError):
                pass

        record: dict = {
            "condition_id": condition_id,
            "token_id_yes": token_id_yes,
            "token_id_no": token_id_no,
            "event_slug": event_slug,
            "sport": sport,
            "category": category,
            "market_type": market_type,
            "question": question[:500],
            "end_date": end_date,
            "player_name": None,
            "stat_type": None,
            "line": None,
            "player_id": None,
            "team1": None,
            "team2": None,
            "yes_price": yes_price,
            "no_price": no_price,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "volume": _safe_float(mkt.get("volume")),
            "liquidity": _safe_float(mkt.get("liquidity")),
            "market_status": "open" if mkt.get("active", True) else "closed",
        }

        # Parse sports-specific fields
        if is_sports:
            if market_type == "player_prop":
                result = parse_player_prop(question)
                if result:
                    player_name, stat_type, line = result
                    record["player_name"] = player_name
                    record["stat_type"] = stat_type
                    record["line"] = line
            elif market_type in ("moneyline", "spread", "total", "nrfi", "season_future"):
                game_info = parse_game_market(question)
                if game_info:
                    record["team1"] = game_info.get("team1")
                    record["team2"] = game_info.get("team2")
                    record["line"] = game_info.get("line")

        parsed.append(record)

    return parsed


def _safe_float(val) -> float | None:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Pricing (batch CLOB calls)
# ---------------------------------------------------------------------------


def _apply_prices(
    parsed_markets: list[dict],
    client: PolymarketClient,  # noqa: ARG001 — kept for future orderbook enrichment
) -> None:
    """Count priced markets. Prices are already extracted from Gamma API outcomePrices.

    The Polymarket Gamma API returns outcomePrices directly in the market dict,
    so no CLOB API call is needed for basic YES/NO prices. This function exists
    as a hook for future orderbook enrichment (bid/ask depth) via the CLOB API.

    Args:
        parsed_markets: List of parsed market dicts (prices already populated).
        client: PolymarketClient instance (reserved for future orderbook calls).
    """
    priced = sum(1 for m in parsed_markets if m.get("yes_price") is not None)
    logger.info(f"Prices available for {priced}/{len(parsed_markets)} markets (from Gamma API)")


# ---------------------------------------------------------------------------
# DB Storage
# ---------------------------------------------------------------------------


def store_markets(engine, parsed_markets: list[dict], snapshot_time: datetime) -> int:
    """Batch upsert parsed Polymarket markets into polymarket_markets.

    Args:
        engine: SQLAlchemy engine.
        parsed_markets: List of parsed market dicts.
        snapshot_time: Snapshot timestamp.

    Returns:
        Number of rows upserted.
    """
    if not parsed_markets:
        return 0

    stmt = text("""
        INSERT INTO polymarket_markets (
            condition_id, token_id_yes, token_id_no, event_slug, sport, category,
            market_type, player_name, stat_type, line, question, player_id,
            team1, team2, yes_price, no_price, yes_bid, yes_ask,
            volume, liquidity, market_status, end_date, snapshot_time
        ) VALUES (
            :condition_id, :token_id_yes, :token_id_no, :event_slug, :sport, :category,
            :market_type, :player_name, :stat_type, :line, :question, :player_id,
            :team1, :team2, :yes_price, :no_price, :yes_bid, :yes_ask,
            :volume, :liquidity, :market_status, :end_date, :snapshot_time
        )
        ON CONFLICT (condition_id)
        DO UPDATE SET
            yes_price = EXCLUDED.yes_price,
            no_price = EXCLUDED.no_price,
            yes_bid = EXCLUDED.yes_bid,
            yes_ask = EXCLUDED.yes_ask,
            volume = EXCLUDED.volume,
            liquidity = EXCLUDED.liquidity,
            market_status = EXCLUDED.market_status,
            player_id = EXCLUDED.player_id,
            category = EXCLUDED.category,
            sport = EXCLUDED.sport,
            team1 = EXCLUDED.team1,
            team2 = EXCLUDED.team2,
            snapshot_time = EXCLUDED.snapshot_time
    """)

    rows = [
        {
            "condition_id": m.get("condition_id", ""),
            "token_id_yes": m.get("token_id_yes"),
            "token_id_no": m.get("token_id_no"),
            "event_slug": m.get("event_slug", ""),
            "sport": m.get("sport"),
            "category": m.get("category", "other"),
            "market_type": m.get("market_type", "binary"),
            "player_name": m.get("player_name"),
            "stat_type": m.get("stat_type"),
            "line": m.get("line"),
            "question": m.get("question", ""),
            "player_id": m.get("player_id"),
            "team1": m.get("team1"),
            "team2": m.get("team2"),
            "yes_price": m.get("yes_price"),
            "no_price": m.get("no_price"),
            "yes_bid": m.get("yes_bid"),
            "yes_ask": m.get("yes_ask"),
            "volume": m.get("volume"),
            "liquidity": m.get("liquidity"),
            "market_status": m.get("market_status", "open"),
            "end_date": m.get("end_date"),
            "snapshot_time": snapshot_time,
        }
        for m in parsed_markets
    ]

    # Batch upsert in chunks of 500 within a single connection.
    # Opening one connection for all chunks avoids 142 Supavisor round-trips
    # (was causing ~24 min runtime; single connection reduces this to ~2-3 min).
    CHUNK = 500
    count = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i:i + CHUNK]
            conn.execute(stmt, chunk)
            count += len(chunk)

    return count


# ---------------------------------------------------------------------------
# Main Scrape Flow
# ---------------------------------------------------------------------------


def scrape_and_store(
    sport: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Full scrape cycle: discover -> parse -> price -> link -> store.

    Args:
        sport: Target sport ('nba', 'mlb', etc.) or None for all categories.
               When None, fetches all active Polymarket events regardless of
               category and classifies them automatically.
        dry_run: Print parsed markets without DB writes.

    Returns:
        Summary dict with counts.
    """
    snapshot_time = datetime.now(UTC)
    stats: dict = {
        "events": 0, "raw_markets": 0, "parsed": 0, "priced": 0,
        "linked": 0, "stored": 0,
        "by_category": {},
        "by_market_type": {},
    }

    client = PolymarketClient()

    # Step 1: Discover events
    if sport is not None:
        tag_id = POLYMARKET_SPORT_TAGS.get(sport)
        if tag_id is None:
            logger.error(f"No sport tag ID for: {sport}")
            return stats
        logger.info(f"Step 1: Fetching {sport.upper()} events (tag_id={tag_id})...")
        events = client.get_events(tag_id=tag_id, active=True, closed=False)
    else:
        logger.info("Step 1: Fetching ALL Polymarket events (all categories)...")
        events = client.get_events(tag_id=None, active=True, closed=False)

    stats["events"] = len(events)
    logger.info(f"Found {len(events)} active events")

    if not events:
        logger.info("No events returned — done")
        return stats

    # Step 2: Parse markets from events
    logger.info("Step 2: Parsing markets...")
    all_parsed: list[dict] = []
    for event in events:
        if sport is not None:
            event_cat, event_sport = "sports", sport
        else:
            event_cat, event_sport = detect_category(event)
        parsed = _parse_event_markets(event, event_cat, event_sport)
        all_parsed.extend(parsed)

    stats["raw_markets"] = len(all_parsed)
    all_parsed = [m for m in all_parsed if m.get("condition_id")]
    stats["parsed"] = len(all_parsed)

    # Tally by category and market_type
    for m in all_parsed:
        c = m.get("category", "other")
        mt = m.get("market_type", "unknown")
        stats["by_category"][c] = stats["by_category"].get(c, 0) + 1
        stats["by_market_type"][mt] = stats["by_market_type"].get(mt, 0) + 1

    logger.info(f"Parsed {len(all_parsed)} markets from {len(events)} events")
    if stats["by_category"]:
        logger.info(f"  By category: {stats['by_category']}")
    if stats["by_market_type"]:
        logger.info(f"  By market type: {stats['by_market_type']}")

    if not all_parsed:
        return stats

    # Step 3: Fetch prices
    logger.info("Step 3: Fetching prices from CLOB API...")
    _apply_prices(all_parsed, client)
    stats["priced"] = sum(1 for m in all_parsed if m.get("yes_price") is not None)

    # Step 4: Link players (sports player props only)
    prop_markets = [m for m in all_parsed if m.get("market_type") == "player_prop" and m.get("player_name")]
    if prop_markets:
        logger.info(f"Step 4: Linking {len(prop_markets)} player prop markets...")
        try:
            engine = get_engine()
            # Group by sport for efficient cache building
            sports_with_props: set[str] = {m["sport"] for m in prop_markets if m.get("sport")}
            for sp in sports_with_props:
                player_cache = build_player_cache(engine, sp)
                fuzzy_cache: dict[str, int | None] = {}
                for m in prop_markets:
                    if m.get("sport") == sp:
                        pid = link_player(m["player_name"], player_cache, fuzzy_cache)
                        if pid:
                            m["player_id"] = pid
                            stats["linked"] += 1
        except Exception as e:
            logger.warning(f"Player linking failed (non-fatal): {e}")
            engine = None  # type: ignore[assignment]
    else:
        logger.info("Step 4: No player props to link")
        engine = None  # type: ignore[assignment]

    logger.info(f"Linked {stats['linked']} players")

    # Step 5: Print or store
    if dry_run:
        logger.info("=== DRY RUN — parsed markets ===")
        for cat, cnt in sorted(stats["by_category"].items()):
            logger.info(f"  [{cat:12s}] {cnt} markets")
        logger.info("  --- sample (first 20) ---")
        for m in all_parsed[:20]:
            logger.info(
                f"  [{m['category']:10s}|{m['market_type']:14s}] "
                f"sport={m.get('sport') or '-':5s} | "
                f"YES={m.get('yes_price') or '?':>6} | "
                f"{m['question'][:60]}"
            )
    else:
        if engine is None:
            engine = get_engine()
        stored = store_markets(engine, all_parsed, snapshot_time)
        stats["stored"] = stored
        logger.info(f"Stored {stored} markets in polymarket_markets")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Polymarket sports prediction markets")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--sport", type=str, choices=["nba", "mlb", "nfl", "nhl", "ncaab", "soccer"],
                       help="Scrape a specific sport only")
    group.add_argument("--all", action="store_true", dest="all_categories",
                       help="Scrape all categories (sports + politics + crypto + etc.)")
    parser.add_argument("--dry-run", action="store_true", help="Print parsed markets without DB writes")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.all_categories:
        mode_label = "ALL CATEGORIES"
        sport_arg = None
    elif args.sport:
        mode_label = args.sport.upper()
        sport_arg = args.sport
    else:
        # Default: all categories
        mode_label = "ALL CATEGORIES (default)"
        sport_arg = None

    logger.info("=" * 60)
    logger.info("Polymarket Market Scraper")
    logger.info(f"  Mode:    {mode_label}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("=" * 60)

    stats = scrape_and_store(sport=sport_arg, dry_run=args.dry_run)

    logger.info("=" * 60)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"  Events:      {stats['events']}")
    logger.info(f"  Parsed:      {stats['parsed']}")
    logger.info(f"  Priced:      {stats['priced']}")
    logger.info(f"  Linked:      {stats['linked']}")
    logger.info(f"  Stored:      {stats['stored']}")
    if stats.get("by_category"):
        logger.info(f"  Categories:  {stats['by_category']}")
    if stats.get("by_market_type"):
        logger.info(f"  Mkt types:   {stats['by_market_type']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
