"""
Polymarket Market Scraper
=========================
Discovers sports prediction markets on Polymarket, parses questions,
links to our player database, and stores snapshots in polymarket_markets.

Supported sports: NBA, MLB (extensible via POLYMARKET_SPORT_TAGS)

Steps:
  1. Discover — Fetch sport tag ID, then all active events for that sport
  2. Parse — Classify market type, extract player/game info, collect token IDs
  3. Price — Batch fetch midpoints + orderbooks via CLOB API
  4. Link & Store — Fuzzy match players, upsert into polymarket_markets

Usage:
    python -m src.scrapers.polymarket.polymarket_market_scraper --sport nba --dry-run
    python -m src.scrapers.polymarket.polymarket_market_scraper --sport mlb
"""

import argparse
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


def _parse_event_markets(event: dict, sport: str) -> list[dict]:
    """Parse all markets from a Polymarket event dict.

    Args:
        event: Raw event dict from Gamma API.
        sport: Internal sport key ('nba', 'mlb').

    Returns:
        List of parsed market dicts.
    """
    parsed = []
    markets = event.get("markets", [])
    event_slug = event.get("slug", "")
    end_date = event.get("endDate") or event.get("end_date")

    for mkt in markets:
        question = mkt.get("question", "") or mkt.get("title", "")
        if not question:
            continue

        description = mkt.get("description", "") or ""
        market_type = detect_market_type(question, description)

        # Extract token IDs for CLOB pricing
        tokens = mkt.get("tokens") or mkt.get("clobTokenIds") or []
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
                    # Some events have token IDs as plain strings [yes_id, no_id]
                    if token_id_yes is None:
                        token_id_yes = tok
                    elif token_id_no is None:
                        token_id_no = tok

        condition_id = mkt.get("conditionId") or mkt.get("condition_id") or mkt.get("id", "")

        record: dict = {
            "condition_id": condition_id,
            "token_id_yes": token_id_yes,
            "token_id_no": token_id_no,
            "event_slug": event_slug,
            "sport": sport,
            "market_type": market_type,
            "question": question[:500],
            "end_date": end_date,
            "player_name": None,
            "stat_type": None,
            "line": None,
            "player_id": None,
            "team1": None,
            "team2": None,
            "yes_price": None,
            "no_price": None,
            "yes_bid": None,
            "yes_ask": None,
            "volume": _safe_float(mkt.get("volume")),
            "liquidity": _safe_float(mkt.get("liquidity")),
            "market_status": "open" if mkt.get("active", True) else "closed",
        }

        # Parse player prop
        if market_type == "player_prop":
            result = parse_player_prop(question)
            if result:
                player_name, stat_type, line = result
                record["player_name"] = player_name
                record["stat_type"] = stat_type
                record["line"] = line

        # Parse game-level market
        elif market_type in ("moneyline", "spread", "total"):
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
    client: PolymarketClient,
) -> None:
    """Batch fetch prices for all markets and apply them in-place.

    Fetches midpoints for fast price discovery, then orderbooks for
    bid/ask depth on markets that have both YES and NO tokens.

    Args:
        parsed_markets: List of parsed market dicts (mutated in place).
        client: PolymarketClient instance.
    """
    # Collect all token IDs
    all_token_ids: list[str] = []
    for m in parsed_markets:
        if m.get("token_id_yes"):
            all_token_ids.append(m["token_id_yes"])
        if m.get("token_id_no"):
            all_token_ids.append(m["token_id_no"])

    if not all_token_ids:
        logger.info("No token IDs found — skipping price fetch")
        return

    # Batch fetch midpoints (0.0 to 1.0 scale)
    logger.info(f"Fetching midpoints for {len(all_token_ids)} tokens...")
    midpoints = client.get_batch_midpoints(all_token_ids)

    # Batch fetch orderbooks for bid/ask
    logger.info(f"Fetching orderbooks for {len(all_token_ids)} tokens...")
    orderbooks = client.get_batch_orderbooks(all_token_ids)

    # Apply prices to each market
    for m in parsed_markets:
        yes_tid = m.get("token_id_yes")
        no_tid = m.get("token_id_no")

        # Midpoint prices (convert 0-1 to 0-100 cents)
        if yes_tid and yes_tid in midpoints:
            m["yes_price"] = round(midpoints[yes_tid] * 100, 2)
        if no_tid and no_tid in midpoints:
            m["no_price"] = round(midpoints[no_tid] * 100, 2)

        # If only one token has a midpoint, derive the other
        if m["yes_price"] is not None and m["no_price"] is None:
            m["no_price"] = round(100 - m["yes_price"], 2)
        elif m["no_price"] is not None and m["yes_price"] is None:
            m["yes_price"] = round(100 - m["no_price"], 2)

        # Bid/Ask from orderbook
        if yes_tid and yes_tid in orderbooks:
            ob = orderbooks[yes_tid]
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            if bids:
                best_bid = bids[0] if isinstance(bids[0], int | float) else bids[0].get("price", 0)
                m["yes_bid"] = round(float(best_bid) * 100, 2)
            if asks:
                best_ask = asks[0] if isinstance(asks[0], int | float) else asks[0].get("price", 0)
                m["yes_ask"] = round(float(best_ask) * 100, 2)

    logger.info(f"Applied prices to {sum(1 for m in parsed_markets if m.get('yes_price') is not None)} markets")


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
            condition_id, token_id_yes, token_id_no, event_slug, sport,
            market_type, player_name, stat_type, line, question, player_id,
            team1, team2, yes_price, no_price, yes_bid, yes_ask,
            volume, liquidity, market_status, end_date, snapshot_time
        ) VALUES (
            :condition_id, :token_id_yes, :token_id_no, :event_slug, :sport,
            :market_type, :player_name, :stat_type, :line, :question, :player_id,
            :team1, :team2, :yes_price, :no_price, :yes_bid, :yes_ask,
            :volume, :liquidity, :market_status, :end_date, :snapshot_time
        )
        ON CONFLICT (condition_id, snapshot_time)
        DO UPDATE SET
            yes_price = EXCLUDED.yes_price,
            no_price = EXCLUDED.no_price,
            yes_bid = EXCLUDED.yes_bid,
            yes_ask = EXCLUDED.yes_ask,
            volume = EXCLUDED.volume,
            liquidity = EXCLUDED.liquidity,
            market_status = EXCLUDED.market_status,
            player_id = EXCLUDED.player_id
    """)

    count = 0
    with engine.begin() as conn:
        for m in parsed_markets:
            conn.execute(stmt, {
                "condition_id": m.get("condition_id", ""),
                "token_id_yes": m.get("token_id_yes"),
                "token_id_no": m.get("token_id_no"),
                "event_slug": m.get("event_slug", ""),
                "sport": m.get("sport", ""),
                "market_type": m.get("market_type", ""),
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
            })
            count += 1

    return count


# ---------------------------------------------------------------------------
# Main Scrape Flow
# ---------------------------------------------------------------------------


def scrape_and_store(
    sport: str = "nba",
    dry_run: bool = False,
) -> dict:
    """Full scrape cycle: discover -> parse -> price -> link -> store.

    Args:
        sport: Target sport ('nba' or 'mlb').
        dry_run: Print parsed markets without DB writes.

    Returns:
        Summary dict with counts.
    """
    snapshot_time = datetime.now(UTC)
    stats = {"events": 0, "raw_markets": 0, "parsed": 0, "priced": 0, "linked": 0, "stored": 0}

    tag_id = POLYMARKET_SPORT_TAGS.get(sport)
    if tag_id is None:
        logger.error(f"No sport tag ID for: {sport}")
        return stats

    client = PolymarketClient()

    # Step 1: Discover events
    logger.info(f"Step 1: Fetching {sport.upper()} events (tag_id={tag_id})...")
    events = client.get_events(tag_id=tag_id, active=True, closed=False)
    stats["events"] = len(events)
    logger.info(f"Found {len(events)} active events")

    # Step 2: Parse markets from events
    logger.info("Step 2: Parsing markets...")
    all_parsed: list[dict] = []
    for event in events:
        parsed = _parse_event_markets(event, sport)
        all_parsed.extend(parsed)

    stats["raw_markets"] = len(all_parsed)
    # Filter to only markets with a condition_id
    all_parsed = [m for m in all_parsed if m.get("condition_id")]
    stats["parsed"] = len(all_parsed)
    logger.info(f"Parsed {len(all_parsed)} markets from {len(events)} events")

    if not all_parsed:
        logger.info("No markets to process — done")
        return stats

    # Step 3: Fetch prices
    logger.info("Step 3: Fetching prices from CLOB API...")
    _apply_prices(all_parsed, client)
    stats["priced"] = sum(1 for m in all_parsed if m.get("yes_price") is not None)

    # Step 4: Link players
    logger.info("Step 4: Linking players...")
    try:
        engine = get_engine()
        player_cache = build_player_cache(engine, sport)
        fuzzy_cache: dict[str, int | None] = {}

        for m in all_parsed:
            if m.get("player_name") and m["market_type"] == "player_prop":
                pid = link_player(m["player_name"], player_cache, fuzzy_cache)
                if pid:
                    m["player_id"] = pid
                    stats["linked"] += 1
    except Exception as e:
        logger.warning(f"Player linking failed (non-fatal): {e}")
        engine = None  # type: ignore[assignment]

    logger.info(f"Linked {stats['linked']} players")

    # Step 5: Print or store
    if dry_run:
        logger.info("=== DRY RUN — parsed markets ===")
        prop_count = sum(1 for m in all_parsed if m["market_type"] == "player_prop")
        ml_count = sum(1 for m in all_parsed if m["market_type"] == "moneyline")
        spread_count = sum(1 for m in all_parsed if m["market_type"] == "spread")
        total_count = sum(1 for m in all_parsed if m["market_type"] == "total")
        logger.info(f"  Player props: {prop_count}")
        logger.info(f"  Moneyline: {ml_count}")
        logger.info(f"  Spread: {spread_count}")
        logger.info(f"  Total: {total_count}")
        for m in all_parsed[:20]:
            logger.info(
                f"  [{m['market_type']:12s}] {m.get('player_name') or m.get('team1') or 'N/A':25s} | "
                f"YES={m.get('yes_price') or '?':>6} | "
                f"pid={'linked' if m.get('player_id') else 'unlinked':8s} | "
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
    parser.add_argument("--sport", type=str, default="nba", choices=["nba", "mlb"])
    parser.add_argument("--dry-run", action="store_true", help="Print parsed markets without DB writes")
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Polymarket Market Scraper")
    logger.info(f"  Sport: {args.sport.upper()}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("=" * 60)

    stats = scrape_and_store(sport=args.sport, dry_run=args.dry_run)

    logger.info("=" * 60)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"  Events: {stats['events']}")
    logger.info(f"  Raw markets: {stats['raw_markets']}")
    logger.info(f"  Parsed: {stats['parsed']}")
    logger.info(f"  Priced: {stats['priced']}")
    logger.info(f"  Linked: {stats['linked']}")
    logger.info(f"  Stored: {stats['stored']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
