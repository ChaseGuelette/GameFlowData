"""
Kalshi Refresh Job
==================
Orchestrates the full Kalshi pipeline:
  1. Scrape markets (or skip if no credentials)
  2. Fetch orderbook snapshots
  3. Compute edges against MC samples
  4. Send Discord alert for high-edge markets

Follows the edge_refresh_job.py pattern: CLI-driven, subprocess-friendly,
graceful degradation when credentials are missing.

Usage:
    python src/orchestration/kalshi_refresh_job.py
    python src/orchestration/kalshi_refresh_job.py --mock --dry-run
    python src/orchestration/kalshi_refresh_job.py --sport nba --date 2026-03-31
    python src/orchestration/kalshi_refresh_job.py --skip-discord
"""

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KalshiRefreshJob")


def run(
    target_date: date,
    sport: str = "nba",
    dry_run: bool = False,
    mock: bool = False,
    skip_discord: bool = False,
) -> dict:
    """Run the full Kalshi refresh pipeline.

    Args:
        target_date: Date to process.
        sport: Target sport ("nba" or "mlb").
        dry_run: Print results without DB writes.
        mock: Use synthetic data instead of API.
        skip_discord: Skip Discord alerts.

    Returns:
        Summary dict.
    """
    summary = {"scrape": {}, "edges": {}, "alerts_sent": False}

    # Step 1: Scrape markets
    logger.info("Step 1: Scraping Kalshi markets...")
    try:
        from src.scrapers.kalshi.kalshi_market_scraper import scrape_and_store

        scrape_stats = scrape_and_store(sport=sport, dry_run=dry_run, mock=mock)
        summary["scrape"] = scrape_stats
        logger.info(f"Scrape: {scrape_stats.get('parsed', 0)} parsed, {scrape_stats.get('stored', 0)} stored")
    except Exception as e:
        logger.error(f"Market scrape failed: {e}", exc_info=True)
        summary["scrape"] = {"error": str(e)}

    # Step 2: Fetch orderbook snapshots (skip in dry-run/mock)
    if not dry_run and not mock:
        logger.info("Step 2: Fetching orderbook snapshots...")
        try:
            _fetch_orderbooks(target_date, sport)
        except Exception as e:
            logger.warning(f"Orderbook fetch failed (non-fatal): {e}")
    else:
        logger.info("Step 2: Skipping orderbook fetch (dry-run/mock)")

    # Step 3: Compute edges
    if not dry_run:
        logger.info("Step 3: Computing edges...")
        try:
            from src.models.kalshi_edge import KalshiEdgeCalculator

            calc = KalshiEdgeCalculator()
            edge_stats = calc.compute_edges(target_date, sport=sport)
            summary["edges"] = edge_stats
            logger.info(
                f"Edges: {edge_stats.get('matched', 0)} matched, "
                f"{edge_stats.get('updated', 0)} updated"
            )
        except Exception as e:
            logger.error(f"Edge computation failed: {e}", exc_info=True)
            summary["edges"] = {"error": str(e)}
    else:
        logger.info("Step 3: Skipping edge computation (dry-run)")

    # Step 4: Discord alerts
    if not skip_discord and not dry_run:
        logger.info("Step 4: Checking for high-edge markets...")
        try:
            alerts_sent = _send_high_edge_alerts(target_date, sport)
            summary["alerts_sent"] = alerts_sent
        except Exception as e:
            logger.warning(f"Discord alert failed (non-fatal): {e}")
    else:
        logger.info("Step 4: Skipping Discord alerts")

    return summary


def _fetch_orderbooks(target_date: date, sport: str) -> int:
    """Fetch and store orderbook snapshots for open markets.

    Returns:
        Number of orderbooks fetched.
    """
    from datetime import UTC, datetime

    from sqlalchemy import text

    from src.db.client import get_engine
    from src.scrapers.kalshi.kalshi_client import KalshiClient

    client = KalshiClient()
    if not client.is_authenticated:
        logger.info("No Kalshi credentials — skipping orderbook fetch")
        return 0

    engine = get_engine()

    # Get open market tickers
    query = text("""
        SELECT DISTINCT ticker FROM kalshi_markets
        WHERE sport = :sport
          AND snapshot_time::date = :target_date
          AND market_status = 'open'
    """)
    with engine.connect() as conn:
        tickers = [row[0] for row in conn.execute(query, {"sport": sport, "target_date": target_date}).fetchall()]

    if not tickers:
        logger.info("No open markets to fetch orderbooks for")
        return 0

    snapshot_time = datetime.now(UTC)
    insert_stmt = text("""
        INSERT INTO kalshi_orderbook_snapshots (
            ticker, snapshot_time, yes_bid, yes_ask, yes_bid_size, yes_ask_size,
            depth, mid_price, spread, total_bid_depth, total_ask_depth
        ) VALUES (
            :ticker, :snapshot_time, :yes_bid, :yes_ask, :yes_bid_size, :yes_ask_size,
            :depth, :mid_price, :spread, :total_bid_depth, :total_ask_depth
        )
    """)

    count = 0
    with engine.begin() as conn:
        for ticker in tickers:
            result = client.get_orderbook(ticker)
            if not result:
                continue

            ob = result.get("orderbook", {})
            yes_bids = ob.get("yes", [])
            no_bids = ob.get("no", [])

            best_bid = yes_bids[0][0] if yes_bids else 0
            best_ask = (100 - no_bids[0][0]) if no_bids else 100
            best_bid_size = yes_bids[0][1] if yes_bids else 0
            best_ask_size = no_bids[0][1] if no_bids else 0

            mid = (best_bid + best_ask) / 2.0
            spread = best_ask - best_bid
            total_bid_depth = sum(level[1] for level in yes_bids) if yes_bids else 0
            total_ask_depth = sum(level[1] for level in no_bids) if no_bids else 0

            import json
            conn.execute(insert_stmt, {
                "ticker": ticker,
                "snapshot_time": snapshot_time,
                "yes_bid": best_bid,
                "yes_ask": best_ask,
                "yes_bid_size": best_bid_size,
                "yes_ask_size": best_ask_size,
                "depth": json.dumps({"yes": yes_bids, "no": no_bids}),
                "mid_price": mid,
                "spread": spread,
                "total_bid_depth": total_bid_depth,
                "total_ask_depth": total_ask_depth,
            })
            count += 1

    logger.info(f"Fetched {count} orderbook snapshots")
    return count


def _send_high_edge_alerts(target_date: date, sport: str, min_edge: float = 0.05) -> bool:
    """Send Discord alert if any markets have fee-adjusted edge >= threshold.

    Returns:
        True if alert was sent.
    """
    from sqlalchemy import text

    from src.db.client import get_engine

    engine = get_engine()

    query = text("""
        SELECT DISTINCT ON (ticker)
            ticker, player_name, stat_type, line,
            yes_price, bid_ask_spread, volume, open_interest,
            maker_fee_adjusted_edge, close_time, model_prob, kalshi_implied
        FROM kalshi_markets
        WHERE sport = :sport
          AND snapshot_time::date = :target_date
          AND market_status = 'open'
          AND maker_fee_adjusted_edge IS NOT NULL
          AND maker_fee_adjusted_edge >= :min_edge
        ORDER BY ticker, snapshot_time DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {
            "sport": sport, "target_date": target_date, "min_edge": min_edge,
        }).fetchall()

    if not rows:
        logger.info("No high-edge Kalshi markets found")
        return False

    logger.info(f"Found {len(rows)} Kalshi markets with edge >= {min_edge:.0%}")

    try:
        from src.discord_bot.alerts import send_kalshi_alert_sync

        return send_kalshi_alert_sync(
            markets=[dict(row._mapping) for row in rows],
            target_date=target_date,
            sport=sport,
        )
    except Exception as e:
        logger.warning(f"Failed to send Kalshi Discord alert: {e}")
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kalshi market refresh pipeline")
    parser.add_argument("--date", type=str, default=date.today().isoformat(), help="Target date (YYYY-MM-DD)")
    parser.add_argument("--sport", type=str, default="nba", choices=["nba", "mlb"])
    parser.add_argument("--dry-run", action="store_true", help="No DB writes or API calls")
    parser.add_argument("--mock", action="store_true", help="Use synthetic data")
    parser.add_argument("--skip-discord", action="store_true", help="Skip Discord alerts")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = date.fromisoformat(args.date)

    logger.info("=" * 60)
    logger.info("Kalshi Refresh Job")
    logger.info(f"  Date: {target_date}")
    logger.info(f"  Sport: {args.sport.upper()}")
    logger.info(f"  Mock: {args.mock}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info(f"  Skip Discord: {args.skip_discord}")
    logger.info("=" * 60)

    # Check credentials early
    has_creds = bool(os.getenv("KALSHI_API_KEY"))
    if not has_creds and not args.mock:
        logger.info("KALSHI_API_KEY not set — exiting gracefully (use --mock for testing)")
        sys.exit(0)

    summary = run(
        target_date=target_date,
        sport=args.sport,
        dry_run=args.dry_run,
        mock=args.mock,
        skip_discord=args.skip_discord,
    )

    logger.info("=" * 60)
    logger.info("KALSHI REFRESH COMPLETE")
    logger.info(f"  Scrape: {summary.get('scrape', {})}")
    logger.info(f"  Edges: {summary.get('edges', {})}")
    logger.info(f"  Alerts sent: {summary.get('alerts_sent', False)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
