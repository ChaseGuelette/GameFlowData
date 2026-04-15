"""
Arbitrage Scan Job
==================
Orchestrates the Polymarket-Kalshi arbitrage scanning pipeline:
  1. Scrape Polymarket markets (store in polymarket_markets)
  2. Match against Kalshi markets (find cross-platform pairs)
  3. Scan for pure arbs and soft arbs
  4. Send Discord alerts for significant opportunities
  5. Log summary

Runs on Railway every 10 minutes via scheduler.py.
Exits gracefully if Polymarket API is unreachable.

Usage:
    python src/orchestration/arb_scan_job.py
    python src/orchestration/arb_scan_job.py --sport mlb --dry-run
    python src/orchestration/arb_scan_job.py --skip-discord --date 2026-04-12
"""

import argparse
import logging
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
logger = logging.getLogger("ArbScanJob")


def run(
    target_date: date,
    sport: str = "mlb",
    mode: str = "sport",
    dry_run: bool = False,
    skip_discord: bool = False,
    skip_scrape: bool = False,
) -> dict:
    """Run the full arbitrage scanning pipeline.

    Args:
        target_date: Date to scan.
        sport: Target sport ('nba' or 'mlb'). Ignored when mode='all'.
        mode: 'sport' (sport-specific scrape) or 'all' (all Polymarket categories).
        dry_run: No DB writes; print results instead.
        skip_discord: Skip Discord alerts.
        skip_scrape: Skip Polymarket scrape (use existing DB data).

    Returns:
        Summary dict with step results.
    """
    summary: dict = {
        "scrape": {},
        "kalshi_matched": 0,
        "pure_arbs": 0,
        "soft_arbs": 0,
        "alerts_sent": False,
    }

    # Step 1: Scrape Polymarket markets
    if not skip_scrape:
        if mode == "all":
            logger.info("Step 1: Scraping ALL Polymarket categories...")
        else:
            logger.info(f"Step 1: Scraping Polymarket {sport.upper()} markets...")
        try:
            from src.scrapers.polymarket.polymarket_market_scraper import scrape_and_store

            sport_arg = None if mode == "all" else sport
            scrape_stats = scrape_and_store(sport=sport_arg, dry_run=dry_run)
            summary["scrape"] = scrape_stats
            logger.info(
                f"Scrape: {scrape_stats.get('events', 0)} events, "
                f"{scrape_stats.get('parsed', 0)} markets, "
                f"{scrape_stats.get('priced', 0)} priced"
            )
            if scrape_stats.get("by_category"):
                logger.info(f"  Categories: {scrape_stats['by_category']}")
        except Exception as e:
            logger.error(f"Polymarket scrape failed: {e}", exc_info=True)
            summary["scrape"] = {"error": str(e)}
    else:
        logger.info("Step 1: Skipping Polymarket scrape (--skip-scrape)")

    # Steps 2-3: Match Kalshi ↔ Polymarket and scan for arbs
    logger.info("Steps 2-3: Matching + scanning for arbs...")
    try:
        from src.arbitrage.arb_scanner import ArbScanner

        scanner = ArbScanner()
        result = scanner.scan(target_date=target_date, sport=sport, dry_run=dry_run)

        summary["kalshi_matched"] = result.n_kalshi_matched
        summary["pure_arbs"] = len(result.pure_arbs)
        summary["soft_arbs"] = len(result.soft_arbs)

        logger.info(f"Kalshi matched: {result.n_kalshi_matched}")
        logger.info(
            f"Opportunities: {len(result.pure_arbs)} pure arbs, "
            f"{len(result.soft_arbs)} soft arbs"
        )

        if dry_run:
            _print_opportunities(result)

    except Exception as e:
        logger.error(f"Arb scan failed: {e}", exc_info=True)
        summary["scan_error"] = str(e)
        return summary

    # Step 4: Discord alerts
    if not skip_discord and not dry_run:
        logger.info("Step 4: Sending Discord alerts...")
        try:
            alerts_sent = _send_arb_alerts(result, sport)
            summary["alerts_sent"] = alerts_sent
        except Exception as e:
            logger.warning(f"Discord alert failed (non-fatal): {e}")
    else:
        logger.info("Step 4: Skipping Discord alerts")

    return summary


def _print_opportunities(result) -> None:
    """Print arb opportunities to log (dry-run mode)."""
    if result.pure_arbs:
        logger.info("=== PURE ARBS ===")
        for opp in result.pure_arbs:
            logger.info(
                f"  {opp.player_name} {opp.stat_type} {opp.line} | "
                f"Kalshi {opp.kalshi_side} {opp.kalshi_price}c + "
                f"Poly {opp.poly_side} {opp.poly_price:.0f}c | "
                f"Net margin: {opp.net_margin:.1f}c"
            )

    if result.soft_arbs:
        logger.info(f"=== SOFT ARBS (top 5 of {len(result.soft_arbs)}) ===")
        for opp in result.soft_arbs[:5]:
            logger.info(
                f"  {opp.player_name} {opp.stat_type} {opp.line} | "
                f"Discrepancy: {(opp.price_discrepancy or 0):.1%} | "
                f"Kalshi {opp.kalshi_price}c vs Poly {opp.poly_price:.0f}c"
            )


def _send_arb_alerts(result, sport: str) -> bool:
    """Send Discord alerts for significant opportunities.

    Returns:
        True if any alert was sent.
    """
    from src.discord_bot.alerts import send_arb_alert_sync

    all_opps = []

    # Always alert on pure arbs
    all_opps.extend(result.pure_arbs)

    # Alert on soft arbs with >= 8% discrepancy
    significant_soft = [o for o in result.soft_arbs if (o.price_discrepancy or 0) >= 0.08]
    all_opps.extend(significant_soft[:5])

    if not all_opps:
        logger.info("No significant opportunities to alert on")
        return False

    return send_arb_alert_sync(all_opps, sport=sport)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket-Kalshi arbitrage scan pipeline")
    parser.add_argument("--date", type=str, default=date.today().isoformat(), help="Target date (YYYY-MM-DD)")
    parser.add_argument("--sport", type=str, default="mlb", choices=["nba", "mlb"])
    parser.add_argument("--mode", type=str, default="sport", choices=["sport", "all"],
                        help="'sport' for sport-specific scrape, 'all' for all Polymarket categories")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes, print results")
    parser.add_argument("--skip-discord", action="store_true", help="Skip Discord alerts")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip Polymarket scrape step")
    return parser.parse_args()


def main():
    args = parse_args()
    target_date = date.fromisoformat(args.date)

    logger.info("=" * 60)
    logger.info("Arb Scan Job")
    logger.info(f"  Date:         {target_date}")
    logger.info(f"  Mode:         {args.mode.upper()}")
    if args.mode == "sport":
        logger.info(f"  Sport:        {args.sport.upper()}")
    logger.info(f"  Dry run:      {args.dry_run}")
    logger.info(f"  Skip Discord: {args.skip_discord}")
    logger.info(f"  Skip Scrape:  {args.skip_scrape}")
    logger.info("=" * 60)

    summary = run(
        target_date=target_date,
        sport=args.sport,
        mode=args.mode,
        dry_run=args.dry_run,
        skip_discord=args.skip_discord,
        skip_scrape=args.skip_scrape,
    )

    logger.info("=" * 60)
    logger.info("ARB SCAN COMPLETE")
    logger.info(f"  Scrape:          {summary.get('scrape', {})}")
    logger.info(f"  Kalshi matched:  {summary.get('kalshi_matched', 0)}")
    logger.info(f"  Pure arbs:       {summary.get('pure_arbs', 0)}")
    logger.info(f"  Soft arbs:       {summary.get('soft_arbs', 0)}")
    logger.info(f"  Alerts sent:     {summary.get('alerts_sent', False)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
