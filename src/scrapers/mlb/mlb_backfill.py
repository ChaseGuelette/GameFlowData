"""
MLB Historical Backfill
=======================
Orchestrates full season backfill of MLB game stats using mlb_stats_scraper.
Scrapes schedule and boxscores for specified seasons.

Scope: 2022-2025 seasons (~9,720 games total, ~291K player-game rows)
Runtime: ~2-4 hours per season at rate-limited pace.

Usage:
    python -m src.scrapers.mlb.mlb_backfill                          # All seasons 2022-2025
    python -m src.scrapers.mlb.mlb_backfill --seasons 2024 2025      # Specific seasons
    python -m src.scrapers.mlb.mlb_backfill --seasons 2025 --limit 100  # Limited for testing
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.scrapers.mlb.mlb_stats_scraper import MLBStatsScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBBackfill")

DEFAULT_SEASONS = [2022, 2023, 2024, 2025]


def run_backfill(seasons: list[int], limit: int | None = None, schedule_only: bool = False):
    """Run full backfill for specified seasons."""
    engine = get_engine()
    scraper = MLBStatsScraper(engine)

    print("=" * 60)
    print("MLB HISTORICAL BACKFILL")
    print("=" * 60)
    print(f"Seasons: {seasons}")
    print(f"Boxscore limit per season: {limit or 'unlimited'}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    total_games = 0
    total_processed = 0
    total_failed = 0

    for season in seasons:
        print(f"\n{'='*60}")
        print(f"SEASON {season}")
        print(f"{'='*60}")

        # Step 1: Scrape schedule
        new_games = scraper.scrape_schedule(season, game_type="R")
        total_games += new_games
        print(f"  Schedule: {new_games} new games")

        if schedule_only:
            continue

        # Step 2: Update scores (ensures we know which games are Final)
        scraper.update_scores(season)

        # Step 3: Scrape boxscores for all Final games missing stats
        processed, failed = scraper.scrape_boxscores(limit=limit)
        total_processed += processed
        total_failed += failed
        print(f"  Boxscores: {processed} processed, {failed} failed")

        # Brief pause between seasons
        if season != seasons[-1]:
            logger.info("Pausing 10s between seasons...")
            time.sleep(10)

    print(f"\n{'='*60}")
    print("BACKFILL SUMMARY")
    print(f"{'='*60}")
    print(f"Seasons processed: {len(seasons)}")
    print(f"New games in schedule: {total_games}")
    print(f"Boxscores processed: {total_processed}")
    print(f"Boxscores failed: {total_failed}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    parser = argparse.ArgumentParser(description="MLB Historical Backfill")
    parser.add_argument(
        "--seasons", nargs="+", type=int, default=DEFAULT_SEASONS,
        help=f"Seasons to backfill (default: {DEFAULT_SEASONS})",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit boxscores per season (useful for testing)",
    )
    parser.add_argument(
        "--schedule-only", action="store_true",
        help="Only scrape schedules, skip boxscores",
    )

    args = parser.parse_args()
    run_backfill(args.seasons, limit=args.limit, schedule_only=args.schedule_only)


if __name__ == "__main__":
    main()
