"""
MLB Statcast Backfill
=====================
Orchestrates bulk Statcast data backfill across multiple seasons.
Iterates day-by-day through MLB seasons (late March - October),
with a progress file for resume capability.

Rate limits ~1 req/sec per pybaseball recommendations.

Usage:
    python -m src.scrapers.mlb.mlb_statcast_backfill --seasons 2024 2025
    python -m src.scrapers.mlb.mlb_statcast_backfill --start-date 2025-04-01 --end-date 2025-06-30
    python -m src.scrapers.mlb.mlb_statcast_backfill --seasons 2024 --no-resume
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.scrapers.mlb.mlb_statcast_scraper import MLBStatcastScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBStatcastBackfill")

PROGRESS_FILE = Path(__file__).parent / "mlb_statcast_backfill_progress.json"

# MLB regular season approximate date ranges
SEASON_RANGES = {
    2022: ("2022-04-07", "2022-10-05"),
    2023: ("2023-03-30", "2023-10-01"),
    2024: ("2024-03-20", "2024-09-29"),
    2025: ("2025-03-27", "2025-09-28"),
    2026: ("2026-03-26", "2026-09-27"),
}

RATE_LIMIT_DELAY = 1.0  # seconds between day fetches


def load_progress() -> set[str]:
    """Load set of already-processed dates from progress file."""
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            return set(data.get("completed_dates", []))
        except (json.JSONDecodeError, KeyError):
            return set()
    return set()


def save_progress(completed: set[str]):
    """Save processed dates to progress file."""
    PROGRESS_FILE.write_text(json.dumps({
        "completed_dates": sorted(completed),
        "last_updated": datetime.now().isoformat(),
    }, indent=2))


def get_date_range(seasons: list[int] | None = None,
                   start_date: str | None = None,
                   end_date: str | None = None) -> list[str]:
    """Build list of dates to process."""
    dates = []

    if start_date and end_date:
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif seasons:
        for season in sorted(seasons):
            if season in SEASON_RANGES:
                s, e = SEASON_RANGES[season]
            else:
                # Fallback: April-September
                s = f"{season}-03-25"
                e = f"{season}-10-05"
            current = datetime.strptime(s, "%Y-%m-%d")
            end = datetime.strptime(e, "%Y-%m-%d")
            while current <= end:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)

    return dates


def run_backfill(seasons: list[int] | None = None,
                 start_date: str | None = None,
                 end_date: str | None = None,
                 no_resume: bool = False):
    """Run Statcast backfill for the specified date range."""
    engine = get_engine()
    scraper = MLBStatcastScraper(engine)

    all_dates = get_date_range(seasons, start_date, end_date)
    if not all_dates:
        print("ERROR: No dates to process. Specify --seasons or --start-date/--end-date")
        sys.exit(1)

    # Filter out already-completed dates
    completed = set() if no_resume else load_progress()
    remaining = [d for d in all_dates if d not in completed]

    print("=" * 60)
    print("MLB STATCAST BACKFILL")
    print("=" * 60)
    print(f"Total dates in range: {len(all_dates)}")
    print(f"Already completed: {len(completed & set(all_dates))}")
    print(f"Remaining: {len(remaining)}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if not remaining:
        print("All dates already processed! Use --no-resume to reprocess.")
        return

    total_batting = 0
    total_pitching = 0
    errors = 0

    for date_str in tqdm(remaining, desc="Statcast backfill", unit="day"):
        try:
            result = scraper.scrape_day(date_str)
            total_batting += result["batting_rows"]
            total_pitching += result["pitching_rows"]
            completed.add(date_str)

            # Save progress every 10 days
            if len(completed) % 10 == 0:
                save_progress(completed)

        except Exception as e:
            logger.error(f"Error processing {date_str}: {e}")
            errors += 1

        time.sleep(RATE_LIMIT_DELAY)

    # Final save
    save_progress(completed)

    print(f"\n{'='*60}")
    print("BACKFILL SUMMARY")
    print(f"{'='*60}")
    print(f"Days processed: {len(remaining) - errors}")
    print(f"Errors: {errors}")
    print(f"Total batting rows: {total_batting}")
    print(f"Total pitching rows: {total_pitching}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    parser = argparse.ArgumentParser(description="MLB Statcast Backfill")
    parser.add_argument(
        "--seasons", nargs="+", type=int,
        help="Seasons to backfill (e.g. 2024 2025)",
    )
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore progress file and reprocess all dates")

    args = parser.parse_args()

    if not args.seasons and not (args.start_date and args.end_date):
        print("ERROR: Specify --seasons or --start-date/--end-date")
        sys.exit(1)

    run_backfill(
        seasons=args.seasons,
        start_date=args.start_date,
        end_date=args.end_date,
        no_resume=args.no_resume,
    )


if __name__ == "__main__":
    main()
