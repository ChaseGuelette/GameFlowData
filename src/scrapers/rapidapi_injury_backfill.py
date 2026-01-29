"""
RapidAPI NBA Injury Reports - Historical Backfill Script

Fetches daily injury reports from the RapidAPI NBA Injuries Reports endpoint
and stores them in the rapidapi_injuries table for historical analysis.

Usage:
    python src/scrapers/rapidapi_injury_backfill.py
    python src/scrapers/rapidapi_injury_backfill.py --start 2024-10-22 --end 2025-01-29
    python src/scrapers/rapidapi_injury_backfill.py --start 2021-10-19 --delay 1.5
    python src/scrapers/rapidapi_injury_backfill.py --dry-run --start 2025-01-01 --end 2025-01-05

Environment:
    RAPIDAPI_KEY - Your RapidAPI key (required)
    DATABASE_URL - PostgreSQL connection string (required)
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.client import get_engine

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RapidAPIInjuryBackfill")

# --- Constants ---

RAPIDAPI_HOST = "nba-injuries-reports.p.rapidapi.com"
RAPIDAPI_BASE_URL = f"https://{RAPIDAPI_HOST}/injuries/nba"

# NBA season openers (regular season start dates)
SEASON_OPENERS = {
    "2021-22": date(2021, 10, 19),
    "2022-23": date(2022, 10, 18),
    "2023-24": date(2023, 10, 24),
    "2024-25": date(2024, 10, 22),
    "2025-26": date(2025, 10, 21),
}

DEFAULT_START = date(2021, 10, 19)  # 2021-22 season opener


# --- Table Setup ---

DDL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS public.rapidapi_injuries (
    id bigserial PRIMARY KEY,
    report_date date NOT NULL,
    team text,
    player text NOT NULL,
    status text NOT NULL,
    reason text,
    report_time text,
    injury_category text,
    injury_detail text,
    scraped_at timestamp with time zone DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rapidapi_inj_date
    ON public.rapidapi_injuries(report_date);
CREATE INDEX IF NOT EXISTS idx_rapidapi_inj_player
    ON public.rapidapi_injuries(player);
CREATE INDEX IF NOT EXISTS idx_rapidapi_inj_team
    ON public.rapidapi_injuries(team);
CREATE INDEX IF NOT EXISTS idx_rapidapi_inj_status
    ON public.rapidapi_injuries(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rapidapi_inj_unique
    ON public.rapidapi_injuries(report_date, COALESCE(team, ''), player, status, COALESCE(reason, ''));
"""

DDL_MIGRATE_TABLE = """
ALTER TABLE public.rapidapi_injuries ALTER COLUMN team DROP NOT NULL;
DROP INDEX IF EXISTS idx_rapidapi_inj_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_rapidapi_inj_unique
    ON public.rapidapi_injuries(report_date, COALESCE(team, ''), player, status, COALESCE(reason, ''));
"""


def ensure_table(engine) -> None:
    """Create the rapidapi_injuries table if it doesn't exist, and migrate if needed."""
    with engine.begin() as conn:
        conn.execute(text(DDL_CREATE_TABLE))
        # Migrate existing table: drop NOT NULL on team, rebuild unique index
        conn.execute(text(DDL_MIGRATE_TABLE))
    logger.info("Ensured rapidapi_injuries table exists.")


# --- Reason Parsing ---

def parse_reason(reason: str | None) -> tuple[str | None, str | None]:
    """
    Parse the `reason` field into (category, detail).

    Examples:
        "Injury/Illness - Right Shoulder; Soreness" -> ("Injury/Illness", "Right Shoulder; Soreness")
        "G League - Two-Way"                        -> ("G League", "Two-Way")
        "Return to Competition Reconditioning"      -> ("Return to Competition Reconditioning", None)
        "Personal Reasons"                          -> ("Personal Reasons", None)
        "Rest"                                      -> ("Rest", None)
        "Team Suspension"                           -> ("Team Suspension", None)
        ""                                          -> (None, None)
    """
    if not reason or not reason.strip():
        return None, None

    reason = reason.strip()

    if " - " in reason:
        parts = reason.split(" - ", 1)
        return parts[0].strip(), parts[1].strip() if len(parts) > 1 else None

    return reason, None


# --- API Client ---

class RapidAPIInjuryClient:
    """HTTP client for the RapidAPI NBA Injuries Reports endpoint."""

    def __init__(self, api_key: str, delay: float = 1.0, max_retries: int = 3):
        self.api_key = api_key
        self.delay = delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": RAPIDAPI_HOST,
        })
        self._last_request_time: float | None = None

    def _rate_limit(self) -> None:
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def fetch_date(self, target_date: date) -> list[dict] | None:
        """
        Fetch injury reports for a single date.

        Returns:
            List of injury record dicts, or None on failure.
        """
        url = f"{RAPIDAPI_BASE_URL}/{target_date.isoformat()}"

        for attempt in range(self.max_retries):
            self._rate_limit()
            try:
                response = self.session.get(url, timeout=15)

                if response.status_code == 429:
                    wait = (2 ** attempt) * 5
                    logger.warning(f"Rate limited (429). Waiting {wait}s before retry.")
                    time.sleep(wait)
                    continue

                response.raise_for_status()

                try:
                    data = response.json()
                except (ValueError, KeyError):
                    logger.warning(f"{target_date}: Non-JSON response: {response.text[:200]}")
                    return []

                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "message" in data:
                    logger.warning(f"{target_date}: API message: {data['message']}")
                    return []
                else:
                    logger.warning(f"{target_date}: Unexpected response format: {type(data)}")
                    return []

            except requests.exceptions.Timeout:
                logger.warning(f"{target_date}: Timeout (attempt {attempt + 1}/{self.max_retries})")
            except requests.exceptions.HTTPError as e:
                logger.error(f"{target_date}: HTTP error: {e}")
                if response.status_code in (401, 403):
                    logger.error("Authentication failed. Check RAPIDAPI_KEY.")
                    return None
                if response.status_code >= 500:
                    wait = (2 ** attempt) * 2
                    logger.warning(f"Server error. Waiting {wait}s before retry.")
                    time.sleep(wait)
                    continue
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"{target_date}: Request failed: {e}")

            if attempt < self.max_retries - 1:
                wait = (2 ** attempt) * self.delay
                time.sleep(wait)

        logger.error(f"{target_date}: Failed after {self.max_retries} attempts.")
        return None


# --- Storage ---

def get_scraped_dates(engine) -> set[date]:
    """Get set of dates already present in the table (for resume support)."""
    query = text("SELECT DISTINCT report_date FROM rapidapi_injuries")
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return {row[0] for row in rows}


def store_records(engine, target_date: date, records: list[dict]) -> int:
    """
    Insert injury records for a date. Uses ON CONFLICT DO NOTHING for idempotency.

    Returns:
        Number of rows inserted.
    """
    if not records:
        return 0

    stmt = text("""
        INSERT INTO rapidapi_injuries
            (report_date, team, player, status, reason, report_time, injury_category, injury_detail)
        VALUES
            (:report_date, :team, :player, :status, :reason, :report_time, :injury_category, :injury_detail)
        ON CONFLICT (report_date, COALESCE(team, ''), player, status, COALESCE(reason, ''))
        DO NOTHING
    """)

    inserted = 0
    skipped = 0
    with engine.begin() as conn:
        for rec in records:
            player = rec.get("player") or None
            status = rec.get("status") or None
            if not player or not status:
                skipped += 1
                logger.debug(f"Skipping record with null player/status: {rec}")
                continue

            category, detail = parse_reason(rec.get("reason"))
            params = {
                "report_date": target_date,
                "team": rec.get("team") or None,
                "player": player,
                "status": status,
                "reason": rec.get("reason") or None,
                "report_time": rec.get("reportTime") or None,
                "injury_category": category,
                "injury_detail": detail,
            }
            result = conn.execute(stmt, params)
            inserted += result.rowcount

    if skipped > 0:
        logger.warning(f"{target_date}: Skipped {skipped} records with missing player/status")
    return inserted


# --- Main Backfill Logic ---

def generate_date_range(start: date, end: date) -> list[date]:
    """Generate list of dates from start to end inclusive."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def run_backfill(
    engine,
    client: RapidAPIInjuryClient,
    start_date: date,
    end_date: date,
    skip_existing: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Run the historical backfill.

    Returns:
        Summary dict with counts.
    """
    all_dates = generate_date_range(start_date, end_date)
    logger.info(f"Backfill range: {start_date} to {end_date} ({len(all_dates)} days)")

    # Resume support: skip dates already in DB
    if skip_existing and not dry_run:
        existing = get_scraped_dates(engine)
        dates_to_fetch = [d for d in all_dates if d not in existing]
        skipped = len(all_dates) - len(dates_to_fetch)
        if skipped > 0:
            logger.info(f"Skipping {skipped} dates already in database. {len(dates_to_fetch)} remaining.")
    else:
        dates_to_fetch = all_dates

    if not dates_to_fetch:
        logger.info("All dates already scraped. Nothing to do.")
        return {"total": len(all_dates), "fetched": 0, "records": 0, "errors": 0, "skipped": len(all_dates)}

    stats = {
        "total": len(all_dates),
        "fetched": 0,
        "records": 0,
        "empty_days": 0,
        "errors": 0,
        "skipped": len(all_dates) - len(dates_to_fetch),
    }

    for i, target_date in enumerate(dates_to_fetch):
        progress = f"[{i + 1}/{len(dates_to_fetch)}]"

        if dry_run:
            logger.info(f"{progress} Would fetch: {target_date}")
            stats["fetched"] += 1
            continue

        records = client.fetch_date(target_date)

        if records is None:
            logger.error(f"{progress} {target_date}: FAILED")
            stats["errors"] += 1
            continue

        if len(records) == 0:
            logger.debug(f"{progress} {target_date}: No injury reports (off-season or no data)")
            stats["empty_days"] += 1
            stats["fetched"] += 1
            continue

        try:
            inserted = store_records(engine, target_date, records)
        except Exception as e:
            logger.error(f"{progress} {target_date}: DB error storing {len(records)} records: {e}")
            stats["errors"] += 1
            continue

        stats["fetched"] += 1
        stats["records"] += inserted
        logger.info(f"{progress} {target_date}: {len(records)} reports, {inserted} new rows")

        # Periodic progress summary
        if (i + 1) % 50 == 0:
            logger.info(
                f"--- Progress: {stats['fetched']} fetched, "
                f"{stats['records']} records stored, "
                f"{stats['errors']} errors ---"
            )

    return stats


# --- CLI ---

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical NBA injury reports from RapidAPI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Full backfill (2021-present):
    python src/scrapers/rapidapi_injury_backfill.py

  Specific range:
    python src/scrapers/rapidapi_injury_backfill.py --start 2024-10-22 --end 2025-01-29

  Dry run (no DB writes, no API calls):
    python src/scrapers/rapidapi_injury_backfill.py --dry-run --start 2025-01-01 --end 2025-01-05

  Custom rate limit:
    python src/scrapers/rapidapi_injury_backfill.py --delay 2.0
        """,
    )
    parser.add_argument(
        "--start",
        type=str,
        default=DEFAULT_START.isoformat(),
        help=f"Start date YYYY-MM-DD (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=date.today().isoformat(),
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds between API requests (default: 1.0)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't skip dates already in the database",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be fetched without making API calls or DB writes",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Validate API key
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        logger.error("RAPIDAPI_KEY not found in environment. Add it to your .env file.")
        sys.exit(1)

    # Parse dates
    try:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
    except ValueError as e:
        logger.error(f"Invalid date format: {e}. Use YYYY-MM-DD.")
        sys.exit(1)

    if start_date > end_date:
        logger.error(f"Start date ({start_date}) is after end date ({end_date}).")
        sys.exit(1)

    logger.info("=" * 70)
    logger.info("RapidAPI NBA Injury Reports - Historical Backfill")
    logger.info(f"  Range: {start_date} to {end_date}")
    logger.info(f"  Delay: {args.delay}s between requests")
    logger.info(f"  Resume: {'disabled' if args.no_resume else 'enabled'}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("=" * 70)

    # Initialize
    engine = get_engine()

    if not args.dry_run:
        ensure_table(engine)

    client = RapidAPIInjuryClient(api_key=api_key, delay=args.delay)

    # Run
    stats = run_backfill(
        engine=engine,
        client=client,
        start_date=start_date,
        end_date=end_date,
        skip_existing=not args.no_resume,
        dry_run=args.dry_run,
    )

    # Summary
    logger.info("=" * 70)
    logger.info("BACKFILL COMPLETE")
    logger.info(f"  Total days in range: {stats['total']}")
    logger.info(f"  Skipped (already in DB): {stats['skipped']}")
    logger.info(f"  Fetched: {stats['fetched']}")
    logger.info(f"  Empty days: {stats.get('empty_days', 0)}")
    logger.info(f"  Records stored: {stats['records']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 70)

    if stats["errors"] > 0:
        logger.warning(
            f"{stats['errors']} dates failed. Re-run the script to retry "
            f"(resume will skip already-fetched dates)."
        )


if __name__ == "__main__":
    main()
