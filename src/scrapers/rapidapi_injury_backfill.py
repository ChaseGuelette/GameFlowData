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
import hashlib
import logging
import os
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.append(str(Path(__file__).resolve().parents[2]))

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from src.db.client import get_engine
from src.scrapers.nba_injury_db import (
    completed_dates,
    ensure_capture_columns,
    historical_request_dates,
    persist_capture,
    prepare_records,
    require_capture_schema,
    saved_captures,
)

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
    scraped_at timestamp with time zone DEFAULT now(),
    player_id bigint,
    nba_team_id bigint,
    game_id text,
    source_observations jsonb
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

def ensure_table(engine) -> None:
    """Create the rapidapi_injuries table for explicit local/dev bootstrap.

    Recurring production scraper runs should not perform schema migrations or
    drop/rebuild indexes. Structural changes belong in database migrations.
    """
    with engine.begin() as conn:
        conn.execute(text(DDL_CREATE_TABLE))
    ensure_capture_columns(engine)
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
        self.last_capture = None

    def _rate_limit(self) -> None:
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def fetch_date_response(
        self, target_date: date, *, timeout: tuple[float, float] = (5.0, 15.0)
    ) -> requests.Response:
        """One streamed request for evidence capture; caller reads/closes it.

        No retries, JSON coercion, redirects or storage. Keep the legacy
        fetch_date path unchanged for scheduled production callers.
        """
        self._rate_limit()
        return self.session.get(
            f"{RAPIDAPI_BASE_URL}/{target_date.isoformat()}",
            timeout=timeout,
            stream=True,
            allow_redirects=False,
        )

    def fetch_date(self, target_date: date) -> list[dict] | None:
        """
        Fetch injury reports for a single date.

        Returns:
            List of injury record dicts, or None on failure.
        """
        self.last_capture = None
        url = f"{RAPIDAPI_BASE_URL}/{target_date.isoformat()}"

        for attempt in range(self.max_retries):
            self._rate_limit()
            try:
                response = self.session.get(url, timeout=15, allow_redirects=False)

                if response.status_code == 429:
                    wait = (2 ** attempt) * 5
                    logger.warning(f"Rate limited (429). Waiting {wait}s before retry.")
                    time.sleep(wait)
                    continue

                if response.status_code == 404:
                    from src.scrapers.nba_injury_pilot import is_missing_report

                    receipt = {"http_status": 404, "body_complete": True, "requested_date": target_date.isoformat()}
                    if is_missing_report(receipt, response.content):
                        self.last_capture = {
                            "retrieved_at_utc": datetime.now(UTC).isoformat(),
                            "raw_body_sha256": hashlib.sha256(response.content).hexdigest(),
                            "http_status": 404, "missing_report": True,
                        }
                        return []
                response.raise_for_status()
                if response.status_code != 200:
                    logger.error("Unexpected HTTP status %s", response.status_code)
                    return None

                try:
                    data = response.json()
                except (ValueError, KeyError):
                    logger.warning(f"{target_date}: Non-JSON response; not an empty injury report")
                    return None

                if isinstance(data, list):
                    self.last_capture = {
                        "retrieved_at_utc": datetime.now(UTC).isoformat(),
                        "raw_body_sha256": hashlib.sha256(response.content).hexdigest(),
                        "http_status": 200,
                    }
                    return data
                elif isinstance(data, dict) and "message" in data:
                    logger.warning(f"{target_date}: API error object; refusing an empty-success checkpoint")
                    return None
                else:
                    logger.warning(f"{target_date}: Unexpected response format: {type(data)}")
                    return None

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

def get_scraped_dates(engine, start_date=date.min, end_date=date.max) -> set[date]:
    """Resume only atomically completed DB captures, not arbitrary legacy rows."""
    return completed_dates(engine, start_date, end_date)


def store_records(engine, target_date: date, records: list[dict], *, capture_metadata=None) -> int:
    """Upsert existing injury rows and commit a DB-backed capture checkpoint.

    report_date uses each payload's game date. Requested date/report observations
    remain on the same injury rows in source_observations. Returns rows affected,
    not a claim that every row was newly inserted.
    """
    return persist_capture(engine, target_date, records, parse_reason, capture_metadata)


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
    request_dates=None,
    max_dates: int = 600,
) -> dict:
    """
    Run the historical backfill.

    Returns:
        Summary dict with counts.
    """
    all_dates = sorted(set(request_dates)) if request_dates is not None else generate_date_range(start_date, end_date)
    if not all_dates or max_dates < 1 or len(all_dates) > max_dates:
        raise ValueError("Requested dates exceed --max-dates or are empty")
    if not dry_run:
        require_capture_schema(engine)
    logger.info(f"Backfill range: {start_date} to {end_date} ({len(all_dates)} days)")

    # Resume support: skip dates already in DB
    if skip_existing and not dry_run:
        existing = get_scraped_dates(engine, min(all_dates), max(all_dates))
        today_et = datetime.now(ZoneInfo("America/New_York")).date()
        dates_to_fetch = [d for d in all_dates if d not in existing or d >= today_et]
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
            break

        try:
            stored = store_records(engine, target_date, records, capture_metadata=getattr(client, "last_capture", None))
        except Exception as e:
            logger.error("%s %s: DB capture failed (%s)", progress, target_date, type(e).__name__)
            stats["errors"] += 1
            break

        stats["fetched"] += 1
        stats["records"] += stored
        stats["empty_days"] += not records
        logger.info(f"{progress} {target_date}: {len(records)} observations, {stored} rows upserted; DB checkpoint committed")

        # Periodic progress summary
        if (i + 1) % 50 == 0:
            logger.info(
                f"--- Progress: {stats['fetched']} fetched, "
                f"{stats['records']} records stored, "
                f"{stats['errors']} errors ---"
            )

    return stats


# --- CLI ---

def import_captures(engine, path, start_date, end_date, *, dry_run=False, skip_existing=True, max_dates=600):
    """Import the already downloaded captures, without any HTTP requests."""
    dates, count, invalid, missing = set(), 0, 0, 0
    for requested, records, metadata in saved_captures(path):
        if not start_date <= requested <= end_date:
            continue
        dates.add(requested)
        if len(dates) > max_dates:
            raise ValueError("Capture import exceeds --max-dates")
        _, metrics = prepare_records(requested, records, parse_reason, metadata)
        count += len(records)
        invalid += metrics["invalid_status_count"]
        missing += metrics["missing_report"]
    if not dates:
        raise ValueError("No completed captures selected for import")
    logger.info("Capture import: %s request dates, %s observations, %s invalid statuses retained as raw evidence, %s missing reports",
                len(dates), count, invalid, missing)
    if dry_run:
        logger.info("DRY RUN: validated captures; zero API requests and zero database access")
        return {"dates": len(dates), "observations": count, "invalid_statuses": invalid, "missing_reports": missing, "dry_run": True}
    require_capture_schema(engine)
    existing = get_scraped_dates(engine, min(dates), max(dates)) if skip_existing else set()
    stored, processed = 0, 0
    for requested, records, metadata in saved_captures(path):
        if requested not in dates or requested in existing:
            continue
        stored += store_records(engine, requested, records, capture_metadata=metadata)
        processed += 1
        logger.info("Imported %s (%s observations); DB checkpoint committed", requested, len(records))
    logger.info("DB import complete: %s captures, %s rows upserted; no API calls", processed, stored)
    return {"captures_imported": processed, "rows_upserted": stored, "api_requests": 0}


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
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help=(
            "Explicitly create bootstrap table/indexes for local/dev use. "
            "Production schema changes belong in database migrations."
        ),
    )
    parser.add_argument("--schema-only", action="store_true", help="With --ensure-schema: apply/read back schema and exit")
    parser.add_argument("--import-captures", type=Path, help="Import a completed capture manifest/directory into the DB; no API calls")
    parser.add_argument("--seasons", nargs="+", help="Read historical game dates from the DB, plus D-1; no props filter")
    parser.add_argument("--max-dates", type=int, default=600, help="Maximum request dates in this invocation (default 600)")
    return parser.parse_args()


def main():
    args = parse_args()

    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key and not args.dry_run and not args.import_captures and not args.schema_only:
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

    if args.max_dates < 1 or args.delay < 1:
        raise ValueError("--max-dates must be positive and --delay must be at least one second")
    if args.schema_only and not args.ensure_schema:
        raise ValueError("--schema-only requires explicit --ensure-schema")
    if args.import_captures and args.seasons:
        raise ValueError("Choose saved captures or DB season selection, not both")

    logger.info("=" * 70)
    logger.info("RapidAPI NBA Injury Reports - Historical Backfill")
    logger.info(f"  Range: {start_date} to {end_date}")
    logger.info(f"  Delay: {args.delay}s between requests")
    logger.info(f"  Resume: {'disabled' if args.no_resume else 'enabled'}")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info("=" * 70)

    # Initialize. Dry-run intentionally avoids DB connections and API calls so
    # scheduled-command validation cannot mutate schema or consume quota.
    engine = None if args.dry_run and not args.seasons else get_engine()

    if args.ensure_schema:
        if args.dry_run:
            logger.info("Dry run requested; skipping --ensure-schema bootstrap.")
        elif args.schema_only:
            ensure_capture_columns(engine)
        else:
            ensure_table(engine)
    elif not args.dry_run:
        logger.info("Skipping schema bootstrap; production schema is managed by database migrations.")

    if args.schema_only:
        logger.info("Schema preview complete" if args.dry_run else "Schema applied and column readback verified")
        return
    if args.import_captures:
        import_captures(engine, args.import_captures, start_date, end_date,
                        dry_run=args.dry_run, skip_existing=not args.no_resume, max_dates=args.max_dates)
        return
    request_dates = historical_request_dates(engine, args.seasons, start_date, end_date) if args.seasons else None
    if args.seasons and args.dry_run:
        logger.info("Season preview used a read-only DB date lookup; no API calls or DB writes")

    client = RapidAPIInjuryClient(api_key or "dry-run", delay=args.delay, max_retries=1)

    # Run
    stats = run_backfill(
        engine=engine,
        client=client,
        start_date=start_date,
        end_date=end_date,
        skip_existing=not args.no_resume,
        dry_run=args.dry_run,
        request_dates=request_dates,
        max_dates=args.max_dates,
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
        raise SystemExit(1)


if __name__ == "__main__":
    main()
