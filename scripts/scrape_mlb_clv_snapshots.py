#!/usr/bin/env python3
"""Targeted MLB CLV snapshot scraper.

Scrapes dense, game-relative Odds API historical player-prop snapshots into
mlb_player_props_clv_snapshots. This is intentionally separate from
mlb_raw_player_props so CLV/timing research does not bloat the production raw
props table.

Default target is batter_hits only, with:
- close grid: T-60, T-30, T-15, T-5 before each game's commence_time
- fixed decision grid: 09:30, 10:30, 11:30, 12:30, 13:30, 15:30, 17:30 ET

The grid is generated per game based on commence_time, so early and late MLB
starts are handled correctly.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from psycopg2 import extras
from sqlalchemy import create_engine, text

sys.path.append(str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBCLVSnapshots")

load_dotenv()

SPORT_KEY = "baseball_mlb"
DEFAULT_TABLE = "mlb_player_props_clv_snapshots"
DEFAULT_REGIONS = "us,us2,us_ex,us_dfs"
DEFAULT_MARKETS = ("batter_hits",)
DEFAULT_DISCOVERY_HOURS_ET = (6, 9, 12, 15, 18, 21)
DEFAULT_CLOSE_OFFSETS = (60, 30, 15, 5)
DEFAULT_FIXED_TIMES_ET = (
    "09:30",
    "10:30",
    "11:30",
    "12:30",
    "13:30",
    "15:30",
    "17:30",
)
ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Event:
    event_id: str
    commence_time: datetime
    home_team: str | None
    away_team: str | None


@dataclass(frozen=True)
class SnapshotTask:
    event: Event
    requested_snapshot_time: datetime
    scrape_reason: str
    target_offset_minutes: int | None


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def parse_hhmm(value: str) -> dtime:
    hour_s, minute_s = value.split(":", 1)
    return dtime(int(hour_s), int(minute_s), tzinfo=ET)


def reason_for_fixed_time(value: str) -> str:
    return "fixed_decision_" + value.replace(":", "")


class MLBCLVSnapshotScraper:
    def __init__(
        self,
        *,
        api_key: str,
        database_url: str,
        markets: list[str],
        table_name: str,
        regions: str,
        request_sleep_seconds: float,
    ) -> None:
        self.api_key = api_key
        self.engine = create_engine(database_url)
        self.markets = markets
        self.table_name = table_name
        self.regions = regions
        self.request_sleep_seconds = request_sleep_seconds
        self.session = requests.Session()

    def ensure_table_exists(self) -> None:
        query = text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = :table_name
            """
        )
        with self.engine.connect() as conn:
            exists = conn.execute(query, {"table_name": self.table_name}).scalar()
        if not exists:
            raise RuntimeError(
                f"Target table public.{self.table_name} does not exist. "
                "Apply database/migrations/030_mlb_clv_snapshot_table.sql first."
            )

    def get_events_at_snapshot(self, snapshot_time: datetime) -> list[Event]:
        url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events"
        params = {"apiKey": self.api_key, "date": iso_z(snapshot_time)}
        response = self.session.get(url, params=params, timeout=20)
        if response.status_code == 429:
            raise RuntimeError("The Odds API rate limited event discovery (429).")
        response.raise_for_status()
        events = []
        for row in response.json().get("data", []):
            commence = parse_iso_utc(row.get("commence_time"))
            event_id = row.get("id")
            if not commence or not event_id:
                continue
            events.append(
                Event(
                    event_id=str(event_id),
                    commence_time=commence,
                    home_team=row.get("home_team"),
                    away_team=row.get("away_team"),
                )
            )
        return events

    def discover_events(self, start_date: date, end_date: date, discovery_hours_et: Iterable[int]) -> dict[str, Event]:
        events: dict[str, Event] = {}
        # Add one buffer day so late UTC/ET boundary cases can still be discovered,
        # then filter by ET commence date below.
        for day in iter_dates(start_date, end_date):
            for hour in discovery_hours_et:
                snapshot = datetime.combine(day, dtime(hour, 0, tzinfo=ET)).astimezone(UTC)
                try:
                    for event in self.get_events_at_snapshot(snapshot):
                        event_et_date = event.commence_time.astimezone(ET).date()
                        if start_date <= event_et_date <= end_date:
                            events[event.event_id] = event
                except Exception as exc:
                    logger.warning("Event discovery failed at %s: %s", iso_z(snapshot), exc)
                time.sleep(self.request_sleep_seconds)
        return events

    def fetch_event_odds(self, task: SnapshotTask) -> tuple[dict | None, int]:
        url = f"https://api.the-odds-api.com/v4/historical/sports/{SPORT_KEY}/events/{task.event.event_id}/odds"
        params = {
            "apiKey": self.api_key,
            "date": iso_z(task.requested_snapshot_time),
            "regions": self.regions,
            "markets": ",".join(self.markets),
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        # Use a one-off request so parallel workers do not share a requests.Session.
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 422:
            return None, 0
        if response.status_code == 429:
            raise RuntimeError("The Odds API rate limited odds fetch (429).")
        response.raise_for_status()
        return response.json().get("data", {}), int(response.headers.get("x-requests-last", 0))

    def parse_rows(self, task: SnapshotTask, data: dict | None) -> list[tuple]:
        if not data or "bookmakers" not in data:
            return []
        rows = []
        api_game_id = str(data.get("id") or task.event.event_id)
        response_commence = parse_iso_utc(data.get("commence_time"))
        effective_commence = response_commence or task.event.commence_time
        if task.requested_snapshot_time >= effective_commence:
            logger.warning(
                "Skipping post-commence snapshot for %s at %s; response commence_time=%s",
                api_game_id,
                iso_z(task.requested_snapshot_time),
                iso_z(effective_commence),
            )
            return []
        commence_time = iso_z(effective_commence)
        home_team = data.get("home_team") or task.event.home_team
        away_team = data.get("away_team") or task.event.away_team
        requested_ts = iso_z(task.requested_snapshot_time)

        for book in data.get("bookmakers", []):
            bookmaker = book.get("key")
            if not bookmaker:
                continue
            bookmaker_name = book.get("title")
            bookmaker_last_update = book.get("last_update")
            for market in book.get("markets", []):
                market_key = market.get("key")
                market_last_update = market.get("last_update")
                if market_key not in self.markets:
                    continue
                for outcome in market.get("outcomes", []):
                    api_player_name = outcome.get("description")
                    outcome_label = outcome.get("name")
                    if not api_player_name or not outcome_label:
                        continue
                    rows.append(
                        (
                            api_game_id,
                            task.event.event_id,
                            None,  # player_id populated later by linker if needed
                            api_player_name,
                            bookmaker,
                            bookmaker_name,
                            market_key,
                            outcome_label,
                            outcome.get("point"),
                            outcome.get("price"),
                            commence_time,
                            home_team,
                            away_team,
                            requested_ts,  # snapshot_time: historical response corresponds to requested snapshot
                            requested_ts,
                            market_last_update,
                            bookmaker_last_update,
                            task.scrape_reason,
                            task.target_offset_minutes,
                        )
                    )
        return rows

    def insert_rows(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        # uq_mlb_clv_snapshot_quote is an expression unique index, not a named
        # constraint, so use broad ON CONFLICT DO NOTHING for idempotent retries.
        query = f"""
            INSERT INTO {self.table_name} (
                api_game_id,
                odds_api_event_id,
                player_id,
                api_player_name,
                bookmaker,
                bookmaker_name,
                market_key,
                outcome_label,
                line,
                odds_american,
                commence_time,
                home_team,
                away_team,
                snapshot_time,
                requested_snapshot_time,
                market_last_update,
                bookmaker_last_update,
                scrape_reason,
                target_offset_minutes
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """  # nosec B608 - table_name is controlled by CLI/default for internal ops
        conn = self.engine.raw_connection()
        try:
            with conn.cursor() as cur:
                extras.execute_values(cur, query, rows, page_size=1000)
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def count_table_rows(self) -> int:
        with self.engine.connect() as conn:
            return int(conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}")).scalar() or 0)

    def existing_task_keys(self, tasks: list[SnapshotTask]) -> set[tuple[str, datetime]]:
        """Return (event_id, requested_snapshot_time) pairs already present.

        This makes the historical scraper resume-capable after interruption. A
        task is considered complete if at least one row exists for its event and
        snapshot time in any requested market.
        """
        if not tasks:
            return set()
        event_ids = sorted({task.event.event_id for task in tasks})
        timestamps = sorted({task.requested_snapshot_time for task in tasks})
        event_placeholders = ", ".join(f":event_{i}" for i in range(len(event_ids)))
        time_placeholders = ", ".join(f":ts_{i}" for i in range(len(timestamps)))
        market_placeholders = ", ".join(f":market_{i}" for i in range(len(self.markets)))
        params: dict[str, object] = {}
        for i, event_id in enumerate(event_ids):
            params[f"event_{i}"] = event_id
        for i, ts in enumerate(timestamps):
            params[f"ts_{i}"] = ts
        for i, market in enumerate(self.markets):
            params[f"market_{i}"] = market
        query = text(f"""
            SELECT DISTINCT api_game_id, snapshot_time
            FROM {self.table_name}
            WHERE api_game_id IN ({event_placeholders})
              AND snapshot_time IN ({time_placeholders})
              AND market_key IN ({market_placeholders})
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return {(str(row[0]), row[1].astimezone(UTC)) for row in rows}

    def scrape_one_task(self, task: SnapshotTask) -> tuple[int, int]:
        data, credits = self.fetch_event_odds(task)
        rows = self.parse_rows(task, data)
        inserted_attempt = self.insert_rows(rows)
        return inserted_attempt, credits

    def scrape_tasks(
        self,
        tasks: list[SnapshotTask],
        *,
        limit: int | None = None,
        max_workers: int = 1,
    ) -> tuple[int, int, int]:
        total_rows = 0
        total_credits = 0
        attempted = 0
        selected_tasks = tasks[:limit] if limit else tasks

        if max_workers > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {executor.submit(self.scrape_one_task, task): task for task in selected_tasks}
                for idx, future in enumerate(as_completed(future_to_task), start=1):
                    task = future_to_task[future]
                    try:
                        inserted_attempt, credits = future.result()
                    except Exception as exc:
                        logger.error(
                            "[%d/%d] FAILED %s %s at %s: %s",
                            idx,
                            len(selected_tasks),
                            task.scrape_reason,
                            task.event.event_id,
                            iso_z(task.requested_snapshot_time),
                            exc,
                        )
                        continue
                    total_rows += inserted_attempt
                    total_credits += credits
                    attempted += 1
                    logger.info(
                        "[%d/%d] done %s %s at %s | rows before dedupe=%d | credits=%d",
                        idx,
                        len(selected_tasks),
                        task.scrape_reason,
                        task.event.event_id,
                        iso_z(task.requested_snapshot_time),
                        inserted_attempt,
                        credits,
                    )
            return attempted, total_rows, total_credits

        for idx, task in enumerate(selected_tasks, start=1):
            logger.info(
                "[%d/%d] %s %s %s vs %s at %s",
                idx,
                len(selected_tasks),
                task.scrape_reason,
                task.event.event_id,
                task.event.away_team,
                task.event.home_team,
                iso_z(task.requested_snapshot_time),
            )
            inserted_attempt, credits = self.scrape_one_task(task)
            total_rows += inserted_attempt
            total_credits += credits
            attempted += 1
            if self.request_sleep_seconds > 0:
                time.sleep(self.request_sleep_seconds)
        return attempted, total_rows, total_credits


def build_tasks(
    events: Iterable[Event],
    *,
    close_offsets: Iterable[int],
    fixed_times_et: Iterable[str],
) -> list[SnapshotTask]:
    tasks_by_key: dict[tuple[str, datetime, str], SnapshotTask] = {}
    parsed_fixed = [(value, parse_hhmm(value)) for value in fixed_times_et]

    for event in events:
        # Close snapshots: per-game, relative to commence_time.
        for offset in close_offsets:
            requested = event.commence_time - timedelta(minutes=offset)
            if requested >= event.commence_time:
                continue
            reason = f"close_t_minus_{offset}"
            task = SnapshotTask(event, requested, reason, -offset)
            tasks_by_key[(event.event_id, requested, reason)] = task

        # Fixed ET decision snapshots on the game's ET commence date. Skip fixed
        # times at/after commence; those are invalid for pregame CLV selection.
        game_day_et = event.commence_time.astimezone(ET).date()
        for label, hhmm in parsed_fixed:
            requested = datetime.combine(game_day_et, hhmm).astimezone(UTC)
            if requested >= event.commence_time:
                continue
            reason = reason_for_fixed_time(label)
            task = SnapshotTask(event, requested, reason, None)
            tasks_by_key[(event.event_id, requested, reason)] = task

    return sorted(tasks_by_key.values(), key=lambda t: (t.requested_snapshot_time, t.event.event_id, t.scrape_reason))


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape targeted MLB CLV player-prop snapshots")
    parser.add_argument("--start-date", required=True, help="ET commence start date, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="ET commence end date, YYYY-MM-DD")
    parser.add_argument("--markets", nargs="+", default=list(DEFAULT_MARKETS), help="Odds API player prop markets")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Target table name")
    parser.add_argument("--regions", default=DEFAULT_REGIONS, help="Odds API regions")
    parser.add_argument("--database-url", default=None, help="Override DB URL; defaults to DATABASE_URL")
    parser.add_argument("--api-key", default=None, help="Override Odds API key; defaults to ODDS_API_KEY")
    parser.add_argument("--close-offsets-minutes", nargs="+", type=int, default=list(DEFAULT_CLOSE_OFFSETS))
    parser.add_argument("--fixed-times-et", nargs="+", default=list(DEFAULT_FIXED_TIMES_ET))
    parser.add_argument("--discovery-hours-et", nargs="+", type=int, default=list(DEFAULT_DISCOVERY_HOURS_ET))
    parser.add_argument("--dry-run", action="store_true", help="Discover events/build tasks but do not fetch odds or insert rows")
    parser.add_argument("--limit", type=int, default=None, help="Limit odds tasks for sample runs")
    parser.add_argument("--request-sleep-seconds", type=float, default=0.20)
    parser.add_argument("--max-workers", type=int, default=1, help="Parallel odds fetch workers; keep modest to avoid API rate limits")
    parser.add_argument("--no-skip-existing", action="store_true", help="Refetch tasks even when rows already exist for event/snapshot/market")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("ODDS_API_KEY")
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not api_key:
        logger.error("Missing ODDS_API_KEY")
        return 2
    if not database_url:
        logger.error("Missing DATABASE_URL")
        return 2

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if end < start:
        logger.error("--end-date must be >= --start-date")
        return 2

    scraper = MLBCLVSnapshotScraper(
        api_key=api_key,
        database_url=database_url,
        markets=list(args.markets),
        table_name=args.table,
        regions=args.regions,
        request_sleep_seconds=args.request_sleep_seconds,
    )
    if not args.dry_run:
        scraper.ensure_table_exists()

    logger.info("Discovering MLB events from %s through %s by ET commence date", start, end)
    events = scraper.discover_events(start, end, args.discovery_hours_et)
    tasks = build_tasks(events.values(), close_offsets=args.close_offsets_minutes, fixed_times_et=args.fixed_times_et)

    # The Odds API charges by market and region group per event odds request.
    # Player props have historically been ~10 credits per market per region group
    # in this project. DEFAULT_REGIONS has four groups, so batter_hits is usually
    # ~40 credits per event/snapshot odds request.
    region_count = len([region for region in args.regions.split(",") if region.strip()])
    estimated_credits = len(tasks) * len(args.markets) * region_count * 10
    logger.info("Markets: %s", ",".join(args.markets))
    logger.info("Events discovered: %d", len(events))
    logger.info("Snapshot tasks: %d", len(tasks))
    logger.info("Estimated credits: ~%s", f"{estimated_credits:,}")
    by_reason: dict[str, int] = {}
    for task in tasks:
        by_reason[task.scrape_reason] = by_reason.get(task.scrape_reason, 0) + 1
    for reason, count in sorted(by_reason.items()):
        logger.info("  %s: %d", reason, count)

    if not args.no_skip_existing:
        existing = scraper.existing_task_keys(tasks)
        before = len(tasks)
        tasks = [
            task for task in tasks
            if (task.event.event_id, task.requested_snapshot_time.astimezone(UTC)) not in existing
        ]
        logger.info("Resume filter: skipped %d/%d already-present event/snapshot tasks", before - len(tasks), before)
        logger.info("Remaining tasks after resume filter: %d", len(tasks))

    if args.dry_run:
        return 0

    attempted, rows, credits = scraper.scrape_tasks(tasks, limit=args.limit, max_workers=max(1, args.max_workers))
    table_count = scraper.count_table_rows()
    logger.info(
        "Done. Attempted tasks: %d | row insert attempts before dedupe: %d | reported credits: %d | table rows now: %d",
        attempted,
        rows,
        credits,
        table_count,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
