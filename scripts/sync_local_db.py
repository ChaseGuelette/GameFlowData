#!/usr/bin/env python3
"""Sync Supabase tables to local Postgres for offline training & backtesting.

Usage:
    python scripts/sync_local_db.py              # Sync all tables (smart incremental)
    python scripts/sync_local_db.py --full        # Force full refresh of all tables
    python scripts/sync_local_db.py --sport mlb   # MLB tables only
    python scripts/sync_local_db.py --sport nba   # NBA tables only
    python scripts/sync_local_db.py --tables mlb_players mlb_park_factors  # Specific tables

Prerequisites:
    - Local Postgres running on localhost:5432
    - DATABASE_URL env var pointing to Supabase (from .env)
    - LOCAL_DATABASE_URL env var set in .env

Setup (one-time):
    1. Install Postgres: https://www.postgresql.org/download/windows/
    2. Set LOCAL_DATABASE_URL in .env
    3. python scripts/sync_local_db.py --full --sport mlb   (~10 GB, ~10-15 min)
    4. python scripts/sync_local_db.py --full --sport nba   (~30 GB, ~30-40 min)

To use local DB for backtesting:
    python src/backtesting/mlb/run_mlb_sweep.py --local ...
    python src/models/mlb/mlb_batter_train_pipeline.py --local ...
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import ForeignKeyConstraint, MetaData, create_engine, inspect

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("sync_local_db")

# ---------------------------------------------------------------------------
# Table registry: {table_name: (date_column | None, strategy)}
#   "full"        = truncate + reload every time (for small reference tables)
#   "incremental" = only fetch rows newer than local max date
# ---------------------------------------------------------------------------

MLB_TABLES: dict[str, tuple[str | None, str]] = {
    "mlb_game_schedule":                    ("game_date", "full"),
    "mlb_park_factors":                     (None, "full"),
    "mlb_game_weather":                     ("game_date", "incremental"),
    "mlb_players":                          (None, "full"),
    "mlb_teams":                            (None, "full"),
    "mlb_player_season_advanced":           (None, "full"),
    "mlb_player_game_stats_batting":        ("game_date", "incremental"),
    "mlb_player_game_stats_pitching":       ("game_date", "incremental"),
    "mlb_player_game_statcast_batting":     ("game_date", "incremental"),
    "mlb_player_average_batting":           ("game_date", "incremental"),
    "mlb_player_average_pitching":          ("game_date", "incremental"),
    "mlb_player_average_statcast_batting":  ("game_date", "incremental"),
    "mlb_player_average_statcast_pitching": ("game_date", "incremental"),
    "mlb_raw_game_lines":                   ("commence_time", "incremental"),
    "mlb_raw_player_props":                 ("snapshot_time", "incremental"),
    "mlb_pitcher_inning_stats":             ("game_date", "incremental"),
    "mlb_bullpen_daily_status":             ("game_date", "incremental"),
    "mlb_game_lineups":                      ("game_date", "incremental"),
    "mlb_game_umpires":                      ("game_date", "incremental"),
    "mlb_player_props_clv_snapshots":        ("snapshot_time", "incremental"),
}

NBA_TABLES: dict[str, tuple[str | None, str]] = {
    "players":                      (None, "full"),
    "teams":                        (None, "full"),
    "game_id_map":                  (None, "full"),
    "league_priors_history":        (None, "full"),
    "rapidapi_injuries":            (None, "full"),
    "player_game_stats":            ("game_date", "incremental"),
    "player_average_game_stats":    ("game_date", "incremental"),
    "player_game_advanced_stats":   (None, "full"),
    "player_average_advanced_stats":("game_date", "incremental"),
    "player_position_history":      ("snapshot_date", "incremental"),
    "team_game_stats":              ("game_date", "incremental"),
    "team_average_game_stats":      ("game_date", "incremental"),
    "team_allowed_by_position":     ("game_date", "incremental"),
    "raw_player_props_combined":    ("commence_time", "incremental"),
    "raw_game_lines_staging":       ("commence_time", "incremental"),
}

DEFAULT_LOCAL_URL = "postgresql://postgres:***@localhost:5432/gameflow_local"


def _to_iso(value: datetime | date) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "+00:00"
        return value.isoformat()
    return datetime.combine(value, datetime.min.time()).isoformat() + "+00:00"


def parse_sync_date(value: str | date | datetime | None) -> datetime | None:
    """Parse date-ish values used by --start-date/--end-date and local max dates."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value)
    try:
        if len(text) == 10:
            return datetime.strptime(text, "%Y-%m-%d")
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception as exc:
        raise ValueError(f"Invalid date {value!r}; expected YYYY-MM-DD or ISO timestamp") from exc


def build_where_clause(
    date_col: str | None,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    incremental_max: datetime | date | str | None = None,
    include_incremental: bool = True,
) -> tuple[str, dict[str, str]]:
    """Build SQL WHERE + params for date-window/incremental query filters.

    CLI --end-date is inclusive; the generated predicate is exclusive of the
    following midnight so date windows are copy-safe for timestamp columns.
    """
    if not date_col:
        return "", {}

    start = parse_sync_date(start_date)
    end = parse_sync_date(end_date)
    incr = parse_sync_date(incremental_max) if incremental_max is not None else None

    clauses: list[str] = []
    params: dict[str, str] = {}

    lower_bound = start
    lower_key = "start_date"
    if include_incremental and incr is not None and (start is None or incr >= start):
        lower_bound = incr
        lower_key = "incremental_max"

    if lower_bound is not None:
        params[lower_key] = _to_iso(lower_bound)
        op = ">" if lower_key == "incremental_max" else ">="
        clauses.append(f'"{date_col}" {op} %({lower_key})s')

    if end is not None:
        end_exclusive = (end + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        params["end_exclusive"] = _to_iso(end_exclusive)
        clauses.append(f'"{date_col}" < %(end_exclusive)s')

    return " AND ".join(clauses), params


def build_table_plan(args: argparse.Namespace) -> dict[str, tuple[str | None, str]]:
    """Build table plan and fail on unknown tables unless explicitly allowed."""
    all_configs = {**MLB_TABLES, **NBA_TABLES}
    tables: dict[str, tuple[str | None, str]] = {}
    if args.tables:
        for table_name in args.tables:
            if table_name not in all_configs:
                if not getattr(args, "allow_unknown_full_refresh", False):
                    raise ValueError(
                        f"Unknown table '{table_name}'. Use --allow-unknown-full-refresh for explicit full-refresh fallback."
                    )
                tables[table_name] = (None, "full")
            else:
                tables[table_name] = all_configs[table_name]
    else:
        if args.sport in ("mlb", "all"):
            tables.update(MLB_TABLES)
        if args.sport in ("nba", "all"):
            tables.update(NBA_TABLES)
    return tables


def get_local_url() -> str:
    return os.getenv("LOCAL_DATABASE_URL", DEFAULT_LOCAL_URL)


def redact_database_url(url: str | None) -> str:
    """Redact password material before logging database URLs."""
    if not url:
        return ""
    try:
        from urllib.parse import urlsplit, urlunsplit

        parts = urlsplit(url)
        if "@" not in parts.netloc:
            return url
        userinfo, hostinfo = parts.netloc.rsplit("@", 1)
        user = userinfo.split(":", 1)[0]
        safe_netloc = f"{user}:***@{hostinfo}" if user else f"***@{hostinfo}"
        return urlunsplit((parts.scheme, safe_netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return "<redacted database url>"


def ensure_local_db() -> None:
    """Create the local database if it doesn't exist."""
    local_url = get_local_url()
    db_name = local_url.rsplit("/", 1)[-1].split("?")[0]
    base_url = local_url.rsplit("/", 1)[0] + "/postgres"

    try:
        conn = psycopg2.connect(base_url)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if not cur.fetchone():
                cur.execute(f'CREATE DATABASE "{db_name}"')
                logger.info("Created database '%s'", db_name)
            else:
                logger.info("Database '%s' exists", db_name)
        conn.close()
    except psycopg2.OperationalError as e:
        logger.error("Cannot connect to local Postgres: %s", e)
        logger.error(
            "Install Postgres: https://www.postgresql.org/download/windows/\n"
            "Or Docker: docker run -d --name gameflow-pg -p 5432:5432 "
            "-e POSTGRES_PASSWORD=postgres postgres:16"
        )
        sys.exit(1)


def ensure_table_schema(remote_engine, local_engine, table_name: str) -> bool:
    """Reflect table schema from remote and create it locally if needed."""
    local_insp = inspect(local_engine)
    if table_name in local_insp.get_table_names():
        return True

    remote_meta = MetaData()
    try:
        remote_meta.reflect(bind=remote_engine, only=[table_name])
    except Exception as e:
        logger.warning("Could not reflect '%s': %s", table_name, e)
        return False

    if table_name not in remote_meta.tables:
        logger.warning("Table '%s' not found on remote", table_name)
        return False

    table = remote_meta.tables[table_name]

    # Strip foreign key constraints (referenced tables may not exist locally)
    for fkc in list(table.constraints):
        if isinstance(fkc, ForeignKeyConstraint):
            table.constraints.discard(fkc)
    for col in table.columns:
        col.foreign_keys.clear()

    local_meta = MetaData()
    table.to_metadata(local_meta)
    local_meta.create_all(bind=local_engine)
    logger.info("  Created table '%s'", table_name)
    return True


def get_local_max_date(local_conn, table_name: str, date_col: str):
    """Get the max date value from local table for incremental sync."""
    with local_conn.cursor() as cur:
        cur.execute(f'SELECT MAX("{date_col}") FROM "{table_name}"')
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def sync_table(
    remote_conn,
    local_conn,
    table_name: str,
    date_col: str | None,
    strategy: str,
    force_full: bool,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
) -> int:
    """Sync a single table from remote to local using COPY."""
    t0 = time.time()

    where_clause = ""
    where_params: dict[str, str] = {}
    mode = "full"

    if strategy == "incremental" and date_col and not force_full:
        max_date = get_local_max_date(local_conn, table_name, date_col)
        where_clause, where_params = build_where_clause(
            date_col,
            start_date=start_date,
            end_date=end_date,
            incremental_max=max_date,
            include_incremental=True,
        )
        mode = "incremental"
    elif date_col and (start_date or end_date):
        where_clause, where_params = build_where_clause(
            date_col,
            start_date=start_date,
            end_date=end_date,
            incremental_max=None,
            include_incremental=False,
        )
        mode = "window"

    # Count rows to sync and report date bounds where available.
    count_sql = f'SELECT COUNT(*) FROM "{table_name}"'
    if where_clause:
        count_sql += f" WHERE {where_clause}"
    min_value = max_value = None
    with remote_conn.cursor() as cur:
        try:
            cur.execute(count_sql, where_params)
            count = cur.fetchone()[0]
            if date_col and count:
                bounds_sql = f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table_name}"'
                if where_clause:
                    bounds_sql += f" WHERE {where_clause}"
                cur.execute(bounds_sql, where_params)
                min_value, max_value = cur.fetchone()
        except Exception:
            remote_conn.rollback()
            count = None

    if dry_run:
        logger.info(
            "  %s: dry-run would sync %s rows (%s) where=%s params=%s min_%s=%s max_%s=%s",
            table_name,
            f"{count:,}" if count is not None else "?",
            mode,
            where_clause or "<none>",
            where_params,
            date_col or "date",
            min_value,
            date_col or "date",
            max_value,
        )
        return 0

    if count == 0 and mode == "incremental":
        logger.info("  %s: up to date", table_name)
        return 0

    count_str = f"{count:,}" if count is not None else "?"
    logger.info("  %s: syncing %s rows (%s)...", table_name, count_str, mode)

    # Truncate for full refresh
    if mode == "full":
        with local_conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
        local_conn.commit()

    # Stream via temp file (handles tables of any size without eating RAM)
    with tempfile.TemporaryFile(mode="w+b") as tmp:
        # Export from remote
        with remote_conn.cursor() as cur:
            if where_clause:
                select_sql = cur.mogrify(
                    f'SELECT * FROM "{table_name}" WHERE {where_clause}', where_params
                ).decode("utf-8")
                copy_sql = f"COPY ({select_sql}) TO STDOUT WITH CSV HEADER"
            else:
                copy_sql = f'COPY "{table_name}" TO STDOUT WITH CSV HEADER'
            cur.copy_expert(copy_sql, tmp)

        file_size = tmp.tell()
        if file_size == 0:
            logger.info("  %s: empty result", table_name)
            return 0

        tmp.seek(0)

        # Read header to get column list (handles column ordering)
        header_line = tmp.readline().decode("utf-8").strip()
        columns = [c.strip('"') for c in header_line.split(",")]
        tmp.seek(0)

        # Import to local. Incremental/date-window syncs stage then upsert on id
        # when available so local mirrors recover from partial prior copies or
        # remote rows whose snapshot_time moved after they were first mirrored.
        with local_conn.cursor() as cur:
            col_list = ", ".join(f'"{c}"' for c in columns)
            if mode != "full" and "id" in columns:
                temp_table = f"_sync_{table_name}_{int(time.time() * 1000)}"
                cur.execute(
                    f'CREATE TEMP TABLE "{temp_table}" (LIKE "{table_name}" INCLUDING DEFAULTS) ON COMMIT DROP'
                )
                copy_in_sql = f'COPY "{temp_table}" ({col_list}) FROM STDIN WITH CSV HEADER'
                cur.copy_expert(copy_in_sql, tmp)
                update_cols = [c for c in columns if c != "id"]
                if update_cols:
                    update_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
                    insert_sql = (
                        f'INSERT INTO "{table_name}" ({col_list}) SELECT {col_list} FROM "{temp_table}" '
                        f'ON CONFLICT ("id") DO UPDATE SET {update_sql}'
                    )
                else:
                    insert_sql = (
                        f'INSERT INTO "{table_name}" ({col_list}) SELECT {col_list} FROM "{temp_table}" '
                        'ON CONFLICT ("id") DO NOTHING'
                    )
                cur.execute(insert_sql)
            else:
                copy_in_sql = f'COPY "{table_name}" ({col_list}) FROM STDIN WITH CSV HEADER'
                cur.copy_expert(copy_in_sql, tmp)

        local_conn.commit()

    elapsed = time.time() - t0
    size_mb = file_size / (1024 * 1024)
    rows = count or 0
    logger.info(
        "  %s: done (%s rows, %.1f MB, %.1f s, min_%s=%s, max_%s=%s)",
        table_name,
        count_str,
        size_mb,
        elapsed,
        date_col or "date",
        min_value,
        date_col or "date",
        max_value,
    )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Supabase tables to local Postgres for offline training & backtesting"
    )
    parser.add_argument(
        "--full", action="store_true", help="Force full refresh of all tables"
    )
    parser.add_argument(
        "--sport",
        choices=["mlb", "nba", "all"],
        default="all",
        help="Which sport's tables to sync (default: all)",
    )
    parser.add_argument(
        "--tables", nargs="+", help="Sync specific tables by name"
    )
    parser.add_argument("--start-date", help="Inclusive lower date bound for date/timestamp-backed tables (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Inclusive upper date bound for date/timestamp-backed tables (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Report planned counts without copying rows")
    parser.add_argument("--allow-unknown-full-refresh", action="store_true", help="Allow unknown --tables to fall back to full refresh")
    parser.add_argument("--allow-large-full-refresh", action="store_true", help="Allow unbounded full refresh of very large tables such as dense CLV snapshots")
    args = parser.parse_args()

    remote_url = os.getenv("DATABASE_URL")
    if not remote_url:
        logger.error("DATABASE_URL not set. Add it to your .env file.")
        sys.exit(1)

    local_url = get_local_url()

    # Build table list
    try:
        tables = build_table_plan(args)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(2)

    large_unbounded = [
        table_name
        for table_name in tables
        if table_name == "mlb_player_props_clv_snapshots"
        and args.full
        and not (args.start_date or args.end_date)
        and not args.allow_large_full_refresh
    ]
    if large_unbounded:
        logger.error(
            "Refusing unbounded full refresh for large table(s): %s. Use --start-date/--end-date or --allow-large-full-refresh.",
            ", ".join(large_unbounded),
        )
        sys.exit(2)

    logger.info("Syncing %d tables to local Postgres", len(tables))
    logger.info("Remote: %s", redact_database_url(remote_url))
    logger.info("Local:  %s", redact_database_url(local_url))
    if args.full:
        logger.info("Mode:   FULL REFRESH")

    # Ensure local database exists
    ensure_local_db()

    # Create engines for schema reflection
    remote_engine = create_engine(remote_url)
    local_engine = create_engine(local_url)

    # Create tables locally if needed
    logger.info("Checking schemas...")
    for table_name in tables:
        ensure_table_schema(remote_engine, local_engine, table_name)

    # Sync data using psycopg2 COPY
    remote_conn = psycopg2.connect(remote_url)
    local_conn = psycopg2.connect(local_url)

    total_rows = 0
    failed = []
    t_start = time.time()

    try:
        for table_name, (date_col, strategy) in tables.items():
            try:
                rows = sync_table(
                    remote_conn,
                    local_conn,
                    table_name,
                    date_col,
                    strategy,
                    args.full,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    dry_run=args.dry_run,
                )
                total_rows += rows
            except Exception as e:
                logger.error("  %s: FAILED - %s", table_name, e)
                local_conn.rollback()
                failed.append(table_name)
    finally:
        remote_conn.close()
        local_conn.close()
        remote_engine.dispose()
        local_engine.dispose()

    elapsed = time.time() - t_start
    logger.info("=" * 60)
    logger.info("Sync complete: %s rows in %.1f s", f"{total_rows:,}", elapsed)
    if failed:
        logger.warning("Failed tables: %s", ", ".join(failed))
    logger.info(
        "Use local DB: python src/backtesting/mlb/run_mlb_sweep.py --local ..."
    )


if __name__ == "__main__":
    main()
