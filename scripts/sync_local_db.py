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
import json
import logging
import os
import sys
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import ForeignKeyConstraint, MetaData, create_engine, inspect
from sqlalchemy.pool import NullPool

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
    "mlb_active_roster":                     ("roster_date", "incremental"),
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
SYNC_STATE_SCHEMA_VERSION = 1
DEFAULT_STATE_FILE = Path("logs/sync/sync_state.json")


def _utc_now() -> str:
    """Return UTC timestamp as a compact ISO string."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _serialize_partition(value: Any | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime) or isinstance(value, date):
        return _to_iso(value)
    return str(value)


def _format_int_or_none(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:,}"


def _make_table_result(
    *,
    table_name: str,
    date_column: str | None,
    strategy: str,
    mode: str,
    rows_selected: int | None,
    rows_copied: int | None,
    remote_min_partition: Any | None,
    remote_max_partition: Any | None,
    local_max_partition_after_sync: Any | None,
    duration_seconds: float,
    status: str,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "table": table_name,
        "date_column": date_column,
        "strategy": strategy,
        "mode": mode,
        "rows_selected": rows_selected,
        "rows_copied": rows_copied,
        "remote_min_partition": _serialize_partition(remote_min_partition),
        "remote_max_partition": _serialize_partition(remote_max_partition),
        "local_max_partition": _serialize_partition(local_max_partition_after_sync),
        "duration_seconds": round(duration_seconds, 3),
        "status": status,
        "error": error,
    }


def _build_initial_state(sport: str, full: bool, start_date: str | None, end_date: str | None, tables: dict[str, tuple[str | None, str]]) -> dict[str, Any]:
    if full:
        mode = "full"
    elif start_date or end_date:
        mode = "window"
    else:
        mode = "incremental"

    return {
        "schema_version": SYNC_STATE_SCHEMA_VERSION,
        "started_at": _utc_now(),
        "finished_at": None,
        "status": "running",
        "mode": mode,
        "sport": sport,
        "failed_tables": [],
        "start_date": start_date,
        "end_date": end_date,
        "tables_planned": list(tables.keys()),
        "tables": [],
    }


def write_sync_state(state: dict[str, Any], state_path: str | Path) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def load_sync_state(state_path: str | Path) -> dict[str, Any]:
    path = Path(state_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def render_sync_state_table(state: dict[str, Any]) -> str:
    tables = state.get("tables", [])

    header = (
        f"Schema v{state.get('schema_version', 'n/a')} | "
        f"Status: {state.get('status', 'unknown')} | "
        f"Mode: {state.get('mode', 'unknown')} | "
        f"Sport: {state.get('sport', 'unknown')}"
    )
    started = state.get("started_at")
    finished = state.get("finished_at")
    if started or finished:
        header += f" | Started: {started or '-'} | Finished: {finished or '-'}"

    lines = [header, "-"]
    if tables:
        col_table = "{:<30}"
        col_status = "{:<12}"
        col_rows = "{:>11}"
        col_mode = "{:<11}"
        col_strategy = "{:<12}"
        col_time = "{:>9}"
        title = (
            f"{col_table.format('table'):30}"
            f"{col_status.format('status'):12}"
            f"{col_mode.format('mode'):11}"
            f"{col_strategy.format('strategy'):12}"
            f"{col_rows.format('rows'):>11}"
            f"{col_rows.format('copied'):>11}"
            f"{'min_partition':>22}"
            f"{'max_partition':>22}"
            f"{'local_max':>22}"
            f"{col_time.format('secs'):>9}"
        )
        lines.append(title)
        lines.append(" ".join(["-" * 30, "-" * 12, "-" * 11, "-" * 12, "-" * 11, "-" * 11, "-" * 22, "-" * 22, "-" * 22, "-" * 9]))
        for table in tables:
            row_status = str(table.get("status", ""))
            row_mode = str(table.get("mode", ""))
            row_strategy = str(table.get("strategy", ""))
            rows_selected = table.get("rows_selected")
            rows_copied = table.get("rows_copied")
            row = (
                f"{col_table.format(table.get('table', '')):30}"
                f"{col_status.format(row_status):12}"
                f"{col_mode.format(row_mode):11}"
                f"{col_strategy.format(row_strategy):12}"
                f"{col_rows.format(_format_int_or_none(rows_selected)):11}"
                f"{col_rows.format(_format_int_or_none(rows_copied)):11}"
                f"{str(table.get('remote_min_partition', '-') or '-'):>22}"
                f"{str(table.get('remote_max_partition', '-') or '-'):>22}"
                f"{str(table.get('local_max_partition', '-') or '-'):>22}"
                f"{str(col_time.format(table.get('duration_seconds', 0))):>9}"
            )
            lines.append(row)
    else:
        lines.append("No table summaries available")

    failed = state.get("failed_tables", [])
    lines.append(f"Failed tables: {', '.join(failed) if failed else 'none'}")
    return "\n".join(lines)


def print_sync_status(state_path: str | Path) -> int:
    try:
        state = load_sync_state(state_path)
    except FileNotFoundError:
        print(f"No sync journal found at {state_path}")
        print("Run scripts/sync_local_db.py without --status to create one.")
        return 1
    except json.JSONDecodeError:
        print(f"Unable to read sync journal at {state_path}: invalid JSON")
        return 1

    print(render_sync_state_table(state))
    return 0


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


def remote_connect_kwargs() -> dict:
    """Connection options for read-only Supabase export sessions."""
    return {
        "application_name": "gameflow:sync_local_db:remote_copy",
        "connect_timeout": 15,
        "options": "-c statement_timeout=1800000 -c idle_in_transaction_session_timeout=60000",
    }


def local_connect_kwargs() -> dict:
    return {
        "application_name": "gameflow:sync_local_db:local_copy",
        "connect_timeout": 15,
    }


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


def get_primary_key_columns(local_conn, table_name: str) -> list[str]:
    """Return local table primary-key columns in constraint order."""
    with local_conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
              AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def get_identity_always_columns(local_conn, table_name: str) -> set[str]:
    """Return GENERATED ALWAYS identity columns for local table."""
    with local_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND identity_generation = 'ALWAYS'
            """,
            (table_name,),
        )
        return {row[0] for row in cur.fetchall()}


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
) -> dict[str, Any]:
    """Sync a single table from remote to local using COPY.

    Returns a compact execution record suitable for persisting to the sync journal.
    """
    t0 = time.time()

    where_clause = ""
    where_params: dict[str, str] = {}
    mode = "full"
    pre_local_max = None

    if strategy == "incremental" and date_col and not force_full:
        pre_local_max = get_local_max_date(local_conn, table_name, date_col)
        where_clause, where_params = build_where_clause(
            date_col,
            start_date=start_date,
            end_date=end_date,
            incremental_max=pre_local_max,
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
        elapsed = time.time() - t0
        return _make_table_result(
            table_name=table_name,
            date_column=date_col,
            strategy=strategy,
            mode=mode,
            rows_selected=count,
            rows_copied=0,
            remote_min_partition=min_value,
            remote_max_partition=max_value,
            local_max_partition_after_sync=None,
            duration_seconds=elapsed,
            status="dry_run",
        )

    if count == 0 and mode == "incremental":
        logger.info("  %s: up to date", table_name)
        elapsed = time.time() - t0
        return _make_table_result(
            table_name=table_name,
            date_column=date_col,
            strategy=strategy,
            mode=mode,
            rows_selected=0,
            rows_copied=0,
            remote_min_partition=min_value,
            remote_max_partition=max_value,
            local_max_partition_after_sync=pre_local_max,
            duration_seconds=elapsed,
            status="up_to_date",
        )

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
            elapsed = time.time() - t0
            return _make_table_result(
                table_name=table_name,
                date_column=date_col,
                strategy=strategy,
                mode=mode,
                rows_selected=0,
                rows_copied=0,
                remote_min_partition=min_value,
                remote_max_partition=max_value,
                local_max_partition_after_sync=None,
                duration_seconds=elapsed,
                status="empty",
            )

        tmp.seek(0)

        # Read header to get column list (handles column ordering)
        header_line = tmp.readline().decode("utf-8").strip()
        columns = [c.strip('"') for c in header_line.split(",")]
        tmp.seek(0)

        # Import to local. Incremental/date-window syncs stage then upsert on the
        # actual primary key (including composite PKs) so historical gap repairs
        # update existing local rows instead of failing on duplicates. Staging via
        # CREATE TABLE AS avoids GENERATED ALWAYS identity restrictions during COPY.
        with local_conn.cursor() as cur:
            col_list = ", ".join(f'"{c}"' for c in columns)
            conflict_cols = get_primary_key_columns(local_conn, table_name)
            identity_always_cols = get_identity_always_columns(local_conn, table_name)
            insert_override = " OVERRIDING SYSTEM VALUE" if identity_always_cols.intersection(columns) else ""

            if mode != "full" and conflict_cols:
                temp_table = f"_sync_{table_name}_{int(time.time() * 1000)}"
                cur.execute(
                    f'CREATE TEMP TABLE "{temp_table}" ON COMMIT DROP AS SELECT * FROM "{table_name}" WHERE false'
                )
                copy_in_sql = f'COPY "{temp_table}" ({col_list}) FROM STDIN WITH CSV HEADER'
                cur.copy_expert(copy_in_sql, tmp)

                conflict_list = ", ".join(f'"{c}"' for c in conflict_cols)
                update_cols = [c for c in columns if c not in conflict_cols]
                if update_cols:
                    update_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
                    insert_sql = (
                        f'INSERT INTO "{table_name}" ({col_list}){insert_override} '
                        f'SELECT {col_list} FROM "{temp_table}" '
                        f'ON CONFLICT ({conflict_list}) DO UPDATE SET {update_sql}'
                    )
                else:
                    insert_sql = (
                        f'INSERT INTO "{table_name}" ({col_list}){insert_override} '
                        f'SELECT {col_list} FROM "{temp_table}" '
                        f'ON CONFLICT ({conflict_list}) DO NOTHING'
                    )
                cur.execute(insert_sql)
            else:
                copy_in_sql = f'COPY "{table_name}" ({col_list}) FROM STDIN WITH CSV HEADER'
                cur.copy_expert(copy_in_sql, tmp)

        local_conn.commit()

    elapsed = time.time() - t0
    size_mb = file_size / (1024 * 1024)
    rows = count or 0
    local_max = None
    if date_col:
        try:
            local_max = get_local_max_date(local_conn, table_name, date_col)
        except Exception as exc:
            logger.warning("  %s: copied successfully but local max lookup failed: %s", table_name, exc)
            try:
                local_conn.rollback()
            except Exception:
                pass
    logger.info(
        "  %s: done (%s rows, %.1f MB, %.1f s, min_%s=%s, max_%s=%s, local_max_%s=%s)",
        table_name,
        count_str,
        size_mb,
        elapsed,
        date_col or "date",
        min_value,
        date_col or "date",
        max_value,
        date_col or "date",
        local_max,
    )
    return _make_table_result(
        table_name=table_name,
        date_column=date_col,
        strategy=strategy,
        mode=mode,
        rows_selected=rows,
        rows_copied=rows,
        remote_min_partition=min_value,
        remote_max_partition=max_value,
        local_max_partition_after_sync=local_max,
        duration_seconds=elapsed,
        status="success",
    )


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
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_FILE),
        help="Path to sync state JSON journal",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print persisted sync state and exit",
    )
    args = parser.parse_args()

    state_path = Path(args.state_file)

    if args.status:
        sys.exit(print_sync_status(state_path))

    sync_state = _build_initial_state(args.sport, args.full, args.start_date, args.end_date, {})
    write_sync_state(sync_state, state_path)

    remote_url = os.getenv("DATABASE_URL")
    if not remote_url:
        message = "DATABASE_URL not set. Add it to your .env file."
        logger.error(message)
        sync_state.update(status="failed", finished_at=_utc_now(), error=message)
        write_sync_state(sync_state, state_path)
        sys.exit(1)

    local_url = get_local_url()

    # Build table list
    try:
        tables = build_table_plan(args)
    except ValueError as exc:
        logger.error(str(exc))
        sync_state.update(status="failed", finished_at=_utc_now(), error=str(exc))
        write_sync_state(sync_state, state_path)
        sys.exit(2)

    sync_state["tables_planned"] = list(tables.keys())
    write_sync_state(sync_state, state_path)

    large_unbounded = [
        table_name
        for table_name in tables
        if table_name == "mlb_player_props_clv_snapshots"
        and args.full
        and not (args.start_date or args.end_date)
        and not args.allow_large_full_refresh
    ]
    if large_unbounded:
        message = (
            "Refusing unbounded full refresh for large table(s): "
            f"{', '.join(large_unbounded)}. Use --start-date/--end-date or "
            "--allow-large-full-refresh."
        )
        logger.error(message)
        sync_state.update(status="failed", finished_at=_utc_now(), error=message)
        write_sync_state(sync_state, state_path)
        sys.exit(2)

    logger.info("Syncing %d tables to local Postgres", len(tables))
    logger.info("Remote: %s", redact_database_url(remote_url))
    logger.info("Local:  %s", redact_database_url(local_url))
    if args.full:
        logger.info("Mode:   FULL REFRESH")

    total_rows = 0
    table_results: list[dict[str, Any]] = []
    failed: list[str] = []
    remote_conn = None
    local_conn = None

    t_start = time.time()
    try:
        # Ensure local database exists
        ensure_local_db()

        # Create engines for schema reflection
        remote_engine = create_engine(
            remote_url,
            poolclass=NullPool,
            connect_args=remote_connect_kwargs(),
        )
        local_engine = create_engine(
            local_url,
            poolclass=NullPool,
            connect_args=local_connect_kwargs(),
        )

        # Create tables locally if needed
        logger.info("Checking schemas...")
        for table_name in tables:
            ensure_table_schema(remote_engine, local_engine, table_name)

        # Sync data using psycopg2 COPY
        remote_conn = psycopg2.connect(remote_url, **remote_connect_kwargs())
        remote_conn.autocommit = True
        local_conn = psycopg2.connect(local_url, **local_connect_kwargs())

        for table_name, (date_col, strategy) in tables.items():
            table_start = time.time()
            try:
                result = sync_table(
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
            except Exception as e:
                logger.error("  %s: FAILED - %s", table_name, e)
                if local_conn is not None:
                    local_conn.rollback()
                failed.append(table_name)
                safe_error = str(e).replace(remote_url, redact_database_url(remote_url)).replace(
                    local_url, redact_database_url(local_url)
                )
                result = _make_table_result(
                    table_name=table_name,
                    date_column=date_col,
                    strategy=strategy,
                    mode="full" if args.full else ("window" if (args.start_date or args.end_date) else "incremental"),
                    rows_selected=None,
                    rows_copied=0,
                    remote_min_partition=None,
                    remote_max_partition=None,
                    local_max_partition_after_sync=None,
                    duration_seconds=time.time() - table_start,
                    status="failed",
                    error=f"{type(e).__name__}: {safe_error.splitlines()[0]}",
                )

            table_results.append(result)
            if result.get("status") == "failed":
                if table_name not in failed:
                    failed.append(table_name)
            total_rows += int(result.get("rows_copied") or 0)
            sync_state["tables"] = table_results
            sync_state["failed_tables"] = failed
            sync_state["total_rows"] = total_rows
            write_sync_state(sync_state, state_path)

        elapsed = time.time() - t_start
        sync_state["status"] = "dry_run" if args.dry_run else ("failed" if failed else "success")
        sync_state["tables"] = table_results
        sync_state["failed_tables"] = failed
        sync_state["finished_at"] = _utc_now()
        sync_state["total_rows"] = total_rows
        sync_state["runtime_seconds"] = round(elapsed, 3)
        write_sync_state(sync_state, state_path)

        logger.info("=" * 60)
        logger.info("Sync complete: %s rows in %.1f s", f"{total_rows:,}", elapsed)
        if failed:
            logger.warning("Failed tables: %s", ", ".join(failed))
        logger.info("Use local DB: python src/backtesting/mlb/run_mlb_sweep.py --local ...")

        if failed:
            sys.exit(1)
        return
    except Exception as exc:
        safe_error = str(exc).replace(remote_url, redact_database_url(remote_url)).replace(
            local_url, redact_database_url(local_url)
        )
        sync_state["status"] = "failed"
        sync_state["tables"] = table_results
        sync_state["failed_tables"] = failed
        sync_state["finished_at"] = _utc_now()
        sync_state["total_rows"] = total_rows
        sync_state["runtime_seconds"] = round(time.time() - t_start, 3)
        sync_state["error"] = f"{type(exc).__name__}: {safe_error.splitlines()[0]}"
        write_sync_state(sync_state, state_path)
        logger.error("Sync aborted before completion: %s", exc)
        raise
    finally:
        if remote_conn is not None:
            remote_conn.close()
        if local_conn is not None:
            local_conn.close()
        if "remote_engine" in locals():
            remote_engine.dispose()
        if "local_engine" in locals():
            local_engine.dispose()


if __name__ == "__main__":
    main()
