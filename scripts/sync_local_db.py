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
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import ForeignKeyConstraint, MetaData, create_engine, inspect
from sqlalchemy.pool import NullPool

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
ID_WINDOW_TABLE = "raw_game_lines_staging"
MAX_ID_WINDOW_SPAN = 100_000
NBA_SUPPORT_TABLES = {
    name: ("report_date" if name == "rapidapi_injuries" else config[0])
    for name, config in NBA_TABLES.items()
    if name not in {"raw_player_props_combined", "raw_game_lines_staging"}
}
NBA_SUPPORT_STATE_FILE = Path("logs/sync/nba_support_sync.json")
NBA_SUPPORT_MAX_ROWS = 100_000


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
    if state.get("mode") == "nba_support":
        lines.append(
            f"Operation: {state.get('operation')} | Inclusive window: {state.get('start_date')}"
            f" through {state.get('end_date')} | Row budget/table: {state.get('max_table_rows')}"
        )
        lines.append(
            f"Confirmed committed rows: {state.get('total_rows_committed', 0)} | "
            f"Readback-verified rows: {state.get('total_rows_verified', 0)}"
        )
        for table in tables:
            lines.append(
                f"{table['table']}: {table.get('scope', 'preflight incomplete')}; "
                f"commit={table.get('commit_outcome')}; verified={table.get('rows_verified', 0)}"
            )
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


def validate_id_window_args(args: argparse.Namespace) -> None:
    """Reject unsafe or ambiguous ID-window requests before database actions."""
    if not getattr(args, "id_window", False):
        if getattr(args, "start_after_id", None) is not None or getattr(args, "end_id", None) is not None:
            raise ValueError("--start-after-id/--end-id require --id-window")
        if getattr(args, "replay_id_span", 10_000) != 10_000:
            raise ValueError("--replay-id-span requires --id-window")
        return

    if args.tables != [ID_WINDOW_TABLE]:
        raise ValueError(f"--id-window requires exactly --tables {ID_WINDOW_TABLE}")
    if args.full:
        raise ValueError("--id-window cannot be combined with --full")
    if args.start_date or args.end_date:
        raise ValueError("--id-window cannot be combined with date bounds")
    has_start = args.start_after_id is not None
    has_end = args.end_id is not None
    if has_start != has_end:
        raise ValueError("--start-after-id and --end-id must be supplied together")
    if args.replay_id_span < 0:
        raise ValueError("--replay-id-span must be nonnegative")
    if has_start:
        if args.start_after_id <= 0 or args.end_id <= 0:
            raise ValueError("explicit ID bounds must be positive")
        if args.end_id <= args.start_after_id:
            raise ValueError("--end-id must be greater than --start-after-id")
        if args.end_id - args.start_after_id > MAX_ID_WINDOW_SPAN:
            raise ValueError(
                f"ID window exceeds {MAX_ID_WINDOW_SPAN:,}; choose an explicit owner-bounded window"
            )


def _database_identity(url: str) -> tuple[str, int | None, str]:
    parts = urlsplit(url)
    return ((parts.hostname or "").lower(), parts.port, parts.path.rstrip("/").lower())


def _is_loopback_host(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        address = ip_address(host)
    except ValueError:
        return False
    return (getattr(address, "ipv4_mapped", None) or address).is_loopback


def validate_id_window_routing(remote_url: str, local_url: str) -> None:
    """Require a non-loopback source and a distinct loopback destination."""
    remote_identity = _database_identity(remote_url)
    local_identity = _database_identity(local_url)
    if not remote_identity[0] or _is_loopback_host(remote_identity[0]):
        raise ValueError("ID-window remote DATABASE_URL must be non-loopback canonical storage")
    if not _is_loopback_host(local_identity[0]):
        raise ValueError("ID-window LOCAL_DATABASE_URL must use localhost/loopback")
    if remote_identity == local_identity:
        raise ValueError("ID-window remote and local database identities must be distinct")


def _connected_database_identity(conn) -> tuple[str, int, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT host(inet_server_addr()), inet_server_port(), current_database()")
        row = cur.fetchone()
    if not row or any(value is None for value in row):
        raise ValueError("Cannot verify the connected database endpoint")
    address = ip_address(str(row[0]))
    address = getattr(address, "ipv4_mapped", None) or address
    return str(address), int(row[1]), str(row[2])


def verify_connected_id_window_routing(remote_conn, local_conn) -> None:
    """Verify actual server identities before any target mutation."""
    remote_identity = _connected_database_identity(remote_conn)
    local_identity = _connected_database_identity(local_conn)
    if ip_address(remote_identity[0]).is_loopback:
        raise ValueError("Connected ID-window source must be non-loopback canonical storage")
    if not ip_address(local_identity[0]).is_loopback:
        raise ValueError("Connected ID-window destination must be loopback local storage")
    if remote_identity == local_identity:
        raise ValueError("Connected ID-window source and destination identities must be distinct")


def get_table_contract(conn, table_name: str) -> tuple[list[tuple[str, str, bool]], list[str]]:
    """Return ordered column type/nullability contract and ordered primary key."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        if not cur.fetchone()[0]:
            raise ValueError(f"required existing table public.{table_name} is missing")
        cur.execute(
            """
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (table_name,),
        )
        columns = [(str(name), str(pg_type), bool(not_null)) for name, pg_type, not_null in cur.fetchall()]
    return columns, get_primary_key_columns(conn, table_name)


def verify_id_window_contract(remote_conn, local_conn) -> None:
    remote_columns, remote_pk = get_table_contract(remote_conn, ID_WINDOW_TABLE)
    local_columns, local_pk = get_table_contract(local_conn, ID_WINDOW_TABLE)
    if remote_columns != local_columns:
        raise ValueError("remote/local raw_game_lines_staging column type/nullability contract differs")
    if remote_pk != ["staging_id"] or local_pk != ["staging_id"]:
        raise ValueError("raw_game_lines_staging primary key must be exactly staging_id on both databases")


def get_id_extents(conn, table_name: str) -> tuple[int | None, int | None]:
    with conn.cursor() as cur:
        cur.execute(f'SELECT MIN("staging_id"), MAX("staging_id") FROM "{table_name}"')
        row = cur.fetchone()
    return (int(row[0]), int(row[1])) if row and row[0] is not None else (None, None)


def resolve_id_window(remote_conn, local_conn, args: argparse.Namespace) -> tuple[int, int] | None:
    """Freeze one bounded source ID interval without selecting outside it."""
    source_min, source_max = get_id_extents(remote_conn, ID_WINDOW_TABLE)
    if source_min is None or source_max is None:
        logger.info("  %s: source empty; nothing to sync", ID_WINDOW_TABLE)
        return None
    if args.start_after_id is not None:
        lower, upper = int(args.start_after_id), int(args.end_id)
        if lower < source_min - 1:
            raise ValueError(f"--start-after-id cannot be below source minimum minus one ({source_min - 1})")
    else:
        _, local_max = get_id_extents(local_conn, ID_WINDOW_TABLE)
        local_watermark = local_max if local_max is not None else source_min - 1
        lower = max(source_min - 1, local_watermark - args.replay_id_span)
        upper = source_max
    if upper > source_max:
        raise ValueError(f"--end-id {upper} exceeds frozen source maximum {source_max}")
    if upper <= lower:
        logger.info("  %s: no pending/replay ID interval (%s, %s]", ID_WINDOW_TABLE, lower, upper)
        return None
    span = upper - lower
    if span > MAX_ID_WINDOW_SPAN:
        raise ValueError(
            f"frozen ID gap {span:,} exceeds {MAX_ID_WINDOW_SPAN:,}; rerun with explicit "
            "--start-after-id and --end-id owner-bounded windows"
        )
    return lower, upper


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
    id_bounds: tuple[int, int] | None = None,
) -> dict[str, Any]:
    """Sync a single table from remote to local using COPY.

    Returns a compact execution record suitable for persisting to the sync journal.
    """
    t0 = time.time()

    where_clause = ""
    where_params: dict[str, Any] = {}
    mode = "full"
    pre_local_max = None
    partition_col = date_col

    if id_bounds is not None:
        lower_id, upper_id = id_bounds
        where_clause = '"staging_id" > %(lower_id)s AND "staging_id" <= %(upper_id)s'
        where_params = {"lower_id": lower_id, "upper_id": upper_id}
        mode = "id_window"
        partition_col = "staging_id"
        pre_local_max = lower_id
    elif strategy == "incremental" and date_col and not force_full:
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
            if partition_col and count:
                bounds_sql = f'SELECT MIN("{partition_col}"), MAX("{partition_col}") FROM "{table_name}"'
                if where_clause:
                    bounds_sql += f" WHERE {where_clause}"
                cur.execute(bounds_sql, where_params)
                min_value, max_value = cur.fetchone()
        except Exception:
            remote_conn.rollback()
            if id_bounds is not None:
                raise
            count = None

    if dry_run:
        logger.info(
            "  %s: dry-run would sync %s rows (%s) where=%s params=%s min_%s=%s max_%s=%s",
            table_name,
            f"{count:,}" if count is not None else "?",
            mode,
            where_clause or "<none>",
            where_params,
            partition_col or "date",
            min_value,
            partition_col or "date",
            max_value,
        )
        elapsed = time.time() - t0
        return _make_table_result(
            table_name=table_name,
            date_column=partition_col,
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

    if count == 0 and mode in {"incremental", "id_window"}:
        logger.info("  %s: up to date", table_name)
        elapsed = time.time() - t0
        return _make_table_result(
            table_name=table_name,
            date_column=partition_col,
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
                date_column=partition_col,
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
    if partition_col:
        try:
            local_max = get_local_max_date(local_conn, table_name, partition_col)
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
        partition_col or "date",
        min_value,
        partition_col or "date",
        max_value,
        partition_col or "date",
        local_max,
    )
    return _make_table_result(
        table_name=table_name,
        date_column=partition_col,
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


def validate_nba_support_args(args: argparse.Namespace) -> None:
    """Validate the opt-in replay before environment loading, connections or writes."""
    if not args.nba_support:
        if args.execute:
            raise ValueError("--execute requires --nba-support")
        if args.max_table_rows is not None:
            raise ValueError("--max-table-rows requires --nba-support")
        return
    if (
        args.full or args.id_window or args.allow_unknown_full_refresh
        or args.allow_large_full_refresh or args.start_after_id is not None
        or args.end_id is not None or args.replay_id_span is not None
        or args.sport not in {"all", "nba"}
    ):
        raise ValueError("--nba-support cannot use full-refresh, ID-window or MLB options")
    if args.execute and args.dry_run:
        raise ValueError("--execute and --dry-run are mutually exclusive")
    if args.max_table_rows is None:
        args.max_table_rows = 50_000
    if not 1 <= args.max_table_rows <= NBA_SUPPORT_MAX_ROWS:
        raise ValueError("--max-table-rows must be between 1 and 100000")
    if args.status:
        return
    if not args.tables or len(set(args.tables)) != len(args.tables):
        raise ValueError("--nba-support requires an explicit, duplicate-free --tables list")
    if any(name not in NBA_SUPPORT_TABLES for name in args.tables):
        raise ValueError("--nba-support accepts only registered NBA support tables, not raw odds or MLB")
    bounds = []
    for value in (args.start_date, args.end_date):
        try:
            parsed = date.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError("--nba-support requires concrete YYYY-MM-DD start and end dates") from None
        if parsed.isoformat() != value:
            raise ValueError("--nba-support dates must be exactly YYYY-MM-DD, without time components")
        bounds.append(parsed)
    if not 0 <= (bounds[1] - bounds[0]).days < 31 or bounds[1] == date.max:
        raise ValueError("--nba-support requires an ordered inclusive window of at most 31 days with a following midnight")


def _support_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_nba_support_selection(
    table_name: str, columns: list[str], start_date: str, end_date: str, budget: int,
) -> tuple[str, dict[str, Any], str]:
    """Explicit replay selection; never consult the local watermark or created_at."""
    if table_name not in NBA_SUPPORT_TABLES or not 1 <= budget <= NBA_SUPPORT_MAX_ROWS:
        raise ValueError("Invalid NBA support selection")
    col_list = ", ".join(f's.{_support_identifier(name)}' for name in columns)
    select = f'SELECT {col_list} FROM public.{_support_identifier(table_name)} AS s'
    params: dict[str, Any] = {"row_limit": budget + 1}
    date_col = NBA_SUPPORT_TABLES[table_name]
    if date_col or table_name == "player_game_advanced_stats":
        params.update(
            start_date=date.fromisoformat(start_date),
            end_exclusive=date.fromisoformat(end_date) + timedelta(days=1),
        )
        if date_col:
            select += (
                f' WHERE s.{_support_identifier(date_col)} >= %(start_date)s'
                f' AND s.{_support_identifier(date_col)} < %(end_exclusive)s'
            )
            scope = f"date window on {date_col}"
        else:
            select += (
                ' WHERE EXISTS (SELECT 1 FROM public."player_game_stats" AS g'
                ' WHERE g."player_id" = s."player_id" AND g."game_id" = s."game_id"'
                ' AND g."game_date" >= %(start_date)s AND g."game_date" < %(end_exclusive)s)'
            )
            scope = "exact player_id/game_id EXISTS in canonical player_game_stats date window"
    else:
        scope = "full reference key set (row-budgeted UPSERT; date bounds do not filter this table)"
    return select + " LIMIT %(row_limit)s", params, scope


def get_nba_support_contract(conn, table_name: str) -> tuple[list[tuple], list[str]]:
    """Inspect existing public relations, including identity/generated restrictions."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                   a.attnotnull, a.attidentity, a.attgenerated
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s AND c.relkind IN ('r', 'p')
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (table_name,),
        )
        columns = list(cur.fetchall())
    if not columns or any(col[4] or col[3] not in {"", "a", "d"} for col in columns):
        raise ValueError("Missing support table or unsupported generated/identity columns")
    pk = get_primary_key_columns(conn, f"public.{_support_identifier(table_name)}")
    if not pk or not set(pk).issubset({col[0] for col in columns}):
        raise ValueError("Support table requires an existing nonempty primary key")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indisvalid AND indisready AND indimmediate FROM pg_catalog.pg_index"
            " WHERE indrelid = %s::regclass AND indisprimary",
            (f"public.{_support_identifier(table_name)}",),
        )
        if cur.fetchone() != (True,):
            raise ValueError("Support primary key must be valid, ready and nondeferrable")
    return columns, pk


def _nba_support_match_count(cur, table_name: str, stage: str, pk: list[str]) -> int:
    keys = " AND ".join(f't.{_support_identifier(c)} = s.{_support_identifier(c)}' for c in pk)
    cur.execute(
        f'SELECT COUNT(*) FROM pg_temp.{_support_identifier(stage)} AS s'
        f' JOIN public.{_support_identifier(table_name)} AS t ON {keys}'
        ' WHERE pg_catalog.to_jsonb(t) IS NOT DISTINCT FROM pg_catalog.to_jsonb(s)'
    )
    row = cur.fetchone()
    if not row or type(row[0]) is not int:
        raise ValueError("Unknown local stage/value match count")
    return row[0]


def execute_nba_support_table(remote_conn, local_conn, plan: dict, result: dict, checkpoint) -> None:
    """One keyed transaction, verified before commit and again against preserved stage."""
    table_name = result["table"]
    stage = "_nba_support_stage"
    qualified_stage = f'pg_temp.{_support_identifier(stage)}'
    col_list = ", ".join(_support_identifier(c) for c in plan["columns"])
    pk = plan["pk"]
    conflict = ", ".join(_support_identifier(c) for c in pk)
    updates = [c for c in plan["columns"] if c not in pk]
    action = "DO UPDATE SET " + ", ".join(
        f'{_support_identifier(c)} = EXCLUDED.{_support_identifier(c)}' for c in updates
    ) if updates else "DO NOTHING"
    override = " OVERRIDING SYSTEM VALUE" if plan["identity_always"] else ""
    expected = result["rows_selected"]
    try:
        with tempfile.TemporaryFile(mode="w+b") as tmp:
            with remote_conn.cursor() as cur:
                query = cur.mogrify(plan["select"], plan["params"]).decode("utf-8")
                cur.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", tmp)
                if cur.rowcount != expected:
                    raise ValueError("Selected/exported row counts differ or export count is unknown")
                result["rows_exported"] = cur.rowcount
            tmp.seek(0)
            with local_conn.cursor() as cur:
                cur.execute(
                    f'CREATE TEMP TABLE {_support_identifier(stage)} ON COMMIT PRESERVE ROWS AS'
                    f' SELECT {col_list} FROM public.{_support_identifier(table_name)} WHERE false'
                )
                cur.copy_expert(f"COPY {qualified_stage} ({col_list}) FROM STDIN WITH CSV HEADER", tmp)
                if cur.rowcount != expected:
                    raise ValueError("Selected/copied row counts differ or COPY count is unknown")
                result["rows_copied"] = cur.rowcount
                cur.execute(f"SELECT COUNT(*) FROM {qualified_stage}")
                row = cur.fetchone()
                if not row or type(row[0]) is not int or row[0] != expected:
                    raise ValueError("Selected/staged row counts differ or staged count is unknown")
                result["rows_staged"] = row[0]
                cur.execute(
                    f'INSERT INTO public.{_support_identifier(table_name)} ({col_list}){override}'
                    f' SELECT {col_list} FROM {qualified_stage} WHERE true'
                    f' ON CONFLICT ({conflict}) {action}'
                )
                if _nba_support_match_count(cur, table_name, stage, pk) != expected:
                    raise ValueError("Local keys/values differ from selected stage before commit")
            # A durable prepared receipt is ambiguous after process loss, but a
            # checkpoint exception here proves the data commit was not attempted.
            result.update(status="prepared", commit_outcome="pending", rows_committed=None)
            try:
                checkpoint()
            except Exception:
                result.update(status="failed", commit_outcome="not_attempted", rows_committed=0)
                raise
            result.update(status="committing", commit_outcome="unknown")
            local_conn.commit()
            result.update(commit_outcome="confirmed", rows_committed=expected, status="committed_unverified")
            checkpoint()
            with local_conn.cursor() as cur:
                if _nba_support_match_count(cur, table_name, stage, pk) != expected:
                    raise ValueError("Committed local keys/values differ from selected stage")
            result.update(status="success", rows_verified=expected)
    finally:
        # A commit/readback failure cannot undo an earlier commit. Keep its outcome
        # in the receipt; rollback only the current transaction, then remove stage.
        local_conn.rollback()
        with local_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
        local_conn.commit()


def run_nba_support(args: argparse.Namespace, state_path: Path) -> int:
    """Isolated support replay: all-table SELECT preflight precedes any DB writes."""
    if state_path.resolve() == DEFAULT_STATE_FILE.resolve():
        logger.error("NBA support mode refuses the generic sync journal")
        return 2
    if state_path.exists():
        # Preserve even completed receipts; never overwrite an unrelated/recovery journal.
        logger.error("NBA support journal already exists; archive it or choose a new --state-file")
        return 2
    remote_url = os.getenv("DATABASE_URL")
    local_url = os.getenv("LOCAL_DATABASE_URL")
    if not remote_url or not local_url:
        logger.error("NBA support mode requires explicit DATABASE_URL and LOCAL_DATABASE_URL in the environment")
        return 2
    try:
        if any(urlsplit(url).scheme not in {"postgres", "postgresql"} for url in (remote_url, local_url)):
            raise ValueError("Unsupported connection URL")
        validate_id_window_routing(remote_url, local_url)
    except Exception:
        logger.error("NBA support routing requires a non-loopback source and distinct loopback target URLs")
        return 2
    state = _build_initial_state("nba", False, args.start_date, args.end_date, {})
    state.update(
        mode="nba_support", operation="execute" if args.execute else "preview",
        tables_planned=args.tables, max_table_rows=args.max_table_rows,
        total_rows_committed=0, total_rows_verified=0,
        note="Diagnostic replay receipt, not a keyset-resume checkpoint; local-only rows are retained",
        phase="connect",
    )
    remote_conn = local_conn = None
    active_result = None

    def checkpoint() -> None:
        state["total_rows_committed"] = sum(r.get("rows_committed") or 0 for r in state["tables"])
        state["total_rows_verified"] = sum(r.get("rows_verified") or 0 for r in state["tables"])
        state["total_rows"] = state["total_rows_committed"]
        write_sync_state(state, state_path)

    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive reservation prevents two invocations from replacing a receipt.
        with state_path.open("x", encoding="utf-8") as journal:
            json.dump(state, journal, indent=2)
        kwargs = {
            "connect_timeout": 10,
            "options": (
                "-c default_transaction_read_only=on -c statement_timeout=30000"
                " -c lock_timeout=3000 -c idle_in_transaction_session_timeout=60000"
                " -c search_path=pg_catalog,public -c timezone=UTC -c datestyle=ISO,YMD"
                " -c client_encoding=UTF8"
            ),
            "application_name": "gameflow:sync_local_db:nba_support",
        }
        remote_conn = psycopg2.connect(remote_url, **kwargs)
        remote_conn.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)
        local_conn = psycopg2.connect(local_url, **kwargs)
        local_conn.set_session(readonly=True, autocommit=False)
        verify_connected_id_window_routing(remote_conn, local_conn)
        plans = []
        logger.info("NBA support %s: %s through %s (inclusive); budget=%s/table",
                    state["operation"], args.start_date, args.end_date, args.max_table_rows)
        for table_name in args.tables:
            active_result = {
                "table": table_name, "mode": "nba_support", "strategy": "keyed_upsert",
                "status": "preflight", "rows_selected": None, "rows_copied": 0,
                "rows_committed": 0, "rows_verified": 0, "commit_outcome": "not_attempted",
            }
            state["tables"].append(active_result)
            state["phase"] = "schema_preflight"
            source_columns, source_pk = get_nba_support_contract(remote_conn, table_name)
            target_columns, target_pk = get_nba_support_contract(local_conn, table_name)
            if source_columns != target_columns or source_pk != target_pk:
                raise ValueError("Remote/local ordered columns, types, nullability, identity or primary keys differ")
            columns = [c[0] for c in source_columns]
            select, params, scope = build_nba_support_selection(
                table_name, columns, args.start_date, args.end_date, args.max_table_rows,
            )
            active_result["scope"] = scope
            state["phase"] = "bounded_count"
            with remote_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM ({select}) AS bounded_selection", params)
                row = cur.fetchone()
            if not row or type(row[0]) is not int or not 0 <= row[0] <= args.max_table_rows:
                logger.error("NBA support bounded count failed or exceeded budget for public.%s", table_name)
                raise ValueError("Unknown selection count or per-table row budget exceeded")
            active_result.update(rows_selected=row[0], scope=scope, status="ready")
            plans.append({
                "columns": columns, "pk": source_pk, "select": select, "params": params,
                "identity_always": any(c[3] == "a" for c in target_columns),
            })
            logger.info("  public.%s: %s rows; %s", table_name, row[0], scope)
        checkpoint()
        if not args.execute:
            for result in state["tables"]:
                result["status"] = "preview"
            state.update(status="preview", phase="complete", finished_at=_utc_now())
            checkpoint()
            logger.info("NBA support preview complete; no database mutations")
            return 0
        # End the SELECT-only target preflight before enabling explicit write transactions.
        local_conn.rollback()
        local_conn.set_session(readonly=False, autocommit=False)
        state["phase"] = "table_execute_and_readback"
        for plan, result in zip(plans, state["tables"], strict=True):
            active_result = result
            result["status"] = "executing"
            checkpoint()
            execute_nba_support_table(remote_conn, local_conn, plan, result, checkpoint)
            checkpoint()
            logger.info("  public.%s: committed and read back %s selected rows", result["table"], result["rows_verified"])
        state.update(status="success", phase="complete", finished_at=_utc_now())
        checkpoint()
        return 0
    except FileExistsError:
        logger.error("NBA support journal reservation failed; existing receipt was preserved")
        return 2
    except Exception as exc:
        # Driver messages/tracebacks may contain DSNs, passwords or data. Never emit them.
        if active_result is not None:
            if active_result["status"] == "success":
                active_result["status"] = "committed_verified_error"
            elif active_result["commit_outcome"] == "not_attempted":
                active_result["status"] = "failed"
            elif active_result["commit_outcome"] == "unknown":
                active_result["status"] = "commit_unknown"
            else:
                active_result["status"] = "committed_unverified"
            state["failed_tables"] = [active_result["table"]]
        state.update(status="failed", finished_at=_utc_now(), error=f"NBA support aborted ({type(exc).__name__}); no exception details logged")
        try:
            checkpoint()
        except Exception:
            logger.error("Unable to update NBA support diagnostic receipt")
        logger.error("NBA support aborted; confirmed prior commits=%s rows; inspect receipt before replay", state["total_rows_committed"])
        return 1
    finally:
        for conn in (remote_conn, local_conn):
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


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
    parser.add_argument("--nba-support", action="store_true", help="Bounded NBA support-table replay; DB-read-only preview unless --execute")
    parser.add_argument("--execute", action="store_true", help="Write selected NBA support rows (requires --nba-support)")
    parser.add_argument("--max-table-rows", type=int, help="NBA support per-table row budget (default 50000; cap 100000)")
    parser.add_argument("--id-window", action="store_true", help="Sync one bounded staging_id window for raw_game_lines_staging")
    parser.add_argument("--start-after-id", type=int, help="Exclusive staging_id lower bound (requires --end-id)")
    parser.add_argument("--end-id", type=int, help="Inclusive frozen staging_id upper bound")
    parser.add_argument("--replay-id-span", type=int, help="Derived-mode rewind from local MAX(staging_id), default 10000")
    parser.add_argument("--allow-unknown-full-refresh", action="store_true", help="Allow unknown --tables to fall back to full refresh")
    parser.add_argument("--allow-large-full-refresh", action="store_true", help="Allow unbounded full refresh of very large tables such as dense CLV snapshots")
    parser.add_argument(
        "--state-file",
        default=None,
        help="Path to sync state JSON journal",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print persisted sync state and exit",
    )
    args = parser.parse_args()

    try:
        validate_nba_support_args(args)
    except ValueError as exc:
        parser.error(str(exc))
    state_path = Path(args.state_file) if args.state_file is not None else (
        NBA_SUPPORT_STATE_FILE if args.nba_support else DEFAULT_STATE_FILE
    )

    if args.status:
        sys.exit(print_sync_status(state_path))

    if args.nba_support:
        # Match the existing operator credential flow only after argument/status
        # validation; import, help and status remain environment-file independent.
        load_dotenv()
        sys.exit(run_nba_support(args, state_path))

    # Never load environment files during import, help or status.
    load_dotenv()
    if args.replay_id_span is None:
        args.replay_id_span = 10_000
    try:
        validate_id_window_args(args)
    except ValueError as exc:
        parser.error(str(exc))

    sync_state = _build_initial_state(args.sport, args.full, args.start_date, args.end_date, {})
    if args.id_window:
        sync_state["mode"] = "id_window"
    write_sync_state(sync_state, state_path)

    remote_url = os.getenv("DATABASE_URL")
    if not remote_url:
        message = "DATABASE_URL not set. Add it to your .env file."
        logger.error(message)
        sync_state.update(status="failed", finished_at=_utc_now(), error=message)
        write_sync_state(sync_state, state_path)
        sys.exit(1)

    local_url = get_local_url()

    if args.id_window:
        try:
            validate_id_window_routing(remote_url, local_url)
        except ValueError as exc:
            logger.error(str(exc))
            sync_state.update(status="failed", finished_at=_utc_now(), error=str(exc))
            write_sync_state(sync_state, state_path)
            sys.exit(2)

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
        if args.id_window:
            remote_conn = psycopg2.connect(remote_url, **remote_connect_kwargs())
            remote_conn.set_session(readonly=True, autocommit=False)
            id_local_kwargs = local_connect_kwargs()
            id_local_kwargs["options"] = (
                "-c statement_timeout=1800000 -c idle_in_transaction_session_timeout=60000"
            )
            local_conn = psycopg2.connect(local_url, **id_local_kwargs)
            if args.dry_run:
                local_conn.set_session(readonly=True, autocommit=False)
            verify_connected_id_window_routing(remote_conn, local_conn)
            verify_id_window_contract(remote_conn, local_conn)
            id_bounds = resolve_id_window(remote_conn, local_conn, args)
            if id_bounds is None:
                sync_state.update(
                    status="dry_run" if args.dry_run else "success",
                    finished_at=_utc_now(),
                    total_rows=0,
                )
                write_sync_state(sync_state, state_path)
                logger.info("ID-window sync complete: no rows selected")
                return
            logger.info("Mode: ID WINDOW (%s, %s]", id_bounds[0], id_bounds[1])
        else:
            id_bounds = None
            # Preserve legacy automatic database/schema initialization.
            ensure_local_db()
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
            logger.info("Checking schemas...")
            for table_name in tables:
                ensure_table_schema(remote_engine, local_engine, table_name)
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
                    id_bounds=id_bounds,
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
                    mode="id_window" if args.id_window else ("full" if args.full else ("window" if (args.start_date or args.end_date) else "incremental")),
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
