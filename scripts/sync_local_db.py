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

DEFAULT_LOCAL_URL = "postgresql://postgres:postgres@localhost:5432/gameflow_local"


def get_local_url() -> str:
    return os.getenv("LOCAL_DATABASE_URL", DEFAULT_LOCAL_URL)


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
) -> int:
    """Sync a single table from remote to local using COPY."""
    t0 = time.time()

    where_clause = ""
    mode = "full"

    if strategy == "incremental" and date_col and not force_full:
        max_date = get_local_max_date(local_conn, table_name, date_col)
        if max_date is not None:
            where_clause = f"\"{date_col}\" > '{max_date}'"
            mode = "incremental"

    # Count rows to sync
    count_sql = f'SELECT COUNT(*) FROM "{table_name}"'
    if where_clause:
        count_sql += f" WHERE {where_clause}"
    with remote_conn.cursor() as cur:
        try:
            cur.execute(count_sql)
            count = cur.fetchone()[0]
        except Exception:
            remote_conn.rollback()
            count = None

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
                copy_sql = (
                    f'COPY (SELECT * FROM "{table_name}" WHERE {where_clause}) '
                    f"TO STDOUT WITH CSV HEADER"
                )
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

        # Import to local
        with local_conn.cursor() as cur:
            col_list = ", ".join(f'"{c}"' for c in columns)
            copy_in_sql = (
                f'COPY "{table_name}" ({col_list}) FROM STDIN WITH CSV HEADER'
            )
            cur.copy_expert(copy_in_sql, tmp)

        local_conn.commit()

    elapsed = time.time() - t0
    size_mb = file_size / (1024 * 1024)
    rows = count or 0
    logger.info(
        "  %s: done (%s rows, %.1f MB, %.1f s)", table_name, count_str, size_mb, elapsed
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
    args = parser.parse_args()

    remote_url = os.getenv("DATABASE_URL")
    if not remote_url:
        logger.error("DATABASE_URL not set. Add it to your .env file.")
        sys.exit(1)

    local_url = get_local_url()

    # Build table list
    tables: dict[str, tuple[str | None, str]] = {}
    if args.tables:
        all_configs = {**MLB_TABLES, **NBA_TABLES}
        for t in args.tables:
            tables[t] = all_configs.get(t, (None, "full"))
    else:
        if args.sport in ("mlb", "all"):
            tables.update(MLB_TABLES)
        if args.sport in ("nba", "all"):
            tables.update(NBA_TABLES)

    logger.info("Syncing %d tables to local Postgres", len(tables))
    logger.info("Remote: %s...", remote_url[:50])
    logger.info("Local:  %s", local_url)
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
                    remote_conn, local_conn, table_name, date_col, strategy, args.full
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
