#!/usr/bin/env python3
"""Safely link dense MLB CLV snapshots to GameFlow game_id/player_id.

This script replaces the unsafe full-table linker. It is safe-by-default:

- default mode is metadata-only preflight, no writes
- execution requires --execute and an explicit --max-batches value
- every write runs in a small transaction-scoped batch
- no regex normalization is applied across the full dense table
- no broad reports run automatically

Recommended first real run is a one-batch smoke test:

    python scripts/link_mlb_clv_snapshots.py --execute --max-batches 1 --batch-size 500 --only-games --skip-report

Do not run large batches/full linking until the one-batch runtime and row counts are reviewed.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.append(str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBCLVSnapshotLinker")

DEFAULT_TABLE = "mlb_player_props_clv_snapshots"
DEFAULT_ID_COLUMN = "id"
MAX_BATCHES_WITHOUT_LARGE_FLAG = 100
MAX_BATCH_SIZE_WITHOUT_LARGE_FLAG = 10_000
REQUIRED_LINK_COLUMNS = {
    "game_id": "INTEGER",
    "linked_player_name": "TEXT",
    "game_link_method": "TEXT",
    "player_link_method": "TEXT",
    "linked_at": "TIMESTAMPTZ",
}

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TEMP_FILE_LIMIT_RE = re.compile(r"^\d+(kB|MB|GB|TB)$", re.IGNORECASE)


def quote_ident(identifier: str) -> str:
    """Validate and quote a single SQL identifier.

    The linker builds table/column SQL from CLI args. Restricting identifiers to a
    conservative pattern prevents accidental SQL injection in operational scripts.
    """
    if not _IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def table_sql(table: str) -> str:
    return f"public.{quote_ident(table)}"


def norm_sql(expr: str) -> str:
    return f"regexp_replace(lower(coalesce({expr}, '')), '[^a-z0-9]+', '', 'g')"


def build_batch_id_sql(table: str, id_column: str, *, link_games: bool, link_players: bool) -> str:
    """Return bounded id-window selection SQL.

    Deliberately selects by monotonically increasing id only, not by
    game_id/player_id predicates. On the dense remote table there is no approved
    partial index for every unlinked predicate, so filtering while selecting can
    force long scans after earlier batches are linked. The UPDATE statements keep
    the game/player NULL predicates; this selector only defines a small id window.
    """
    _ = (link_games, link_players)  # kept for testable call shape / future predicates
    idq = quote_ident(id_column)
    return f"""
        SELECT {idq} AS id
        FROM {table_sql(table)}
        WHERE {idq} > :last_id
        ORDER BY {idq}
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    """


def build_game_update_sql(table: str, id_column: str) -> str:
    idq = quote_ident(id_column)
    return f"""
        UPDATE {table_sql(table)} c
           SET game_id = m.game_id,
               game_link_method = 'event_key_unique_commence_time:batched',
               linked_at = now()
          FROM tmp_mlb_clv_batch_ids b
          JOIN tmp_mlb_clv_event_map m
            ON true
         WHERE c.{idq} = b.id
           AND c.game_id IS NULL
           AND (
                (c.api_game_id IS NOT NULL AND c.api_game_id IS NOT DISTINCT FROM m.api_game_id)
             OR (c.odds_api_event_id IS NOT NULL AND c.odds_api_event_id IS NOT DISTINCT FROM m.odds_api_event_id)
           )
    """


def build_player_update_sql(table: str, id_column: str) -> str:
    idq = quote_ident(id_column)
    return f"""
        UPDATE {table_sql(table)} c
           SET player_id = p.player_id,
               linked_player_name = p.linked_name,
               player_link_method = 'normalized_name:mlb_players.player_name:batched',
               linked_at = now()
          FROM tmp_mlb_clv_batch_ids b
          JOIN tmp_mlb_clv_batch_player_keys k
            ON k.id = b.id
          JOIN tmp_mlb_clv_player_map p
            ON p.norm_name = k.norm_name
         WHERE c.{idq} = b.id
           AND c.player_id IS NULL
           AND c.api_player_name IS NOT NULL
    """


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safely link dense MLB CLV snapshots in bounded batches")
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--id-column", default=DEFAULT_ID_COLUMN)
    parser.add_argument("--database-url", default=None, help="Defaults to DATABASE_URL")
    parser.add_argument("--local", action="store_true", help="Use LOCAL_DATABASE_URL instead of DATABASE_URL")
    parser.add_argument("--mode", choices=["preflight", "run"], default="preflight")
    parser.add_argument("--execute", action="store_true", help="Alias for --mode run; still requires --max-batches")
    parser.add_argument("--max-batches", type=int, default=0, help="Required for --execute; use 1 for the first smoke test")
    parser.add_argument("--start-id", type=int, default=0, help="Resume id-window scan after this id; use prior max_id after interrupted runs")
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--allow-large-run", action="store_true", help="Allow >100 batches or batch size >10k after smoke testing")
    parser.add_argument("--only-games", action="store_true", help="Only run game linking")
    parser.add_argument("--only-players", action="store_true", help="Only run player linking")
    parser.add_argument("--commence-tolerance-seconds", type=int, default=900)
    parser.add_argument(
        "--second-pass-games",
        action="store_true",
        help="After strict game matching, allow a unique nearest same-team schedule match for still-unmapped event keys.",
    )
    parser.add_argument(
        "--fallback-game-tolerance-seconds",
        type=int,
        default=21_600,
        help="Max absolute schedule-time delta for --second-pass-games (default 6h).",
    )
    parser.add_argument(
        "--fallback-game-date-slop-days",
        type=int,
        default=1,
        help="Allow schedule game_time_utc date to differ from dense commence_time date by this many days in second pass.",
    )
    parser.add_argument("--statement-timeout-ms", type=int, default=60_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    parser.add_argument("--idle-timeout-ms", type=int, default=60_000)
    parser.add_argument("--temp-file-limit", default="1GB", help="Postgres temp_file_limit for the session, e.g. 512MB or 1GB")
    parser.add_argument("--require-temp-file-limit", action="store_true", help="Fail if the DB role cannot SET temp_file_limit")
    parser.add_argument("--sleep-seconds", type=float, default=0.25, help="Pause between batches to reduce pressure")
    parser.add_argument("--sample-rows", type=int, default=0, help="Optional preflight sample row count; default 0 avoids scanning the dense table")
    parser.add_argument("--report", default="backtest_results/audits/mlb_clv_snapshot_link_report.md")
    parser.add_argument("--skip-report", action="store_true", help="Skip post-run summary; recommended for smoke tests")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.execute:
        args.mode = "run"
    quote_ident(args.table)
    quote_ident(args.id_column)
    if args.only_games and args.only_players:
        raise SystemExit("Choose at most one of --only-games or --only-players")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    if args.start_id < 0:
        raise SystemExit("--start-id cannot be negative")
    if args.sample_rows < 0:
        raise SystemExit("--sample-rows cannot be negative")
    if args.fallback_game_tolerance_seconds <= 0:
        raise SystemExit("--fallback-game-tolerance-seconds must be positive")
    if args.fallback_game_date_slop_days < 0:
        raise SystemExit("--fallback-game-date-slop-days cannot be negative")
    if not _TEMP_FILE_LIMIT_RE.fullmatch(args.temp_file_limit):
        raise SystemExit("--temp-file-limit must look like 512MB, 1GB, or 2GB")
    if args.mode == "preflight":
        args.execute = False
        args.max_batches = 0
        return
    args.execute = True
    if args.max_batches <= 0:
        raise SystemExit("--execute requires explicit --max-batches. Start with --max-batches 1.")
    if args.max_batches > MAX_BATCHES_WITHOUT_LARGE_FLAG and not args.allow_large_run:
        raise SystemExit(f"--max-batches > {MAX_BATCHES_WITHOUT_LARGE_FLAG} requires --allow-large-run")
    if args.batch_size > MAX_BATCH_SIZE_WITHOUT_LARGE_FLAG and not args.allow_large_run:
        raise SystemExit(f"--batch-size > {MAX_BATCH_SIZE_WITHOUT_LARGE_FLAG} requires --allow-large-run")


def link_games_enabled(args: argparse.Namespace) -> bool:
    return not args.only_players


def link_players_enabled(args: argparse.Namespace) -> bool:
    return not args.only_games


@dataclass
class BatchResult:
    batch_number: int
    selected_rows: int
    max_id: int
    game_updates: int
    player_updates: int
    seconds: float


def load_dependencies() -> tuple[Any, Any]:
    from dotenv import load_dotenv
    from sqlalchemy import create_engine, text

    load_dotenv()
    return create_engine, text


def get_database_url(args: argparse.Namespace) -> str:
    env_name = "LOCAL_DATABASE_URL" if args.local else "DATABASE_URL"
    database_url = args.database_url or os.getenv(env_name)
    if not database_url:
        raise SystemExit(f"Missing {env_name}; pass --database-url or set the environment variable")
    return database_url


def table_columns(conn: Any, table: str, text: Any) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table
                """
            ),
            {"table": table},
        )
    }


def supports_schedule_link(conn: Any, text: Any) -> tuple[bool, str | None]:
    cols = table_columns(conn, "mlb_game_schedule", text)
    if "game_id" not in cols:
        return False, None
    if "game_time_utc" in cols:
        return True, "game_time_utc"
    if "commence_time" in cols:
        return True, "commence_time"
    return False, None


def set_session_safety(conn: Any, args: argparse.Namespace, text: Any) -> None:
    conn.execute(text(f"SET statement_timeout = {int(args.statement_timeout_ms)}"))
    conn.execute(text(f"SET lock_timeout = {int(args.lock_timeout_ms)}"))
    conn.execute(text(f"SET idle_in_transaction_session_timeout = {int(args.idle_timeout_ms)}"))
    try:
        conn.execute(text(f"SET temp_file_limit = '{args.temp_file_limit}'"))
    except Exception as exc:
        conn.rollback()
        if args.require_temp_file_limit:
            raise
        logger.warning(
            "Could not SET temp_file_limit=%s for this DB role; continuing with bounded batches only: %s",
            args.temp_file_limit,
            exc,
        )
        conn.execute(text(f"SET statement_timeout = {int(args.statement_timeout_ms)}"))
        conn.execute(text(f"SET lock_timeout = {int(args.lock_timeout_ms)}"))
        conn.execute(text(f"SET idle_in_transaction_session_timeout = {int(args.idle_timeout_ms)}"))


def ensure_schema_safe(conn: Any, args: argparse.Namespace, text: Any) -> None:
    cols = table_columns(conn, args.table, text)
    missing = [col for col in [args.id_column, "player_id", "api_player_name"] if col not in cols]
    missing.extend(col for col in REQUIRED_LINK_COLUMNS if col not in cols)
    if link_games_enabled(args):
        for col in ["game_id", "commence_time", "api_game_id", "odds_api_event_id"]:
            if col not in cols and col not in missing:
                missing.append(col)
    if missing:
        raise RuntimeError(
            "Target table is missing required columns. Apply/review migration first; "
            f"missing={sorted(missing)}"
        )


def run_preflight(conn: Any, args: argparse.Namespace, text: Any) -> dict[str, Any]:
    set_session_safety(conn, args, text)
    cols = table_columns(conn, args.table, text)
    indexes = [
        dict(row)
        for row in conn.execute(
            text(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = :table
                ORDER BY indexname
                """
            ),
            {"table": args.table},
        ).mappings()
    ]
    approx = conn.execute(
        text(
            """
            SELECT reltuples::bigint AS approx_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = :table
            """
        ),
        {"table": args.table},
    ).scalar()
    sample = []
    if args.sample_rows > 0:
        sample_sql = f"""
            SELECT {quote_ident(args.id_column)} AS id, game_id, player_id, api_game_id, odds_api_event_id, api_player_name
            FROM {table_sql(args.table)}
            WHERE game_id IS NULL OR player_id IS NULL
            ORDER BY {quote_ident(args.id_column)}
            LIMIT :sample_rows
        """
        sample = [dict(row) for row in conn.execute(text(sample_sql), {"sample_rows": args.sample_rows}).mappings()]
    active = [
        dict(row)
        for row in conn.execute(
            text(
                """
                SELECT pid, state, wait_event_type, wait_event, now() - query_start AS age, left(query, 180) AS query
                FROM pg_stat_activity
                WHERE query ILIKE :needle
                  AND pid <> pg_backend_pid()
                ORDER BY query_start NULLS LAST
                LIMIT 10
                """
            ),
            {"needle": f"%{args.table}%"},
        ).mappings()
    ]
    return {
        "table": args.table,
        "id_column": args.id_column,
        "approx_rows": int(approx or 0),
        "columns_present": sorted(cols),
        "index_count": len(indexes),
        "indexes": indexes,
        "sample_unlinked_or_partial_rows": sample,
        "active_sessions_touching_table": active,
    }


def create_batch_ids(conn: Any, args: argparse.Namespace, text: Any, last_id: int) -> tuple[int, int]:
    conn.execute(text("DROP TABLE IF EXISTS tmp_mlb_clv_batch_ids"))
    sql = f"""
        CREATE TEMP TABLE tmp_mlb_clv_batch_ids ON COMMIT DROP AS
        {build_batch_id_sql(args.table, args.id_column, link_games=link_games_enabled(args), link_players=link_players_enabled(args))}
    """
    conn.execute(text(sql), {"last_id": last_id, "batch_size": args.batch_size})
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_batch_ids (id)"))
    row = conn.execute(text("SELECT COUNT(*) AS n, COALESCE(MAX(id), :last_id) AS max_id FROM tmp_mlb_clv_batch_ids"), {"last_id": last_id}).mappings().one()
    return int(row["n"] or 0), int(row["max_id"] or last_id)


def create_event_map_for_batch(conn: Any, args: argparse.Namespace, time_col: str, text: Any) -> dict[str, int]:
    conn.execute(text("DROP TABLE IF EXISTS tmp_mlb_clv_event_keys"))
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE tmp_mlb_clv_event_keys ON COMMIT DROP AS
            SELECT
              c.api_game_id,
              c.odds_api_event_id,
              MIN(c.home_team) AS home_team,
              MIN(c.away_team) AS away_team,
              MIN(c.commence_time) AS commence_time,
              COUNT(*) AS snapshot_rows
            FROM {table_sql(args.table)} c
            JOIN tmp_mlb_clv_batch_ids b
              ON c.{quote_ident(args.id_column)} = b.id
            WHERE c.game_id IS NULL
              AND c.commence_time IS NOT NULL
              AND (c.api_game_id IS NOT NULL OR c.odds_api_event_id IS NOT NULL)
            GROUP BY c.api_game_id, c.odds_api_event_id
            """
        )
    )
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_event_keys (commence_time)"))
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_event_keys (api_game_id)"))
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_event_keys (odds_api_event_id)"))
    conn.execute(text("DROP TABLE IF EXISTS tmp_mlb_clv_event_map"))
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE tmp_mlb_clv_event_map ON COMMIT DROP AS
            WITH candidates AS (
              SELECT
                e.api_game_id,
                e.odds_api_event_id,
                s.game_id,
                COUNT(*) OVER (PARTITION BY e.api_game_id, e.odds_api_event_id) AS n_matches
              FROM tmp_mlb_clv_event_keys e
              JOIN public.mlb_game_schedule s
                ON s.{quote_ident(time_col)} IS NOT NULL
               AND abs(extract(epoch from (s.{quote_ident(time_col)}::timestamptz - e.commence_time))) <= :tolerance_seconds
              JOIN public.mlb_teams ht
                ON ht.team_id = s.home_team_id
               AND ht.team_name = e.home_team
              JOIN public.mlb_teams at
                ON at.team_id = s.away_team_id
               AND at.team_name = e.away_team
            )
            SELECT DISTINCT api_game_id, odds_api_event_id, game_id
            FROM candidates
            WHERE n_matches = 1
            """
        ),
        {"tolerance_seconds": int(args.commence_tolerance_seconds)},
    )
    if args.second_pass_games:
        conn.execute(
            text(
                f"""
                WITH fallback_candidates AS (
                  SELECT
                    e.api_game_id,
                    e.odds_api_event_id,
                    s.game_id,
                    abs(extract(epoch from (s.{quote_ident(time_col)}::timestamptz - e.commence_time))) AS delta_seconds,
                    ROW_NUMBER() OVER (
                      PARTITION BY e.api_game_id, e.odds_api_event_id
                      ORDER BY abs(extract(epoch from (s.{quote_ident(time_col)}::timestamptz - e.commence_time))), s.game_id
                    ) AS rn,
                    COUNT(*) OVER (
                      PARTITION BY e.api_game_id, e.odds_api_event_id,
                                   abs(extract(epoch from (s.{quote_ident(time_col)}::timestamptz - e.commence_time)))
                    ) AS n_at_delta
                  FROM tmp_mlb_clv_event_keys e
                  LEFT JOIN tmp_mlb_clv_event_map existing
                    ON existing.api_game_id IS NOT DISTINCT FROM e.api_game_id
                   AND existing.odds_api_event_id IS NOT DISTINCT FROM e.odds_api_event_id
                  JOIN public.mlb_game_schedule s
                    ON s.{quote_ident(time_col)} IS NOT NULL
                   AND abs(extract(epoch from (s.{quote_ident(time_col)}::timestamptz - e.commence_time))) <= :fallback_tolerance_seconds
                   AND abs((s.{quote_ident(time_col)}::timestamptz::date - e.commence_time::date)) <= :fallback_date_slop_days
                  JOIN public.mlb_teams ht
                    ON ht.team_id = s.home_team_id
                   AND ht.team_name = e.home_team
                  JOIN public.mlb_teams at
                    ON at.team_id = s.away_team_id
                   AND at.team_name = e.away_team
                  WHERE existing.game_id IS NULL
                )
                INSERT INTO tmp_mlb_clv_event_map (api_game_id, odds_api_event_id, game_id)
                SELECT api_game_id, odds_api_event_id, game_id
                FROM fallback_candidates
                WHERE rn = 1 AND n_at_delta = 1
                """
            ),
            {
                "fallback_tolerance_seconds": int(args.fallback_game_tolerance_seconds),
                "fallback_date_slop_days": int(args.fallback_game_date_slop_days),
            },
        )
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_event_map (api_game_id)"))
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_event_map (odds_api_event_id)"))
    row = conn.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM tmp_mlb_clv_event_keys) AS event_keys,
              (SELECT COUNT(*) FROM tmp_mlb_clv_event_map) AS mapped_event_keys
            """
        )
    ).mappings().one()
    return {k: int(v or 0) for k, v in row.items()}


def create_player_map_for_batch(conn: Any, args: argparse.Namespace, text: Any) -> dict[str, int]:
    conn.execute(text("DROP TABLE IF EXISTS tmp_mlb_clv_player_map"))
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE tmp_mlb_clv_player_map ON COMMIT DROP AS
            WITH names AS (
              SELECT
                {norm_sql('player_name')} AS norm_name,
                player_id,
                player_name
              FROM public.mlb_players
              WHERE player_id IS NOT NULL AND player_name IS NOT NULL
            ), unique_names AS (
              SELECT norm_name, MIN(player_id) AS player_id, MIN(player_name) AS linked_name
              FROM names
              WHERE norm_name <> ''
              GROUP BY norm_name
              HAVING COUNT(DISTINCT player_id) = 1
            )
            SELECT * FROM unique_names
            """
        )
    )
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_player_map (norm_name)"))
    conn.execute(text("DROP TABLE IF EXISTS tmp_mlb_clv_batch_player_keys"))
    conn.execute(
        text(
            f"""
            CREATE TEMP TABLE tmp_mlb_clv_batch_player_keys ON COMMIT DROP AS
            SELECT c.{quote_ident(args.id_column)} AS id,
                   {norm_sql('c.api_player_name')} AS norm_name
            FROM {table_sql(args.table)} c
            JOIN tmp_mlb_clv_batch_ids b
              ON c.{quote_ident(args.id_column)} = b.id
            WHERE c.player_id IS NULL
              AND c.api_player_name IS NOT NULL
            """
        )
    )
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_batch_player_keys (id)"))
    conn.execute(text("CREATE INDEX ON tmp_mlb_clv_batch_player_keys (norm_name)"))
    row = conn.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM tmp_mlb_clv_player_map) AS mapped_player_names,
              (SELECT COUNT(*) FROM tmp_mlb_clv_batch_player_keys) AS batch_player_keys
            """
        )
    ).mappings().one()
    return {k: int(v or 0) for k, v in row.items()}


def run_one_batch(conn: Any, args: argparse.Namespace, text: Any, *, batch_number: int, last_id: int, time_col: str | None) -> BatchResult | None:
    started = time.monotonic()
    n_selected, max_id = create_batch_ids(conn, args, text, last_id)
    if n_selected == 0:
        return None

    game_updates = 0
    player_updates = 0
    if link_games_enabled(args) and time_col is not None:
        event_stats = create_event_map_for_batch(conn, args, time_col, text)
        logger.info("Batch %d event map: %s", batch_number, event_stats)
        result = conn.execute(text(build_game_update_sql(args.table, args.id_column)))
        game_updates = int(result.rowcount or 0)
    if link_players_enabled(args):
        player_stats = create_player_map_for_batch(conn, args, text)
        logger.info("Batch %d player map: %s", batch_number, player_stats)
        result = conn.execute(text(build_player_update_sql(args.table, args.id_column)))
        player_updates = int(result.rowcount or 0)
    return BatchResult(
        batch_number=batch_number,
        selected_rows=n_selected,
        max_id=max_id,
        game_updates=game_updates,
        player_updates=player_updates,
        seconds=time.monotonic() - started,
    )


def run_batches(engine: Any, args: argparse.Namespace, text: Any) -> list[BatchResult]:
    results: list[BatchResult] = []
    with engine.connect() as conn:
        set_session_safety(conn, args, text)
        ensure_schema_safe(conn, args, text)
        ok, time_col = supports_schedule_link(conn, text)
        if link_games_enabled(args) and (not ok or time_col is None):
            raise RuntimeError("Cannot link games: mlb_game_schedule lacks game_id/game_time_utc/commence_time")
        conn.commit()

        last_id = int(args.start_id)
        for batch_number in range(1, args.max_batches + 1):
            with conn.begin():
                result = run_one_batch(conn, args, text, batch_number=batch_number, last_id=last_id, time_col=time_col)
            if result is None:
                logger.info("No more candidate rows after last_id=%d", last_id)
                break
            results.append(result)
            last_id = result.max_id
            logger.info(
                "Batch %d complete: selected=%d game_updates=%d player_updates=%d max_id=%d seconds=%.2f",
                result.batch_number,
                result.selected_rows,
                result.game_updates,
                result.player_updates,
                result.max_id,
                result.seconds,
            )
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    return results


def write_report(results: list[BatchResult], output_path: Path | None) -> None:
    if output_path is None:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_selected = sum(r.selected_rows for r in results)
    total_game = sum(r.game_updates for r in results)
    total_player = sum(r.player_updates for r in results)
    lines = [
        "# MLB CLV Snapshot Batched Link Report",
        "",
        f"- batches: {len(results)}",
        f"- selected_rows: {total_selected}",
        f"- game_updates: {total_game}",
        f"- player_updates: {total_player}",
        "",
        "## Batches",
        "",
        "| batch | selected | game_updates | player_updates | max_id | seconds |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        lines.append(f"| {r.batch_number} | {r.selected_rows} | {r.game_updates} | {r.player_updates} | {r.max_id} | {r.seconds:.2f} |")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(args)

    create_engine, text = load_dependencies()
    database_url = get_database_url(args)
    engine = create_engine(database_url, pool_pre_ping=True)

    if args.mode == "preflight":
        logger.info("Running metadata-only preflight; no writes will be performed")
        with engine.connect() as conn:
            preflight = run_preflight(conn, args, text)
        logger.info("Preflight summary: table=%s approx_rows=%s index_count=%s active_sessions=%s sample_rows=%s", preflight["table"], preflight["approx_rows"], preflight["index_count"], len(preflight["active_sessions_touching_table"]), len(preflight["sample_unlinked_or_partial_rows"]))
        logger.info("To run a bounded smoke test, use --execute --max-batches 1 --batch-size 500 --skip-report")
        return 0

    logger.warning(
        "Executing bounded linker: table=%s batch_size=%d max_batches=%d only_games=%s only_players=%s local=%s",
        args.table,
        args.batch_size,
        args.max_batches,
        args.only_games,
        args.only_players,
        args.local,
    )
    results = run_batches(engine, args, text)
    if not args.skip_report:
        write_report(results, Path(args.report) if args.report else None)
    logger.info("Finished bounded linker: batches=%d game_updates=%d player_updates=%d", len(results), sum(r.game_updates for r in results), sum(r.player_updates for r in results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
