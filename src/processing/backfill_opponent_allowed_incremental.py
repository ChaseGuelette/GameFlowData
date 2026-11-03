"""Incrementally refresh opponent-allowed features from complete season history."""

import argparse
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text
from tqdm import tqdm

from src.db.client import get_engine


class IncompleteAdvancedStatsError(RuntimeError):
    """The requested derivation would use incomplete advanced possessions."""


@contextmanager
def bounded_read_connection(engine):
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '120s'"))
        yield conn


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an ISO date (YYYY-MM-DD)") from exc


def discover_source_completed_end_date(engine) -> date | None:
    query = text("""
        SELECT MAX(tgs.game_date::DATE)
        FROM team_game_stats AS tgs
        WHERE tgs.game_date::DATE <= CURRENT_DATE
    """)
    with bounded_read_connection(engine) as conn:
        value = conn.execute(query).scalar()
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise RuntimeError("Completed source end date has an unexpected database type")


def count_target_source_games(engine, start_date: date, end_date: date) -> int:
    query = text("""
        SELECT COUNT(DISTINCT tgs.game_id)
        FROM team_game_stats AS tgs
        WHERE tgs.game_date::DATE BETWEEN :start_date AND :end_date
    """)
    with bounded_read_connection(engine) as conn:
        return int(conn.execute(query, {"start_date": start_date, "end_date": end_date}).scalar() or 0)


def require_complete_advanced_history(engine, start_date: date, end_date: date) -> None:
    query = text("""
        WITH target_seasons AS (
            SELECT DISTINCT tgs.season_id
            FROM team_game_stats AS tgs
            WHERE tgs.game_date::DATE BETWEEN :start_date AND :end_date
        ), required_pairs AS (
            SELECT DISTINCT pgs.game_id, pgs.player_id
            FROM team_game_stats AS tgs
            JOIN target_seasons AS seasons ON seasons.season_id = tgs.season_id
            JOIN player_game_stats AS pgs
              ON pgs.game_id = tgs.game_id
             AND pgs.team_id != tgs.team_id
             AND pgs.min > 0
            WHERE tgs.game_date::DATE <= :end_date
        )
        SELECT
            COUNT(*) AS required_pairs,
            COUNT(*) FILTER (WHERE adv.game_id IS NULL OR adv.possessions IS NULL) AS missing_pairs
        FROM required_pairs AS required
        LEFT JOIN player_game_advanced_stats AS adv
          ON adv.game_id = required.game_id
         AND adv.player_id = required.player_id
    """)
    with bounded_read_connection(engine) as conn:
        row = conn.execute(query, {"start_date": start_date, "end_date": end_date}).one()
    required_pairs, missing_pairs = int(row[0]), int(row[1])
    if required_pairs == 0:
        raise IncompleteAdvancedStatsError(
            "Source games exist but no positive-minute player/game history is available for derivation"
        )
    if missing_pairs:
        raise IncompleteAdvancedStatsError(
            f"Advanced possessions are incomplete: {missing_pairs} of {required_pairs} required player/game pairs are missing"
        )


def backfill_opponent_allowed_incremental(
    engine,
    days_back: int = 30,
    lookback_buffer: int = 15,
    start_date: date | None = None,
    end_date: date | None = None,
):
    """Refresh target dates using all preceding games in their seasons."""
    del lookback_buffer  # Retained only for caller compatibility; calendar buffers are unsafe for L15/season windows.
    if days_back < 0:
        raise ValueError("days_back must be zero or positive")
    today = date.today()
    if start_date is not None and start_date > today:
        raise ValueError("start_date cannot be in the future")
    if end_date is not None and end_date > today:
        raise ValueError("end_date cannot be in the future")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start_date must not be after end_date")

    target_end = end_date or discover_source_completed_end_date(engine)
    if target_end is None:
        print("No completed source game date is available; nothing to refresh.")
        return
    target_start = start_date or (target_end - timedelta(days=days_back))
    if target_start > target_end:
        raise ValueError("start_date must not be after the completed source end date")

    print("--- Incremental Opponent Allowed Stats ---")
    print(f"  Target date range: {target_start} to {target_end}")
    print("  History range: full target season(s) through target end")

    target_games = count_target_source_games(engine, target_start, target_end)
    if target_games == 0:
        print("No source games exist in the target range; nothing to refresh.")
        return

    require_complete_advanced_history(engine, target_start, target_end)
    df = fetch_raw_allowed_stats_incremental(engine, target_start, target_end)
    if df.empty:
        raise RuntimeError("Source games exist but the full-history opponent derivation produced zero rows")

    print(f"Loaded {len(df):,} full-history rows. Computing rolling windows...")
    df_processed = compute_rolling_metrics(df)
    df_final = df_processed.dropna(subset=["games_l5"])
    df_final = df_final.dropna(subset=["team_id", "game_id", "game_date", "position_group"])
    game_dates = pd.to_datetime(df_final["game_date"]).dt.date
    df_final = df_final[(game_dates >= target_start) & (game_dates <= target_end)]
    if df_final.empty:
        raise RuntimeError("Source games exist in the target range but no derived rows are eligible to write")

    print(f"Computed {len(df_final):,} rows in target range. Inserting...")
    batch_insert_to_db(engine, df_final)
    print("--- Complete ---")


def fetch_raw_allowed_stats_incremental(engine, start_date: date, end_date: date | None = None) -> pd.DataFrame:
    """Fetch complete history for seasons represented in the requested target range."""
    if end_date is None:
        end_date = date.today()
    query = text("""
        WITH target_seasons AS (
            SELECT DISTINCT season_id
            FROM team_game_stats
            WHERE game_date::DATE BETWEEN :start_date AND :end_date
        )
        SELECT
            tgs.team_id, tgs.game_id, tgs.game_date::DATE as game_date, tgs.season_id,
            ph.position_group,
            COALESCE(SUM(pgs.pts), 0) as pts,
            COALESCE(SUM(pgs.reb), 0) as reb,
            COALESCE(SUM(pgs.ast), 0) as ast,
            COALESCE(SUM(pgs.fg3m), 0) as threes,
            COALESCE(SUM(pgs.stl), 0) as stl,
            COALESCE(SUM(pgs.blk), 0) as blk,
            COALESCE(SUM(pgs.tov), 0) as tov,
            COALESCE(SUM(pgs.fta), 0) as fta,
            COALESCE(SUM(pgs.oreb), 0) as oreb,
            COALESCE(SUM(pgs.pf), 0) as pf,
            SUM(adv.possessions) as poss_faced
        FROM team_game_stats tgs
        JOIN target_seasons seasons ON seasons.season_id = tgs.season_id
        JOIN player_game_stats pgs
            ON tgs.game_id = pgs.game_id AND tgs.team_id != pgs.team_id AND pgs.min > 0
        JOIN player_game_advanced_stats adv
            ON pgs.game_id = adv.game_id AND pgs.player_id = adv.player_id
        LEFT JOIN LATERAL (
            SELECT position_group FROM player_position_history ph
            WHERE ph.player_id = pgs.player_id AND ph.snapshot_date < tgs.game_date::DATE
            ORDER BY ph.snapshot_date DESC LIMIT 1
        ) ph ON TRUE
        WHERE tgs.game_date::DATE <= :end_date
        GROUP BY tgs.team_id, tgs.game_id, tgs.game_date, tgs.season_id, ph.position_group
    """)

    with bounded_read_connection(engine) as conn:
        return pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})


def compute_rolling_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute L5, L15, and season rolling windows for opponent-allowed stats.

    Same logic as full version.
    """
    df = df.sort_values(["team_id", "position_group", "game_date"]).reset_index(drop=True)

    metrics = [
        "pts",
        "reb",
        "ast",
        "threes",
        "stl",
        "blk",
        "tov",
        "fta",
        "oreb",
        "pf",
        "poss_faced",
    ]

    grouper = df.groupby(["team_id", "position_group", "season_id"])

    result_dfs = []

    # Rolling Windows (L5, L15)
    for w_name, w_size in [("l5", 5), ("l15", 15)]:
        rolled = grouper[metrics].apply(lambda x: x.shift(1).rolling(w_size, min_periods=1).mean())
        if isinstance(rolled.index, pd.MultiIndex):
            rolled = rolled.reset_index(drop=True)
        rolled.columns = [
            f"{c}_allowed_{w_name}" if c != "poss_faced" else f"poss_faced_{w_name}" for c in rolled.columns
        ]

        counts = grouper["pts"].apply(lambda x: x.shift(1).rolling(w_size, min_periods=1).count())
        if isinstance(counts.index, pd.MultiIndex):
            counts = counts.reset_index(drop=True)
        result_dfs.extend([rolled, counts.rename(f"games_{w_name}")])

    # Season Window
    szn = grouper[metrics].apply(lambda x: x.shift(1).expanding().mean())
    if isinstance(szn.index, pd.MultiIndex):
        szn = szn.reset_index(drop=True)
    szn.columns = [f"{c}_allowed_szn" if c != "poss_faced" else "poss_faced_szn" for c in szn.columns]

    szn_c = grouper["pts"].apply(lambda x: x.shift(1).expanding().count())
    if isinstance(szn_c.index, pd.MultiIndex):
        szn_c = szn_c.reset_index(drop=True)
    result_dfs.extend([szn, szn_c.rename("games_szn")])

    df_full = pd.concat([df[["team_id", "game_id", "game_date", "position_group"]]] + result_dfs, axis=1)

    # Pace Adjustment
    base_stats = ["pts", "reb", "ast", "threes", "stl", "blk", "tov", "fta", "oreb", "pf"]

    for w in ["l5", "l15", "szn"]:
        poss_col = f"poss_faced_{w}"
        for stat in base_stats:
            num_col = f"{stat}_allowed_{w}"
            # Naming Exceptions
            if stat == "tov":
                rate_col = f"tov_per100_forced_{w}"
            elif stat == "pts":
                rate_col = f"off_rtg_allowed_{w}"
            else:
                rate_col = f"{stat}_per100_allowed_{w}"

            df_full[rate_col] = np.where((df_full[poss_col] > 0), (df_full[num_col] / df_full[poss_col]) * 100, 0)

    return df_full


def batch_insert_to_db(engine, df: pd.DataFrame, batch_size: int = 100):
    """
    Insert/update rows using UPSERT.

    Same logic as full version.
    """
    valid_cols = [
        "team_id",
        "game_id",
        "game_date",
        "position_group",
        "games_l5",
        "games_l15",
        "games_szn",
        "poss_faced_l5",
        "poss_faced_l15",
        "poss_faced_szn",
        # Core
        "pts_allowed_l5",
        "pts_allowed_l15",
        "pts_allowed_szn",
        "reb_allowed_l5",
        "reb_allowed_l15",
        "reb_allowed_szn",
        "ast_allowed_l5",
        "ast_allowed_l15",
        "ast_allowed_szn",
        "stl_allowed_l5",
        "stl_allowed_l15",
        "stl_allowed_szn",
        "blk_allowed_l5",
        "blk_allowed_l15",
        "blk_allowed_szn",
        "threes_allowed_l5",
        "threes_allowed_l15",
        "threes_allowed_szn",
        "tov_forced_l5",
        "tov_forced_l15",
        "tov_forced_szn",
        # Sharp
        "fta_allowed_l5",
        "fta_allowed_l15",
        "fta_allowed_szn",
        "oreb_allowed_l5",
        "oreb_allowed_l15",
        "oreb_allowed_szn",
        "pf_allowed_l5",
        "pf_allowed_l15",
        "pf_allowed_szn",
        # Core Rates
        "off_rtg_allowed_l5",
        "off_rtg_allowed_l15",
        "off_rtg_allowed_szn",
        "reb_per100_allowed_l5",
        "reb_per100_allowed_l15",
        "reb_per100_allowed_szn",
        "ast_per100_allowed_l5",
        "ast_per100_allowed_l15",
        "ast_per100_allowed_szn",
        "stl_per100_allowed_l5",
        "stl_per100_allowed_l15",
        "stl_per100_allowed_szn",
        "blk_per100_allowed_l5",
        "blk_per100_allowed_l15",
        "blk_per100_allowed_szn",
        "threes_per100_allowed_l5",
        "threes_per100_allowed_l15",
        "threes_per100_allowed_szn",
        "tov_per100_forced_l5",
        "tov_per100_forced_l15",
        "tov_per100_forced_szn",
        # Sharp Rates
        "fta_per100_allowed_l5",
        "fta_per100_allowed_l15",
        "fta_per100_allowed_szn",
        "oreb_per100_allowed_l5",
        "oreb_per100_allowed_l15",
        "oreb_per100_allowed_szn",
        "pf_per100_allowed_l5",
        "pf_per100_allowed_l15",
        "pf_per100_allowed_szn",
    ]

    rename_map = {
        "tov_allowed_l5": "tov_forced_l5",
        "tov_allowed_l15": "tov_forced_l15",
        "tov_allowed_szn": "tov_forced_szn",
    }
    df = df.rename(columns=rename_map)[valid_cols].replace({np.nan: None})

    cols = ", ".join(valid_cols)
    conflict_cols = ["team_id", "game_id", "position_group"]
    update_cols = [c for c in valid_cols if c not in conflict_cols]
    upsert_sql = (
        f"INSERT INTO team_allowed_by_position ({cols}) VALUES %s "  # nosec
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
        + ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    )

    # Convert to list of tuples for execute_values
    rows = [tuple(row[c] for c in valid_cols) for row in df.to_dict(orient="records")]
    for i in tqdm(range(0, len(rows), batch_size)):
        batch = rows[i : i + batch_size]
        if batch:
            raw_conn = engine.raw_connection()
            try:
                cur = raw_conn.cursor()
                cur.execute("SET LOCAL statement_timeout = '120s'")
                execute_values(cur, upsert_sql, batch, page_size=batch_size)
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                raw_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Incremental opponent allowed stats backfill (last N days only)"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days to update (default: 30)",
    )
    parser.add_argument(
        "--lookback-buffer",
        type=int,
        default=15,
        help="Deprecated compatibility option; full target-season history is always fetched",
    )
    parser.add_argument("--start-date", type=parse_date, default=None, help="First target date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=parse_date, default=None, help="Last target date (YYYY-MM-DD)")

    args = parser.parse_args()
    today = date.today()
    if args.days_back < 0:
        parser.error("--days-back must be zero or positive")
    if args.start_date is not None and args.start_date > today:
        parser.error("--start-date cannot be in the future")
    if args.end_date is not None and args.end_date > today:
        parser.error("--end-date cannot be in the future")
    if args.start_date is not None and args.end_date is not None and args.start_date > args.end_date:
        parser.error("--start-date must not be after --end-date")

    engine = get_engine()
    backfill_opponent_allowed_incremental(
        engine,
        days_back=args.days_back,
        lookback_buffer=args.lookback_buffer,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
