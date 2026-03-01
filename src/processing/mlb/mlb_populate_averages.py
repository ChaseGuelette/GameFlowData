"""
MLB Populate Rolling Average Stats (Full Backfill)
====================================================
Calculates rolling averages for batting and pitching, then TRUNCATE + reload
into mlb_player_average_batting and mlb_player_average_pitching.

Pattern: src/processing/populate_average_stats.py (NBA equivalent).

Key design decisions:
  - shift(1) on every stat so game N's average uses only games 1..N-1
  - Rate stats (BA, OBP, SLG, OPS, ERA, WHIP, K/9, BB/9) use rolling *sums*
    of numerator/denominator — NOT the mean of per-game rates
  - IP is stored as true decimal (6.333, not 6.1) so rolling math works directly

Usage:
    python -m src.processing.mlb.mlb_populate_averages --table all
    python -m src.processing.mlb.mlb_populate_averages --table batting
    python -m src.processing.mlb.mlb_populate_averages --table pitching
    python -m src.processing.mlb.mlb_populate_averages --table all --season 2024
"""

import argparse
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.processing.mlb.mlb_config import (
    BATCH_SIZE,
    BATTING_STATS,
    BATTING_STD_STATS,
    BATTING_WINDOWS,
    PITCHING_STATS,
    PITCHING_STD_STATS,
    PITCHING_WINDOWS,
)
from src.processing.populate_average_stats import rolling_with_groupby

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# DATA FETCHING
# ============================================================================


def fetch_batting_stats(engine, season: int | None = None) -> pd.DataFrame:
    """Fetch all batting game stats (excluding DNP rows)."""
    query = """
        SELECT
            player_id, game_id, game_date::date AS game_date, season, team_id,
            pa, ab, r, h, doubles, triples, hr, rbi, bb, so, sb, cs, hbp, sf, tb
        FROM mlb_player_game_stats_batting
        WHERE did_not_play = false
    """
    if season:
        query += f" AND season = {int(season)}"
    query += " ORDER BY player_id, season, game_date"

    logger.info("Fetching batting stats...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    logger.info(f"Fetched {len(df):,} batting rows")
    return df


def fetch_pitching_stats(engine, season: int | None = None) -> pd.DataFrame:
    """Fetch all pitching game stats (excluding DNP rows)."""
    query = """
        SELECT
            player_id, game_id, game_date::date AS game_date, season, team_id,
            is_starter, ip, h_allowed, r_allowed, er, bb, so, hr_allowed,
            pitches_thrown, outs_recorded
        FROM mlb_player_game_stats_pitching
        WHERE did_not_play = false
    """
    if season:
        query += f" AND season = {int(season)}"
    query += " ORDER BY player_id, season, game_date"

    logger.info("Fetching pitching stats...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    logger.info(f"Fetched {len(df):,} pitching rows")
    return df


# ============================================================================
# BATTING ROLLING AVERAGES
# ============================================================================


def _count_games_in_window(df: pd.DataFrame, group_cols: list[str], days: int = 7) -> pd.Series:
    """Count prior games within a calendar window for each row."""
    result = pd.Series(0, index=df.index, dtype=int)
    for _, group in df.groupby(group_cols):
        dates = pd.to_datetime(group["game_date"]).values
        for i, idx in enumerate(group.index):
            current_date = dates[i]
            count = 0
            for j in range(i - 1, -1, -1):
                diff = (current_date - dates[j]) / pd.Timedelta(days=1)
                if diff <= days:
                    count += 1
                else:
                    break
            result.loc[idx] = count
    return result


def calculate_batting_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all rolling batting averages, std devs, rate stats, and context."""
    logger.info("Calculating batting averages...")

    group_cols = ["player_id", "season"]
    df = df.sort_values(group_cols + ["game_date"]).copy()
    group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)

    # Game number (1-indexed) and window game counts
    df["game_number"] = df.groupby(group_cols).cumcount() + 1
    for wname, wsize in BATTING_WINDOWS.items():
        if wsize is not None:
            df[f"games_{wname}"] = (df["game_number"] - 1).clip(upper=wsize)
        else:
            df[f"games_{wname}"] = df["game_number"] - 1

    # Rolling averages: 12 stats × 4 windows
    for stat in BATTING_STATS:
        if stat not in df.columns:
            logger.warning(f"Column {stat} not found, skipping")
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in BATTING_WINDOWS.items():
            col = f"avg_{stat}_{wname}"
            df[col] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Std devs at L5
    for stat in BATTING_STD_STATS:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        df[f"std_{stat}_l5"] = rolling_with_groupby(shifted, group_key, window=5, min_periods=2, agg="std")

    # Rate stats at L10 — from rolling SUMS (not avg of per-game rates)
    _calculate_batting_rate_stats(df, group_cols, group_key)

    # Context: rest_days and games_last_7d
    game_dates = pd.to_datetime(df["game_date"])
    prev_date = game_dates.groupby([df[c] for c in group_cols]).shift(1)
    df["rest_days"] = (game_dates - prev_date).dt.days.clip(0, 14).fillna(4).astype(int)
    df["games_last_7d"] = _count_games_in_window(df, group_cols, days=7)

    logger.info("Batting averages calculated")
    return df


def _calculate_batting_rate_stats(df: pd.DataFrame, group_cols: list[str], group_key: pd.Series):
    """Compute batting rate stats at L10 from rolling sums of components."""
    window = 10

    # Shifted component sums
    ab_shifted = df.groupby(group_cols)["ab"].shift(1)
    h_shifted = df.groupby(group_cols)["h"].shift(1)
    bb_shifted = df.groupby(group_cols)["bb"].shift(1)
    tb_shifted = df.groupby(group_cols)["tb"].shift(1)

    # Need hbp and sf for OBP denominator
    hbp_shifted = df.groupby(group_cols)["hbp"].shift(1) if "hbp" in df.columns else pd.Series(0, index=df.index)
    sf_shifted = df.groupby(group_cols)["sf"].shift(1) if "sf" in df.columns else pd.Series(0, index=df.index)

    sum_ab = rolling_with_groupby(ab_shifted, group_key, window=window, agg="sum")
    sum_h = rolling_with_groupby(h_shifted, group_key, window=window, agg="sum")
    sum_bb = rolling_with_groupby(bb_shifted, group_key, window=window, agg="sum")
    sum_tb = rolling_with_groupby(tb_shifted, group_key, window=window, agg="sum")
    sum_hbp = rolling_with_groupby(hbp_shifted, group_key, window=window, agg="sum")
    sum_sf = rolling_with_groupby(sf_shifted, group_key, window=window, agg="sum")

    # BA = H / AB
    df["avg_batting_avg_l10"] = (sum_h / sum_ab).where(sum_ab > 0)

    # OBP = (H + BB + HBP) / (AB + BB + HBP + SF)
    obp_num = sum_h + sum_bb + sum_hbp
    obp_den = sum_ab + sum_bb + sum_hbp + sum_sf
    df["avg_obp_l10"] = (obp_num / obp_den).where(obp_den > 0)

    # SLG = TB / AB
    df["avg_slg_l10"] = (sum_tb / sum_ab).where(sum_ab > 0)

    # OPS = OBP + SLG
    df["avg_ops_l10"] = df["avg_obp_l10"].fillna(0) + df["avg_slg_l10"].fillna(0)
    df.loc[df["avg_obp_l10"].isna() & df["avg_slg_l10"].isna(), "avg_ops_l10"] = None


# ============================================================================
# PITCHING ROLLING AVERAGES
# ============================================================================


def calculate_pitching_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all rolling pitching averages, std devs, rate stats, and context."""
    logger.info("Calculating pitching averages...")

    group_cols = ["player_id", "season"]
    df = df.sort_values(group_cols + ["game_date"]).copy()
    group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)

    # Game number (1-indexed)
    df["game_number"] = df.groupby(group_cols).cumcount() + 1

    # Rolling averages: 8 stats × 3 windows
    for stat in PITCHING_STATS:
        if stat not in df.columns:
            logger.warning(f"Column {stat} not found, skipping")
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        for wname, wsize in PITCHING_WINDOWS.items():
            col = f"avg_{stat}_{wname}"
            df[col] = rolling_with_groupby(shifted, group_key, window=wsize)

    # Std devs: so at L3, er at L3
    for stat in PITCHING_STD_STATS:
        if stat not in df.columns:
            continue
        shifted = df.groupby(group_cols)[stat].shift(1)
        df[f"std_{stat}_l3"] = rolling_with_groupby(shifted, group_key, window=3, min_periods=2, agg="std")

    # Derived rate stats at L5 from rolling sums
    _calculate_pitching_rate_stats(df, group_cols, group_key)

    # Context features
    _calculate_pitching_context(df, group_cols)

    logger.info("Pitching averages calculated")
    return df


def _calculate_pitching_rate_stats(df: pd.DataFrame, group_cols: list[str], group_key: pd.Series):
    """Compute ERA, WHIP, K/9, BB/9 at L5 from rolling sums."""
    window = 5

    ip_shifted = df.groupby(group_cols)["ip"].shift(1)
    er_shifted = df.groupby(group_cols)["er"].shift(1)
    bb_shifted = df.groupby(group_cols)["bb"].shift(1)
    so_shifted = df.groupby(group_cols)["so"].shift(1)
    h_shifted = df.groupby(group_cols)["h_allowed"].shift(1)

    sum_ip = rolling_with_groupby(ip_shifted, group_key, window=window, agg="sum")
    sum_er = rolling_with_groupby(er_shifted, group_key, window=window, agg="sum")
    sum_bb = rolling_with_groupby(bb_shifted, group_key, window=window, agg="sum")
    sum_so = rolling_with_groupby(so_shifted, group_key, window=window, agg="sum")
    sum_h = rolling_with_groupby(h_shifted, group_key, window=window, agg="sum")

    # ERA = 9 * ER / IP
    df["avg_era_l5"] = (9 * sum_er / sum_ip).where(sum_ip > 0)

    # WHIP = (BB + H) / IP
    df["avg_whip_l5"] = ((sum_bb + sum_h) / sum_ip).where(sum_ip > 0)

    # K/9 = 9 * SO / IP
    df["avg_k_per_9_l5"] = (9 * sum_so / sum_ip).where(sum_ip > 0)

    # BB/9 = 9 * BB / IP
    df["avg_bb_per_9_l5"] = (9 * sum_bb / sum_ip).where(sum_ip > 0)


def _calculate_pitching_context(df: pd.DataFrame, group_cols: list[str]):
    """Compute days_rest, pitch_count_last_start, starts_l3/l5/szn."""

    # days_rest
    game_dates = pd.to_datetime(df["game_date"])
    prev_date = game_dates.groupby([df[c] for c in group_cols]).shift(1)
    df["days_rest"] = (game_dates - prev_date).dt.days.clip(0, 14).fillna(5).astype(int)

    # pitch_count_last_start — shifted pitches_thrown
    if "pitches_thrown" in df.columns:
        df["pitch_count_last_start"] = df.groupby(group_cols)["pitches_thrown"].shift(1)
    else:
        df["pitch_count_last_start"] = None

    # starts counts (shifted) — rolling sum of is_starter
    if "is_starter" in df.columns:
        group_key = df[group_cols].apply(lambda x: tuple(x), axis=1)
        starter_float = df["is_starter"].astype(float)
        shifted_starter = df.groupby(group_cols)["is_starter"].shift(1).astype(float)

        df["starts_l3"] = rolling_with_groupby(shifted_starter, group_key, window=3, agg="sum")
        df["starts_l5"] = rolling_with_groupby(shifted_starter, group_key, window=5, agg="sum")

        # Season-to-date starts (expanding sum of shifted)
        df["starts_szn"] = rolling_with_groupby(shifted_starter, group_key, window=None, agg="mean")
        # expanding mean * count = sum, but easier to just compute directly
        szn_result = pd.Series(index=df.index, dtype=float)
        for key, group_idx in shifted_starter.groupby(group_key).groups.items():
            group_data = shifted_starter.loc[group_idx]
            szn_result.loc[group_idx] = group_data.expanding(min_periods=1).sum()
        df["starts_szn"] = szn_result
    else:
        df["starts_l3"] = None
        df["starts_l5"] = None
        df["starts_szn"] = None


# ============================================================================
# DATABASE INSERT
# ============================================================================


def insert_batting_averages(engine, df: pd.DataFrame):
    """TRUNCATE + batch insert into mlb_player_average_batting."""
    logger.info("Inserting batting averages...")

    # Build column list
    columns = ["player_id", "game_id", "game_date", "season", "team_id", "game_number"]
    for wname in BATTING_WINDOWS:
        columns.append(f"games_{wname}")

    for stat in BATTING_STATS:
        for wname in BATTING_WINDOWS:
            col = f"avg_{stat}_{wname}"
            if col in df.columns:
                columns.append(col)

    for stat in BATTING_STD_STATS:
        col = f"std_{stat}_l5"
        if col in df.columns:
            columns.append(col)

    for col in ["avg_batting_avg_l10", "avg_obp_l10", "avg_slg_l10", "avg_ops_l10"]:
        if col in df.columns:
            columns.append(col)

    for col in ["rest_days", "games_last_7d"]:
        if col in df.columns:
            columns.append(col)

    insert_df = df[[c for c in columns if c in df.columns]].copy()

    # Round numerics
    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)

    # Cast smallint columns
    for col in ["games_l5", "games_l10", "games_l20", "games_szn", "rest_days", "games_last_7d"]:
        if col in insert_df.columns:
            insert_df[col] = insert_df[col].astype("Int64")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE mlb_player_average_batting"))

        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i : i + BATCH_SIZE]
            batch.to_sql("mlb_player_average_batting", conn, if_exists="append", index=False, method="multi")
            if (i // BATCH_SIZE + 1) % 50 == 0 or i + BATCH_SIZE >= len(insert_df):
                logger.info(f"Inserted {min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,} batting rows")

    logger.info(f"Inserted {len(insert_df):,} batting average rows total")


def insert_pitching_averages(engine, df: pd.DataFrame):
    """TRUNCATE + batch insert into mlb_player_average_pitching."""
    logger.info("Inserting pitching averages...")

    columns = ["player_id", "game_id", "game_date", "season", "team_id"]

    for stat in PITCHING_STATS:
        for wname in PITCHING_WINDOWS:
            col = f"avg_{stat}_{wname}"
            if col in df.columns:
                columns.append(col)

    for col in ["avg_era_l5", "avg_whip_l5", "avg_k_per_9_l5", "avg_bb_per_9_l5"]:
        if col in df.columns:
            columns.append(col)

    for stat in PITCHING_STD_STATS:
        col = f"std_{stat}_l3"
        if col in df.columns:
            columns.append(col)

    for col in ["game_number", "days_rest", "pitch_count_last_start", "starts_l3", "starts_l5", "starts_szn"]:
        if col in df.columns:
            columns.append(col)

    insert_df = df[[c for c in columns if c in df.columns]].copy()

    # Round numerics
    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)

    # Cast smallint columns
    for col in ["days_rest", "starts_l3", "starts_l5", "starts_szn"]:
        if col in insert_df.columns:
            insert_df[col] = insert_df[col].astype("Int64")

    if "pitch_count_last_start" in insert_df.columns:
        insert_df["pitch_count_last_start"] = insert_df["pitch_count_last_start"].astype("Int64")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE mlb_player_average_pitching"))

        for i in range(0, len(insert_df), BATCH_SIZE):
            batch = insert_df.iloc[i : i + BATCH_SIZE]
            batch.to_sql("mlb_player_average_pitching", conn, if_exists="append", index=False, method="multi")
            if (i // BATCH_SIZE + 1) % 50 == 0 or i + BATCH_SIZE >= len(insert_df):
                logger.info(f"Inserted {min(i + BATCH_SIZE, len(insert_df)):,}/{len(insert_df):,} pitching rows")

    logger.info(f"Inserted {len(insert_df):,} pitching average rows total")


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="MLB Populate Rolling Average Stats (Full Backfill)")
    parser.add_argument(
        "--table",
        choices=["batting", "pitching", "all"],
        default="all",
        help="Which table(s) to populate",
    )
    parser.add_argument("--season", type=int, default=None, help="Specific season year (e.g. 2024)")
    args = parser.parse_args()

    engine = get_engine()

    logger.info("=" * 60)
    logger.info("MLB ROLLING AVERAGE POPULATION (FULL BACKFILL)")
    logger.info(f"Table: {args.table}")
    logger.info(f"Season: {args.season or 'ALL'}")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        if args.table in ("batting", "all"):
            df = fetch_batting_stats(engine, args.season)
            if not df.empty:
                df = calculate_batting_averages(df)
                insert_batting_averages(engine, df)
            del df

        if args.table in ("pitching", "all"):
            df = fetch_pitching_stats(engine, args.season)
            if not df.empty:
                df = calculate_pitching_averages(df)
                insert_pitching_averages(engine, df)
            del df

        elapsed = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"Completed in {elapsed}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
