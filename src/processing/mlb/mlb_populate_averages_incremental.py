"""
MLB Incremental Rolling Average Stats Update
==============================================
Lightweight version of mlb_populate_averages.py for daily cron jobs.

Only processes players who had games on the target date.
Fetches all season-to-date games for correct SZN expanding averages.
Uses UPSERT (ON CONFLICT DO UPDATE) instead of TRUNCATE.

Pattern: src/processing/populate_average_stats_incremental.py (NBA equivalent).

Usage:
    python -m src.processing.mlb.mlb_populate_averages_incremental
    python -m src.processing.mlb.mlb_populate_averages_incremental --date 2024-09-15
"""

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine
from src.processing.mlb.mlb_config import (
    BATTING_STATS,
    BATTING_STD_STATS,
    BATTING_WINDOWS,
    PITCHING_STATS,
    PITCHING_STD_STATS,
    PITCHING_WINDOWS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# DATA FETCHING
# ============================================================================


def get_season_for_date(engine, target_date: date) -> int | None:
    """Get the season year for the target date from existing batting data."""
    query = text("""
        SELECT DISTINCT season
        FROM mlb_player_game_stats_batting
        WHERE game_date::date = :td
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"td": target_date}).fetchone()
    return row[0] if row else None


def get_batter_ids_on_date(engine, target_date: date) -> list[int]:
    """Get player IDs with batting games on target date."""
    query = text("""
        SELECT DISTINCT player_id
        FROM mlb_player_game_stats_batting
        WHERE game_date::date = :td AND did_not_play = false
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"td": target_date}).fetchall()
    ids = [r[0] for r in rows]
    logger.info(f"Found {len(ids)} batters on {target_date}")
    return ids


def get_pitcher_ids_on_date(engine, target_date: date) -> list[int]:
    """Get player IDs with pitching games on target date."""
    query = text("""
        SELECT DISTINCT player_id
        FROM mlb_player_game_stats_pitching
        WHERE game_date::date = :td AND did_not_play = false
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"td": target_date}).fetchall()
    ids = [r[0] for r in rows]
    logger.info(f"Found {len(ids)} pitchers on {target_date}")
    return ids


def fetch_batter_season_games(
    engine, player_ids: list[int], season: int, target_date: date
) -> pd.DataFrame:
    """Fetch all season-to-date batting games for given players."""
    if not player_ids:
        return pd.DataFrame()
    player_tuple = tuple(player_ids)
    query = f"""
        SELECT
            player_id, game_id, game_date::date AS game_date, season, team_id,
            pa, ab, r, h, doubles, triples, hr, rbi, bb, so, sb, cs, hbp, sf, tb
        FROM mlb_player_game_stats_batting
        WHERE player_id IN {player_tuple}
          AND season = :season
          AND game_date::date <= :td
          AND did_not_play = false
        ORDER BY player_id, game_date
    """
    df = pd.read_sql(text(query), engine, params={"season": season, "td": target_date})
    logger.info(f"Fetched {len(df):,} batter season games for {len(player_ids)} players")
    return df


def fetch_pitcher_season_games(
    engine, player_ids: list[int], season: int, target_date: date
) -> pd.DataFrame:
    """Fetch all season-to-date pitching games for given players."""
    if not player_ids:
        return pd.DataFrame()
    player_tuple = tuple(player_ids)
    query = f"""
        SELECT
            player_id, game_id, game_date::date AS game_date, season, team_id,
            is_starter, ip, h_allowed, r_allowed, er, bb, so, hr_allowed,
            pitches_thrown, outs_recorded
        FROM mlb_player_game_stats_pitching
        WHERE player_id IN {player_tuple}
          AND season = :season
          AND game_date::date <= :td
          AND did_not_play = false
        ORDER BY player_id, game_date
    """
    df = pd.read_sql(text(query), engine, params={"season": season, "td": target_date})
    logger.info(f"Fetched {len(df):,} pitcher season games for {len(player_ids)} players")
    return df


# ============================================================================
# PER-PLAYER ROLLING CALCULATIONS
# ============================================================================


def calculate_batting_rolling_for_player(player_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all batting rolling stats for a single player's season data."""
    player_df = player_df.sort_values("game_date").copy()

    # Game number
    player_df["game_number"] = range(1, len(player_df) + 1)
    for wname, wsize in BATTING_WINDOWS.items():
        if wsize is not None:
            player_df[f"games_{wname}"] = (player_df["game_number"] - 1).clip(upper=wsize)
        else:
            player_df[f"games_{wname}"] = player_df["game_number"] - 1

    # Rolling averages
    for stat in BATTING_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        for wname, wsize in BATTING_WINDOWS.items():
            col = f"avg_{stat}_{wname}"
            if wsize is None:
                player_df[col] = shifted.expanding(min_periods=1).mean()
            else:
                player_df[col] = shifted.rolling(window=wsize, min_periods=1).mean()

    # Std devs at L5
    for stat in BATTING_STD_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        player_df[f"std_{stat}_l5"] = shifted.rolling(window=5, min_periods=2).std()

    # Rate stats at L10 from rolling sums
    _calc_batting_rates_single(player_df, window=10)

    # Context
    game_dates = pd.to_datetime(player_df["game_date"])
    prev_date = game_dates.shift(1)
    player_df["rest_days"] = (game_dates - prev_date).dt.days.clip(0, 14).fillna(4).astype(int)

    player_df["games_last_7d"] = 0
    dates = game_dates.values
    for i in range(len(dates)):
        count = 0
        for j in range(i - 1, -1, -1):
            diff = (dates[i] - dates[j]) / pd.Timedelta(days=1)
            if diff <= 7:
                count += 1
            else:
                break
        player_df.iloc[i, player_df.columns.get_loc("games_last_7d")] = count

    return player_df


def _calc_batting_rates_single(df: pd.DataFrame, window: int = 10):
    """Compute L10 rate stats for a single player (no groupby needed)."""
    ab_s = df["ab"].shift(1) if "ab" in df.columns else pd.Series(0, index=df.index)
    h_s = df["h"].shift(1) if "h" in df.columns else pd.Series(0, index=df.index)
    bb_s = df["bb"].shift(1) if "bb" in df.columns else pd.Series(0, index=df.index)
    tb_s = df["tb"].shift(1) if "tb" in df.columns else pd.Series(0, index=df.index)
    hbp_s = df["hbp"].shift(1) if "hbp" in df.columns else pd.Series(0, index=df.index)
    sf_s = df["sf"].shift(1) if "sf" in df.columns else pd.Series(0, index=df.index)

    sum_ab = ab_s.rolling(window=window, min_periods=1).sum()
    sum_h = h_s.rolling(window=window, min_periods=1).sum()
    sum_bb = bb_s.rolling(window=window, min_periods=1).sum()
    sum_tb = tb_s.rolling(window=window, min_periods=1).sum()
    sum_hbp = hbp_s.rolling(window=window, min_periods=1).sum()
    sum_sf = sf_s.rolling(window=window, min_periods=1).sum()

    df["avg_batting_avg_l10"] = (sum_h / sum_ab).where(sum_ab > 0)

    obp_num = sum_h + sum_bb + sum_hbp
    obp_den = sum_ab + sum_bb + sum_hbp + sum_sf
    df["avg_obp_l10"] = (obp_num / obp_den).where(obp_den > 0)

    df["avg_slg_l10"] = (sum_tb / sum_ab).where(sum_ab > 0)

    df["avg_ops_l10"] = df["avg_obp_l10"].fillna(0) + df["avg_slg_l10"].fillna(0)
    df.loc[df["avg_obp_l10"].isna() & df["avg_slg_l10"].isna(), "avg_ops_l10"] = None


def calculate_pitching_rolling_for_player(player_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all pitching rolling stats for a single player's season data."""
    player_df = player_df.sort_values("game_date").copy()

    # Game number
    player_df["game_number"] = range(1, len(player_df) + 1)

    # Rolling averages
    for stat in PITCHING_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        for wname, wsize in PITCHING_WINDOWS.items():
            col = f"avg_{stat}_{wname}"
            if wsize is None:
                player_df[col] = shifted.expanding(min_periods=1).mean()
            else:
                player_df[col] = shifted.rolling(window=wsize, min_periods=1).mean()

    # Std devs
    for stat in PITCHING_STD_STATS:
        if stat not in player_df.columns:
            continue
        shifted = player_df[stat].shift(1)
        player_df[f"std_{stat}_l3"] = shifted.rolling(window=3, min_periods=2).std()

    # Rate stats at L5
    _calc_pitching_rates_single(player_df, window=5)

    # Context
    game_dates = pd.to_datetime(player_df["game_date"])
    prev_date = game_dates.shift(1)
    player_df["days_rest"] = (game_dates - prev_date).dt.days.clip(0, 14).fillna(5).astype(int)

    if "pitches_thrown" in player_df.columns:
        player_df["pitch_count_last_start"] = player_df["pitches_thrown"].shift(1)
    else:
        player_df["pitch_count_last_start"] = None

    if "is_starter" in player_df.columns:
        starter_shifted = player_df["is_starter"].astype(float).shift(1)
        player_df["starts_l3"] = starter_shifted.rolling(window=3, min_periods=1).sum()
        player_df["starts_l5"] = starter_shifted.rolling(window=5, min_periods=1).sum()
        player_df["starts_szn"] = starter_shifted.expanding(min_periods=1).sum()
    else:
        player_df["starts_l3"] = None
        player_df["starts_l5"] = None
        player_df["starts_szn"] = None

    return player_df


def _calc_pitching_rates_single(df: pd.DataFrame, window: int = 5):
    """Compute L5 rate stats for a single pitcher."""
    ip_s = df["ip"].shift(1) if "ip" in df.columns else pd.Series(0, index=df.index)
    er_s = df["er"].shift(1) if "er" in df.columns else pd.Series(0, index=df.index)
    bb_s = df["bb"].shift(1) if "bb" in df.columns else pd.Series(0, index=df.index)
    so_s = df["so"].shift(1) if "so" in df.columns else pd.Series(0, index=df.index)
    h_s = df["h_allowed"].shift(1) if "h_allowed" in df.columns else pd.Series(0, index=df.index)

    sum_ip = ip_s.rolling(window=window, min_periods=1).sum()
    sum_er = er_s.rolling(window=window, min_periods=1).sum()
    sum_bb = bb_s.rolling(window=window, min_periods=1).sum()
    sum_so = so_s.rolling(window=window, min_periods=1).sum()
    sum_h = h_s.rolling(window=window, min_periods=1).sum()

    df["avg_era_l5"] = (9 * sum_er / sum_ip).where(sum_ip > 0)
    df["avg_whip_l5"] = ((sum_bb + sum_h) / sum_ip).where(sum_ip > 0)
    df["avg_k_per_9_l5"] = (9 * sum_so / sum_ip).where(sum_ip > 0)
    df["avg_bb_per_9_l5"] = (9 * sum_bb / sum_ip).where(sum_ip > 0)


# ============================================================================
# INCREMENTAL PIPELINE
# ============================================================================


def calculate_incremental(
    df: pd.DataFrame, target_date: date, id_col: str, calc_fn
) -> pd.DataFrame:
    """Calculate rolling averages for all entities, return only target-date rows."""
    results = []
    entity_ids = df[id_col].unique()

    for i, entity_id in enumerate(entity_ids):
        entity_df = df[df[id_col] == entity_id].copy()
        entity_df = calc_fn(entity_df)

        target_row = entity_df[entity_df["game_date"] == target_date]
        if not target_row.empty:
            results.append(target_row)

        if (i + 1) % 50 == 0:
            logger.info(f"Processed {i + 1}/{len(entity_ids)} entities")

    if results:
        result_df = pd.concat(results, ignore_index=True)
        logger.info(f"Generated {len(result_df)} rows for {target_date}")
        return result_df
    return pd.DataFrame()


# ============================================================================
# DATABASE UPSERT
# ============================================================================


def _build_upsert_sql(table: str, cols: list[str], conflict_cols: list[str]) -> str:
    """Build a parameterized UPSERT query."""
    col_list = ", ".join(cols)
    placeholder_list = ", ".join([f":{c}" for c in cols])
    update_cols = [c for c in cols if c not in conflict_cols]
    update_list = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    return f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholder_list})
        ON CONFLICT ({', '.join(conflict_cols)})
        DO UPDATE SET {update_list}
    """


def upsert_batting_averages(engine, df: pd.DataFrame):
    """Upsert batting average rows for target date."""
    if df.empty:
        return
    logger.info(f"Upserting {len(df)} batting rows...")

    base_cols = ["player_id", "game_id", "game_date", "season", "team_id", "game_number"]
    window_cols = [f"games_{w}" for w in BATTING_WINDOWS]
    avg_cols = [c for c in df.columns if c.startswith("avg_")]
    std_cols = [c for c in df.columns if c.startswith("std_")]
    ctx_cols = [c for c in ["rest_days", "games_last_7d"] if c in df.columns]

    all_cols = base_cols + window_cols + avg_cols + std_cols + ctx_cols
    insert_df = df[[c for c in all_cols if c in df.columns]].copy()

    # Round and cast
    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)

    for col in window_cols + ctx_cols:
        if col in insert_df.columns:
            insert_df[col] = insert_df[col].astype("Int64")

    cols = list(insert_df.columns)
    upsert_sql = _build_upsert_sql("mlb_player_average_batting", cols, ["player_id", "game_id"])

    records = insert_df.where(insert_df.notna(), None).to_dict(orient="records")
    with engine.begin() as conn:
        if records:
            conn.execute(text(upsert_sql), records)
    logger.info(f"Upserted {len(records)} batting rows")


def upsert_pitching_averages(engine, df: pd.DataFrame):
    """Upsert pitching average rows for target date."""
    if df.empty:
        return
    logger.info(f"Upserting {len(df)} pitching rows...")

    base_cols = ["player_id", "game_id", "game_date", "season", "team_id"]
    avg_cols = [c for c in df.columns if c.startswith("avg_")]
    std_cols = [c for c in df.columns if c.startswith("std_")]
    ctx_cols = [c for c in ["game_number", "days_rest", "pitch_count_last_start",
                             "starts_l3", "starts_l5", "starts_szn"] if c in df.columns]

    all_cols = base_cols + avg_cols + std_cols + ctx_cols
    insert_df = df[[c for c in all_cols if c in df.columns]].copy()

    for col in insert_df.columns:
        if col.startswith("avg_") or col.startswith("std_"):
            insert_df[col] = insert_df[col].round(4)

    for col in ["days_rest", "starts_l3", "starts_l5", "starts_szn"]:
        if col in insert_df.columns:
            insert_df[col] = insert_df[col].astype("Int64")

    if "pitch_count_last_start" in insert_df.columns:
        insert_df["pitch_count_last_start"] = insert_df["pitch_count_last_start"].astype("Int64")

    cols = list(insert_df.columns)
    upsert_sql = _build_upsert_sql("mlb_player_average_pitching", cols, ["player_id", "game_id"])

    records = insert_df.where(insert_df.notna(), None).to_dict(orient="records")
    with engine.begin() as conn:
        if records:
            conn.execute(text(upsert_sql), records)
    logger.info(f"Upserted {len(records)} pitching rows")


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="MLB Incremental Rolling Average Stats Update")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument(
        "--type", choices=["batting", "pitching"], default=None,
        help="Only update batting or pitching averages. Defaults to both.",
    )
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    do_batting = args.type in (None, "batting")
    do_pitching = args.type in (None, "pitching")

    engine = get_engine()

    type_label = args.type or "batting + pitching"
    logger.info("=" * 60)
    logger.info(f"MLB Incremental average stats update for {target_date} ({type_label})")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        season = get_season_for_date(engine, target_date)
        if not season:
            logger.info("No game data found for target date. Nothing to update.")
            return
        logger.info(f"Season: {season}")

        # Batting
        if do_batting:
            batter_ids = get_batter_ids_on_date(engine, target_date)
            if batter_ids:
                df = fetch_batter_season_games(engine, batter_ids, season, target_date)
                if not df.empty:
                    result = calculate_incremental(
                        df, target_date, "player_id", calculate_batting_rolling_for_player
                    )
                    upsert_batting_averages(engine, result)
            else:
                logger.info("No batters found for target date.")

        # Pitching
        if do_pitching:
            pitcher_ids = get_pitcher_ids_on_date(engine, target_date)
            if pitcher_ids:
                df = fetch_pitcher_season_games(engine, pitcher_ids, season, target_date)
                if not df.empty:
                    result = calculate_incremental(
                        df, target_date, "player_id", calculate_pitching_rolling_for_player
                    )
                    upsert_pitching_averages(engine, result)
            else:
                logger.info("No pitchers found for target date.")

        elapsed = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"Completed in {elapsed}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
