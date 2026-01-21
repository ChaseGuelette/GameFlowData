import warnings

import numpy as np
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from src.db.client import get_engine

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)


def backfill_team_allowed_by_position(engine, seasons: list[str]):
    formatted_seasons = []
    for s in seasons:
        if "-" in s and len(s) == 7:
            start_year = s.split("-")[0]
            formatted_seasons.append(f"2{start_year}")
        else:
            formatted_seasons.append(s)

    print(f"--- Starting Backfill for: {formatted_seasons} ---")
    df = fetch_raw_allowed_stats(engine, formatted_seasons)

    if df.empty:
        print("❌ No data found.")
        return

    print(f"Loaded {len(df):,} rows. Computing rolling windows...")
    df_processed = compute_rolling_metrics(df)

    # Validation Cleaning
    df_final = df_processed.dropna(subset=["games_l5"])
    df_final = df_final.dropna(subset=["team_id", "game_id", "game_date", "position_group"])

    print(f"Computed {len(df_final):,} rows. Inserting...")
    batch_insert_to_db(engine, df_final)
    print("--- Complete ---")


def fetch_raw_allowed_stats(engine, seasons):
    query = text("""
        SELECT
            tgs.team_id, tgs.game_id, tgs.game_date::DATE as game_date, tgs.season_id,
            COALESCE(ph.position_group, 'U') as position_group,

            -- Core Stats
            COALESCE(SUM(pgs.pts), 0) as pts,
            COALESCE(SUM(pgs.reb), 0) as reb,
            COALESCE(SUM(pgs.ast), 0) as ast,
            COALESCE(SUM(pgs.fg3m), 0) as threes,
            COALESCE(SUM(pgs.stl), 0) as stl,
            COALESCE(SUM(pgs.blk), 0) as blk,
            COALESCE(SUM(pgs.tov), 0) as tov,

            -- Sharp Stats (NEW)
            COALESCE(SUM(pgs.fta), 0) as fta,
            COALESCE(SUM(pgs.oreb), 0) as oreb,
            COALESCE(SUM(pgs.pf), 0) as pf,

            -- Possessions (from Advanced)
            COALESCE(SUM(adv.possessions), 0) as poss_faced

        FROM team_game_stats tgs
        JOIN player_game_stats pgs
            ON tgs.game_id = pgs.game_id AND tgs.team_id != pgs.team_id AND pgs.min > 0
        JOIN player_game_advanced_stats adv
            ON pgs.game_id = adv.game_id AND pgs.player_id = adv.player_id
        LEFT JOIN LATERAL (
            SELECT position_group FROM player_position_history ph
            WHERE ph.player_id = pgs.player_id AND ph.snapshot_date < tgs.game_date::DATE
            ORDER BY ph.snapshot_date DESC LIMIT 1
        ) ph ON TRUE
        WHERE tgs.season_id IN :seasons
        GROUP BY tgs.team_id, tgs.game_id, tgs.game_date, tgs.season_id, ph.position_group
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"seasons": tuple(seasons)})


def compute_rolling_metrics(df):
    df = df.sort_values(["team_id", "position_group", "game_date"]).reset_index(drop=True)

    # ADDED Sharp Stats to metrics list
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
        rolled = grouper[metrics].apply(lambda x: x.shift(1).rolling(w_size, min_periods=1).sum())
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
    szn = grouper[metrics].apply(lambda x: x.shift(1).expanding().sum())
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


def batch_insert_to_db(engine, df, batch_size=2000):
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
    vals = ", ".join([f":{c}" for c in valid_cols])
    upsert = text(
        f"INSERT INTO team_allowed_by_position ({cols}) VALUES ({vals}) ON CONFLICT (team_id, game_id, position_group) DO UPDATE SET "
        + ", ".join([f"{c} = EXCLUDED.{c}" for c in valid_cols if c not in ["team_id", "game_id", "position_group"]])
        + ", created_at = NOW();"
    )

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        for i in tqdm(range(0, len(records), batch_size)):
            batch = records[i : i + batch_size]
            if batch:
                conn.execute(upsert, batch)


if __name__ == "__main__":
    engine = get_engine()
    # Remember to TRUNCATE table if re-running!
    backfill_team_allowed_by_position(
        engine,
        seasons=[
            "2018-19",
            "2019-20",
            "2020-21",
            "2021-22",
            "2022-23",
            "2023-24",
            "2024-25",
            "2025-26",
        ],
    )
