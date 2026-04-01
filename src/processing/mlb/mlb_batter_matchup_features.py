"""MLB Batter Matchup Features: opposing starter stats + platoon splits.

Provides both single-game (inference) and bulk (training) computations
for opposing starting pitcher stats and batter platoon splits.
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_opposing_starter_stats(engine: Engine, opp_pitcher_id: int, game_date: str) -> dict:
    """Fetch opposing starter's rolling stats for single-game inference.

    Returns dict with opp_pitcher_* keys, all defaulting to 0 on miss.
    """
    query = text("""
        SELECT
            COALESCE(pa.avg_era_l5, 0) AS opp_pitcher_avg_era_l5,
            COALESCE(pa.avg_whip_l5, 0) AS opp_pitcher_avg_whip_l5,
            COALESCE(pa.avg_k_per_9_l5, 0) AS opp_pitcher_avg_k_per_9_l5,
            COALESCE(pa.avg_bb_per_9_l5, 0) AS opp_pitcher_avg_bb_per_9_l5,
            COALESCE(pa.avg_h_allowed_l5, 0) AS opp_pitcher_avg_h_allowed_l5,
            COALESCE(pa.avg_hr_allowed_l5, 0) AS opp_pitcher_avg_hr_allowed_l5,
            COALESCE(sc.avg_xwoba_against_l5, 0) AS opp_pitcher_xwoba_against_l5,
            COALESCE(sc.avg_hard_hit_pct_against_l5, 0) AS opp_pitcher_hard_hit_pct_against_l5,
            COALESCE(sc.avg_avg_fastball_velo_l5, 0) AS opp_pitcher_avg_fastball_velo_l5,
            LEAST(COALESCE(pa.days_rest, 5), 14) AS opp_pitcher_days_rest
        FROM mlb_player_average_pitching pa
        LEFT JOIN LATERAL (
            SELECT avg_xwoba_against_l5, avg_hard_hit_pct_against_l5,
                   avg_avg_fastball_velo_l5
            FROM mlb_player_average_statcast_pitching sc2
            WHERE sc2.player_id = pa.player_id
              AND sc2.game_date <= :game_date
            ORDER BY sc2.game_date DESC LIMIT 1
        ) sc ON TRUE
        WHERE pa.player_id = :pitcher_id
          AND pa.game_date <= :game_date
        ORDER BY pa.game_date DESC LIMIT 1
    """)

    defaults = {
        "opp_pitcher_avg_era_l5": 0,
        "opp_pitcher_avg_whip_l5": 0,
        "opp_pitcher_avg_k_per_9_l5": 0,
        "opp_pitcher_avg_bb_per_9_l5": 0,
        "opp_pitcher_avg_h_allowed_l5": 0,
        "opp_pitcher_avg_hr_allowed_l5": 0,
        "opp_pitcher_xwoba_against_l5": 0,
        "opp_pitcher_hard_hit_pct_against_l5": 0,
        "opp_pitcher_avg_fastball_velo_l5": 0,
        "opp_pitcher_days_rest": 5,
    }

    with engine.connect() as conn:
        row = conn.execute(query, {"pitcher_id": opp_pitcher_id, "game_date": game_date}).fetchone()

    if row is None:
        return defaults

    return {col: float(getattr(row, col) or defaults[col]) for col in defaults}


def compute_opposing_starter_bulk(engine: Engine, season: int) -> pd.DataFrame:
    """Bulk training: opposing starter stats for every batter-game in a season.

    Returns DataFrame indexed on (game_id, player_id) with opp_pitcher_* columns.
    """
    query = text("""
        WITH opp_starters AS (
            -- Identify the opposing starting pitcher per game per team
            SELECT pgs.player_id AS pitcher_id,
                   pgs.game_id,
                   pgs.team_id AS pitcher_team_id
            FROM mlb_player_game_stats_pitching pgs
            WHERE pgs.is_starter = TRUE
              AND pgs.did_not_play = FALSE
              AND pgs.season = :season
        )
        SELECT
            bgs.player_id,
            bgs.game_id,
            COALESCE(pa.avg_era_l5, 0) AS opp_pitcher_avg_era_l5,
            COALESCE(pa.avg_whip_l5, 0) AS opp_pitcher_avg_whip_l5,
            COALESCE(pa.avg_k_per_9_l5, 0) AS opp_pitcher_avg_k_per_9_l5,
            COALESCE(pa.avg_bb_per_9_l5, 0) AS opp_pitcher_avg_bb_per_9_l5,
            COALESCE(pa.avg_h_allowed_l5, 0) AS opp_pitcher_avg_h_allowed_l5,
            COALESCE(pa.avg_hr_allowed_l5, 0) AS opp_pitcher_avg_hr_allowed_l5,
            COALESCE(sc.avg_xwoba_against_l5, 0) AS opp_pitcher_xwoba_against_l5,
            COALESCE(sc.avg_hard_hit_pct_against_l5, 0) AS opp_pitcher_hard_hit_pct_against_l5,
            COALESCE(sc.avg_avg_fastball_velo_l5, 0) AS opp_pitcher_avg_fastball_velo_l5,
            LEAST(COALESCE(pa.days_rest, 5), 14) AS opp_pitcher_days_rest
        FROM mlb_player_game_stats_batting bgs
        JOIN mlb_game_schedule gs ON gs.game_id = bgs.game_id
        LEFT JOIN opp_starters os
            ON os.game_id = bgs.game_id
           AND os.pitcher_team_id != bgs.team_id
        LEFT JOIN LATERAL (
            SELECT avg_era_l5, avg_whip_l5, avg_k_per_9_l5, avg_bb_per_9_l5,
                   avg_h_allowed_l5, avg_hr_allowed_l5, days_rest
            FROM mlb_player_average_pitching pa2
            WHERE pa2.player_id = os.pitcher_id
              AND pa2.game_date <= bgs.game_date
            ORDER BY pa2.game_date DESC LIMIT 1
        ) pa ON TRUE
        LEFT JOIN LATERAL (
            SELECT avg_xwoba_against_l5, avg_hard_hit_pct_against_l5,
                   avg_avg_fastball_velo_l5
            FROM mlb_player_average_statcast_pitching sc2
            WHERE sc2.player_id = os.pitcher_id
              AND sc2.game_date <= bgs.game_date
            ORDER BY sc2.game_date DESC LIMIT 1
        ) sc ON TRUE
        WHERE bgs.season = :season
          AND bgs.did_not_play = FALSE
          AND gs.game_type = 'R'
    """)

    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '300000'"))  # 5 min
        df = pd.read_sql(query, conn, params={"season": season})

    logger.info("Computed opposing starter features for season %d: %d rows", season, len(df))
    return df


def compute_platoon_splits_bulk(engine: Engine, season: int) -> pd.DataFrame:
    """Bulk training: batter L20 stats vs. same pitcher hand as today's opponent.

    Returns DataFrame with (game_id, player_id) and platoon split columns.
    Games with no opposing starter or missing handedness data get NULLs.
    """
    query = text("""
        WITH opp_starters AS (
            SELECT pgs.player_id AS pitcher_id,
                   pgs.game_id,
                   pgs.team_id AS pitcher_team_id
            FROM mlb_player_game_stats_pitching pgs
            WHERE pgs.is_starter = TRUE
              AND pgs.did_not_play = FALSE
              AND pgs.season = :season
        )
        SELECT
            bgs.player_id,
            bgs.game_id,
            -- is_same_hand: switch hitters (S) always 0
            CASE WHEN bp.bats = 'S' THEN 0
                 WHEN bp.bats IS NOT NULL AND opp_p.throws IS NOT NULL
                      AND bp.bats = opp_p.throws THEN 1
                 ELSE 0 END AS is_same_hand,
            platoon.avg_h_vs_hand AS batter_avg_h_vs_hand_l20,
            platoon.avg_hr_vs_hand AS batter_avg_hr_vs_hand_l20,
            platoon.ops_vs_hand AS batter_avg_ops_vs_hand_l20
        FROM mlb_player_game_stats_batting bgs
        JOIN mlb_game_schedule gs ON gs.game_id = bgs.game_id
        LEFT JOIN mlb_players bp ON bp.player_id = bgs.player_id
        LEFT JOIN opp_starters os
            ON os.game_id = bgs.game_id
           AND os.pitcher_team_id != bgs.team_id
        LEFT JOIN mlb_players opp_p ON opp_p.player_id = os.pitcher_id
        LEFT JOIN LATERAL (
            SELECT
                CASE WHEN COUNT(*) >= 3 THEN AVG(sub.h) END AS avg_h_vs_hand,
                CASE WHEN COUNT(*) >= 3 THEN AVG(sub.hr) END AS avg_hr_vs_hand,
                CASE WHEN SUM(sub.ab) > 0 AND COUNT(*) >= 3
                     THEN (SUM(sub.h) + SUM(sub.bb))::NUMERIC
                          / NULLIF(SUM(sub.ab) + SUM(sub.bb), 0)
                          + SUM(sub.tb)::NUMERIC / NULLIF(SUM(sub.ab), 0)
                END AS ops_vs_hand
            FROM (
                SELECT b2.h, b2.hr, b2.ab, b2.bb, b2.tb
                FROM mlb_player_game_stats_batting b2
                JOIN mlb_player_game_stats_pitching opp2
                    ON opp2.game_id = b2.game_id
                   AND opp2.team_id != b2.team_id
                   AND opp2.is_starter = TRUE
                   AND opp2.did_not_play = FALSE
                JOIN mlb_players opp_p2 ON opp_p2.player_id = opp2.player_id
                WHERE b2.player_id = bgs.player_id
                  AND b2.game_date < bgs.game_date
                  AND b2.did_not_play = FALSE
                  AND opp_p2.throws = opp_p.throws
                ORDER BY b2.game_date DESC LIMIT 20
            ) sub
        ) platoon ON TRUE
        WHERE bgs.season = :season
          AND bgs.did_not_play = FALSE
          AND gs.game_type = 'R'
    """)

    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '900000'"))  # 15 min – heavy LATERAL join, runs once per backtest
        df = pd.read_sql(query, conn, params={"season": season})

    logger.info("Computed platoon splits for season %d: %d rows", season, len(df))
    return df
