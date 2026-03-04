"""MLB matchup features: opposing team batting tendencies and pitcher context.

Computes team-level batting tendencies for pitcher K predictions.
For pitcher K, we need team-level opposing batting stats (K rate, batting avg),
not individual batter matchups.
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_opposing_team_batting_stats(
    engine: Engine, team_id: int, game_date: str, season: int
) -> dict:
    """Get opposing team's batting tendencies from their last 10 games.

    Aggregates mlb_player_game_stats_batting by team for games before game_date.

    Returns dict with:
        opp_team_avg_so_l10: average team strikeouts per game (L10)
        opp_team_avg_batting_avg_l10: team batting average over L10
    """
    query = text("""
        WITH team_games AS (
            SELECT
                b.game_id,
                b.game_date,
                SUM(b.so) AS team_so,
                SUM(b.h)  AS team_h,
                SUM(b.ab) AS team_ab
            FROM mlb_player_game_stats_batting b
            JOIN mlb_game_schedule gs ON b.game_id = gs.game_id
            WHERE b.team_id = :team_id
              AND b.game_date < :game_date
              AND b.season = :season
              AND gs.game_type = 'R'
              AND b.did_not_play = FALSE
            GROUP BY b.game_id, b.game_date
            ORDER BY b.game_date DESC
            LIMIT 10
        )
        SELECT
            AVG(team_so) AS opp_team_avg_so_l10,
            CASE WHEN SUM(team_ab) > 0
                 THEN SUM(team_h)::NUMERIC / SUM(team_ab)
                 ELSE NULL END AS opp_team_avg_batting_avg_l10,
            COUNT(*) AS games_found
        FROM team_games
    """)

    with engine.connect() as conn:
        row = conn.execute(
            query, {"team_id": team_id, "game_date": game_date, "season": season}
        ).fetchone()

    if row is None or row.games_found == 0:
        return {
            "opp_team_avg_so_l10": None,
            "opp_team_avg_batting_avg_l10": None,
        }

    return {
        "opp_team_avg_so_l10": float(row.opp_team_avg_so_l10) if row.opp_team_avg_so_l10 else None,
        "opp_team_avg_batting_avg_l10": float(row.opp_team_avg_batting_avg_l10) if row.opp_team_avg_batting_avg_l10 else None,
    }


def get_pitcher_handedness(engine: Engine, player_id: int) -> str | None:
    """Get pitcher's throwing hand from mlb_players.throws."""
    query = text("SELECT throws FROM mlb_players WHERE player_id = :player_id")
    with engine.connect() as conn:
        row = conn.execute(query, {"player_id": player_id}).fetchone()
    return row.throws if row and row.throws else None


def compute_matchup_features_bulk(engine: Engine, season: int) -> pd.DataFrame:
    """Bulk computation of opposing team batting stats for training.

    For each (game_id, pitcher's team opponent), compute opposing team's
    L10 batting tendencies using window functions.

    Returns DataFrame indexed on (game_id, pitcher_player_id) with columns:
        opp_team_avg_so_l10, opp_team_avg_batting_avg_l10
    """
    query = text("""
        WITH team_game_batting AS (
            -- Aggregate batting stats per team per game
            SELECT
                b.team_id,
                b.game_id,
                b.game_date,
                SUM(b.so) AS team_so,
                SUM(b.h)  AS team_h,
                SUM(b.ab) AS team_ab
            FROM mlb_player_game_stats_batting b
            JOIN mlb_game_schedule gs ON b.game_id = gs.game_id
            WHERE b.season = :season
              AND gs.game_type = 'R'
              AND b.did_not_play = FALSE
            GROUP BY b.team_id, b.game_id, b.game_date
        ),
        team_rolling AS (
            -- Compute L10 rolling averages per team using window functions
            -- LAG offset ensures we only use games BEFORE the current one
            SELECT
                team_id,
                game_id,
                game_date,
                AVG(team_so) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ) AS avg_so_l10,
                SUM(team_h) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ) AS sum_h_l10,
                SUM(team_ab) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                ) AS sum_ab_l10
            FROM team_game_batting
        ),
        pitcher_games AS (
            -- Get starting pitchers and their opponents
            SELECT
                pgs.player_id,
                pgs.game_id,
                pgs.game_date,
                pgs.team_id AS pitcher_team_id,
                CASE
                    WHEN gs.home_team_id = pgs.team_id THEN gs.away_team_id
                    ELSE gs.home_team_id
                END AS opp_team_id
            FROM mlb_player_game_stats_pitching pgs
            JOIN mlb_game_schedule gs ON pgs.game_id = gs.game_id
            WHERE pgs.season = :season
              AND pgs.is_starter = TRUE
              AND pgs.did_not_play = FALSE
              AND gs.game_type = 'R'
        )
        SELECT
            pg.player_id,
            pg.game_id,
            tr.avg_so_l10 AS opp_team_avg_so_l10,
            CASE WHEN tr.sum_ab_l10 > 0
                 THEN tr.sum_h_l10::NUMERIC / tr.sum_ab_l10
                 ELSE NULL END AS opp_team_avg_batting_avg_l10
        FROM pitcher_games pg
        JOIN team_rolling tr
          ON tr.team_id = pg.opp_team_id
         AND tr.game_id = pg.game_id
        ORDER BY pg.game_id
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"season": season})

    logger.info(
        "Computed matchup features for season %d: %d rows", season, len(df)
    )
    return df
