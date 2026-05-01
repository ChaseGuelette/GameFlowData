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


def get_opposing_team_batting_stats(engine: Engine, team_id: int, game_date: str, season: int) -> dict:
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
                SUM(b.ab) AS team_ab,
                SUM(b.pa) AS team_pa
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
        ),
        team_whiff AS (
            -- Team-level whiff% from Statcast batting (swing-weighted)
            SELECT
                SUM(scb.whiff_pct * scb.total_swings) /
                    NULLIF(SUM(scb.total_swings), 0) AS team_whiff_pct
            FROM mlb_player_game_statcast_batting scb
            WHERE scb.game_date IN (
                  SELECT DISTINCT game_date FROM team_games
              )
              AND scb.player_id IN (
                  SELECT DISTINCT b2.player_id
                  FROM mlb_player_game_stats_batting b2
                  WHERE b2.team_id = :team_id
                    AND b2.game_date IN (SELECT DISTINCT game_date FROM team_games)
                    AND b2.did_not_play = FALSE
              )
              AND scb.total_swings > 0
        )
        SELECT
            AVG(tg.team_so) AS opp_team_avg_so_l10,
            CASE WHEN SUM(tg.team_ab) > 0
                 THEN SUM(tg.team_h)::NUMERIC / SUM(tg.team_ab)
                 ELSE NULL END AS opp_team_avg_batting_avg_l10,
            CASE WHEN SUM(tg.team_pa) > 0
                 THEN SUM(tg.team_so)::NUMERIC / SUM(tg.team_pa)
                 ELSE NULL END AS opp_team_k_pct_l10,
            tw.team_whiff_pct AS opp_team_whiff_pct_l10,
            COUNT(*) AS games_found
        FROM team_games tg
        CROSS JOIN team_whiff tw
        GROUP BY tw.team_whiff_pct
    """)

    with engine.connect() as conn:
        row = conn.execute(query, {"team_id": team_id, "game_date": game_date, "season": season}).fetchone()

    if row is None or row.games_found == 0:
        return {
            "opp_team_avg_so_l10": None,
            "opp_team_avg_batting_avg_l10": None,
            "opp_team_k_pct_l10": None,
            "opp_team_whiff_pct_l10": None,
        }

    return {
        "opp_team_avg_so_l10": float(row.opp_team_avg_so_l10) if row.opp_team_avg_so_l10 else None,
        "opp_team_avg_batting_avg_l10": float(row.opp_team_avg_batting_avg_l10)
        if row.opp_team_avg_batting_avg_l10
        else None,
        "opp_team_k_pct_l10": float(row.opp_team_k_pct_l10) if row.opp_team_k_pct_l10 else None,
        "opp_team_whiff_pct_l10": float(row.opp_team_whiff_pct_l10) if row.opp_team_whiff_pct_l10 else None,
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
                SUM(b.ab) AS team_ab,
                SUM(b.pa) AS team_pa
            FROM mlb_player_game_stats_batting b
            JOIN mlb_game_schedule gs ON b.game_id = gs.game_id
            WHERE b.season = :season
              AND gs.game_type = 'R'
              AND b.did_not_play = FALSE
            GROUP BY b.team_id, b.game_id, b.game_date
        ),
        team_game_whiff AS (
            -- Team-level whiff % per game from Statcast batting
            SELECT
                bat.team_id,
                scb.game_date,
                SUM(scb.whiff_pct * scb.total_swings) /
                    NULLIF(SUM(scb.total_swings), 0) AS team_whiff_pct
            FROM mlb_player_game_statcast_batting scb
            JOIN mlb_player_game_stats_batting bat
              ON bat.player_id = scb.player_id
             AND bat.game_date = scb.game_date
             AND bat.did_not_play = FALSE
            WHERE bat.season = :season
              AND scb.total_swings > 0
            GROUP BY bat.team_id, scb.game_date
        ),
        team_rolling AS (
            -- Compute L10 rolling averages per team using window functions
            -- ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING ensures time-travel safety
            SELECT
                tgb.team_id,
                tgb.game_id,
                tgb.game_date,
                AVG(tgb.team_so) OVER w AS avg_so_l10,
                SUM(tgb.team_h) OVER w AS sum_h_l10,
                SUM(tgb.team_ab) OVER w AS sum_ab_l10,
                SUM(tgb.team_so) OVER w AS sum_so_l10,
                SUM(tgb.team_pa) OVER w AS sum_pa_l10,
                AVG(tw.team_whiff_pct) OVER w AS avg_whiff_pct_l10
            FROM team_game_batting tgb
            LEFT JOIN team_game_whiff tw
              ON tw.team_id = tgb.team_id
             AND tw.game_date = tgb.game_date
            WINDOW w AS (
                PARTITION BY tgb.team_id
                ORDER BY tgb.game_date
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            )
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
                 ELSE NULL END AS opp_team_avg_batting_avg_l10,
            CASE WHEN tr.sum_pa_l10 > 0
                 THEN tr.sum_so_l10::NUMERIC / tr.sum_pa_l10
                 ELSE NULL END AS opp_team_k_pct_l10,
            tr.avg_whiff_pct_l10 AS opp_team_whiff_pct_l10
        FROM pitcher_games pg
        JOIN team_rolling tr
          ON tr.team_id = pg.opp_team_id
         AND tr.game_id = pg.game_id
        ORDER BY pg.game_id
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"season": season})

    logger.info("Computed matchup features for season %d: %d rows", season, len(df))
    return df


def get_lineup_k_features(
    engine: Engine, opp_team_id: int, game_id: int, game_date: str, season: int,
    pitcher_throws: str | None = None,
) -> dict:
    """Compute lineup-based features for single-game inference.

    Returns dict with projected_lineup_k_pct and pct_opp_lineup_same_hand.
    Falls back to league-average defaults when lineup has < 3 confirmed batters.
    """
    result = {
        "projected_lineup_k_pct": 0.22,
        "pct_opp_lineup_same_hand": 0.50,
    }

    query = text("""
        SELECT
            gl.player_id,
            COALESCE(psa.k_pct, 0.22) AS k_pct,
            COALESCE(p.bats, 'R') AS bats
        FROM mlb_game_lineups gl
        LEFT JOIN mlb_player_season_advanced psa
            ON psa.player_id = gl.player_id
            AND psa.season = :season
        LEFT JOIN mlb_players p
            ON p.player_id = gl.player_id
        WHERE gl.game_pk = :game_id
            AND gl.team_id = :opp_team_id
            AND gl.is_pitcher = FALSE
        ORDER BY gl.lineup_position
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {
            "game_id": game_id,
            "opp_team_id": opp_team_id,
            "season": season,
        }).fetchall()

    if len(rows) < 3:
        return result

    # Projected lineup K%: simple average of lineup batters' K%
    k_pcts = [float(r.k_pct) for r in rows]
    result["projected_lineup_k_pct"] = sum(k_pcts) / len(k_pcts) if k_pcts else 0.22

    # Same-hand percentage
    if pitcher_throws:
        same_hand_count = sum(
            1 for r in rows
            if r.bats == pitcher_throws and r.bats != "S"
        )
        result["pct_opp_lineup_same_hand"] = same_hand_count / len(rows)

    return result


def compute_lineup_features_bulk(engine: Engine, season: int) -> pd.DataFrame:
    """Bulk computation of lineup-based features for training.

    Returns DataFrame with (game_id, pitcher_player_id, projected_lineup_k_pct, pct_opp_lineup_same_hand).
    Gracefully returns empty DataFrame when mlb_game_lineups has no data for the season.
    """
    query = text("""
        WITH pitcher_games AS (
            SELECT
                pgs.player_id AS pitcher_player_id,
                pgs.game_id,
                pgs.team_id AS pitcher_team_id,
                CASE
                    WHEN gs.home_team_id = pgs.team_id THEN gs.away_team_id
                    ELSE gs.home_team_id
                END AS opp_team_id,
                p.throws AS pitcher_throws
            FROM mlb_player_game_stats_pitching pgs
            JOIN mlb_game_schedule gs ON pgs.game_id = gs.game_id
            LEFT JOIN mlb_players p ON p.player_id = pgs.player_id
            WHERE pgs.season = :season
              AND pgs.is_starter = TRUE
              AND pgs.did_not_play = FALSE
              AND gs.game_type = 'R'
        ),
        lineup_stats AS (
            SELECT
                gl.game_pk,
                gl.team_id,
                COUNT(*) AS lineup_size,
                AVG(COALESCE(psa.k_pct, 0.22)) AS avg_k_pct,
                ARRAY_AGG(COALESCE(bp.bats, 'R')) AS bats_array
            FROM mlb_game_lineups gl
            LEFT JOIN mlb_player_season_advanced psa
                ON psa.player_id = gl.player_id
                AND psa.season = :season
            LEFT JOIN mlb_players bp
                ON bp.player_id = gl.player_id
            WHERE gl.is_pitcher = FALSE
            GROUP BY gl.game_pk, gl.team_id
            HAVING COUNT(*) >= 3
        )
        SELECT
            pg.pitcher_player_id AS player_id,
            pg.game_id,
            ls.avg_k_pct AS projected_lineup_k_pct,
            ls.bats_array,
            pg.pitcher_throws
        FROM pitcher_games pg
        LEFT JOIN lineup_stats ls
            ON ls.game_pk = pg.game_id
            AND ls.team_id = pg.opp_team_id
    """)

    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '120000'"))
        df = pd.read_sql(query, conn, params={"season": season})

    if df.empty:
        logger.info("No lineup features for season %d (mlb_game_lineups may be empty)", season)
        return pd.DataFrame(columns=["player_id", "game_id", "projected_lineup_k_pct", "pct_opp_lineup_same_hand"])

    # Fill defaults for games without lineup data
    df["projected_lineup_k_pct"] = df["projected_lineup_k_pct"].fillna(0.22)

    # Compute pct_opp_lineup_same_hand from bats_array + pitcher_throws
    def _same_hand_pct(row):
        bats_arr = row.get("bats_array")
        throws = row.get("pitcher_throws")
        if not bats_arr or not throws or not isinstance(bats_arr, list):
            return 0.50
        same = sum(1 for b in bats_arr if b == throws and b != "S")
        return same / len(bats_arr) if bats_arr else 0.50

    df["pct_opp_lineup_same_hand"] = df.apply(_same_hand_pct, axis=1)

    logger.info("Computed lineup features for season %d: %d rows", season, len(df))
    return df[["player_id", "game_id", "projected_lineup_k_pct", "pct_opp_lineup_same_hand"]]
