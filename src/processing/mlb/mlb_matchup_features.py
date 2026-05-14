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

LINEUP_K_DEFAULT = 0.22
LINEUP_WHIFF_DEFAULT = 0.22
LINEUP_CHASE_DEFAULT = 0.28
LINEUP_CONTACT_DEFAULT = 1.0 - LINEUP_WHIFF_DEFAULT
LINEUP_SAME_HAND_DEFAULT = 0.50

LINEUP_FEATURE_COLUMNS = [
    "projected_lineup_k_pct",
    "projected_lineup_whiff_pct",
    "projected_lineup_chase_pct",
    "projected_lineup_contact_rate",
    "projected_lineup_same_hand_k_pct",
    "projected_lineup_opposite_hand_k_pct",
    "projected_lineup_hand_k_delta",
    "projected_lineup_top3_k_pct",
    "projected_lineup_mid3_k_pct",
    "projected_lineup_bot3_k_pct",
    "projected_lineup_k_concentration",
    "pct_opp_lineup_same_hand",
]


def _lineup_defaults() -> dict[str, float]:
    return {
        "projected_lineup_k_pct": LINEUP_K_DEFAULT,
        "projected_lineup_whiff_pct": LINEUP_WHIFF_DEFAULT,
        "projected_lineup_chase_pct": LINEUP_CHASE_DEFAULT,
        "projected_lineup_contact_rate": LINEUP_CONTACT_DEFAULT,
        "projected_lineup_same_hand_k_pct": LINEUP_K_DEFAULT,
        "projected_lineup_opposite_hand_k_pct": LINEUP_K_DEFAULT,
        "projected_lineup_hand_k_delta": 0.0,
        "projected_lineup_top3_k_pct": LINEUP_K_DEFAULT,
        "projected_lineup_mid3_k_pct": LINEUP_K_DEFAULT,
        "projected_lineup_bot3_k_pct": LINEUP_K_DEFAULT,
        "projected_lineup_k_concentration": 0.0,
        "pct_opp_lineup_same_hand": LINEUP_SAME_HAND_DEFAULT,
    }


def _table_exists(engine: Engine, table_name: str) -> bool:
    """Return True when an optional public table exists in the target DB."""
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("SELECT to_regclass(:table_name) IS NOT NULL"),
                {"table_name": f"public.{table_name}"},
            ).scalar()
        )


def get_opposing_team_batting_stats(engine: Engine, team_id: int, game_date: str, season: int) -> dict:
    """Get opposing team's batting tendencies from their last 10 games.

    Aggregates mlb_player_game_stats_batting by team for games before game_date.

    Returns dict with:
        opp_team_avg_so_l10: average team strikeouts per game (L10)
        opp_team_avg_batting_avg_l10: team batting average over L10
        opp_team_k_pct_l10: team strikeout rate over L10
        opp_team_whiff_pct_l10: team whiff % over L10
        opp_team_contact_rate_l10: proxy team contact rate over L10 (1 - whiff%)
        opp_team_chase_pct_l10: team chase % over L10, weighted by pitches seen
        opp_team_zone_contact_pct_l10: proxy zone-contact over L10 = zone_pct * (1-whiff_pct)
    """
    query = text("""
        WITH team_games AS (
            SELECT
                b.game_id,
                b.game_date,
                SUM(b.so) AS team_so,
                SUM(b.h) AS team_h,
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
            SELECT
                SUM(COALESCE(scb.whiff_pct, 0) * COALESCE(scb.total_pitches_seen, scb.total_swings, 0))
                    / NULLIF(SUM(COALESCE(scb.total_pitches_seen, scb.total_swings, 0)), 0) AS team_whiff_pct,
                SUM(COALESCE(scb.chase_pct, 0) * COALESCE(scb.total_pitches_seen, scb.total_swings, 0))
                    / NULLIF(SUM(COALESCE(scb.total_pitches_seen, scb.total_swings, 0)), 0) AS team_chase_pct,
                SUM(
                    COALESCE(scb.zone_pct, 0) * (1.0 - COALESCE(scb.whiff_pct, 0))
                    * COALESCE(scb.total_pitches_seen, scb.total_swings, 0)
                ) / NULLIF(SUM(COALESCE(scb.total_pitches_seen, scb.total_swings, 0)), 0)
                    AS team_zone_contact_pct,
                COUNT(*) AS games_found
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
              AND COALESCE(scb.total_pitches_seen, scb.total_swings, 0) > 0
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
            CASE WHEN tw.team_whiff_pct IS NOT NULL THEN 1.0 - tw.team_whiff_pct END AS opp_team_contact_rate_l10,
            tw.team_chase_pct AS opp_team_chase_pct_l10,
            tw.team_zone_contact_pct AS opp_team_zone_contact_pct_l10,
            COALESCE(tw.games_found, 0) AS games_found
        FROM team_games tg
        CROSS JOIN team_whiff tw
        GROUP BY tw.team_whiff_pct, tw.team_chase_pct, tw.team_zone_contact_pct, tw.games_found
    """)

    with engine.connect() as conn:
        row = conn.execute(query, {"team_id": team_id, "game_date": game_date, "season": season}).fetchone()

    if row is None or row.games_found == 0:
        return {
            "opp_team_avg_so_l10": None,
            "opp_team_avg_batting_avg_l10": None,
            "opp_team_k_pct_l10": None,
            "opp_team_whiff_pct_l10": None,
            "opp_team_contact_rate_l10": None,
            "opp_team_chase_pct_l10": None,
            "opp_team_zone_contact_pct_l10": None,
        }

    return {
        "opp_team_avg_so_l10": float(row.opp_team_avg_so_l10) if row.opp_team_avg_so_l10 is not None else None,
        "opp_team_avg_batting_avg_l10": float(row.opp_team_avg_batting_avg_l10)
        if row.opp_team_avg_batting_avg_l10 is not None
        else None,
        "opp_team_k_pct_l10": float(row.opp_team_k_pct_l10) if row.opp_team_k_pct_l10 is not None else None,
        "opp_team_whiff_pct_l10": float(row.opp_team_whiff_pct_l10)
        if row.opp_team_whiff_pct_l10 is not None
        else None,
        "opp_team_contact_rate_l10": float(row.opp_team_contact_rate_l10)
        if row.opp_team_contact_rate_l10 is not None
        else None,
        "opp_team_chase_pct_l10": float(row.opp_team_chase_pct_l10)
        if row.opp_team_chase_pct_l10 is not None
        else None,
        "opp_team_zone_contact_pct_l10": float(row.opp_team_zone_contact_pct_l10)
        if row.opp_team_zone_contact_pct_l10 is not None
        else None,
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
        opp_team_avg_so_l10,
        opp_team_avg_batting_avg_l10,
        opp_team_k_pct_l10,
        opp_team_whiff_pct_l10,
        opp_team_contact_rate_l10,
        opp_team_chase_pct_l10,
        opp_team_zone_contact_pct_l10
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
            -- Team-level whiff / contact proxies per game from Statcast batting
            SELECT
                bat.team_id,
                scb.game_date,
                SUM(scb.whiff_pct * COALESCE(scb.total_pitches_seen, scb.total_swings)) /
                    NULLIF(SUM(COALESCE(scb.total_pitches_seen, scb.total_swings)), 0) AS team_whiff_pct,
                SUM(scb.chase_pct * COALESCE(scb.total_pitches_seen, scb.total_swings)) /
                    NULLIF(SUM(COALESCE(scb.total_pitches_seen, scb.total_swings)), 0) AS team_chase_pct,
                SUM(scb.zone_pct * (1.0 - COALESCE(scb.whiff_pct, 0)) *
                    COALESCE(scb.total_pitches_seen, scb.total_swings)) /
                    NULLIF(SUM(COALESCE(scb.total_pitches_seen, scb.total_swings)), 0) AS team_zone_contact_pct
            FROM mlb_player_game_statcast_batting scb
            JOIN mlb_player_game_stats_batting bat
              ON bat.player_id = scb.player_id
             AND bat.game_date = scb.game_date
             AND bat.did_not_play = FALSE
            WHERE bat.season = :season
              AND COALESCE(scb.total_pitches_seen, scb.total_swings) > 0
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
                AVG(tw.team_whiff_pct) OVER w AS avg_whiff_pct_l10,
                AVG(tw.team_chase_pct) OVER w AS avg_chase_pct_l10,
                AVG(tw.team_zone_contact_pct) OVER w AS avg_zone_contact_pct_l10
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
            tr.avg_whiff_pct_l10 AS opp_team_whiff_pct_l10,
            CASE WHEN tr.avg_whiff_pct_l10 IS NOT NULL THEN 1.0 - tr.avg_whiff_pct_l10 END
                AS opp_team_contact_rate_l10,
            tr.avg_chase_pct_l10 AS opp_team_chase_pct_l10,
            tr.avg_zone_contact_pct_l10 AS opp_team_zone_contact_pct_l10
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

    Returns lineup K/contact profile features and pct_opp_lineup_same_hand.
    Falls back to neutral defaults when lineup has < 3 confirmed batters.
    """
    result = _lineup_defaults()

    if not _table_exists(engine, "mlb_game_lineups"):
        logger.debug("mlb_game_lineups table missing; using lineup feature defaults")
        return result

    query = text("""
        SELECT
            gl.player_id,
            gl.lineup_position,
            CASE gl.lineup_position
                WHEN 1 THEN 1.12 WHEN 2 THEN 1.09 WHEN 3 THEN 1.06
                WHEN 4 THEN 1.03 WHEN 5 THEN 1.00 WHEN 6 THEN 0.97
                WHEN 7 THEN 0.94 WHEN 8 THEN 0.91 WHEN 9 THEN 0.88
                ELSE 1.00
            END AS slot_weight,
            COALESCE(
                CASE WHEN bat.avg_pa_szn > 0 THEN bat.avg_so_szn::float / bat.avg_pa_szn END,
                CASE WHEN bat.avg_pa_l20 > 0 THEN bat.avg_so_l20::float / bat.avg_pa_l20 END,
                CASE WHEN bat.avg_pa_l10 > 0 THEN bat.avg_so_l10::float / bat.avg_pa_l10 END,
                :default_k
            ) AS k_pct,
            COALESCE(sc.avg_whiff_pct_szn, sc.avg_whiff_pct_l10, sc.avg_whiff_pct_l5, :default_whiff) AS whiff_pct,
            COALESCE(sc.avg_chase_pct_szn, sc.avg_chase_pct_l10, sc.avg_chase_pct_l5, :default_chase) AS chase_pct,
            1.0 - COALESCE(sc.avg_whiff_pct_szn, sc.avg_whiff_pct_l10, sc.avg_whiff_pct_l5, :default_whiff) AS contact_rate,
            COALESCE(p.bats, 'R') AS bats
        FROM mlb_game_lineups gl
        LEFT JOIN mlb_players p
            ON p.player_id = gl.player_id
        LEFT JOIN LATERAL (
            SELECT avg_so_szn, avg_pa_szn, avg_so_l20, avg_pa_l20, avg_so_l10, avg_pa_l10
            FROM mlb_player_average_batting b
            WHERE b.player_id = gl.player_id
              AND b.season = :season
              AND b.game_date < :game_date
            ORDER BY b.game_date DESC
            LIMIT 1
        ) bat ON TRUE
        LEFT JOIN LATERAL (
            SELECT avg_whiff_pct_szn, avg_whiff_pct_l10, avg_whiff_pct_l5,
                   avg_chase_pct_szn, avg_chase_pct_l10, avg_chase_pct_l5
            FROM mlb_player_average_statcast_batting scb
            WHERE scb.player_id = gl.player_id
              AND scb.season = :season
              AND scb.game_date < :game_date
            ORDER BY scb.game_date DESC
            LIMIT 1
        ) sc ON TRUE
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
            "game_date": game_date,
            "default_k": LINEUP_K_DEFAULT,
            "default_whiff": LINEUP_WHIFF_DEFAULT,
            "default_chase": LINEUP_CHASE_DEFAULT,
        }).fetchall()

    if len(rows) < 3:
        return result

    def _weighted_avg(items, value_attr: str, fallback: float) -> float:
        weights = [float(getattr(r, "slot_weight") or 1.0) for r in items]
        denom = sum(weights)
        if denom <= 0:
            return fallback
        return sum(float(getattr(r, value_attr) or fallback) * w for r, w in zip(items, weights)) / denom

    result["projected_lineup_k_pct"] = _weighted_avg(rows, "k_pct", LINEUP_K_DEFAULT)
    result["projected_lineup_whiff_pct"] = _weighted_avg(rows, "whiff_pct", LINEUP_WHIFF_DEFAULT)
    result["projected_lineup_chase_pct"] = _weighted_avg(rows, "chase_pct", LINEUP_CHASE_DEFAULT)
    result["projected_lineup_contact_rate"] = _weighted_avg(rows, "contact_rate", LINEUP_CONTACT_DEFAULT)

    if pitcher_throws:
        same_rows = [r for r in rows if r.bats == pitcher_throws and r.bats != "S"]
        opp_rows = [r for r in rows if not (r.bats == pitcher_throws and r.bats != "S")]
        result["pct_opp_lineup_same_hand"] = len(same_rows) / len(rows)
        result["projected_lineup_same_hand_k_pct"] = (
            _weighted_avg(same_rows, "k_pct", result["projected_lineup_k_pct"])
            if same_rows else result["projected_lineup_k_pct"]
        )
        result["projected_lineup_opposite_hand_k_pct"] = (
            _weighted_avg(opp_rows, "k_pct", result["projected_lineup_k_pct"])
            if opp_rows else result["projected_lineup_k_pct"]
        )
        result["projected_lineup_hand_k_delta"] = (
            result["projected_lineup_same_hand_k_pct"] - result["projected_lineup_opposite_hand_k_pct"]
        )

    top_rows = [r for r in rows if r.lineup_position and 1 <= r.lineup_position <= 3]
    mid_rows = [r for r in rows if r.lineup_position and 4 <= r.lineup_position <= 6]
    bot_rows = [r for r in rows if r.lineup_position and 7 <= r.lineup_position <= 9]
    result["projected_lineup_top3_k_pct"] = (
        _weighted_avg(top_rows, "k_pct", result["projected_lineup_k_pct"])
        if top_rows else result["projected_lineup_k_pct"]
    )
    result["projected_lineup_mid3_k_pct"] = (
        _weighted_avg(mid_rows, "k_pct", result["projected_lineup_k_pct"])
        if mid_rows else result["projected_lineup_k_pct"]
    )
    result["projected_lineup_bot3_k_pct"] = (
        _weighted_avg(bot_rows, "k_pct", result["projected_lineup_k_pct"])
        if bot_rows else result["projected_lineup_k_pct"]
    )
    group_k = [
        result["projected_lineup_top3_k_pct"],
        result["projected_lineup_mid3_k_pct"],
        result["projected_lineup_bot3_k_pct"],
    ]
    result["projected_lineup_k_concentration"] = max(group_k) - min(group_k)

    return result


def compute_lineup_features_bulk(engine: Engine, season: int) -> pd.DataFrame:
    """Bulk computation of lineup-based K/contact features for training.

    Uses one SQL query with time-safe latest batter average rows strictly before each game date.
    Gracefully returns empty DataFrame with all expected columns when lineups are unavailable.
    """
    columns = ["player_id", "game_id", *LINEUP_FEATURE_COLUMNS]
    if not _table_exists(engine, "mlb_game_lineups"):
        logger.warning("mlb_game_lineups table missing; skipping bulk lineup features for season %d", season)
        return pd.DataFrame(columns=columns)

    query = text("""
        WITH pitcher_games AS (
            SELECT
                pgs.player_id AS pitcher_player_id,
                pgs.game_id,
                pgs.game_date,
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
        lineup_batters AS (
            SELECT
                pg.pitcher_player_id,
                pg.game_id,
                pg.pitcher_throws,
                gl.lineup_position,
                CASE gl.lineup_position
                    WHEN 1 THEN 1.12 WHEN 2 THEN 1.09 WHEN 3 THEN 1.06
                    WHEN 4 THEN 1.03 WHEN 5 THEN 1.00 WHEN 6 THEN 0.97
                    WHEN 7 THEN 0.94 WHEN 8 THEN 0.91 WHEN 9 THEN 0.88
                    ELSE 1.00
                END AS slot_weight,
                COALESCE(bp.bats, 'R') AS bats,
                COALESCE(
                    CASE WHEN bat.avg_pa_szn > 0 THEN bat.avg_so_szn::float / bat.avg_pa_szn END,
                    CASE WHEN bat.avg_pa_l20 > 0 THEN bat.avg_so_l20::float / bat.avg_pa_l20 END,
                    CASE WHEN bat.avg_pa_l10 > 0 THEN bat.avg_so_l10::float / bat.avg_pa_l10 END,
                    :default_k
                ) AS k_pct,
                COALESCE(sc.avg_whiff_pct_szn, sc.avg_whiff_pct_l10, sc.avg_whiff_pct_l5, :default_whiff) AS whiff_pct,
                COALESCE(sc.avg_chase_pct_szn, sc.avg_chase_pct_l10, sc.avg_chase_pct_l5, :default_chase) AS chase_pct,
                1.0 - COALESCE(sc.avg_whiff_pct_szn, sc.avg_whiff_pct_l10, sc.avg_whiff_pct_l5, :default_whiff) AS contact_rate
            FROM pitcher_games pg
            JOIN mlb_game_lineups gl
              ON gl.game_pk = pg.game_id
             AND gl.team_id = pg.opp_team_id
             AND gl.is_pitcher = FALSE
            LEFT JOIN mlb_players bp
              ON bp.player_id = gl.player_id
            LEFT JOIN LATERAL (
                SELECT avg_so_szn, avg_pa_szn, avg_so_l20, avg_pa_l20, avg_so_l10, avg_pa_l10
                FROM mlb_player_average_batting b
                WHERE b.player_id = gl.player_id
                  AND b.season = :season
                  AND b.game_date < pg.game_date
                ORDER BY b.game_date DESC
                LIMIT 1
            ) bat ON TRUE
            LEFT JOIN LATERAL (
                SELECT avg_whiff_pct_szn, avg_whiff_pct_l10, avg_whiff_pct_l5,
                       avg_chase_pct_szn, avg_chase_pct_l10, avg_chase_pct_l5
                FROM mlb_player_average_statcast_batting scb
                WHERE scb.player_id = gl.player_id
                  AND scb.season = :season
                  AND scb.game_date < pg.game_date
                ORDER BY scb.game_date DESC
                LIMIT 1
            ) sc ON TRUE
        ),
        lineup_stats AS (
            SELECT
                pitcher_player_id,
                game_id,
                pitcher_throws,
                COUNT(*) AS lineup_size,
                SUM(k_pct * slot_weight) / NULLIF(SUM(slot_weight), 0) AS projected_lineup_k_pct,
                SUM(whiff_pct * slot_weight) / NULLIF(SUM(slot_weight), 0) AS projected_lineup_whiff_pct,
                SUM(chase_pct * slot_weight) / NULLIF(SUM(slot_weight), 0) AS projected_lineup_chase_pct,
                SUM(contact_rate * slot_weight) / NULLIF(SUM(slot_weight), 0) AS projected_lineup_contact_rate,
                SUM(CASE WHEN bats = pitcher_throws AND bats <> 'S' THEN 1 ELSE 0 END)::float
                    / NULLIF(COUNT(*), 0) AS pct_opp_lineup_same_hand,
                SUM(CASE WHEN bats = pitcher_throws AND bats <> 'S' THEN k_pct * slot_weight END)
                    / NULLIF(SUM(CASE WHEN bats = pitcher_throws AND bats <> 'S' THEN slot_weight END), 0)
                    AS same_hand_k_pct_raw,
                SUM(CASE WHEN NOT (bats = pitcher_throws AND bats <> 'S') THEN k_pct * slot_weight END)
                    / NULLIF(SUM(CASE WHEN NOT (bats = pitcher_throws AND bats <> 'S') THEN slot_weight END), 0)
                    AS opposite_hand_k_pct_raw,
                SUM(CASE WHEN lineup_position BETWEEN 1 AND 3 THEN k_pct * slot_weight END)
                    / NULLIF(SUM(CASE WHEN lineup_position BETWEEN 1 AND 3 THEN slot_weight END), 0)
                    AS top3_k_pct_raw,
                SUM(CASE WHEN lineup_position BETWEEN 4 AND 6 THEN k_pct * slot_weight END)
                    / NULLIF(SUM(CASE WHEN lineup_position BETWEEN 4 AND 6 THEN slot_weight END), 0)
                    AS mid3_k_pct_raw,
                SUM(CASE WHEN lineup_position BETWEEN 7 AND 9 THEN k_pct * slot_weight END)
                    / NULLIF(SUM(CASE WHEN lineup_position BETWEEN 7 AND 9 THEN slot_weight END), 0)
                    AS bot3_k_pct_raw
            FROM lineup_batters
            GROUP BY pitcher_player_id, game_id, pitcher_throws
            HAVING COUNT(*) >= 3
        )
        SELECT
            pg.pitcher_player_id AS player_id,
            pg.game_id,
            COALESCE(ls.projected_lineup_k_pct, :default_k) AS projected_lineup_k_pct,
            COALESCE(ls.projected_lineup_whiff_pct, :default_whiff) AS projected_lineup_whiff_pct,
            COALESCE(ls.projected_lineup_chase_pct, :default_chase) AS projected_lineup_chase_pct,
            COALESCE(ls.projected_lineup_contact_rate, :default_contact) AS projected_lineup_contact_rate,
            COALESCE(ls.same_hand_k_pct_raw, ls.projected_lineup_k_pct, :default_k) AS projected_lineup_same_hand_k_pct,
            COALESCE(ls.opposite_hand_k_pct_raw, ls.projected_lineup_k_pct, :default_k) AS projected_lineup_opposite_hand_k_pct,
            COALESCE(ls.same_hand_k_pct_raw, ls.projected_lineup_k_pct, :default_k)
              - COALESCE(ls.opposite_hand_k_pct_raw, ls.projected_lineup_k_pct, :default_k)
              AS projected_lineup_hand_k_delta,
            COALESCE(ls.top3_k_pct_raw, ls.projected_lineup_k_pct, :default_k) AS projected_lineup_top3_k_pct,
            COALESCE(ls.mid3_k_pct_raw, ls.projected_lineup_k_pct, :default_k) AS projected_lineup_mid3_k_pct,
            COALESCE(ls.bot3_k_pct_raw, ls.projected_lineup_k_pct, :default_k) AS projected_lineup_bot3_k_pct,
            GREATEST(
                COALESCE(ls.top3_k_pct_raw, ls.projected_lineup_k_pct, :default_k),
                COALESCE(ls.mid3_k_pct_raw, ls.projected_lineup_k_pct, :default_k),
                COALESCE(ls.bot3_k_pct_raw, ls.projected_lineup_k_pct, :default_k)
            ) - LEAST(
                COALESCE(ls.top3_k_pct_raw, ls.projected_lineup_k_pct, :default_k),
                COALESCE(ls.mid3_k_pct_raw, ls.projected_lineup_k_pct, :default_k),
                COALESCE(ls.bot3_k_pct_raw, ls.projected_lineup_k_pct, :default_k)
            ) AS projected_lineup_k_concentration,
            CASE WHEN ls.pitcher_throws IS NULL THEN :default_same_hand
                 ELSE COALESCE(ls.pct_opp_lineup_same_hand, :default_same_hand)
            END AS pct_opp_lineup_same_hand
        FROM pitcher_games pg
        LEFT JOIN lineup_stats ls
          ON ls.pitcher_player_id = pg.pitcher_player_id
         AND ls.game_id = pg.game_id
    """)

    with engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '120000'"))
        df = pd.read_sql(query, conn, params={
            "season": season,
            "default_k": LINEUP_K_DEFAULT,
            "default_whiff": LINEUP_WHIFF_DEFAULT,
            "default_chase": LINEUP_CHASE_DEFAULT,
            "default_contact": LINEUP_CONTACT_DEFAULT,
            "default_same_hand": LINEUP_SAME_HAND_DEFAULT,
        })

    if df.empty:
        logger.info("No lineup features for season %d (mlb_game_lineups may be empty)", season)
        return pd.DataFrame(columns=columns)

    for feature, default in _lineup_defaults().items():
        if feature not in df.columns:
            df[feature] = default
        df[feature] = pd.to_numeric(df[feature], errors="coerce").fillna(default)

    logger.info("Computed lineup features for season %d: %d rows", season, len(df))
    return df[columns]
