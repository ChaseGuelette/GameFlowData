"""
NCAAB Game-Level Feature Store
================================
Builds matchup-level feature rows for spread and total prediction.

Feature row structure: one row per game (not per team).
Target variables: home_margin (home_score - away_score), total_score.

All rolling features are expressed as HOME - AWAY differentials.
Barttorvik ratings use point-in-time lookup (snapshot_date < game_date).

Usage:
    from src.models.ncaab_feature_store import NCAABFeatureStore
    fs = NCAABFeatureStore(engine)
    df = fs.get_training_dataset(seasons=[2023, 2024, 2025])
"""

import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class NCAABFeatureConfig:
    min_games_for_avg: int = 3
    preferred_books: tuple = ("pinnacle", "draftkings", "fanduel", "betmgm")


# Feature lists
SPREAD_FEATURES = [
    # Barttorvik efficiency differentials
    "diff_adj_em", "diff_adj_oe", "diff_adj_de",
    "diff_barthag", "diff_adj_tempo",
    # Rolling box score differentials
    "diff_avg_pts_l5", "diff_avg_pts_l10",
    "diff_avg_poss_l5",
    "diff_avg_efg_pct_l5", "diff_avg_tov_pct_l5",
    "diff_avg_orb_pct_l5", "diff_avg_ft_rate_l5",
    # Defensive Four Factors
    "diff_avg_opp_efg_pct_l5", "diff_avg_opp_tov_pct_l5",
    # Pace
    "avg_tempo_combined",
    # Context
    "home_rest_days", "away_rest_days", "rest_differential",
    "is_neutral_site", "is_tournament",
    # Market
    "line_spread", "line_total",
    # Season progress
    "home_games_szn", "away_games_szn",
]

TOTAL_FEATURES = [
    "diff_adj_oe", "diff_adj_de",
    "avg_tempo_combined",
    "diff_avg_pts_l5", "diff_avg_pts_l10",
    "home_avg_pts_l5", "away_avg_pts_l5",
    "home_avg_poss_l5", "away_avg_poss_l5",
    "diff_avg_efg_pct_l5", "diff_avg_tov_pct_l5",
    "home_bart_adj_oe", "away_bart_adj_oe",
    "home_bart_adj_de", "away_bart_adj_de",
    "is_neutral_site", "is_tournament",
    "line_spread", "line_total",
    "home_rest_days", "away_rest_days",
]


class NCAABFeatureStore:
    def __init__(self, engine, config: NCAABFeatureConfig | None = None):
        self.engine = engine
        self.config = config or NCAABFeatureConfig()

    def get_training_dataset(self, seasons: list[int]) -> pd.DataFrame:
        """Fetch all feature rows for completed games in given seasons.

        Returns one row per game with targets and features.
        """
        min_games = self.config.min_games_for_avg

        query = text("""
            SELECT
                g.game_id, g.game_date, g.season,
                g.home_score, g.away_score,
                (g.home_score - g.away_score) AS home_margin,
                (g.home_score + g.away_score) AS total_score,
                CASE WHEN g.neutral_site THEN 1 ELSE 0 END AS is_neutral_site,
                CASE WHEN g.season_type = 'tournament' THEN 1 ELSE 0 END AS is_tournament,

                -- Home team rolling averages
                h_avg.avg_pts_l5 AS home_avg_pts_l5,
                h_avg.avg_pts_l10 AS home_avg_pts_l10,
                h_avg.avg_poss_l5 AS home_avg_poss_l5,
                h_avg.avg_efg_pct_l5 AS home_avg_efg_pct_l5,
                h_avg.avg_tov_pct_l5 AS home_avg_tov_pct_l5,
                h_avg.avg_orb_pct_l5 AS home_avg_orb_pct_l5,
                h_avg.avg_ft_rate_l5 AS home_avg_ft_rate_l5,
                h_avg.avg_opp_efg_pct_l5 AS home_avg_opp_efg_pct_l5,
                h_avg.avg_opp_tov_pct_l5 AS home_avg_opp_tov_pct_l5,
                h_avg.rest_days AS home_rest_days,
                h_avg.games_szn AS home_games_szn,

                -- Away team rolling averages
                a_avg.avg_pts_l5 AS away_avg_pts_l5,
                a_avg.avg_pts_l10 AS away_avg_pts_l10,
                a_avg.avg_poss_l5 AS away_avg_poss_l5,
                a_avg.avg_efg_pct_l5 AS away_avg_efg_pct_l5,
                a_avg.avg_tov_pct_l5 AS away_avg_tov_pct_l5,
                a_avg.avg_orb_pct_l5 AS away_avg_orb_pct_l5,
                a_avg.avg_ft_rate_l5 AS away_avg_ft_rate_l5,
                a_avg.avg_opp_efg_pct_l5 AS away_avg_opp_efg_pct_l5,
                a_avg.avg_opp_tov_pct_l5 AS away_avg_opp_tov_pct_l5,
                a_avg.rest_days AS away_rest_days,
                a_avg.games_szn AS away_games_szn,

                -- Barttorvik ratings (point-in-time)
                h_bart.adj_oe AS home_bart_adj_oe,
                h_bart.adj_de AS home_bart_adj_de,
                h_bart.adj_em AS home_bart_adj_em,
                h_bart.barthag AS home_bart_barthag,
                h_bart.adj_tempo AS home_bart_adj_tempo,
                a_bart.adj_oe AS away_bart_adj_oe,
                a_bart.adj_de AS away_bart_adj_de,
                a_bart.adj_em AS away_bart_adj_em,
                a_bart.barthag AS away_bart_barthag,
                a_bart.adj_tempo AS away_bart_adj_tempo

            FROM ncaab_game_schedule g

            -- Home team rolling averages
            JOIN ncaab_team_rolling_averages h_avg
                ON h_avg.game_id = g.game_id AND h_avg.team_id = g.home_team_id

            -- Away team rolling averages
            JOIN ncaab_team_rolling_averages a_avg
                ON a_avg.game_id = g.game_id AND a_avg.team_id = g.away_team_id

            -- Barttorvik: most recent snapshot BEFORE game_date (point-in-time)
            LEFT JOIN LATERAL (
                SELECT adj_oe, adj_de, adj_em, barthag, adj_tempo
                FROM ncaab_barttorvik_ratings
                WHERE team_id = g.home_team_id
                  AND snapshot_date < g.game_date
                  AND season = g.season
                ORDER BY snapshot_date DESC
                LIMIT 1
            ) h_bart ON TRUE

            LEFT JOIN LATERAL (
                SELECT adj_oe, adj_de, adj_em, barthag, adj_tempo
                FROM ncaab_barttorvik_ratings
                WHERE team_id = g.away_team_id
                  AND snapshot_date < g.game_date
                  AND season = g.season
                ORDER BY snapshot_date DESC
                LIMIT 1
            ) a_bart ON TRUE

            WHERE g.is_final = TRUE
              AND g.season = ANY(:seasons)
              AND h_avg.games_szn >= :min_games
              AND a_avg.games_szn >= :min_games
            ORDER BY g.game_date
        """)

        df = pd.read_sql(query, self.engine, params={
            "seasons": seasons,
            "min_games": min_games,
        })
        logger.info(f"Fetched {len(df):,} game rows for seasons {seasons}")

        # Add betting lines
        df = self._add_betting_lines(df)

        # Compute differentials
        df = self._compute_differentials(df)

        return df

    def get_features_for_date(self, game_date: date) -> pd.DataFrame:
        """Get feature rows for all scheduled games on a given date.

        For inference: returns features for upcoming games.
        """
        # Same query but filtered to a single date, without requiring is_final
        min_games = self.config.min_games_for_avg

        query = text("""
            SELECT
                g.game_id, g.game_date, g.season,
                g.home_team_id, g.away_team_id,
                CASE WHEN g.neutral_site THEN 1 ELSE 0 END AS is_neutral_site,
                CASE WHEN g.season_type = 'tournament' THEN 1 ELSE 0 END AS is_tournament,

                h_avg.avg_pts_l5 AS home_avg_pts_l5,
                h_avg.avg_pts_l10 AS home_avg_pts_l10,
                h_avg.avg_poss_l5 AS home_avg_poss_l5,
                h_avg.avg_efg_pct_l5 AS home_avg_efg_pct_l5,
                h_avg.avg_tov_pct_l5 AS home_avg_tov_pct_l5,
                h_avg.avg_orb_pct_l5 AS home_avg_orb_pct_l5,
                h_avg.avg_ft_rate_l5 AS home_avg_ft_rate_l5,
                h_avg.avg_opp_efg_pct_l5 AS home_avg_opp_efg_pct_l5,
                h_avg.avg_opp_tov_pct_l5 AS home_avg_opp_tov_pct_l5,
                h_avg.rest_days AS home_rest_days,
                h_avg.games_szn AS home_games_szn,

                a_avg.avg_pts_l5 AS away_avg_pts_l5,
                a_avg.avg_pts_l10 AS away_avg_pts_l10,
                a_avg.avg_poss_l5 AS away_avg_poss_l5,
                a_avg.avg_efg_pct_l5 AS away_avg_efg_pct_l5,
                a_avg.avg_tov_pct_l5 AS away_avg_tov_pct_l5,
                a_avg.avg_orb_pct_l5 AS away_avg_orb_pct_l5,
                a_avg.avg_ft_rate_l5 AS away_avg_ft_rate_l5,
                a_avg.avg_opp_efg_pct_l5 AS away_avg_opp_efg_pct_l5,
                a_avg.avg_opp_tov_pct_l5 AS away_avg_opp_tov_pct_l5,
                a_avg.rest_days AS away_rest_days,
                a_avg.games_szn AS away_games_szn,

                h_bart.adj_oe AS home_bart_adj_oe,
                h_bart.adj_de AS home_bart_adj_de,
                h_bart.adj_em AS home_bart_adj_em,
                h_bart.barthag AS home_bart_barthag,
                h_bart.adj_tempo AS home_bart_adj_tempo,
                a_bart.adj_oe AS away_bart_adj_oe,
                a_bart.adj_de AS away_bart_adj_de,
                a_bart.adj_em AS away_bart_adj_em,
                a_bart.barthag AS away_bart_barthag,
                a_bart.adj_tempo AS away_bart_adj_tempo

            FROM ncaab_game_schedule g
            JOIN ncaab_team_rolling_averages h_avg
                ON h_avg.game_id = g.game_id AND h_avg.team_id = g.home_team_id
            JOIN ncaab_team_rolling_averages a_avg
                ON a_avg.game_id = g.game_id AND a_avg.team_id = g.away_team_id
            LEFT JOIN LATERAL (
                SELECT adj_oe, adj_de, adj_em, barthag, adj_tempo
                FROM ncaab_barttorvik_ratings
                WHERE team_id = g.home_team_id AND snapshot_date <= :game_date AND season = g.season
                ORDER BY snapshot_date DESC LIMIT 1
            ) h_bart ON TRUE
            LEFT JOIN LATERAL (
                SELECT adj_oe, adj_de, adj_em, barthag, adj_tempo
                FROM ncaab_barttorvik_ratings
                WHERE team_id = g.away_team_id AND snapshot_date <= :game_date AND season = g.season
                ORDER BY snapshot_date DESC LIMIT 1
            ) a_bart ON TRUE
            WHERE g.game_date = :game_date
              AND h_avg.games_szn >= :min_games AND a_avg.games_szn >= :min_games
        """)

        df = pd.read_sql(query, self.engine, params={
            "game_date": game_date,
            "min_games": min_games,
        })

        if not df.empty:
            df = self._add_betting_lines(df)
            df = self._compute_differentials(df)

        return df

    def _add_betting_lines(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add spread and total lines from ncaab_raw_game_lines."""
        if df.empty:
            return df

        game_ids = df["game_id"].unique().tolist()
        books_str = ", ".join(f"'{b}'" for b in self.config.preferred_books)

        # Get spread lines (home team perspective)
        spread_query = text(f"""
            SELECT DISTINCT ON (game_id)
                game_id, line AS line_spread
            FROM ncaab_raw_game_lines
            WHERE game_id = ANY(:game_ids)
              AND market_key = 'spreads'
              AND bookmaker IN ({books_str})
              AND line IS NOT NULL
            ORDER BY game_id, snapshot_time DESC
        """)

        total_query = text(f"""
            SELECT DISTINCT ON (game_id)
                game_id, line AS line_total
            FROM ncaab_raw_game_lines
            WHERE game_id = ANY(:game_ids)
              AND market_key = 'totals'
              AND outcome_label = 'Over'
              AND bookmaker IN ({books_str})
              AND line IS NOT NULL
            ORDER BY game_id, snapshot_time DESC
        """)

        with self.engine.connect() as conn:
            spreads = pd.read_sql(spread_query, conn, params={"game_ids": game_ids})
            totals = pd.read_sql(total_query, conn, params={"game_ids": game_ids})

        df = df.merge(spreads, on="game_id", how="left")
        df = df.merge(totals, on="game_id", how="left")

        # Fill missing lines with 0 (model should handle gracefully)
        df["line_spread"] = df["line_spread"].fillna(0).astype(float)
        df["line_total"] = df["line_total"].fillna(0).astype(float)

        return df

    def _compute_differentials(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute all diff_* columns = home - away."""
        # Rolling stat differentials
        diff_pairs = [
            ("avg_pts_l5", "avg_pts_l5"),
            ("avg_pts_l10", "avg_pts_l10"),
            ("avg_poss_l5", "avg_poss_l5"),
            ("avg_efg_pct_l5", "avg_efg_pct_l5"),
            ("avg_tov_pct_l5", "avg_tov_pct_l5"),
            ("avg_orb_pct_l5", "avg_orb_pct_l5"),
            ("avg_ft_rate_l5", "avg_ft_rate_l5"),
            ("avg_opp_efg_pct_l5", "avg_opp_efg_pct_l5"),
            ("avg_opp_tov_pct_l5", "avg_opp_tov_pct_l5"),
        ]

        for stat, _ in diff_pairs:
            home_col = f"home_{stat}"
            away_col = f"away_{stat}"
            if home_col in df.columns and away_col in df.columns:
                df[f"diff_{stat}"] = df[home_col] - df[away_col]

        # Barttorvik differentials
        bart_stats = ["adj_em", "adj_oe", "adj_de", "barthag", "adj_tempo"]
        for stat in bart_stats:
            home_col = f"home_bart_{stat}"
            away_col = f"away_bart_{stat}"
            if home_col in df.columns and away_col in df.columns:
                df[f"diff_{stat}"] = df[home_col] - df[away_col]

        # Combined tempo
        if "home_bart_adj_tempo" in df.columns and "away_bart_adj_tempo" in df.columns:
            df["avg_tempo_combined"] = (df["home_bart_adj_tempo"] + df["away_bart_adj_tempo"]) / 2
        else:
            df["avg_tempo_combined"] = np.nan

        # Rest differential
        if "home_rest_days" in df.columns and "away_rest_days" in df.columns:
            df["rest_differential"] = df["home_rest_days"] - df["away_rest_days"]

        return df
