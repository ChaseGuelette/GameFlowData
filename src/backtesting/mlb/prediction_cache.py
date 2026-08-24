"""Prediction-cache helpers for the MLB backtest sweep.

This module owns the pre-edge prediction records built from schedule rows,
feature stores, and the model suite. It deliberately does not fetch prop lines,
compute betting edges, or serialize sweep results.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

import numpy as np
import pandas as pd

from src.models.mlb.features.batter_inference_loader import BatterInferenceLoader
from src.models.mlb.features.pitcher_inference_loader import PitcherInferenceLoader
from src.models.mlb.features.requests import DateFeatureRequest, FeatureMode, PlayerGameFeatureRequest

logger = logging.getLogger("MLBBacktestSweep")

# Mapping from sweep stat key → batter feature store short stat name
BATTER_STAT_FS_MAP: dict[str, str] = {
    "batter_hits": "hits",
    "batter_total_bases": "total_bases",
    "batter_rbis": "rbis",
    "batter_runs_scored": "runs",
}


@dataclass
class DatePrediction:
    """Cached prediction for one player on one date (pre-edge)."""

    game_date: date
    player_id: int
    game_id: int
    team_id: int
    opponent_id: int
    stat: str
    model_type: str
    pred_mean: float
    pred_median: float
    pred_q10: float
    pred_q25: float
    pred_q50: float
    pred_q75: float
    pred_q90: float
    samples: np.ndarray  # MC samples, used for edge calc per config


def extract_probable_pitchers(games: list[dict], game_date: date) -> list[dict]:
    """Extract probable pitcher feature-store inputs from schedule rows."""
    pitchers: list[dict] = []
    for game in games:
        for side, opp_side, is_home in [("home", "away", True), ("away", "home", False)]:
            pitcher_id = game.get(f"probable_pitcher_{side}_id")
            if pitcher_id:
                pitchers.append({
                    "player_id": int(pitcher_id),
                    "game_id": int(game["game_id"]),
                    "team_id": game[f"{side}_team_id"],
                    "opponent_id": game[f"{opp_side}_team_id"],
                    "is_home": is_home,
                    "venue_id": game.get("venue_id", 0),
                    "season": game.get("season", game_date.year),
                })
    return pitchers


def build_predictions_for_date(
    *,
    pitcher_feature_store,
    batter_feature_store,
    suite,
    game_date: date,
    games: list[dict],
    stats: list[str],
    matchup_cache: dict[int, tuple[pd.DataFrame, pd.DataFrame]] | None = None,
    as_of_time: datetime | None = None,
) -> list[DatePrediction]:
    """Build cached pitcher and batter predictions for one game date."""
    predictions: list[DatePrediction] = []
    pitchers = extract_probable_pitchers(games, game_date)

    pitcher_stats = [s for s in stats if s.startswith("pitcher_") and suite.has_stat(s)]
    if pitcher_stats and pitchers:
        predictions.extend(_build_pitcher_predictions(
            pitcher_feature_store=pitcher_feature_store,
            suite=suite,
            game_date=game_date,
            pitchers=pitchers,
            pitcher_stats=pitcher_stats,
            as_of_time=as_of_time,
        ))

    batter_stats = [s for s in stats if s.startswith("batter_") and suite.has_stat(s)]
    if batter_stats and batter_feature_store is not None:
        predictions.extend(_build_batter_predictions(
            batter_feature_store=batter_feature_store,
            suite=suite,
            game_date=game_date,
            batter_stats=batter_stats,
            matchup_cache=matchup_cache,
            as_of_time=as_of_time,
        ))

    return predictions


def _build_pitcher_predictions(
    *,
    pitcher_feature_store,
    suite,
    game_date: date,
    pitchers: list[dict],
    pitcher_stats: list[str],
    as_of_time: datetime | None,
) -> list[DatePrediction]:
    predictions: list[DatePrediction] = []
    pitcher_loader = PitcherInferenceLoader(pitcher_feature_store)
    for pitcher in pitchers:
        try:
            request = PlayerGameFeatureRequest(
                player_id=pitcher["player_id"],
                game_id=pitcher["game_id"],
                game_date=str(game_date),
                team_id=pitcher["team_id"],
                opp_team_id=pitcher["opponent_id"],
                venue_id=pitcher.get("venue_id", 0),
                season=pitcher["season"],
                is_home=pitcher["is_home"],
                as_of_time=as_of_time,
            )
            features = pitcher_loader.load_player_game(request)
            if features is None:
                continue

            for pitcher_stat in pitcher_stats:
                pred = suite.predict(
                    pitcher_stat,
                    player_id=pitcher["player_id"],
                    game_id=pitcher["game_id"],
                    features=features,
                )
                predictions.append(DatePrediction(
                    game_date=game_date,
                    player_id=pred.player_id,
                    game_id=int(pred.game_id),
                    team_id=pitcher["team_id"],
                    opponent_id=pitcher["opponent_id"],
                    stat=pred.stat,
                    model_type="quantile",
                    pred_mean=pred.mean,
                    pred_median=pred.median,
                    pred_q10=pred.q10,
                    pred_q25=pred.q25,
                    pred_q50=pred.q50,
                    pred_q75=pred.q75,
                    pred_q90=pred.q90,
                    samples=pred.samples,
                ))
        except Exception as exc:
            logger.warning(f"Error predicting pitcher {pitcher['player_id']}: {exc}")
    return predictions


def _build_batter_predictions(
    *,
    batter_feature_store,
    suite,
    game_date: date,
    batter_stats: list[str],
    matchup_cache: dict[int, tuple[pd.DataFrame, pd.DataFrame]] | None,
    as_of_time: datetime | None,
) -> list[DatePrediction]:
    predictions: list[DatePrediction] = []
    batter_loader = BatterInferenceLoader(batter_feature_store)
    date_request = DateFeatureRequest(
        game_date=str(game_date),
        mode=FeatureMode.BACKTEST,
        as_of_time=as_of_time,
    )
    for batter_stat in batter_stats:
        short_stat = BATTER_STAT_FS_MAP.get(batter_stat)
        if short_stat is None:
            logger.warning(f"No feature-store mapping for {batter_stat}, skipping")
            continue

        try:
            features_df = batter_loader.load_date(
                date_request,
                stat=short_stat,
                matchup_cache=matchup_cache,
            )
        except Exception as exc:
            logger.warning(f"Error loading batter features for {batter_stat} on {game_date}: {exc}")
            continue

        if features_df.empty:
            continue

        for _, row in features_df.iterrows():
            try:
                player_id = int(row["player_id"])
                game_id_val = int(row["game_id"])
                features = row.to_dict()

                pred = suite.predict(batter_stat, player_id, game_id_val, features)

                predictions.append(DatePrediction(
                    game_date=game_date,
                    player_id=pred.player_id,
                    game_id=int(pred.game_id),
                    team_id=int(row.get("team_id", 0)),
                    opponent_id=int(row.get("opp_team_id", 0)),
                    stat=batter_stat,
                    model_type=suite.get_model_type(batter_stat),
                    pred_mean=pred.mean,
                    pred_median=pred.median,
                    pred_q10=pred.q10,
                    pred_q25=pred.q25,
                    pred_q50=pred.q50,
                    pred_q75=pred.q75,
                    pred_q90=pred.q90,
                    samples=pred.samples,
                ))
            except Exception as exc:
                logger.warning(
                    f"Error predicting {batter_stat} for player {row.get('player_id', '?')}: {exc}"
                )
    return predictions
