"""Edge and base-probability helpers for the MLB backtest sweep.

This module owns the promotion-critical probability and edge-calculation seam:
American odds conversion, lowest-vig line selection, empirical sample CDF
probabilities, optional Black-Litterman posterior blending, and the vectorized
base-probability cache used by fast sweep evaluation.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from src.backtesting.mlb.prediction_cache import DatePrediction

_MIN_PROB: float = 1e-6
_MAX_PROB: float = 1.0 - 1e-6


def odds_to_prob(odds: float) -> float:
    """Convert American odds to raw implied probability."""
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def select_sharpest_line(
    lines: pd.DataFrame,
    player_id: int,
    game_id: int,
    market_key: str,
) -> dict | None:
    """Find the lowest-vig line for a player/market from all bookmakers."""
    mask = (
        (lines["player_id"] == player_id)
        & (lines["game_id"] == game_id)
        & (lines["market_key"] == market_key)
    )
    player_lines = lines[mask]

    if player_lines.empty:
        return None

    best_line = None
    best_booksum = 999.0

    for _, row in player_lines.iterrows():
        over_odds = row["over_odds"]
        under_odds = row["under_odds"]
        if pd.isna(over_odds) or pd.isna(under_odds):
            continue

        booksum = odds_to_prob(over_odds) + odds_to_prob(under_odds)
        if booksum < best_booksum:
            best_booksum = booksum
            best_line = {
                "line": row["line"],
                "over_odds": over_odds,
                "under_odds": under_odds,
                "bookmaker": row["bookmaker"],
                "selected_snapshot_time": row.get("selected_snapshot_time"),
                "over_snapshot_time": row.get("over_snapshot_time"),
                "under_snapshot_time": row.get("under_snapshot_time"),
                "selected_decision_time": row.get("selected_decision_time"),
                "quote_decision_policy": row.get("quote_decision_policy"),
            }

    return best_line


def compute_edges_for_config(
    predictions: list[DatePrediction],
    lines_df: pd.DataFrame | None,
    bl_blender: Any,
    actuals: dict[tuple[int, str], float] | None,
) -> list[dict]:
    """Calculate edges for predictions using a specific BL config.

    bl_blender can be:
    - None: no BL blending (baseline)
    - BlackLittermanBlender: single blender for all stats
    - dict[str, BlackLittermanBlender]: per-stat blenders
    """
    results = []

    for pred in predictions:
        row = {
            "game_date": pred.game_date,
            "player_id": pred.player_id,
            "game_id": pred.game_id,
            "team_id": pred.team_id,
            "opponent_id": pred.opponent_id,
            "stat": pred.stat,
            "model_type": pred.model_type,
            "pred_mean": pred.pred_mean,
            "pred_median": pred.pred_median,
            "pred_q10": pred.pred_q10,
            "pred_q25": pred.pred_q25,
            "pred_q50": pred.pred_q50,
            "pred_q75": pred.pred_q75,
            "pred_q90": pred.pred_q90,
        }

        line_info = None
        if lines_df is not None and not lines_df.empty:
            line_info = select_sharpest_line(lines_df, pred.player_id, pred.game_id, pred.stat)

        if line_info is None:
            results.append(row)
            continue

        line_val = line_info["line"]
        over_odds = line_info["over_odds"]
        under_odds = line_info["under_odds"]
        samples = pred.samples

        # Empirical CDF from MC samples. Preserve legacy 5%-95% clipping here.
        over_prob = float((samples > line_val).mean())
        over_prob = min(max(over_prob, 0.05), 0.95)
        under_prob = 1 - over_prob

        raw_over = odds_to_prob(over_odds)
        raw_under = odds_to_prob(under_odds)
        booksum = raw_over + raw_under
        implied_over = raw_over / booksum
        implied_under = raw_under / booksum

        stat_blender = None
        if isinstance(bl_blender, dict):
            stat_blender = bl_blender.get(pred.stat)
        elif bl_blender is not None:
            stat_blender = bl_blender

        if stat_blender is not None:
            bl_result = stat_blender.blend_prediction(
                samples=samples,
                line=line_val,
                over_odds=over_odds,
                under_odds=under_odds,
            )
            over_prob = bl_result["posterior_over"]
            under_prob = bl_result["posterior_under"]

        row["line"] = line_val
        row["over_odds"] = over_odds
        row["under_odds"] = under_odds
        row["bookmaker"] = line_info["bookmaker"]
        row["selected_snapshot_time"] = line_info.get("selected_snapshot_time")
        row["over_snapshot_time"] = line_info.get("over_snapshot_time")
        row["under_snapshot_time"] = line_info.get("under_snapshot_time")
        row["selected_decision_time"] = line_info.get("selected_decision_time")
        row["quote_decision_policy"] = line_info.get("quote_decision_policy")
        row["over_prob"] = over_prob
        row["under_prob"] = under_prob
        row["implied_over"] = implied_over
        row["implied_under"] = implied_under
        row["over_edge"] = over_prob - implied_over
        row["under_edge"] = under_prob - implied_under

        if stat_blender is not None:
            row["bl_over_prob"] = bl_result["posterior_over"]
            row["bl_under_prob"] = bl_result["posterior_under"]
            row["bl_over_edge"] = bl_result["posterior_over"] - implied_over
            row["bl_under_edge"] = bl_result["posterior_under"] - implied_under

        if actuals:
            actual = actuals.get((pred.player_id, pred.stat))
            if actual is not None:
                row["actual"] = actual

        results.append(row)

    return results


def build_config_edge_frame(config: Any, precomputed_df: pd.DataFrame) -> pd.DataFrame:
    """Apply one sweep config's BL/posterior edge math to precomputed base probabilities."""
    model_over = precomputed_df["model_over"].values.copy()
    market_over = precomputed_df["market_over"].values
    market_under = precomputed_df["market_under"].values
    z_raw = precomputed_df["z_raw"].values
    model_logit = precomputed_df["model_logit"].values
    market_logit = precomputed_df["market_logit"].values

    if config.tau is None:
        posterior_over = model_over.copy()
    else:
        confidence = np.minimum(z_raw / config.z_max, 1.0)
        weight = np.minimum(config.tau * confidence, config.max_weight)
        posterior_logit = market_logit + weight * (model_logit - market_logit)
        posterior_over = 1.0 / (1.0 + np.exp(-posterior_logit))

    posterior_under = 1.0 - posterior_over

    return precomputed_df.assign(
        over_prob=posterior_over,
        under_prob=posterior_under,
        implied_over=market_over,
        implied_under=market_under,
        over_edge=posterior_over - market_over,
        under_edge=posterior_under - market_under,
    )


def precompute_mlb_base_probs(
    game_dates: list[date],
    date_predictions: dict[date, list[DatePrediction]],
    date_lines: dict[date, pd.DataFrame],
    date_actuals: dict[date, dict[tuple[int, str], float]],
) -> pd.DataFrame:
    """Build flat DataFrame with model probs, market probs, z_raw, and logits.

    This is computed once before the sweep and reused by the fast per-config path.
    """
    rows = []
    for gd in game_dates:
        preds = date_predictions.get(gd)
        if not preds:
            continue
        lines_df = date_lines.get(gd)
        if lines_df is None or lines_df.empty:
            continue

        ldf = lines_df.dropna(subset=["over_odds", "under_odds"]).copy()
        if ldf.empty:
            continue

        over_arr = ldf["over_odds"].values.astype(float)
        under_arr = ldf["under_odds"].values.astype(float)
        raw_over = np.where(over_arr > 0, 100.0 / (over_arr + 100.0), np.abs(over_arr) / (np.abs(over_arr) + 100.0))
        raw_under = np.where(under_arr > 0, 100.0 / (under_arr + 100.0), np.abs(under_arr) / (np.abs(under_arr) + 100.0))
        ldf["_raw_over"] = raw_over
        ldf["_raw_under"] = raw_under
        ldf["_booksum"] = raw_over + raw_under

        best_idx = ldf.groupby(["player_id", "game_id", "market_key"])["_booksum"].idxmin()
        best_lines = ldf.loc[best_idx.values].set_index(["player_id", "game_id", "market_key"])

        actuals_dict = date_actuals.get(gd, {})

        for pred in preds:
            try:
                bl_row = best_lines.loc[(pred.player_id, pred.game_id, pred.stat)]
            except KeyError:
                continue

            line_val = float(bl_row["line"])
            over_odds = float(bl_row["over_odds"])
            under_odds = float(bl_row["under_odds"])
            raw_o = float(bl_row["_raw_over"])
            raw_u = float(bl_row["_raw_under"])
            booksum = raw_o + raw_u

            model_over = float((pred.samples > line_val).mean())
            model_over = min(max(model_over, _MIN_PROB), _MAX_PROB)
            market_over = min(max(raw_o / booksum, _MIN_PROB), _MAX_PROB)

            s_std = float(pred.samples.std())
            z_raw = abs(float(pred.samples.mean()) - line_val) / max(s_std, 1e-6)

            model_logit = float(np.log(model_over / (1.0 - model_over)))
            market_logit = float(np.log(market_over / (1.0 - market_over)))

            rows.append({
                "game_date": gd,
                "player_id": pred.player_id,
                "game_id": pred.game_id,
                "stat": pred.stat,
                "line": line_val,
                "over_odds": over_odds,
                "under_odds": under_odds,
                "bookmaker": bl_row["bookmaker"],
                "selected_snapshot_time": bl_row.get("selected_snapshot_time"),
                "over_snapshot_time": bl_row.get("over_snapshot_time"),
                "under_snapshot_time": bl_row.get("under_snapshot_time"),
                "selected_decision_time": bl_row.get("selected_decision_time"),
                "quote_decision_policy": bl_row.get("quote_decision_policy"),
                "model_over": model_over,
                "market_over": market_over,
                "market_under": 1.0 - market_over,
                "z_raw": z_raw,
                "model_logit": model_logit,
                "market_logit": market_logit,
                "actual": actuals_dict.get((pred.player_id, pred.stat)),
            })

    return pd.DataFrame(rows)
