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

PREFERRED_BOOK_ROUTING_BOOKS: tuple[str, ...] = (
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "williamhill_us",
    "betrivers",
    "fanatics",
    "hardrockbet",
    "hardrockbet_oh",
)


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


def _allowed_side_mask(df: pd.DataFrame, allowed_bets: set[tuple[str, str]] | None, side: str) -> pd.Series:
    if allowed_bets is None:
        return pd.Series(True, index=df.index)
    allowed_stats = {stat for stat, allowed_side in allowed_bets if allowed_side == side}
    return df["stat"].isin(allowed_stats)


def apply_book_routing_policy(
    df: pd.DataFrame,
    *,
    edge_threshold: float,
    book_routing_policy: str = "lowest_vig",
    allowed_bets: set[tuple[str, str]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Select one routed candidate row per player/game/stat for simulation.

    Returns `(selected_rows, candidate_rows)`. Candidate rows keep all bookmaker
    quotes and include edge-threshold/routing diagnostics for audit output.
    """
    if df.empty:
        return df.copy(), df.copy()

    candidates = df.copy()
    candidates["book_routing_policy"] = book_routing_policy
    if "candidate_booksum" not in candidates.columns:
        candidates["candidate_booksum"] = candidates["implied_over"] + candidates["implied_under"]
    if "preferred_book_candidate" not in candidates.columns:
        candidates["preferred_book_candidate"] = candidates["bookmaker"].astype(str).str.lower().isin(PREFERRED_BOOK_ROUTING_BOOKS)

    over_allowed = _allowed_side_mask(candidates, allowed_bets, "over")
    under_allowed = _allowed_side_mask(candidates, allowed_bets, "under")
    candidates["over_clears_edge_threshold"] = over_allowed & (candidates["over_edge"] >= edge_threshold)
    candidates["under_clears_edge_threshold"] = under_allowed & (candidates["under_edge"] >= edge_threshold)
    candidates["clears_edge_threshold"] = candidates["over_clears_edge_threshold"] | candidates["under_clears_edge_threshold"]

    over_edge = candidates["over_edge"].where(over_allowed, -np.inf)
    under_edge = candidates["under_edge"].where(under_allowed, -np.inf)
    candidates["candidate_best_edge"] = np.maximum(over_edge, under_edge)
    candidates["candidate_best_side"] = np.where(over_edge >= under_edge, "over", "under")
    candidates.loc[~np.isfinite(candidates["candidate_best_edge"]), "candidate_best_side"] = None
    candidates["selected_by_policy"] = False
    candidates["selected_reason"] = None

    selected_indices: list[int] = []
    group_cols = ["game_date", "player_id", "game_id", "stat"]
    for _, group in candidates.groupby(group_cols, dropna=False):
        if book_routing_policy == "preferred_book_first":
            preferred_clear = group[group["preferred_book_candidate"] & group["clears_edge_threshold"]]
            if not preferred_clear.empty:
                idx = preferred_clear.sort_values(["candidate_best_edge", "candidate_booksum"], ascending=[False, True]).index[0]
                reason = "preferred_book_cleared_edge"
            else:
                fallback_clear = group[group["clears_edge_threshold"]]
                if not fallback_clear.empty:
                    idx = fallback_clear.sort_values(["candidate_best_edge", "candidate_booksum"], ascending=[False, True]).index[0]
                    reason = "fallback_book_cleared_edge"
                else:
                    idx = group.sort_values("candidate_booksum", ascending=True).index[0]
                    reason = "no_candidate_cleared_edge"
        else:
            idx = group.sort_values("candidate_booksum", ascending=True).index[0]
            reason = "lowest_vig_candidate"
        selected_indices.append(idx)
        candidates.loc[idx, "selected_by_policy"] = True
        candidates.loc[idx, "selected_reason"] = reason

    candidates["selected_candidate_rank"] = candidates.groupby(group_cols)["candidate_best_edge"].rank(method="first", ascending=False)
    selected = candidates.loc[selected_indices].copy()
    selected["selected_bookmaker"] = selected["bookmaker"]
    selected["selected_line"] = selected["line"]
    selected["selected_side"] = selected["candidate_best_side"]
    selected["selected_price"] = np.where(selected["selected_side"] == "over", selected["over_odds"], selected["under_odds"])
    selected["preferred_book_selected"] = selected["preferred_book_candidate"]
    return selected.reset_index(drop=True), candidates.reset_index(drop=True)


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

        actuals_dict = date_actuals.get(gd, {})

        for pred in preds:
            pred_lines = ldf[
                (ldf["player_id"] == pred.player_id)
                & (ldf["game_id"] == pred.game_id)
                & (ldf["market_key"] == pred.stat)
            ]
            if pred_lines.empty:
                continue

            s_std = float(pred.samples.std())
            actual = actuals_dict.get((pred.player_id, pred.stat))
            for _, bl_row in pred_lines.iterrows():
                line_val = float(bl_row["line"])
                over_odds = float(bl_row["over_odds"])
                under_odds = float(bl_row["under_odds"])
                raw_o = float(bl_row["_raw_over"])
                raw_u = float(bl_row["_raw_under"])
                booksum = raw_o + raw_u

                model_over = float((pred.samples > line_val).mean())
                model_over = min(max(model_over, _MIN_PROB), _MAX_PROB)
                market_over = min(max(raw_o / booksum, _MIN_PROB), _MAX_PROB)

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
                    "candidate_booksum": booksum,
                    "preferred_book_candidate": str(bl_row["bookmaker"]).strip().lower() in PREFERRED_BOOK_ROUTING_BOOKS,
                    "selected_snapshot_time": bl_row.get("selected_snapshot_time"),
                    "over_snapshot_time": bl_row.get("over_snapshot_time"),
                    "under_snapshot_time": bl_row.get("under_snapshot_time"),
                    "selected_decision_time": bl_row.get("selected_decision_time"),
                    "quote_decision_policy": bl_row.get("quote_decision_policy"),
                    "over_market_last_update": bl_row.get("over_market_last_update"),
                    "under_market_last_update": bl_row.get("under_market_last_update"),
                    "over_bookmaker_last_update": bl_row.get("over_bookmaker_last_update"),
                    "under_bookmaker_last_update": bl_row.get("under_bookmaker_last_update"),
                    "over_bookmaker": bl_row.get("bookmaker"),
                    "under_bookmaker": bl_row.get("bookmaker"),
                    "model_over": model_over,
                    "market_over": market_over,
                    "market_under": 1.0 - market_over,
                    "z_raw": z_raw,
                    "model_logit": model_logit,
                    "market_logit": market_logit,
                    "actual": actual,
                })

    return pd.DataFrame(rows)
