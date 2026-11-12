# pyright: reportPrivateUsage=false
"""Bounded diagnostic comparison of incumbent NBA Monte Carlo sampler settings.

The normal path acquires the canonical cohort once, freezes component quantile
forecasts, then replays three explicit sampler arms with paired RandomState draws.
No model is fit, promoted, or mutated and no alternate settings are presented to
the ordinary sweep validator as canonical defaults.
"""
from __future__ import annotations

import copy
import json
import logging
import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.backtesting.backtest_harness import BacktestHarness, _derive_replay_combo_samples
from src.backtesting.nba.historical_execution import validate_saved_attachment
from src.backtesting.nba.sweep_config import BOOKMAKERS, CellConfig, SweepConfig, build_plan
from src.backtesting.nba.sweep_runner import (
    KEYS,
    PREDICTION_COLUMNS,
    Runtime,
    _candidate_edges,
    _cell_simulator,
    _rename_no_replace,
    _schema_frame,
    _write_csv,
    _write_json,
    acquire_shared_inputs,
    bootstrap_runtime,
    content_digest,
    evaluate_cell,
    isolated_frame,
    json_safe,
)
from src.config.combo_config import STAT_TO_MARKET
from src.models.black_litterman import BlackLittermanBlender
from src.models.monte_carlo import MonteCarloPredictor
from src.models.nba_artifacts import canonical_sha256, create_exclusive_incomplete_dir, sha256_file

logger = logging.getLogger(__name__)

VARIANTS = ("control", "no_reb_widening", "no_extra_lower_tail")
QCOLS = ("q10", "q25", "q50", "q75", "q90")
PCOLS = tuple(f"pred_{name}" for name in QCOLS)
PROB_EPSILON = 1e-12


class ComparisonFailure(RuntimeError):  # noqa: N818 - public sanitized boundary
    """Sanitized public failure boundary."""

    def __init__(self, stage: str, error_type: str = "invariant") -> None:
        self.stage = stage
        self.error_type = error_type
        super().__init__(f"Sampler comparison failed: stage={stage}, type={error_type}")


@dataclass(frozen=True)
class ComparisonRequest:
    sweep: SweepConfig
    seeds: tuple[int, ...]
    max_days: int = 31
    max_seeds: int = 5
    max_samples: int = 20_000
    max_player_games: int = 10_000
    max_quote_rows: int = 250_000
    max_total_samples: int = 3_000_000_000

    def __post_init__(self) -> None:
        if not self.sweep.local:
            raise ValueError("NBA sampler comparison requires --local")
        if not self.sweep.diagnostic_history:
            raise ValueError("Execution contract requires --diagnostic-history")
        if len(self.sweep.cells()) != 2 or self.sweep.cells()[0].tau is not None or self.sweep.cells()[1].tau is None:
            raise ValueError("Comparison requires exactly raw and one fixed BL replay mode")
        if type(self.seeds) is not tuple or not self.seeds or len(set(self.seeds)) != len(self.seeds):
            raise ValueError("Seeds must be an ordered nonempty unique list")
        if any(type(seed) is not int or not 0 <= seed <= 2**32 - 1 for seed in self.seeds):
            raise ValueError("Seeds must be RandomState integers in 0..4294967295")
        if self.sweep.seed != self.seeds[0]:
            raise ValueError("Acquisition seed must equal the first comparison seed for canonical parity")
        bounds = {
            "max_days": (self.max_days, 1, 31),
            "max_seeds": (self.max_seeds, 1, 5),
            "max_samples": (self.max_samples, 1, 20_000),
            "max_player_games": (self.max_player_games, 1, 10_000),
            "max_quote_rows": (self.max_quote_rows, 1, 250_000),
            "max_total_samples": (self.max_total_samples, 1, 10_000_000_000),
        }
        for name, (value, low, high) in bounds.items():
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be in {low}..{high}")
        days = (self.sweep.end - self.sweep.start).days + 1
        if days > self.max_days:
            raise ValueError(f"Requested {days} days exceeds max_days={self.max_days}")
        if len(self.seeds) > self.max_seeds:
            raise ValueError(f"Requested {len(self.seeds)} seeds exceeds max_seeds={self.max_seeds}")
        if self.sweep.n_samples > self.max_samples:
            raise ValueError(f"Requested n_samples exceeds max_samples={self.max_samples}")
        requested_ceiling = (
            self.max_player_games
            * self.sweep.n_samples
            * len(self.sweep.stats)
            * len(VARIANTS)
            * len(self.seeds)
        )
        if requested_ceiling > self.max_total_samples:
            raise ValueError(
                f"Bounded workload ceiling {requested_ceiling} exceeds max_total_samples={self.max_total_samples}; "
                "lower --max-player-games, --n-samples, or seeds"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "seeds": list(self.seeds),
            "variants": list(VARIANTS),
            "max_days": self.max_days,
            "max_seeds": self.max_seeds,
            "max_samples": self.max_samples,
            "max_player_games": self.max_player_games,
            "max_quote_rows": self.max_quote_rows,
            "max_total_samples": self.max_total_samples,
            "probability_epsilon": PROB_EPSILON,
            "quote_weighting": "quote-row-weighted; repeated books/lines are retained",
        }


class FrozenQuantileHead:
    """Instance-local recording head that becomes cache-only after acquisition."""

    def __init__(self, original: Any, label: str) -> None:
        self.original = original
        self.label = label
        self.all_feature_names = list(original.all_feature_names)
        self._cache: dict[str, pd.DataFrame] = {}
        self._inputs: dict[str, pd.DataFrame] = {}
        self.original_calls = 0
        self.cache_calls = 0
        self.frozen = False

    def predict_quantiles(self, features: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(features, pd.DataFrame) or list(features.columns) != self.all_feature_names:
            raise ComparisonFailure("quantile_input_contract")
        key = content_digest(features)
        if key in self._cache:
            if content_digest(features) != content_digest(self._inputs[key]):
                raise ComparisonFailure("quantile_cache_collision")
            self.cache_calls += 1
            return self._cache[key].copy(deep=True)
        if self.frozen:
            raise ComparisonFailure("quantile_cache_miss_after_freeze")
        result = self.original.predict_quantiles(features)
        if not isinstance(result, pd.DataFrame):
            raise ComparisonFailure("quantile_output_contract")
        result = result.reset_index(drop=True)
        if list(result.columns) != list(QCOLS) or len(result) != len(features):
            raise ComparisonFailure("quantile_output_contract")
        values = result.to_numpy(dtype=float)
        if not np.isfinite(values).all() or (np.diff(values, axis=1) < -1e-9).any():
            raise ComparisonFailure("invalid_component_quantiles")
        self.original_calls += 1
        self._inputs[key] = features.copy(deep=True)
        self._cache[key] = result.copy(deep=True)
        return result.copy(deep=True)

    def freeze(self) -> None:
        if not self._cache:
            raise ComparisonFailure("empty_quantile_cache")
        self.frozen = True

    def receipt(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "all_feature_names": self.all_feature_names,
            "original_calls": self.original_calls,
            "cache_calls": self.cache_calls,
            "batches": [
                {
                    "prepared_input_identity": key,
                    "rows": len(self._inputs[key]),
                    "columns": list(self._inputs[key].columns),
                    "dtypes": [str(dtype) for dtype in self._inputs[key].dtypes],
                    "quantile_identity": content_digest(self._cache[key]),
                    "quantile_dtypes": [str(dtype) for dtype in self._cache[key].dtypes],
                }
                for key in self._cache
            ],
        }


@dataclass
class FrozenPipeline:
    pipeline: Any
    heads: dict[str, FrozenQuantileHead]

    @classmethod
    def attach(cls, runtime: Runtime, stats: tuple[str, ...]) -> FrozenPipeline:
        source: Any = runtime.harness.model_pipeline
        detached: Any = copy.copy(source)
        minutes = FrozenQuantileHead(source.minutes_model, "minutes")
        rates = {stat: FrozenQuantileHead(source.rate_models[stat], f"{stat}_rate") for stat in stats}
        detached.minutes_model = minutes
        detached.rate_models = {**source.rate_models, **rates}
        runtime.harness.model_pipeline = detached
        predictor: Any = runtime.harness.predictor
        predictor.pipeline = detached
        return cls(detached, {"minutes": minutes, **rates})

    def freeze(self) -> int:
        for head in self.heads.values():
            head.freeze()
        return self.original_calls

    @property
    def original_calls(self) -> int:
        return sum(head.original_calls for head in self.heads.values())

    def receipts(self) -> list[dict[str, Any]]:
        return [self.heads[name].receipt() for name in self.heads]


def _same_rng(left: tuple, right: tuple) -> bool:
    return left[0] == right[0] and np.array_equal(left[1], right[1]) and left[2:] == right[2:]


def _variant_settings(canonical: dict[str, Any], seed: int, variant: str) -> dict[str, Any]:
    settings = copy.deepcopy(canonical)
    settings["random_state"] = seed
    settings["combined_calibration_offsets"] = None
    if variant == "no_reb_widening":
        settings["variance_inflation"]["reb"] = 1.0
    elif variant == "no_extra_lower_tail":
        settings["tail_adjustment"]["lower_tail_multiplier"] = 1.0
    elif variant != "control":
        raise ComparisonFailure("unknown_variant")
    return settings


def _observed_bounds(request: ComparisonRequest, shared: Any) -> dict[str, int]:
    player_games = sum(len(record["eligible_features"]) for record in shared.evidence["dates"].values())
    quote_rows = sum(len(record["quotes"]) for record in shared.evidence["dates"].values())
    for record in shared.evidence["dates"].values():
        features = record["eligible_features"]
        if not features.empty and features.duplicated(["player_id", "game_id"]).any():
            raise ComparisonFailure("duplicate_eligible_player_game")
    if player_games > request.max_player_games:
        raise ComparisonFailure("player_game_bound")
    if quote_rows > request.max_quote_rows:
        raise ComparisonFailure("quote_row_bound")
    workload = player_games * request.sweep.n_samples * len(request.sweep.stats) * len(VARIANTS) * len(request.seeds)
    if workload > request.max_total_samples:
        raise ComparisonFailure("observed_sample_workload_bound")
    return {"eligible_player_games": player_games, "quote_rows": quote_rows, "sample_workload": workload}


def _component_forecasts(shared: Any, frozen: FrozenPipeline, stats: tuple[str, ...]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day, record in shared.evidence["dates"].items():
        features = record["eligible_features"]
        if features.empty:
            continue
        heads = {"minutes": frozen.heads["minutes"], **{stat: frozen.heads[stat] for stat in stats}}
        for component, head in heads.items():
            prepared = MonteCarloPredictor._prepare_features_batch(  # type: ignore[misc]
                shared_dummy_predictor(frozen.pipeline), features, head.all_feature_names
            )
            quantiles = head.predict_quantiles(prepared)
            prepared_identity = content_digest(prepared)
            quantile_identity = content_digest(quantiles)
            for index, feature in enumerate(features.to_dict("records")):
                rows.append(
                    {
                        "game_date": day,
                        "player_id": feature["player_id"],
                        "game_id": feature["game_id"],
                        "component": "minutes" if component == "minutes" else f"{component}_rate",
                        "prepared_input_identity": prepared_identity,
                        "quantile_identity": quantile_identity,
                        "feature_count": len(head.all_feature_names),
                        **dict(zip(QCOLS, quantiles.iloc[index].to_numpy())),
                    }
                )
    return pd.DataFrame(rows)


def shared_dummy_predictor(pipeline: Any) -> MonteCarloPredictor:
    """Create no draws; used only to reuse the canonical feature preparation method."""
    return MonteCarloPredictor(pipeline, n_samples=1, random_state=0, combined_calibration_offsets=None)


def _sample_records(shared: Any, pipeline: Any, settings: dict[str, Any], stats: tuple[str, ...]) -> tuple[dict, tuple]:
    predictor = MonteCarloPredictor(pipeline, **settings)
    records: dict[date, dict[str, Any]] = {}
    for day, original in shared.evidence["dates"].items():
        features = original["eligible_features"]
        prediction_list: list[dict[str, Any]] = []
        samples: dict[tuple, np.ndarray] = {}
        if not features.empty:
            prediction_list, samples = predictor.predict_batch_for_date(isolated_frame(features), stats=list(stats))
            combo_predictions, combo_samples = _derive_replay_combo_samples(samples, prediction_list)
            prediction_list = copy.deepcopy(prediction_list) + combo_predictions
            samples = {**samples, **combo_samples}
            for prediction in prediction_list:
                prediction["game_date"] = day
        predictions = pd.DataFrame(prediction_list) if prediction_list else pd.DataFrame(columns=PREDICTION_COLUMNS)
        records[day] = {
            "predictions": predictions,
            "quotes": isolated_frame(original["quotes"]),
            "samples": samples,
            "eligible_features": isolated_frame(features),
        }
    state = predictor.rng.get_state(legacy=True)
    if not isinstance(state, tuple):
        raise ComparisonFailure("legacy_rng_state_contract")
    return records, state


def _assert_control_samples(shared: Any, records: dict) -> None:
    for day, original in shared.evidence["dates"].items():
        observed = records[day]["samples"]
        expected = original["samples"]
        if set(observed) != set(expected):
            raise ComparisonFailure("control_sample_key_parity")
        if any(not np.array_equal(observed[key], expected[key]) for key in expected):
            raise ComparisonFailure("control_sample_value_parity")


def _replay(request: ComparisonRequest, shared: Any, records: dict, cell: CellConfig) -> dict[str, Any]:
    cfg = request.sweep
    defaults = _cell_simulator(cfg, cell)
    blender = BlackLittermanBlender(cell.bl_config()) if cell.bl_config() is not None else None
    harness = BacktestHarness(
        engine=None,
        feature_store=None,
        model_pipeline=None,
        predictor=None,
        edge_threshold=cell.edge_threshold,
        starting_bankroll=cfg.starting_bankroll,
        flat_bet_size=cfg.flat_bet_size,
        max_bet_pct=defaults.max_bet_pct,
        kelly_fraction=defaults.kelly_fraction,
        stats=list(cfg.market_stats),
        bookmakers=list(cfg.bookmakers),
        min_minutes_avg=cfg.min_minutes_avg,  # pyright: ignore[reportArgumentType]
        allowed_bets=list(cfg.allowed_bets) if cfg.allowed_bets else None,
        bl_blender=blender,
        bl_sizing_blender=None,
        stat_config=None,
        historical_policy=validate_saved_attachment(cfg.historical) if cfg.historical else None,
    )
    selected_frames: list[pd.DataFrame] = []
    edge_frames: list[pd.DataFrame] = []
    schemas = shared.evidence["output_schemas"]
    for day in shared.evidence["dates"]:
        edges = _candidate_edges(harness, records[day], schemas)
        if not edges.empty:
            edge_frames.append(edges)
            selected = harness._filter_best_bets(isolated_frame(edges))
            if not selected.empty:
                selected_frames.append(selected)
    predictions = (
        pd.concat(selected_frames, ignore_index=True).sort_values(["game_date", "game_id", "player_id"])
        if selected_frames
        else pd.DataFrame()
    )
    all_edges = (
        pd.concat(edge_frames, ignore_index=True).sort_values(["game_date", "player_id", "stat", "bookmaker"])
        if edge_frames
        else pd.DataFrame()
    )
    actuals = isolated_frame(shared.evidence["actuals"])
    voids = isolated_frame(shared.evidence["voids"])
    simulator = harness._simulator
    if not predictions.empty:
        for day in sorted(predictions["game_date"].unique()):
            if not voids.empty:
                simulator.resolve_voids(voids)
            if not actuals.empty:
                simulator.resolve_bets(actuals)
            simulator.evaluate_predictions(cast(pd.DataFrame, predictions.loc[predictions["game_date"] == day]), day)
    if not voids.empty:
        simulator.resolve_voids(voids)
    if not actuals.empty:
        simulator.resolve_bets(actuals)
    bets = simulator.to_dataframe()
    if not predictions.empty and not actuals.empty:
        predictions = harness._merge_actuals(predictions, actuals)
    elif "actual" not in predictions:
        predictions["actual"] = np.nan
    metrics = harness._metrics_calc.calculate(predictions, bets, starting_bankroll=cfg.starting_bankroll)
    predictions = _schema_frame(predictions, schemas["files"]["predictions.csv"])
    bets = _schema_frame(bets, schemas["files"]["bets.csv"])
    all_edges = _schema_frame(all_edges, schemas["files"]["all_bookmaker_edges.csv"])
    return {"predictions": predictions, "bets": bets, "all_edges": all_edges, "metrics": metrics}


def _assert_replay_parity(reference: Any, observed: dict[str, Any]) -> None:
    for name, frame in (("predictions", reference.predictions_df), ("bets", reference.bets_df),
                        ("all_edges", reference.all_edges_df)):
        try:
            pd.testing.assert_frame_equal(frame.reset_index(drop=True), observed[name].reset_index(drop=True),
                                          check_dtype=False, check_exact=True)
        except AssertionError:
            raise ComparisonFailure(f"canonical_{name}_parity") from None
    if reference.metrics.to_dict() != observed["metrics"].to_dict():
        raise ComparisonFailure("canonical_metrics_parity")


def _actual_lookup(shared: Any) -> tuple[dict[tuple, float], set[tuple]]:
    actuals = isolated_frame(shared.evidence["actuals"])
    if actuals.duplicated(KEYS).any():
        raise ComparisonFailure("duplicate_actual_keys")
    lookup = {
        (player_id, game_id, stat): float(value)
        for player_id, game_id, stat, value in actuals[[*KEYS, "actual_value"]].itertuples(index=False, name=None)
        if value is not None and value is not pd.NA and math.isfinite(float(value))
    }
    voids = isolated_frame(shared.evidence["voids"])
    if not voids.empty and voids.duplicated(["player_id", "game_id"]).any():
        raise ComparisonFailure("duplicate_void_keys")
    void_keys = set(voids[["player_id", "game_id"]].itertuples(index=False, name=None)) if not voids.empty else set()
    return lookup, void_keys


def _quote_inventory(shared: Any) -> tuple[list[dict[str, Any]], str, dict[str, int]]:
    market_to_stat = {market: stat for stat, market in STAT_TO_MARKET.items()}
    rows: list[dict[str, Any]] = []
    excluded = Counter()
    for day, record in shared.evidence["dates"].items():
        available = set(record["samples"])
        for ordinal, quote in enumerate(record["quotes"].to_dict("records")):
            stat = market_to_stat.get(quote.get("market_key"))
            sample_key = (quote.get("player_id"), quote.get("game_id"), stat)
            if stat is None:
                excluded["unsupported_market"] += 1
                continue
            if sample_key not in available:
                excluded["ineligible_or_missing_sample"] += 1
                continue
            identity = content_digest((day, ordinal, quote))
            rows.append({"quote_row_id": identity, "game_date": day, "stat": stat, **quote})
    identity = canonical_sha256([row["quote_row_id"] for row in rows])
    return rows, identity, dict(excluded)


def _devig(over_odds: float, under_odds: float) -> float:
    over = 100 / (over_odds + 100) if over_odds > 0 else -over_odds / (-over_odds + 100)
    under = 100 / (under_odds + 100) if under_odds > 0 else -under_odds / (-under_odds + 100)
    return over / (over + under)


def _required_float(value: Any) -> float:
    if value is None:
        raise ValueError("missing numeric quote value")
    return float(value)


def _probability_rows(
    quote_inventory: list[dict[str, Any]], quote_identity: str, records: dict, shared: Any,
    variant: str, seed: int, bl_cell: CellConfig,
) -> pd.DataFrame:
    actuals, voids = _actual_lookup(shared)
    rows: list[dict[str, Any]] = []
    observed_ids: list[str] = []
    blender = BlackLittermanBlender(bl_cell.bl_config())
    for quote in quote_inventory:
        sample_key = (quote["player_id"], quote["game_id"], quote["stat"])
        samples = records[quote["game_date"]]["samples"].get(sample_key)
        if not isinstance(samples, np.ndarray) or samples.ndim != 1 or len(samples) != request_n_samples(shared):
            raise ComparisonFailure("variant_sample_contract")
        observed_ids.append(quote["quote_row_id"])
        line = quote.get("line")
        over_odds = quote.get("over_odds")
        under_odds = quote.get("under_odds")
        status = "scored"
        actual = actuals.get(sample_key)
        market_over = np.nan
        raw_over = np.nan
        bl_over = np.nan
        if (quote["player_id"], quote["game_id"]) in voids:
            status = "void"
        elif actual is None:
            status = "missing_actual"
        try:
            line_value = _required_float(line)
            over_value = _required_float(over_odds)
            under_value = _required_float(under_odds)
            if not all(math.isfinite(value) for value in (line_value, over_value, under_value)) or not over_value or not under_value:
                raise ValueError
            raw_over = float((samples > line_value).mean())
            market_over = _devig(over_value, under_value)
            bl_over = float(blender.blend_prediction(samples, line_value, int(over_value), int(under_value))["posterior_over"])
            if status == "scored" and actual == line_value:
                status = "push"
        except (TypeError, ValueError, OverflowError):
            status = "invalid_quote"
            line_value = np.nan
        base = {
            "variant": variant,
            "seed": seed,
            "quote_identity": quote_identity,
            "quote_row_id": quote["quote_row_id"],
            "game_date": quote["game_date"],
            "player_id": quote["player_id"],
            "game_id": quote["game_id"],
            "stat": quote["stat"],
            "bookmaker": quote.get("bookmaker"),
            "line": line_value,
            "over_odds": over_odds,
            "under_odds": under_odds,
            "actual": actual,
            "outcome_over": float(actual > line_value) if status == "scored" and actual is not None else np.nan,
            "market_over": market_over,
            "score_status": status,
            "weighting": "quote_row",
        }
        for name, value in quote.items():
            base.setdefault(name, value)
        rows.append({**base, "mode": "raw", "over_prob": raw_over})
        rows.append({**base, "mode": "bl", "over_prob": bl_over})
    if canonical_sha256(observed_ids) != quote_identity:
        raise ComparisonFailure("quote_cohort_drift")
    return pd.DataFrame(rows)


def request_n_samples(shared: Any) -> int:
    return int(shared.evidence["predictor_settings"]["n_samples"])


def _numeric_column(frame: pd.DataFrame, name: str) -> np.ndarray:
    column = frame[name]
    if not isinstance(column, pd.Series):
        raise ComparisonFailure("ambiguous_numeric_column")
    converted = cast(pd.Series, pd.to_numeric(column, errors="coerce"))
    return converted.to_numpy(dtype=np.float64, na_value=np.nan)


def _probability_score(group: pd.DataFrame) -> dict[str, Any]:
    counts = group["score_status"].value_counts().to_dict()
    valid = cast(pd.DataFrame, group.loc[group["score_status"] == "scored"]).copy()
    result: dict[str, Any] = {"n": len(valid), **{f"excluded_{key}": int(value) for key, value in counts.items() if key != "scored"}}
    if valid.empty:
        return {**result, "brier": None, "log_loss": None, "market_brier": None, "market_log_loss": None}
    p = _numeric_column(valid, "over_prob")
    market = _numeric_column(valid, "market_over")
    outcome = _numeric_column(valid, "outcome_over")
    if not np.isfinite(p).all() or not np.isfinite(market).all() or ((p < 0) | (p > 1)).any():
        raise ComparisonFailure("probability_score_contract")
    p_clip = np.clip(p, PROB_EPSILON, 1 - PROB_EPSILON)
    market_clip = np.clip(market, PROB_EPSILON, 1 - PROB_EPSILON)
    return {
        **result,
        "brier": float(np.mean((p - outcome) ** 2)),
        "log_loss": float(np.mean(-(outcome * np.log(p_clip) + (1 - outcome) * np.log(1 - p_clip)))),
        "market_brier": float(np.mean((market - outcome) ** 2)),
        "market_log_loss": float(np.mean(-(outcome * np.log(market_clip) + (1 - outcome) * np.log(1 - market_clip)))),
    }


def _prediction_score(predictions: pd.DataFrame, actual_lookup: dict[tuple, float]) -> list[dict[str, Any]]:
    if predictions.empty:
        return []
    frame = predictions.copy()
    frame["actual"] = [actual_lookup.get(key, np.nan) for key in frame[KEYS].itertuples(index=False, name=None)]
    rows = []
    for stat, group in [("ALL", frame), *list(frame.groupby("stat", sort=True))]:
        actual = _numeric_column(group, "actual")
        median = _numeric_column(group, "pred_q50")
        valid = np.isfinite(actual) & np.isfinite(median)
        values: dict[str, Any] = {"stat": stat, "n": int(valid.sum())}
        if valid.any():
            error = median[valid] - actual[valid]
            values.update(mae=float(np.mean(np.abs(error))), rmse=float(np.sqrt(np.mean(error**2))), bias=float(np.mean(error)))
        pinballs = []
        for quantile, column in zip((0.1, 0.25, 0.5, 0.75, 0.9), PCOLS):
            pred = _numeric_column(group, column)
            q_valid = np.isfinite(actual) & np.isfinite(pred)
            residual = actual[q_valid] - pred[q_valid]
            pinball = float(np.mean(np.maximum(quantile * residual, (quantile - 1) * residual))) if q_valid.any() else None
            values[f"q{int(quantile * 100)}_coverage"] = float(np.mean(residual <= 0)) if q_valid.any() else None
            values[f"q{int(quantile * 100)}_pinball"] = pinball
            if pinball is not None:
                pinballs.append(pinball)
        values["mean_pinball"] = float(np.mean(pinballs)) if pinballs else None
        rows.append(values)
    return rows


def _score_rows(probabilities: pd.DataFrame, prediction_frames: dict[tuple[str, int], pd.DataFrame], shared: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, group in probabilities.groupby(["variant", "seed", "mode", "stat"], sort=True):
        assert isinstance(key, tuple)
        variant, seed, mode, stat = key
        rows.append({"row_type": "seed", "scope": "quote_probability", "variant": variant, "seed": seed,
                     "mode": mode, "stat": stat, **_probability_score(group)})
    for key, group in probabilities.groupby(["variant", "seed", "mode"], sort=True):
        assert isinstance(key, tuple)
        variant, seed, mode = key
        rows.append({"row_type": "seed", "scope": "quote_probability", "variant": variant, "seed": seed,
                     "mode": mode, "stat": "ALL", **_probability_score(group)})
    actual_lookup, voids = _actual_lookup(shared)
    actual_lookup = {key: value for key, value in actual_lookup.items() if key[:2] not in voids}
    for (variant, seed), predictions in prediction_frames.items():
        for score in _prediction_score(predictions, actual_lookup):
            rows.append({"row_type": "seed", "scope": "prediction_distribution", "variant": variant,
                         "seed": seed, "mode": "samples", **score})
    seed_rows = pd.DataFrame(rows)
    numeric = [name for name in ("brier", "log_loss", "market_brier", "market_log_loss", "mae", "rmse", "bias", "mean_pinball")
               if name in seed_rows]
    aggregate_rows: list[dict[str, Any]] = []
    for keys, group in seed_rows.groupby(["scope", "variant", "mode", "stat"], dropna=False, sort=True):
        assert isinstance(keys, tuple)
        scope, variant, mode, stat = keys
        aggregate: dict[str, Any] = {"row_type": "seed_aggregate", "scope": scope, "variant": variant,
                                     "seed": None, "mode": mode, "stat": stat, "seed_count": group["seed"].nunique()}
        for column in numeric:
            values = _numeric_column(group, column)
            values = values[np.isfinite(values)]
            if values.size:
                aggregate[f"{column}_mean"] = float(np.mean(values))
                aggregate[f"{column}_min"] = float(np.min(values))
                aggregate[f"{column}_max"] = float(np.max(values))
        aggregate_rows.append(aggregate)
    paired_rows: list[dict[str, Any]] = []
    metrics = [name for name in ("brier", "log_loss", "mae", "rmse", "bias", "mean_pinball") if name in seed_rows]
    control = seed_rows[seed_rows["variant"] == "control"]
    for variant in VARIANTS[1:]:
        other = seed_rows[seed_rows["variant"] == variant]
        joined = other.merge(control, on=["scope", "seed", "mode", "stat"], suffixes=("_variant", "_control"))
        for row in joined.to_dict("records"):
            output: dict[str, Any] = {"row_type": "paired_to_control", "scope": row["scope"], "variant": variant,
                                      "seed": row["seed"], "mode": row["mode"], "stat": row["stat"]}
            for metric in metrics:
                left, right = row.get(f"{metric}_variant"), row.get(f"{metric}_control")
                if left is not None and right is not None and math.isfinite(float(left)) and math.isfinite(float(right)):
                    output[f"{metric}_delta"] = float(left) - float(right)
            paired_rows.append(output)
    return pd.concat([seed_rows, pd.DataFrame(aggregate_rows), pd.DataFrame(paired_rows)], ignore_index=True, sort=False)


def _render_report(request: ComparisonRequest, observed: dict[str, int], scores: pd.DataFrame) -> str:
    lines = [
        "# NBA sampler comparison", "", "Diagnostic/development result only; no model was trained, promoted, or selected.", "",
        "## Cohort", "", f"- Eligible player-games: {observed['eligible_player_games']}",
        f"- Acquired quote rows: {observed['quote_rows']}", f"- Bounded component sample workload: {observed['sample_workload']}",
        "- Probability scores are quote-row-weighted; multiple books/lines are intentionally retained.", "",
        "## Arms", "", "- control: current canonical predictor settings", "- no_reb_widening: reb variance inflation = 1.0 only",
        "- no_extra_lower_tail: lower-tail multiplier = 1.0 only", "",
        "## Aggregate ALL quote scores", "",
        "Variant | Mode | Brier mean | Brier range | Log-loss mean", "---|---|---:|---:|---:",
    ]
    subset = cast(pd.DataFrame, scores.loc[(scores["row_type"] == "seed_aggregate") & (scores["scope"] == "quote_probability") & (scores["stat"] == "ALL")])
    for row in subset.to_dict("records"):
        lines.append(
            f"{row['variant']} | {row['mode']} | {row.get('brier_mean')} | "
            f"{row.get('brier_min')}..{row.get('brier_max')} | {row.get('log_loss_mean')}"
        )
    lines.extend([
        "", "## Acceptance boundary", "", "- Same-seed arms use paired RandomState draws and must finish at identical RNG state.",
        "- The first-seed control exactly reproduces canonical acquisition samples and canonical raw/BL replay outputs.",
        "- Component heads are cache-only after acquisition; any unseen prepared batch fails closed.",
        "- Fixed BL confidence may change with sampler output; tau, z_max, max_weight, stake, edge threshold, source, cohort, copula, and clipping policy are not refit.",
        "- This window is development evidence, not untouched confirmation, and this command makes no winner or promotion claim.", "",
    ])
    return "\n".join(lines)


def _implementation_source() -> dict[str, str]:
    root = Path(__file__).resolve().parents[3]
    return {name: sha256_file(root / name) for name in (
        "src/backtesting/nba/sampler_comparison.py", "scripts/compare_nba_samplers.py",
    )}


def prepare_comparison(request: ComparisonRequest) -> dict[str, Any]:
    """Filesystem-only preflight; validates artifact and destination without loading models."""
    plan = build_plan(request.sweep)
    return {
        "command": "compare-nba-samplers",
        "execution": False,
        "state": "preflight_only",
        "output_dir": str(Path(request.sweep.output_dir).resolve(strict=False)),
        "artifact": copy.deepcopy(plan["artifact"]),
        "source_policy": copy.deepcopy(plan["source_policy"]),
        "effective_bookmakers": list(request.sweep.bookmakers),
        "comparison": request.to_dict(),
        "canonical_plan": plan,
        "comparison_source": _implementation_source(),
        "model_deserialized": False,
        "database_or_network_accessed": False,
        "output_created": False,
        "historical_readiness": "unverified",
        "decision_grade": False,
    }


def run_comparison(
    request: ComparisonRequest,
    *,
    runtime_factory: Callable[[SweepConfig, dict], Runtime] | None = None,
    plan: dict[str, Any] | None = None,
) -> Path:
    """Run one canonical acquisition and bounded paired sampler replays."""
    stage = "preflight"
    current_plan = build_plan(request.sweep)
    if plan is not None and canonical_sha256(plan) != canonical_sha256(current_plan):
        raise ComparisonFailure("stale_plan")
    plan = copy.deepcopy(current_plan)
    implementation_source = _implementation_source()
    destination = Path(request.sweep.output_dir).resolve(strict=False)
    root, final = create_exclusive_incomplete_dir(destination.parent, destination.name)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "state": "incomplete",
        "failure": None,
        "canonical_plan": plan,
        "comparison_source": implementation_source,
        "comparison": request.to_dict(),
        "historical_readiness": "unverified",
        "decision_grade": False,
        "outputs": {},
    }
    try:
        _write_json(root / "comparison_manifest.json", manifest)
        stage = "runtime_bootstrap"
        runtime = (bootstrap_runtime if runtime_factory is None else runtime_factory)(request.sweep, plan)
        frozen = FrozenPipeline.attach(runtime, request.sweep.stats)
        stage = "acquisition"
        shared = acquire_shared_inputs(request.sweep, plan, runtime)
        calls_at_freeze = frozen.freeze()
        shared.assert_unchanged(plan)
        observed = _observed_bounds(request, shared)
        logger.info("Frozen acquisition: %d player-games, %d quote rows, %d original head calls",
                    observed["eligible_player_games"], observed["quote_rows"], calls_at_freeze)
        stage = "component_export"
        components = _component_forecasts(shared, frozen, request.sweep.stats)
        quote_inventory, quote_identity, acquisition_exclusions = _quote_inventory(shared)
        if not quote_inventory:
            raise ComparisonFailure("empty_eligible_quote_cohort")
        if len(quote_inventory) > request.max_quote_rows:
            raise ComparisonFailure("eligible_quote_bound")
        probability_frames: list[pd.DataFrame] = []
        selected_frames: list[pd.DataFrame] = []
        bet_frames: list[pd.DataFrame] = []
        metrics: dict[str, Any] = {}
        prediction_frames: dict[tuple[str, int], pd.DataFrame] = {}
        bl_cell = request.sweep.cells()[1]
        for seed in request.seeds:
            rng_states: dict[str, tuple] = {}
            for variant in VARIANTS:
                stage = f"sampling_{variant}_{seed}"
                logger.info("Sampler arm seed=%d variant=%s", seed, variant)
                settings = _variant_settings(plan["predictor_settings"], seed, variant)
                records, rng_state = _sample_records(shared, frozen.pipeline, settings, request.sweep.stats)
                rng_states[variant] = rng_state
                if seed == request.sweep.seed and variant == "control":
                    _assert_control_samples(shared, records)
                probability_frames.append(
                    _probability_rows(quote_inventory, quote_identity, records, shared, variant, seed, bl_cell)
                )
                for cell in request.sweep.cells():
                    mode = "raw" if cell.tau is None else "bl"
                    replay = _replay(request, shared, records, cell)
                    if seed == request.sweep.seed and variant == "control":
                        reference = evaluate_cell(request.sweep, plan, shared, cell)
                        _assert_replay_parity(reference, replay)
                    selected = replay["predictions"].copy()
                    selected.insert(0, "mode", mode)
                    selected.insert(0, "seed", seed)
                    selected.insert(0, "variant", variant)
                    selected_frames.append(selected)
                    bets = replay["bets"].copy()
                    bets.insert(0, "mode", mode)
                    bets.insert(0, "seed", seed)
                    bets.insert(0, "variant", variant)
                    bet_frames.append(bets)
                    metrics[f"{variant}/{seed}/{mode}"] = replay["metrics"].to_dict()
                    if mode == "raw":
                        prediction_frames[(variant, seed)] = records_to_predictions(records)
                    logger.info("Replay seed=%d variant=%s mode=%s bets=%d profit=%.2f",
                                seed, variant, mode, replay["metrics"].total_bets, replay["metrics"].total_profit)
                del records
            if any(not _same_rng(rng_states["control"], rng_states[variant]) for variant in VARIANTS[1:]):
                raise ComparisonFailure("paired_rng_state")
        if frozen.original_calls != calls_at_freeze:
            raise ComparisonFailure("booster_call_after_freeze")
        probabilities = pd.concat(probability_frames, ignore_index=True)
        selected = pd.concat(selected_frames, ignore_index=True, sort=False)
        bets = pd.concat(bet_frames, ignore_index=True, sort=False)
        scores = _score_rows(probabilities, prediction_frames, shared)
        report = _render_report(request, observed, scores)
        stage = "export"
        for name, frame in (
            ("component_forecasts.csv", components),
            ("all_quote_probabilities.csv", probabilities),
            ("summary_scores.csv", scores),
            ("selected_predictions.csv", selected),
            ("bets.csv", bets),
        ):
            manifest["outputs"][name] = _write_csv(root / name, frame)
        _write_json(root / "metrics.json", metrics)
        manifest["outputs"]["metrics.json"] = {"sha256": sha256_file(root / "metrics.json")}
        with (root / "report.md").open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(report)
        manifest["outputs"]["report.md"] = {"sha256": sha256_file(root / "report.md")}
        stage = "completion_verification"
        if frozen.original_calls != calls_at_freeze:
            raise ComparisonFailure("booster_call_after_freeze")
        if build_plan(request.sweep, check_output_collision=False) != plan:
            raise ComparisonFailure("artifact_or_source_drift")
        if _implementation_source() != implementation_source:
            raise ComparisonFailure("comparison_source_drift")
        manifest.update(
            state="complete",
            observed_bounds=observed,
            shared_input_identity=shared.identity,
            quote_cohort_identity=quote_identity,
            quote_inventory_rows=len(quote_inventory),
            acquisition_quote_exclusions=acquisition_exclusions,
            arm_settings={
                str(seed): {variant: _variant_settings(plan["predictor_settings"], seed, variant)
                            for variant in VARIANTS}
                for seed in request.seeds
            },
            component_forecast_identity=content_digest(components),
            component_cache=frozen.receipts(),
            original_head_calls_at_freeze=calls_at_freeze,
            original_head_calls_after_replay=frozen.original_calls,
            canonical_control_sample_parity=True,
            canonical_control_replay_parity=True,
            paired_rng_state=True,
            model_deserialized=runtime.model_deserialized,
        )
        for name, receipt in manifest["outputs"].items():
            if sha256_file(root / name) != receipt["sha256"]:
                raise ComparisonFailure("output_digest_readback")
        _write_json(root / "comparison_manifest.json", manifest, replace=True)
        stored = json.loads((root / "comparison_manifest.json").read_text(encoding="utf-8"))
        if stored != json_safe(manifest) or stored["state"] != "complete":
            raise ComparisonFailure("manifest_readback")
        _rename_no_replace(root, final)
        return final
    except BaseException as exc:
        failure = exc if isinstance(exc, ComparisonFailure) else ComparisonFailure(stage, type(exc).__name__)
        manifest["state"] = "failed"
        manifest["failure"] = {"stage": failure.stage, "type": failure.error_type}
        try:
            _write_json(root / "comparison_manifest.json", manifest, replace=True)
        except Exception:
            pass
        if isinstance(exc, KeyboardInterrupt | SystemExit):
            raise
        raise failure from None


def records_to_predictions(records: dict) -> pd.DataFrame:
    frames = [record["predictions"] for record in records.values() if not record["predictions"].empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=PREDICTION_COLUMNS)


def make_request(
    *,
    model_dir: str,
    start: date,
    end: date,
    stats: tuple[str, ...],
    bookmakers: tuple[str, ...] = BOOKMAKERS,
    seeds: tuple[int, ...],
    n_samples: int,
    min_minutes_avg: float,
    starting_bankroll: float,
    flat_bet_size: float,
    edge_threshold: float,
    bl_tau: float,
    bl_z_max: float,
    bl_max_weight: float,
    output_dir: str,
    local: bool,
    diagnostic_history: bool,
    allowed_bets: tuple[tuple[str, str], ...] | None = None,
    historical_policy: str | None = None,
    historical_source_metadata: str | None = None,
    acquisition_limits: Any = None,
    max_days: int = 31,
    max_seeds: int = 5,
    max_samples: int = 20_000,
    max_player_games: int = 10_000,
    max_quote_rows: int = 250_000,
    max_total_samples: int = 3_000_000_000,
) -> ComparisonRequest:
    if not seeds:
        raise ValueError("At least one seed is required")
    sweep = SweepConfig(
        model_dir=model_dir,
        start=start,
        end=end,
        stats=stats,
        bookmakers=bookmakers,
        n_samples=n_samples,
        seed=seeds[0],
        min_minutes_avg=min_minutes_avg,
        starting_bankroll=starting_bankroll,
        flat_bet_size=flat_bet_size,
        edge_thresholds=(edge_threshold,),
        output_dir=output_dir,
        allowed_bets=allowed_bets,
        bl_taus=(bl_tau,),
        bl_z_max=(bl_z_max,),
        bl_max_weight=(bl_max_weight,),
        max_cells=2,
        local=local,
        diagnostic_history=diagnostic_history,
        historical_policy=historical_policy,
        historical_source_metadata=historical_source_metadata,
        acquisition_limits=acquisition_limits,
    )
    return ComparisonRequest(
        sweep=sweep,
        seeds=seeds,
        max_days=max_days,
        max_seeds=max_seeds,
        max_samples=max_samples,
        max_player_games=max_player_games,
        max_quote_rows=max_quote_rows,
        max_total_samples=max_total_samples,
    )
