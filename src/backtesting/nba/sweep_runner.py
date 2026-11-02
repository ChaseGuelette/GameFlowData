"""One sequential acquisition, immutable evidence, isolated incumbent replay cells.

No cache loading/resume and no alternative probability, BL, sizing or settlement
math. Completed directories alone are publishable, and remain diagnostic forever.
"""
from __future__ import annotations

import copy
import csv
import hashlib
import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType

import numpy as np
import pandas as pd

from src.backtesting.backtest_harness import BacktestHarness, BacktestResult, _derive_replay_combo_samples
from src.backtesting.bet_simulator import Bet, BetSimulator
from src.backtesting.nba.sweep_config import READINESS, CellConfig, SweepConfig, build_plan, validate_plan
from src.config.combo_config import STAT_TO_MARKET
from src.models.black_litterman import BlackLittermanBlender
from src.models.nba_artifacts import (
    _rename_no_replace,
    canonical_sha256,
    create_exclusive_incomplete_dir,
    sha256_file,
    validate_artifact_bundle,
)

KEYS = ["player_id", "game_id", "stat"]
PREDICTION_COLUMNS = KEYS + ["game_date", "player_name", "team_id", "game_time", "pred_mean", "pred_std", "pred_median", "pred_q10", "pred_q25",
                            "pred_q50", "pred_q75", "pred_q90"]
QUOTE_COLUMNS = ["player_id", "game_id", "market_key", "line", "bookmaker", "over_odds", "under_odds"]
QUOTE_METADATA = [f"{side}_{suffix}" for side in ("over", "under", "selected")
                  for suffix in ("snapshot_time", "market_last_update", "bookmaker_last_update", "decision_time", "bookmaker")]
SELECTED_COLUMNS = ["selected_side", "selected_line", "selected_price", "max_edge"]
BL_COLUMNS = ["model_over", "model_under", "market_over", "market_under", "confidence",
              "posterior_over", "posterior_under", "bl_enabled"]
EDGE_COLUMNS = PREDICTION_COLUMNS + ["line", "bookmaker", "over_odds", "under_odds", "sample_status",
                                   "quote_status", "over_prob", "under_prob", "implied_over", "implied_under",
                                   "over_edge", "under_edge", "combo_rejection_reason", "combo_component_evidence",
                                   "market_key", "bet_decision_time", "quote_decision_policy"] + QUOTE_METADATA + BL_COLUMNS
# BetSimulator.to_dataframe exports every current Bet field, converting enums.
# Reading its dataclass contract does not fabricate a wager to discover columns.
BET_COLUMNS = [field.name for field in fields(Bet)]
GENERATED_COLUMNS = {"sample_status", "quote_status", "over_prob", "under_prob", "implied_over", "implied_under",
                     "over_edge", "under_edge", "actual", *SELECTED_COLUMNS, *BL_COLUMNS}


def _output_schemas(records: Mapping) -> dict:
    """Freeze required + lexically ordered observed extensions once per bundle.

    Preserve unknown prediction/quote provenance. Input fields that collide with
    numerical-owner output are namespaced; ambiguous namespaces fail, never drop
    a column. Empty cells use the very same contract as populated cells.
    """
    contracts = {}
    for kind in ("predictions", "quotes"):
        columns = set()
        for record in records.values():
            frame = record[kind]
            if not frame.columns.is_unique or any(type(c) is not str for c in frame.columns):
                raise SweepFailure("input_column_contract")
            columns.update(frame.columns)
        contracts[kind] = sorted(columns)
    pred = set(contracts["predictions"])
    # The harness owns merging quote core fields; preserve colliding predictor
    # evidence under its own namespace before invoking that unchanged seam.
    pred_renames = {c: "prediction__" + c for c in sorted(pred) if c in GENERATED_COLUMNS or c in set(QUOTE_COLUMNS) - set(KEYS)}
    pred = {pred_renames.get(c, c) for c in pred}
    quote_renames = {c: "quote__" + c for c in contracts["quotes"]
                     if c == "stat" or (c not in KEYS and (c in pred or c in GENERATED_COLUMNS))}
    for columns, renames in ((contracts["predictions"], pred_renames), (contracts["quotes"], quote_renames)):
        if len({renames.get(c, c) for c in columns}) != len(columns):
            raise SweepFailure("input_column_namespace_collision")
    quote = {quote_renames.get(c, c) for c in contracts["quotes"]}
    if (pred - set(KEYS)) & quote:
        raise SweepFailure("input_column_namespace_collision")
    observed = pred | quote
    edges = EDGE_COLUMNS + sorted(observed - set(EDGE_COLUMNS))
    predictions = edges + [c for c in [*SELECTED_COLUMNS, "actual"] if c not in edges]
    return {"version": 1, "extension_policy": "required columns then sorted observed input fields; collisions use prediction__/quote__; ambiguous names fail",
            "input_columns": contracts, "prediction_renames": pred_renames, "quote_renames": quote_renames,
            "files": {"predictions.csv": predictions, "bets.csv": BET_COLUMNS.copy(), "all_bookmaker_edges.csv": edges}}


def _schema_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if not frame.columns.is_unique or set(frame.columns) - set(columns):
        raise SweepFailure("undeclared_output_columns")
    return frame.reindex(columns=columns)


class SweepFailure(RuntimeError):  # noqa: N818 - public failure boundary, not an alternate numerical owner
    """Sanitized failure context; never echo incumbent DB/credential exceptions."""

    def __init__(self, stage: str, day: date | None = None, error_type: str = "invariant"):
        self.stage, self.day, self.error_type = stage, day, error_type
        super().__init__(f"Sweep failed: stage={stage}, date={day.isoformat() if day else 'n/a'}, type={error_type}")


def typed_value(value: object) -> object:
    """Deterministic typed encoding, not repr/default=str or numeric coercion.

    Array bytes are logical C-order bytes, including dtype/shape/endianness.
    Object arrays recursively encode supported values; arbitrary objects fail.
    DataFrames include ordered columns, dtypes, index, rows, and observed attrs.
    """
    if value is None:
        return ["null"]
    if value is pd.NA:
        return ["pandas.NA"]
    if value is pd.NaT:
        return ["pandas.NaT"]
    if isinstance(value, np.ma.MaskedArray):
        return ["masked", typed_value(value.data), typed_value(np.ma.getmaskarray(value)),
                typed_value(value.fill_value)]
    if isinstance(value, np.ndarray):
        if value.dtype.hasobject:
            content = [typed_value(item) for item in value.flat]
        else:
            raw = value.tobytes(order="C")
            content = {"bytes_sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)}
        return ["ndarray", value.dtype.str, typed_value(value.dtype.descr), list(value.shape), content]
    if isinstance(value, np.generic):
        if value.dtype.hasobject:
            raise ValueError("Unsupported object NumPy scalar in identity")
        return ["numpy_scalar", value.dtype.str, value.tobytes().hex()]
    if isinstance(value, pd.DataFrame):
        return ["dataframe", typed_value(list(value.columns)), [str(dtype) for dtype in value.dtypes],
                typed_value(list(value.index.names)), typed_value(list(value.index)),
                [[typed_value(item) for item in row] for row in value.itertuples(index=False, name=None)],
                typed_value(value.attrs)]
    if isinstance(value, pd.Timestamp):
        return ["timestamp", value.isoformat(), str(value.tz) if value.tz is not None else None,
                value.fold, value.unit, value.asm8.tobytes().hex()]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(), value.fold, getattr(value.tzinfo, "key", None)]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, time):
        return ["time", value.isoformat(), value.fold, getattr(value.tzinfo, "key", None)]
    if isinstance(value, pd.Timedelta):
        return ["pandas.Timedelta", value.asm8.dtype.str, value.asm8.tobytes().hex()]
    if isinstance(value, timedelta):
        return ["timedelta", value.days, value.seconds, value.microseconds]
    if isinstance(value, Decimal):
        parts = value.as_tuple()
        return ["decimal", parts.sign, list(parts.digits), parts.exponent]
    if type(value) is bool:
        return ["bool", value]
    if type(value) is int:
        return ["int", str(value)]
    if type(value) is float:
        return ["float", value.hex()]
    if type(value) is complex:
        return ["complex", value.real.hex(), value.imag.hex()]
    if type(value) is str:
        return ["str", value]
    if type(value) is bytes:
        return ["bytes", value.hex()]
    if isinstance(value, Mapping):
        return ["mapping", [[typed_value(key), typed_value(item)] for key, item in value.items()]]
    if type(value) in (list, tuple):
        return [type(value).__name__, [typed_value(item) for item in value]]
    raise ValueError(f"Unsupported identity type: {type(value).__name__}")


def content_digest(value: object) -> str:
    return canonical_sha256(typed_value(value))


def json_safe(value: object) -> object:
    """Export finite JSON; nonfinite numeric evidence uses an explicit tagged value."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        return json_safe(value.item())
    if type(value) is float:
        return value if math.isfinite(value) else {"$nonfinite": value.hex()}
    if type(value) in (str, bool, int):
        return value
    if isinstance(value, date | datetime | time):
        return value.isoformat()
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError("JSON evidence requires string keys")
        return {key: json_safe(item) for key, item in value.items()}
    if type(value) in (list, tuple):
        return [json_safe(item) for item in value]
    raise ValueError(f"Unsupported JSON evidence type: {type(value).__name__}")


def isolated_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("Acquisition must return DataFrames")
    result = frame.copy(deep=True)
    for column in result.columns:
        if result[column].dtype == object:
            result[column] = result[column].map(copy.deepcopy)
    result.attrs = copy.deepcopy(frame.attrs)
    return result


def _freeze_sample(value: object) -> object:
    # Numeric arrays backed by immutable bytes cannot have WRITEABLE re-enabled.
    if isinstance(value, np.ndarray) and not isinstance(value, np.ma.MaskedArray) and not value.dtype.hasobject:
        return np.frombuffer(value.tobytes(order="C"), dtype=value.dtype).reshape(value.shape)
    result = copy.deepcopy(value)
    if isinstance(result, np.ndarray):
        result.setflags(write=False)
    return result


def _sample_view(samples: Mapping) -> Mapping:
    # Malformed sequences/object arrays remain malformed evidence, not numeric
    # coerced vectors. Copy only those small unsupported bundles for isolation.
    return MappingProxyType({key: value if isinstance(value, np.ndarray) and not value.dtype.hasobject
                             and not isinstance(value, np.ma.MaskedArray) else copy.deepcopy(value)
                             for key, value in samples.items()})


@dataclass(frozen=True)
class Runtime:
    harness: BacktestHarness
    model_deserialized: bool = False


def _runtime_settings(runtime: Runtime, config: SweepConfig) -> dict:
    harness, predictor = runtime.harness, runtime.harness.predictor
    expected = {"stats": list(config.market_stats), "bookmakers": list(config.bookmakers),
                "min_minutes_avg": config.min_minutes_avg}
    if any(getattr(harness, key) != value for key, value in expected.items()):
        raise ValueError("Runtime acquisition settings disagree with request")
    names = ("n_samples", "use_correlated_sampling", "correlation_config", "variance_inflation",
             "tail_adjustment", "blowout_config", "copula_params", "combined_calibration_offsets")
    settings = {name: copy.deepcopy(getattr(predictor, name)) for name in names}
    settings["random_state"] = config.seed
    if content_digest(predictor.rng.get_state()) != content_digest(np.random.RandomState(config.seed).get_state()):
        raise ValueError("Runtime predictor is not freshly seeded as requested")
    return settings


def _runtime_features(runtime: Runtime) -> dict:
    pipeline = runtime.harness.model_pipeline

    def feature_list(value):
        if isinstance(value, dict):
            result = {}
            for key, names in value.items():
                if type(key) is not float or not 0 < key < 1:
                    raise ValueError("Unsupported pipeline feature quantile key")
                result[str(key)] = copy.deepcopy(names)
            return result
        return copy.deepcopy(value)

    return {"minutes_features": feature_list(pipeline.minutes_features),
            **{f"{stat}_rate_features": feature_list(pipeline.rate_features[stat])
               for stat in runtime.harness.model_pipeline.rate_models}}


def bootstrap_runtime(config: SweepConfig, plan: dict) -> Runtime:
    """Lazy exact loader, embedded calibration preserved; no global offsets loader."""
    validate_plan(config, plan)
    from src.db.client import get_engine
    from src.models.feature_store import FeatureStore
    from src.models.monte_carlo import MonteCarloPredictor
    from src.models.quantile_trainer import PlayerPropsModelPipeline

    engine = get_engine(local=config.local)
    store = FeatureStore(engine)
    pipeline = PlayerPropsModelPipeline.load_all(plan["artifact"]["path"], store)
    predictor = MonteCarloPredictor(pipeline, **copy.deepcopy(plan["predictor_settings"]))
    return Runtime(BacktestHarness(
        engine=engine, feature_store=store, model_pipeline=pipeline, predictor=predictor,
        stats=list(config.market_stats), bookmakers=list(config.bookmakers),
        min_minutes_avg=config.min_minutes_avg, starting_bankroll=config.starting_bankroll,
        flat_bet_size=config.flat_bet_size, allowed_bets=list(config.allowed_bets) if config.allowed_bets else None,
        stat_config=None,
    ), model_deserialized=True)


@dataclass(frozen=True)
class SharedInputs:
    request_identity: str
    evidence: Mapping
    identity: str

    def assert_unchanged(self, plan: dict) -> None:
        if self.request_identity != plan["request_identity"] or content_digest(self.evidence) != self.identity:
            raise SweepFailure("shared_input_identity")
        for name in ("artifact", "source", "acquisition_identity", "acquisition_settings", "source_policy",
                     "predictor_settings", "ordered_features"):
            if canonical_sha256(self.evidence[name]) != canonical_sha256(plan[name]):
                raise SweepFailure("shared_plan_binding")


def _validate_binding(config: SweepConfig, plan: dict, shared: SharedInputs | None = None,
                      cell: CellConfig | None = None) -> None:
    try:
        validate_plan(config, plan)
        if cell is not None and cell not in config.cells():
            raise ValueError("Unplanned cell")
        if shared is not None:
            shared.assert_unchanged(plan)
            expected = tuple(config.start + timedelta(days=i) for i in range((config.end - config.start).days + 1))
            records, acquired = shared.evidence["dates"], shared.evidence["acquired_dates"]
            if (shared.evidence["expected_dates"] != expected or tuple(records) != expected
                    or tuple(sorted(set(acquired))) != acquired or not set(acquired).issubset(expected)):
                raise ValueError("Inconsistent date classification")
            for day, record in records.items():
                if day not in acquired and (record["reason"] != "no_game" or any(record["counts"].values())):
                    raise ValueError("Omitted game date is not classified as loader-observed no_game")
    except SweepFailure:
        raise
    except Exception as exc:
        raise SweepFailure("plan_binding", error_type=type(exc).__name__) from None


def acquire_shared_inputs(config: SweepConfig, plan: dict, runtime: Runtime) -> SharedInputs:
    """Use loader seams once; do not use error-swallowing harness run/_run_date."""
    stage, day = "runtime_settings", None
    try:
        _validate_binding(config, plan)
        settings = _runtime_settings(runtime, config)
        if canonical_sha256(settings) != canonical_sha256(plan["predictor_settings"]):
            raise ValueError("Observed predictor settings disagree with plan")
        features = _runtime_features(runtime)
        if canonical_sha256(features) != canonical_sha256(plan["ordered_features"]):
            raise ValueError("Observed feature order disagrees with artifact")
        harness = runtime.harness
        stage = "game_dates"
        dates = harness._get_game_dates(config.start, config.end)
        if (not isinstance(dates, list) or any(type(value) is not date or not config.start <= value <= config.end
                                              for value in dates) or len(set(dates)) != len(dates)):
            raise ValueError("Invalid acquired game dates")
        dates = sorted(dates)
        stage = "features"
        prefetched = harness.feature_store.get_features_for_date_range(config.start, config.end)
        stage = "quotes"
        lines = harness._prefetch_all_lines(config.start, config.end)
        for mapping in (prefetched, lines):
            if not isinstance(mapping, Mapping) or any(type(key) is not date or key not in dates for key in mapping):
                raise ValueError("Unexpected acquisition date keys")
        stage = "actuals"
        actuals = isolated_frame(harness._get_actuals(config.start, config.end))
        stage = "voids"
        voids = isolated_frame(harness._get_voids(config.start, config.end))
        expected_dates = tuple(config.start + timedelta(days=i) for i in range((config.end - config.start).days + 1))
        records = {}
        for day in expected_dates:
            stage = "date_assembly"
            frame = isolated_frame(prefetched.get(day, pd.DataFrame()))
            quotes = isolated_frame(lines.get(day, pd.DataFrame(columns=QUOTE_COLUMNS)))
            if not quotes.empty:
                if (not set(QUOTE_COLUMNS).issubset(quotes.columns)
                        or not quotes["bookmaker"].isin(config.bookmakers).all()
                        or not quotes["market_key"].isin([STAT_TO_MARKET[stat] for stat in config.market_stats]).all()):
                    raise ValueError("Quote settings/schema disagree with request")
            elif not set(QUOTE_COLUMNS).issubset(quotes.columns):
                quotes = quotes.reindex(columns=list(dict.fromkeys([*quotes.columns, *QUOTE_COLUMNS])))
            before = len(frame)
            eligible = frame
            if "player_avg_min_l5" in eligible.columns:
                eligible = eligible[eligible["player_avg_min_l5"] >= config.min_minutes_avg]
            # Omitted dates describe the game-date loader's observation only;
            # no source-completeness or real-world no-games claim is made.
            reason = ("no_game" if day not in dates else "no_features" if frame.empty
                      else "filtered" if eligible.empty else None)
            predictions, samples = pd.DataFrame(columns=PREDICTION_COLUMNS), {}
            if reason is None:
                eligible = eligible.sort_values(["player_id", "game_id"]).reset_index(drop=True)
                stage = "prediction"
                prediction_list, samples = harness.predictor.predict_batch_for_date(
                    isolated_frame(eligible), stats=list(config.stats))
                if not isinstance(prediction_list, list) or not isinstance(samples, dict):
                    raise ValueError("Malformed prediction result")
                # Reject arbitrary evidence types before combo helper can coerce it.
                content_digest((prediction_list, samples))
                stage = "combo_assembly"
                combo_predictions, combo_samples = _derive_replay_combo_samples(samples, prediction_list)
                prediction_list = copy.deepcopy(prediction_list) + combo_predictions
                samples = {**samples, **combo_samples}
                for prediction in prediction_list:
                    prediction["game_date"] = day
                predictions = pd.DataFrame(prediction_list) if prediction_list else predictions
                reason = "no_predictions" if predictions.empty else "no_quotes" if quotes.empty else "acquired"
            records[day] = {
                "raw_features": frame, "eligible_features": isolated_frame(eligible),
                "predictions": isolated_frame(predictions), "quotes": quotes,
                "samples": MappingProxyType({key: _freeze_sample(value) for key, value in samples.items()}),
                "reason": reason, "counts": {"features": before, "eligible": len(eligible),
                                              "predictions": len(predictions), "quotes": len(quotes),
                                              "sample_bundles": len(samples)},
            }
        day, stage = None, "bundle_identity"
        evidence = MappingProxyType({
            "artifact": copy.deepcopy(plan["artifact"]), "source": copy.deepcopy(plan["source"]),
            "acquisition_identity": plan["acquisition_identity"],
            "acquisition_settings": copy.deepcopy(plan["acquisition_settings"]),
            "source_policy": copy.deepcopy(plan["source_policy"]),
            "predictor_settings": settings, "ordered_features": features,
            "rng_state_after": harness.predictor.rng.get_state(),
            "expected_dates": expected_dates,
            "date_outcome_basis": "game-date loader observation, not verified source absence",
            "acquired_dates": tuple(dates), "dates": MappingProxyType(records),
            "actuals": actuals, "voids": voids, "empty_reason": "no_game_dates" if not dates else None,
            "output_schemas": _output_schemas(records),
        })
        shared = SharedInputs(plan["request_identity"], evidence, content_digest(evidence))
        _validate_binding(config, plan, shared)
        return shared
    except SweepFailure:
        raise
    except Exception as exc:
        raise SweepFailure(stage, day, type(exc).__name__) from None


def _candidate_edges(harness: BacktestHarness, record: Mapping, schemas: dict) -> pd.DataFrame:
    predictions = isolated_frame(record["predictions"]).rename(columns=schemas["prediction_renames"])
    quotes = isolated_frame(record["quotes"]).rename(columns=schemas["quote_renames"])
    if predictions.empty:
        return pd.DataFrame(columns=EDGE_COLUMNS)
    edges = harness._calculate_edges(predictions, quotes, _sample_view(record["samples"]))
    # The protected numerical seam deliberately projects quote columns. Reattach
    # the untouched full quote evidence by the identical left-merge traversal,
    # checking core alignment rather than joining duplicate quotes a second time.
    full_quotes = quotes.copy()
    full_quotes["stat"] = full_quotes["market_key"].map({value: key for key, value in STAT_TO_MARKET.items()})
    full = predictions.merge(full_quotes, on=KEYS, how="left", suffixes=("", "_quote"))
    core = KEYS + ["line", "over_odds", "under_odds", "bookmaker"]
    if len(full) != len(edges) or not full[core].equals(edges[core]):
        raise SweepFailure("quote_evidence_alignment")
    for column in full.columns:
        if column not in edges:
            edges[column] = full[column].values
    edges["quote_status"] = np.where(edges["line"].notna(), "quoted", "no_quote")
    return edges


def evaluate_cell(config: SweepConfig, plan: dict, shared: SharedInputs, cell: CellConfig) -> BacktestResult:
    """Fresh full global BL + simulator/metrics; no acquisition/runtime calls."""
    _validate_binding(config, plan, shared, cell)
    bl_config = cell.bl_config()
    defaults = _cell_simulator(config, cell)
    harness = BacktestHarness(
        engine=None, feature_store=None, model_pipeline=None, predictor=None,
        edge_threshold=cell.edge_threshold, starting_bankroll=config.starting_bankroll,
        flat_bet_size=config.flat_bet_size, max_bet_pct=defaults.max_bet_pct, kelly_fraction=defaults.kelly_fraction,
        stats=list(config.market_stats), bookmakers=list(config.bookmakers), min_minutes_avg=config.min_minutes_avg,
        allowed_bets=list(config.allowed_bets) if config.allowed_bets else None,
        bl_blender=BlackLittermanBlender(bl_config) if bl_config is not None else None,
        bl_sizing_blender=None, stat_config=None,
    )
    schemas = shared.evidence["output_schemas"]
    selected_frames, edge_frames = [], []
    for record in shared.evidence["dates"].values():
        edges = _candidate_edges(harness, record, schemas)
        if not edges.empty:
            edge_frames.append(isolated_frame(edges))
            selected = harness._filter_best_bets(isolated_frame(edges))
            if not selected.empty:
                selected_frames.append(selected)
    predictions = (pd.concat(selected_frames, ignore_index=True).sort_values(["game_date", "game_id", "player_id"])
                   if selected_frames else pd.DataFrame(columns=EDGE_COLUMNS + ["selected_side", "actual"]))
    all_edges = (pd.concat(edge_frames, ignore_index=True).sort_values(["game_date", "player_id", "stat", "bookmaker"])
                 if edge_frames else pd.DataFrame(columns=EDGE_COLUMNS))
    actuals, voids = isolated_frame(shared.evidence["actuals"]), isolated_frame(shared.evidence["voids"])
    simulator = harness._simulator
    for day in sorted(predictions["game_date"].unique()):
        if not voids.empty:
            simulator.resolve_voids(voids)
        if not actuals.empty:
            simulator.resolve_bets(actuals)
        simulator.evaluate_predictions(predictions[predictions["game_date"] == day], day)
    if not voids.empty:
        simulator.resolve_voids(voids)
    if not actuals.empty:
        simulator.resolve_bets(actuals)
    bets = simulator.to_dataframe()

    if not predictions.empty and not actuals.empty:
        predictions = harness._merge_actuals(predictions, actuals)
    elif "actual" not in predictions:
        predictions["actual"] = np.nan
    metrics = harness._metrics_calc.calculate(predictions, bets, starting_bankroll=config.starting_bankroll)
    effective = _effective_config(config, plan, shared, cell, bets.empty, simulator=simulator)
    predictions = _schema_frame(predictions, schemas["files"]["predictions.csv"])
    bets = _schema_frame(bets, schemas["files"]["bets.csv"])
    all_edges = _schema_frame(all_edges, schemas["files"]["all_bookmaker_edges.csv"])
    _validate_binding(config, plan, shared, cell)
    return BacktestResult(predictions, bets, all_edges, metrics, config.start, config.end, effective)


def _cell_simulator(config: SweepConfig, cell: CellConfig) -> BetSimulator:
    # Fresh incumbent defaults are the owner, not duplicated odds/Kelly constants.
    return BetSimulator(edge_threshold=cell.edge_threshold, starting_bankroll=config.starting_bankroll,
                        flat_bet_size=config.flat_bet_size,
                        allowed_bets=set(config.allowed_bets) if config.allowed_bets else None)


def _effective_config(config: SweepConfig, plan: dict, shared: SharedInputs, cell: CellConfig,
                      no_bets: bool, *, simulator: BetSimulator | None = None) -> dict:
    simulator = _cell_simulator(config, cell) if simulator is None else simulator
    bl_config = cell.bl_config()
    if simulator.allowed_bets != (set(config.allowed_bets) if config.allowed_bets else None):
        raise SweepFailure("effective_allowed_bets_drift")
    return {
        **config.to_dict(), **cell.to_dict(), "cell_id": cell.cell_id,
        "edge_threshold": simulator.edge_threshold, "starting_bankroll": simulator.starting_bankroll,
        "flat_bet_size": simulator.flat_bet_size,
        "bl_config": vars(bl_config) if bl_config is not None else None,
        "stat_config": simulator.stat_config, "bl_sizing_blender": None, "max_bet_pct": simulator.max_bet_pct,
        "kelly_fraction": simulator.kelly_fraction, "kelly_used": False,
        "use_bl_for_sizing": simulator.use_bl_for_sizing, "sizing_mode": "flat",
        "min_odds": simulator.min_odds, "max_odds": simulator.max_odds,
        "request_identity": plan["request_identity"], "input_identity": shared.identity,
        "empty_reason": "no_bets" if no_bets else None, **copy.deepcopy(READINESS),
        "output_schema_identity": canonical_sha256(shared.evidence["output_schemas"]),
    }


def _write_json(path: Path, value: dict, *, replace: bool = False) -> None:
    payload = json_safe(value)
    temporary = path.with_suffix(path.suffix + ".tmp") if replace else path
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, sort_keys=True, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    if json.loads(temporary.read_text(encoding="utf-8")) != payload:
        raise SweepFailure("json_readback")
    if replace:
        os.replace(temporary, path)


def _write_csv(path: Path, frame: pd.DataFrame) -> dict:
    if not len(frame.columns) or any(type(column) is not str for column in frame.columns):
        raise SweepFailure("csv_schema")
    exported = isolated_frame(frame)
    for column in exported:
        if exported[column].dtype == object:
            exported[column] = exported[column].map(
                lambda value: json.dumps(typed_value(value), ensure_ascii=False, allow_nan=False)
                if isinstance(value, Mapping | list | tuple | np.ndarray) else value)
    with path.open("x", encoding="utf-8", newline="") as handle:
        exported.to_csv(handle, index=False)
        handle.flush()
        os.fsync(handle.fileno())
    with path.open(encoding="utf-8", newline="") as handle:
        rows = csv.reader(handle)
        if next(rows) != list(frame.columns) or sum(1 for _ in rows) != len(frame):
            raise SweepFailure("csv_readback")
    return {"sha256": sha256_file(path), "rows": len(frame), "columns": list(frame.columns)}


def export_cell(root: Path, result: BacktestResult) -> dict:
    cell_dir = root / "cells" / result.config["cell_id"]
    cell_dir.mkdir(parents=True, exist_ok=False)
    _write_json(cell_dir / "config.json", result.config)
    _write_json(cell_dir / "metrics.json", {**result.metrics.to_dict(), "config": result.config})
    files = {name: _write_csv(cell_dir / name, frame) for name, frame in (
        ("predictions.csv", result.predictions_df), ("bets.csv", result.bets_df),
        ("all_bookmaker_edges.csv", result.all_edges_df),
    )}
    for name in ("config.json", "metrics.json"):
        files[name] = {"sha256": sha256_file(cell_dir / name)}
    return {"cell_id": result.config["cell_id"], "input_identity": result.config["input_identity"],
            "output_schema_identity": result.config["output_schema_identity"],
            "effective_config_identity": canonical_sha256(json_safe(result.config)), "files": files}


def _validate_result(config: SweepConfig, plan: dict, shared: SharedInputs,
                     cell: CellConfig, result: BacktestResult) -> None:
    _validate_binding(config, plan, shared, cell)
    expected = _effective_config(config, plan, shared, cell, result.bets_df.empty)
    if (canonical_sha256(result.config) != canonical_sha256(expected)
            or result.metrics.total_bets != len(result.bets_df)
            or result.start_date != config.start or result.end_date != config.end):
        raise SweepFailure("effective_cell_drift")
    # Reject unsupported values even when a result frame is not shared evidence.
    for name, frame in (("predictions.csv", result.predictions_df), ("bets.csv", result.bets_df),
                        ("all_bookmaker_edges.csv", result.all_edges_df)):
        if list(frame.columns) != shared.evidence["output_schemas"]["files"][name]:
            raise SweepFailure("effective_output_schema_drift")
        content_digest(frame)


def _verify_exports(root: Path, receipts: list[dict], plan: dict, shared: SharedInputs,
                    config: SweepConfig) -> None:
    _validate_binding(config, plan, shared)
    if len(receipts) != plan["cell_count"] or {r["cell_id"] for r in receipts} != {c["cell_id"] for c in plan["cells"]}:
        raise SweepFailure("cell_count_readback")
    for receipt in receipts:
        directory = root / "cells" / receipt["cell_id"]
        if set(receipt["files"]) != {"config.json", "metrics.json", "predictions.csv", "bets.csv", "all_bookmaker_edges.csv"}:
            raise SweepFailure("required_files")
        effective = json.loads((directory / "config.json").read_text(encoding="utf-8"))
        cell = next(cell for cell in config.cells() if cell.cell_id == receipt["cell_id"])
        expected = _effective_config(config, plan, shared, cell, receipt["files"]["bets.csv"]["rows"] == 0)
        if (effective["input_identity"] != shared.identity or effective["request_identity"] != plan["request_identity"]
                or canonical_sha256(effective) != canonical_sha256(expected)
                or receipt["output_schema_identity"] != canonical_sha256(shared.evidence["output_schemas"])
                or canonical_sha256(effective) != receipt["effective_config_identity"]):
            raise SweepFailure("effective_config_readback")
        for name, evidence in receipt["files"].items():
            path = directory / name
            if sha256_file(path) != evidence["sha256"]:
                raise SweepFailure("output_digest_readback")
            if name.endswith(".csv"):
                with path.open(encoding="utf-8", newline="") as handle:
                    rows = csv.reader(handle)
                    columns = shared.evidence["output_schemas"]["files"][name]
                    if (next(rows) != columns or evidence["columns"] != columns
                            or sum(1 for _ in rows) != evidence["rows"]):
                        raise SweepFailure("output_count_readback")
            else:
                json.loads(path.read_text(encoding="utf-8"))


def run_sweep(config: SweepConfig, *, runtime_factory=None, plan: dict | None = None,
              cell_order: tuple[CellConfig, ...] | None = None) -> Path:
    """Exclusive incomplete -> verified completed export, or fail without summary.

    runtime_factory is an operator-probe seam, not an alternative execution CLI.
    cell_order may only permute the complete planned set; never accepts a subset.
    """
    if not config.diagnostic_history:
        raise SweepFailure("diagnostic_history_acknowledgement")
    order = config.cells() if cell_order is None else cell_order
    if len(order) != len(config.cells()) or {cell.cell_id for cell in order} != {cell.cell_id for cell in config.cells()}:
        raise SweepFailure("cell_order")
    current = build_plan(config)
    if plan is not None and canonical_sha256(plan) != canonical_sha256(current):
        raise SweepFailure("stale_plan")
    plan = copy.deepcopy(current)
    destination = Path(config.output_dir).resolve(strict=False)
    root, final = create_exclusive_incomplete_dir(destination.parent, destination.name)
    manifest = {"schema_version": 1, "state": "incomplete", "plan": plan, "completed_cells": [],
                "input_identity": None, "outputs": {}, "failure": None, **copy.deepcopy(READINESS),
                "encoding": {"json_nonfinite": {"$nonfinite": "float.hex()"},
                             "nested_csv_values": "typed_value JSON", "csv_nan": "empty field"}}
    stage = "initial_manifest"
    try:
        _write_json(root / "sweep_manifest.json", manifest)
        stage = "runtime_bootstrap"
        runtime = (bootstrap_runtime if runtime_factory is None else runtime_factory)(config, plan)
        stage = "acquisition"
        shared = acquire_shared_inputs(config, plan, runtime)
        shared.assert_unchanged(plan)
        manifest.update({"input_identity": shared.identity,
                         "model_deserialized": runtime.model_deserialized,
                         "expected_dates": list(shared.evidence["expected_dates"]),
                         "acquired_dates": list(shared.evidence["acquired_dates"]),
                         "date_outcome_basis": shared.evidence["date_outcome_basis"],
                         "output_schemas": copy.deepcopy(shared.evidence["output_schemas"]),
                         "empty_reason": shared.evidence["empty_reason"],
                         "observed": [{"date": day, "reason": record["reason"], "counts": record["counts"]}
                                      for day, record in shared.evidence["dates"].items()],
                         "input_component_digests": {name: content_digest(value)
                                                    for name, value in shared.evidence.items()},
                         "sample_inventory": [
                             {"date": day, "key": typed_value(key), "content_digest": content_digest(value),
                              "dtype": value.dtype.str if isinstance(value, np.ndarray) else None,
                              "shape": list(value.shape) if isinstance(value, np.ndarray) else None,
                              "evidence_type": type(value).__name__}
                             for day, record in shared.evidence["dates"].items()
                             for key, value in record["samples"].items()]})
        _write_json(root / "sweep_manifest.json", manifest, replace=True)
        summaries, receipts = {}, []
        for cell in order:
            stage = "cell_evaluation"
            manifest["active_cell"] = cell.cell_id
            shared.assert_unchanged(plan)
            result = evaluate_cell(config, plan, shared, cell)
            shared.assert_unchanged(plan)
            _validate_result(config, plan, shared, cell, result)
            stage = "cell_export"
            receipts.append(export_cell(root, result))
            summaries[cell.cell_id] = {
                "cell_id": cell.cell_id, **cell.to_dict(), "input_identity": shared.identity,
                "request_identity": plan["request_identity"], "predictions": len(result.predictions_df),
                "candidate_edges": len(result.all_edges_df), **result.metrics.to_dict()["betting"],
                **result.metrics.to_dict()["risk"], "overall_calibration_gap": result.metrics.overall_calibration_gap,
                "metrics_json": json.dumps(json_safe(result.metrics.to_dict()), sort_keys=True, allow_nan=False),
                "historical_readiness": "unverified", "decision_grade": False,
            }
            manifest["completed_cells"] = copy.deepcopy(receipts)
            _write_json(root / "sweep_manifest.json", manifest, replace=True)
        stage = "completion_verification"
        shared.assert_unchanged(plan)
        _verify_exports(root, receipts, plan, shared, config)
        # Strict bytes/manifest inspection again; source/dependency drift also
        # invalidates this execution, not just a matching path/mtime/seed.
        inspection = validate_artifact_bundle(config.model_dir, config.stats, allow_legacy=False)
        if inspection.to_plan_dict() != plan["artifact"] or build_plan(config, check_output_collision=False) != plan:
            raise SweepFailure("artifact_or_source_drift")
        summary = pd.DataFrame([summaries[cell.cell_id] for cell in config.cells()])
        stage = "summary_export"
        manifest["outputs"]["sweep_summary.csv"] = _write_csv(root / "sweep_summary.csv", summary)
        manifest["state"] = "complete"
        manifest["active_cell"] = None
        _write_json(root / "sweep_manifest.json", manifest, replace=True)
        stored = json.loads((root / "sweep_manifest.json").read_text(encoding="utf-8"))
        if stored != json_safe(manifest) or stored["state"] != "complete":
            raise SweepFailure("manifest_readback")
        _verify_exports(root, receipts, plan, shared, config)
        if sha256_file(root / "sweep_summary.csv") != manifest["outputs"]["sweep_summary.csv"]["sha256"]:
            raise SweepFailure("summary_digest_readback")
        stage = "finalize"
        _rename_no_replace(root, final)
        return final
    except BaseException as exc:
        manifest["state"] = "failed"
        failure = exc if isinstance(exc, SweepFailure) else SweepFailure(stage, error_type=type(exc).__name__)
        manifest["failure"] = {"stage": failure.stage, "date": failure.day, "type": failure.error_type}
        # No success-shaped aggregate survives an interrupted/failed run.
        summary_path = root / "sweep_summary.csv"
        if summary_path.exists():
            summary_path.unlink()
        try:
            _write_json(root / "sweep_manifest.json", manifest, replace=True)
        except Exception:
            pass  # Original incomplete receipt still forbids acceptance.
        if isinstance(exc, KeyboardInterrupt | SystemExit):
            raise
        raise failure from None
