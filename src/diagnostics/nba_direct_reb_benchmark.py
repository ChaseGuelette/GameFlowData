"""Standalone direct-total NBA rebound benchmark; no promotion or betting logic."""
from __future__ import annotations

import copy
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.diagnostics.nba_error_stages import DiagnosticError, _scores, file_hash

QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)
QCOLS = tuple(f"q{int(q * 100)}" for q in QUANTILES)
PCOLS = tuple(f"pred_q{int(q * 100)}" for q in QUANTILES)
KEYS = ["player_id", "game_id"]
MODEL_NAME = "reb_direct_total_model.joblib"
logger = logging.getLogger(__name__)
OUTCOME_FIELDS = {
    "actual", "actual_reb", "actual_minutes", "pts", "reb", "ast", "min",
    "did_not_play", "market_over", "over_prob", "profit", "stake",
}
IDENTITY_FIELDS = {"player_id", "game_id", "game_date", "season_id", "team_id", "player_name"}
SOURCE_FILES = (
    "src/models/train_pipeline.py", "src/models/quantile_trainer.py",
    "src/models/hyperparameter_tuner.py", "src/models/features/nba/transforms.py",
    "src/diagnostics/nba_error_stages.py", "scripts/diagnose_nba_error_stages.py",
)


class BenchmarkError(DiagnosticError):
    """Safe benchmark validation failure."""


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(payload).hexdigest()


def _feature_union(run_config: dict) -> list[str]:
    components = run_config.get("feature_controls", {}).get("components", {})
    groups = []
    for name in ("minutes_features", "reb_rate_features"):
        effective = components.get(name, {}).get("effective")
        if not isinstance(effective, dict) or set(effective) != {str(q) for q in QUANTILES}:
            raise BenchmarkError(f"Parent {name} effective feature evidence is missing")
        for names in effective.values():
            if not isinstance(names, list) or any(type(item) is not str for item in names):
                raise BenchmarkError(f"Parent {name} feature evidence is malformed")
            groups.extend(names)
    features = sorted(set(groups))
    forbidden = sorted((set(features) & OUTCOME_FIELDS) | (set(features) & IDENTITY_FIELDS))
    if forbidden:
        raise BenchmarkError(f"Forbidden outcome/identity features: {forbidden}")
    if "prop_line_reb" not in features:
        raise BenchmarkError("Parent equal-information union unexpectedly omits prop_line_reb")
    if {"line_spread", "line_total"} & set(features):
        raise BenchmarkError("Parent-excluded game market features entered direct benchmark")
    if any(name.startswith(("team_out_", "opp_out_")) for name in features) or {
        "player_is_questionable", "player_is_probable"
    } & set(features):
        raise BenchmarkError("Parent-excluded injury features entered direct benchmark")
    return features


def _candidate_union(run_config: dict) -> list[str]:
    components = run_config.get("feature_controls", {}).get("components", {})
    groups: list[str] = []
    for name in ("minutes_features", "reb_rate_features"):
        candidates = components.get(name, {}).get("effective_candidates")
        if not isinstance(candidates, list) or any(type(item) is not str for item in candidates):
            raise BenchmarkError(f"Parent {name} effective candidate evidence is missing")
        groups.extend(candidates)
    candidates = sorted(set(groups))
    forbidden = sorted((set(candidates) & OUTCOME_FIELDS) | (set(candidates) & IDENTITY_FIELDS))
    if forbidden:
        raise BenchmarkError(f"Forbidden outcome/identity feature candidates: {forbidden}")
    if {"line_spread", "line_total"} & set(candidates):
        raise BenchmarkError("Parent-excluded game market features entered candidate pool")
    if any(name.startswith(("team_out_", "opp_out_")) for name in candidates) or {
        "player_is_questionable", "player_is_probable"
    } & set(candidates):
        raise BenchmarkError("Parent-excluded injury features entered candidate pool")
    return candidates


def _validate_diagnostic(diagnostic_dir: Path, prepared: dict) -> dict:
    manifest_path = diagnostic_dir / "diagnostic_manifest.json"
    manifest = _json(manifest_path)
    info = prepared["info"]
    required = {
        "state": "complete", "status": "report_complete", "baseline_reproduced": True,
        "model_identity": info["model_identity"], "reference_manifest_sha256": info["reference_manifest_sha256"],
        "start": info["start"], "end": info["end"], "n_samples": info["n_samples"], "seed": info["seed"],
    }
    for name, expected in required.items():
        if manifest.get(name) != expected:
            raise BenchmarkError(f"Diagnostic receipt mismatch: {name}")
    files = manifest.get("files", {})
    for name in ("totals.csv", "quote_probabilities.csv", "summary.json"):
        path = diagnostic_dir / name
        receipt = files.get(name, {})
        if not path.is_file() or file_hash(path) != receipt.get("sha256"):
            raise BenchmarkError(f"Diagnostic file receipt mismatch: {name}")
    summary = _json(diagnostic_dir / "summary.json")
    if not summary.get("baseline_reproduced") or summary.get("state") != "complete":
        raise BenchmarkError("Diagnostic summary does not prove a complete reproduced baseline")
    return manifest


def prepare_benchmark(
    model_dir: str, sweep_dir: str, diagnostic_dir: str, output_dir: str, *, local: bool,
    max_games: int = 200, max_player_games: int = 2000, max_outcome_rows: int = 10000,
    max_outcome_games: int = 200, max_training_rows: int = 100000, tune: bool = False,
    tuning_trials: int | None = None, reselect_features: bool = False,
) -> dict:
    """Filesystem-only preflight; performs no model load, DB connection, fit, or output creation."""
    from src.diagnostics.nba_error_stages import prepare_diagnostic

    if tuning_trials is not None and not tune:
        raise BenchmarkError("--tuning-trials requires --tune")
    if tune and tuning_trials is not None and not 1 <= tuning_trials <= 200:
        raise BenchmarkError("Bounds: tuning-trials 1..200")
    effective_trials = 100 if tune and tuning_trials is None else tuning_trials
    if not 1 <= max_training_rows <= 150000:
        raise BenchmarkError("Bounds: max-training-rows 1..150000")
    prepared = prepare_diagnostic(
        model_dir, sweep_dir, output_dir, local=local, max_games=max_games,
        max_player_games=max_player_games, max_outcome_rows=max_outcome_rows,
        max_outcome_games=max_outcome_games,
    )
    model = Path(model_dir).resolve(strict=True)
    diagnostic = Path(diagnostic_dir).resolve(strict=True)
    output = Path(output_dir).resolve()
    if output.exists() or Path(f"{output}_incomplete").exists():
        raise BenchmarkError("Benchmark output or incomplete output already exists")
    diag_manifest = _validate_diagnostic(diagnostic, prepared)
    run_config = _json(model / "run_config.json")
    artifact_manifest = _json(model / "artifact_manifest.json")
    hyperparams = _json(model / "best_hyperparams.json")
    train_seasons = run_config.get("train_seasons")
    calibration_season = run_config.get("calibration_season")
    if train_seasons != ["22023", "22024"] or calibration_season != "22025":
        raise BenchmarkError("Unexpected parent training/calibration season contract")
    if run_config.get("cal_end_date") is not None:
        raise BenchmarkError("This parent benchmark requires cal_end_date=null")
    chronology = run_config.get("execution", {}).get("chronology")
    if not isinstance(chronology, dict):
        raise BenchmarkError("Parent chronology receipt missing")
    features = _feature_union(run_config)
    candidates = _candidate_union(run_config)
    tolerance = artifact_manifest.get("training", {}).get("feature_tolerance")
    if reselect_features and (type(tolerance) not in (int, float) or not np.isfinite(tolerance) or tolerance < 0):
        raise BenchmarkError("Parent feature_tolerance is missing or invalid")
    reb_params = hyperparams.get("reb")
    if not isinstance(reb_params, dict):
        raise BenchmarkError("Parent REB hyperparameters missing")
    from src.models.quantile_trainer import QuantileModelConfig

    effective_config = QuantileModelConfig.from_dict({**reb_params, "reg_alpha": reb_params.get("reg_alpha", 0.0),
                                                       "reg_lambda": reb_params.get("reg_lambda", 1.0)})
    info = prepared["info"]
    info.update({
        "command": "nba-direct-reb-benchmark", "diagnostic_dir": str(diagnostic),
        "diagnostic_manifest_sha256": file_hash(diagnostic / "diagnostic_manifest.json"),
        "max_training_rows": max_training_rows, "train_seasons": train_seasons,
        "calibration_season": calibration_season, "cal_end_date": None,
        "evaluation_season": "22026", "feature_count": None if reselect_features else len(features),
        "feature_identity": None if reselect_features else _sha256_json(features), "target": "actual_reb",
        "inherited_feature_count": len(features), "inherited_feature_identity": _sha256_json(features),
        "training_min_actual_minutes": 10.0, "evaluation_actual_minutes_filter": None,
        "model_filename": MODEL_NAME, "architecture": "direct_total_reb_quantiles",
        "decision_grade": False, "full_execution": False,
        "inherited_initial_model_config": effective_config.to_dict(),
        "effective_model_config": None if tune else effective_config.to_dict(),
        "tuning": {"requested": tune, "effective": tune, "objective": "pinball" if tune else None,
                   "shared_parameters": bool(tune), "requested_trials": tuning_trials,
                   "effective_trials": effective_trials},
        "feature_selection": {"requested": reselect_features, "effective": reselect_features,
                              "candidate_count": len(candidates), "frozen_feature_count": len(features),
                              "final_selected_features": None if reselect_features else features,
                              "tolerance": float(tolerance) if reselect_features else None},
        "oracle_arms": [], "oracle_non_executable": False,
    })
    return {
        **prepared, "diagnostic_manifest": diag_manifest, "run_config": run_config,
        "features": features, "feature_candidates": candidates,
        "feature_tolerance": float(tolerance) if reselect_features else None,
        "model_config": effective_config, "info": info,
    }


def load_training_populations(engine, prepared: dict) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Load, cap, validate, and bind train/cal populations to the parent chronology."""
    from src.models.feature_store import FeatureStore
    from src.models.nba_training_chronology import chronology_proof, validate_populations

    info = prepared["info"]
    store = FeatureStore(engine)
    train = store.get_training_dataset(info["train_seasons"])
    calibration = store.get_training_dataset([info["calibration_season"]])
    if len(train) > info["max_training_rows"] or len(calibration) > info["max_training_rows"]:
        raise BenchmarkError("Training/calibration row cap exceeded; no partial population accepted")
    train, calibration = validate_populations(
        train, calibration, info["train_seasons"], info["calibration_season"],
    )
    proof = chronology_proof(train, calibration)
    parent = prepared["run_config"]["execution"]["chronology"]
    for role in ("training", "calibration"):
        if proof[role] != parent.get(role):
            raise BenchmarkError(f"Loaded {role} population differs from parent chronology receipt")
    return train, calibration, proof


def _require_columns(frame: pd.DataFrame, names: list[str], label: str) -> None:
    missing = sorted(set(names) - set(frame.columns))
    if missing:
        raise BenchmarkError(f"{label} missing required columns: {missing}")


def _prediction_frame(suite, frame: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    _require_columns(frame, features, "Prediction population")
    values = suite.predict_quantiles(frame.loc[:, features])
    if list(values.columns) != list(QCOLS) or len(values) != len(frame):
        raise BenchmarkError("Direct quantile prediction contract mismatch")
    array = values.to_numpy(dtype=float)
    if not np.isfinite(array).all() or (np.diff(array, axis=1) < -1e-8).any():
        raise BenchmarkError("Direct quantiles are non-finite or non-monotone")
    return values


def fit_direct_model(prepared: dict, train: pd.DataFrame, calibration: pd.DataFrame):
    """Fit actual_reb directly, clear this suite's offsets, and score calibration only."""
    from src.models.nba_training_chronology import population_proof, temporal_split
    from src.models.quantile_trainer import QuantileModelSuite, prepare_quantile_features

    features = prepared["features"]
    tuning_requested = bool(prepared.get("info", {}).get("tuning", {}).get("effective", False))
    reselect = bool(prepared.get("info", {}).get("feature_selection", {}).get("effective", False))
    candidates = prepared.get("feature_candidates", features) if reselect else features
    _require_columns(train, [*candidates, "actual_reb", "actual_minutes", "game_date"], "Training population")
    _require_columns(calibration, [*candidates, "actual_reb", "game_date"], "Calibration population")
    target = cast(pd.Series, pd.to_numeric(train["actual_reb"], errors="coerce"))
    minutes = cast(pd.Series, pd.to_numeric(train["actual_minutes"], errors="coerce"))
    mask = np.isfinite(target) & (target >= 0) & np.isfinite(minutes) & (minutes >= 10)
    fit = train.loc[mask].reset_index(drop=True)
    if fit.empty:
        raise BenchmarkError("Direct REB fit cohort is empty")
    fit_target = cast(pd.Series, pd.to_numeric(fit["actual_reb"], errors="raise"))
    fit_dates = fit["game_date"].reset_index(drop=True)
    matrix = prepare_quantile_features(fit, candidates)
    per_quantile: dict[float, list[str]] = {q: list(features) for q in QUANTILES}
    selection_boundary = None
    if reselect:
        from src.processing.feature_selection import ImprovedFeatureSelector

        split_values = temporal_split(
            matrix, fit_target, dates=fit_dates, test_size=prepared["model_config"].val_fraction,
            shuffle=False, random_state=prepared["model_config"].random_state,
        )
        inner_X = cast(pd.DataFrame, split_values[0])
        outer_X = cast(pd.DataFrame, split_values[1])
        inner_y = cast(pd.Series, split_values[2])
        inner_dates = fit_dates.iloc[inner_X.index].reset_index(drop=True)
        selector_frame = inner_X.reset_index(drop=True)
        selector_frame["actual_reb"] = inner_y.reset_index(drop=True)
        selector_frame["game_date"] = inner_dates
        selector = ImprovedFeatureSelector(
            n_splits=3, quantiles=list(QUANTILES), tolerance=prepared["feature_tolerance"],
        )
        selected = selector.select_features_per_quantile(
            selector_frame, "actual_reb", list(candidates), model_name="Direct REB",
            date_column="game_date", allow_missing_features=True,
        )
        per_quantile = {q: list(selected.get(q, [])) for q in QUANTILES}
        for q, names in per_quantile.items():
            if not names or len(names) != len(set(names)) or not set(names) <= set(candidates):
                raise BenchmarkError(f"Invalid selected features for quantile {q}")
        selection_boundary = {
            "inner_training_rows": len(inner_X), "outer_validation_rows": len(outer_X),
            "inner_training_end": str(pd.to_datetime(inner_dates).max().date()),
            "outer_validation_start": str(pd.to_datetime(fit_dates.iloc[outer_X.index]).min().date()),
        }
    selected_union = sorted({name for names in per_quantile.values() for name in names})
    matrix = matrix.loc[:, selected_union]
    final_config = copy.deepcopy(prepared["model_config"])
    tuning_evidence: dict[str, Any] = {
        "requested": tuning_requested, "effective": False, "objective": None,
        "shared_parameters": False, "requested_trials": None, "effective_trials": None, "actual_trials": 0,
        "complete_trials": 0, "pruned_trials": 0, "best_trial": None,
        "best_objective_value": None, "winning_coverage_diagnostic": None,
        "best_params": None, "trials": [], "final_effective_config": final_config.to_dict(),
    }
    if tuning_requested:
        from src.models.hyperparameter_tuner import QuantileHyperparameterTuner

        budget = prepared["info"]["tuning"]["effective_trials"]
        logger.info("Starting direct REB pinball tuning: %s shared trials across five quantile heads", budget)
        tuner = QuantileHyperparameterTuner(
            n_trials=budget, per_quantile=False, objective="pinball",
            val_fraction=prepared["model_config"].val_fraction,
            random_state=prepared["model_config"].random_state, storage=None, timeout=None,
        )
        final_config = tuner.tune(
            matrix, fit_target, quantiles=QUANTILES,
            feature_names=cast(Any, per_quantile), dates=fit_dates,
        )
        if tuner.study is None or tuner.result is None:
            raise BenchmarkError("Tuning completed without study evidence")
        trials = [{"number": trial.number, "state": trial.state.name,
                   "value": float(trial.value) if trial.value is not None else None,
                   "params": trial.params} for trial in tuner.study.trials]
        tuning_evidence = {
            "requested": True, "effective": True, "objective": "pinball",
            "shared_parameters": True,
            "requested_trials": prepared["info"]["tuning"].get("requested_trials"),
            "effective_trials": budget, "actual_trials": len(trials),
            "complete_trials": sum(item["state"] == "COMPLETE" for item in trials),
            "pruned_trials": sum(item["state"] == "PRUNED" for item in trials),
            "best_trial": tuner.study.best_trial.number,
            "best_objective_value": float(tuner.study.best_value),
            "winning_coverage_diagnostic": float(tuner.result.best_calibration_gap),
            "best_params": tuner.result.best_params, "trials": trials,
            "final_effective_config": final_config.to_dict(),
        }
        logger.info("Direct REB pinball tuning complete; fitting final suite with winning shared parameters")
    suite = QuantileModelSuite(final_config)
    fit_metrics = suite.train(
        matrix, fit_target, per_quantile, dates=fit_dates,
    )
    suite.calibration_offsets = {q: 0.0 for q in QUANTILES}
    cal_target = cast(pd.Series, pd.to_numeric(calibration["actual_reb"], errors="coerce"))
    cal_mask = np.isfinite(cal_target) & (cal_target >= 0)
    cal = calibration.loc[cal_mask].reset_index(drop=True)
    if cal.empty:
        raise BenchmarkError("Direct REB calibration scoring cohort is empty")
    cal_preds = _prediction_frame(suite, cal, suite.all_feature_names)
    score_frame = pd.DataFrame({"actual": cal["actual_reb"].to_numpy()})
    for q, qcol in zip(QUANTILES, QCOLS, strict=True):
        score_frame[f"pred_q{int(q * 100)}"] = cal_preds[qcol].to_numpy()
    receipt = {
        "fit_metrics": {str(q): {k: float(v) for k, v in values.items()} for q, values in fit_metrics.items()},
        "calibration_scores": _scores(score_frame),
        "direct_fit_subset": {
            **population_proof(fit), "parent_training_rows": len(train), "excluded_rows": int((~mask).sum()),
            "rule": "finite nonnegative actual_reb and actual_minutes >= 10",
        },
        "effective_model_config": suite.config.to_dict(),
        "tuning": tuning_evidence,
        "feature_selection": {
            "requested": reselect, "effective": reselect,
            "policy": ("selector per-quantile lists from inner-training partition" if reselect else
                       "sorted union of parent minutes and REB-rate selected features across all quantiles"),
            "candidate_pool": list(candidates), "candidate_count": len(candidates),
            "selected_per_quantile": {str(q): per_quantile[q] for q in QUANTILES},
            "selected_union": suite.all_feature_names, "selected_union_count": len(suite.all_feature_names),
            "selected_union_identity": _sha256_json(suite.all_feature_names),
            "selector": ({"implementation": "ImprovedFeatureSelector", "n_splits": 3,
                          "tolerance": prepared["feature_tolerance"]} if reselect else None),
            "selection_boundary": selection_boundary,
        },
        "calibration_offsets_forced_zero": {str(q): suite.calibration_offsets[q] for q in QUANTILES},
    }
    return suite, receipt


def _reference_tables(prepared: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    root = Path(prepared["info"]["diagnostic_dir"])
    totals = pd.read_csv(root / "totals.csv", dtype={"player_id": str, "game_id": str}, low_memory=False)
    quotes = pd.read_csv(root / "quote_probabilities.csv", dtype={"player_id": str, "game_id": str}, low_memory=False)
    totals = cast(pd.DataFrame, totals[(totals["arm"] == "baseline") & (totals["stat"] == "reb")]).copy()
    quotes = cast(pd.DataFrame, quotes[(quotes["arm"] == "baseline") & (quotes["stat"] == "reb")]).copy()
    if totals.empty or quotes.empty or totals.duplicated(KEYS).any():
        raise BenchmarkError("Reference REB totals/quotes are empty or totals have duplicate keys")
    return totals, quotes


def _american_probability(odds: float) -> float:
    return 100 / (odds + 100) if odds > 0 else -odds / (-odds + 100)


def _current_reference(shared) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    total_rows, quote_rows, feature_rows = [], [], []
    market_to_stat = {"player_rebounds": "reb"}
    for day, record in shared.evidence["dates"].items():
        features = record["eligible_features"].copy()
        if features.empty:
            continue
        features["player_id"] = features["player_id"].map(str)
        features["game_id"] = features["game_id"].map(str)
        features["game_date"] = day
        feature_rows.append(features)
        predictions = record["predictions"]
        if not predictions.empty:
            reb = predictions[predictions["stat"] == "reb"].copy()
            reb["player_id"] = reb["player_id"].map(str)
            reb["game_id"] = reb["game_id"].map(str)
            total_rows.append(reb)
        quotes = record["quotes"].copy()
        if quotes.empty:
            continue
        quotes["stat"] = quotes["market_key"].map(market_to_stat)
        quotes = quotes[quotes["stat"] == "reb"]
        feature_lookup = {(r.player_id, r.game_id): i for i, r in enumerate(features[KEYS].itertuples(index=False))}
        for quote in quotes.to_dict("records"):
            key = (str(quote["player_id"]), str(quote["game_id"]))
            if key not in feature_lookup:
                continue
            original_player = features.iloc[feature_lookup[key]]["player_id"]
            original_game = features.iloc[feature_lookup[key]]["game_id"]
            sample_key = next((k for k in record["samples"] if str(k[0]) == original_player and str(k[1]) == original_game and k[2] == "reb"), None)
            if sample_key is None:
                raise BenchmarkError("Current canonical REB sample missing")
            line = float(quote["line"])
            po, pu = _american_probability(float(quote["over_odds"])), _american_probability(float(quote["under_odds"]))
            quote_rows.append({
                "player_id": key[0], "game_id": key[1], "game_date": day, "stat": "reb",
                "bookmaker": quote["bookmaker"], "line": line,
                "over_prob": float((np.asarray(record["samples"][sample_key]) > line).mean()),
                "market_over": po / (po + pu),
            })
    if not total_rows or not feature_rows or not quote_rows:
        raise BenchmarkError("Canonical acquisition produced no populated REB reference")
    return pd.concat(total_rows, ignore_index=True), pd.DataFrame(quote_rows), pd.concat(feature_rows, ignore_index=True)


def _assert_reference(saved_totals: pd.DataFrame, saved_quotes: pd.DataFrame, current_totals: pd.DataFrame,
                      current_quotes: pd.DataFrame, actuals: pd.DataFrame) -> None:
    actual = cast(pd.DataFrame, actuals[actuals["stat"] == "reb"]).copy()
    actual["player_id"] = actual["player_id"].map(str)
    actual["game_id"] = actual["game_id"].map(str)
    if actual.duplicated(KEYS).any():
        raise BenchmarkError("Current canonical REB outcomes have duplicate keys")
    outcome_values = cast(pd.DataFrame, actual[KEYS + ["actual_value"]])
    current_totals = current_totals.merge(outcome_values, on=KEYS, how="left")
    columns = KEYS + ["pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90"]
    left = cast(pd.DataFrame, saved_totals[columns + ["actual"]]).sort_values(KEYS).reset_index(drop=True)
    right = cast(pd.DataFrame, current_totals[columns + ["actual_value"]]).rename(columns={"actual_value": "actual"}).sort_values(KEYS).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=False, rtol=1e-8, atol=1e-8)
    except AssertionError:
        raise BenchmarkError("Regenerated control REB totals differ from saved diagnostic baseline") from None
    current_quotes = current_quotes.merge(outcome_values, on=KEYS, how="left")
    qcols = KEYS + ["game_date", "bookmaker", "line", "over_prob", "market_over"]
    order = KEYS + ["game_date", "bookmaker", "line", "over_prob", "market_over"]
    leftq = cast(pd.DataFrame, saved_quotes[qcols + ["actual"]]).copy()
    rightq = cast(pd.DataFrame, current_quotes[qcols + ["actual_value"]]).rename(columns={"actual_value": "actual"}).copy()
    for frame in (leftq, rightq):
        frame["game_date"] = frame["game_date"].map(str)
    leftq = leftq.sort_values(order, kind="stable").reset_index(drop=True)
    rightq = rightq.sort_values(order, kind="stable").reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(leftq, rightq, check_dtype=False, check_exact=False, rtol=1e-8, atol=1e-8)
    except AssertionError:
        raise BenchmarkError("Regenerated control REB quotes differ from saved diagnostic baseline") from None


def evaluate_direct(prepared: dict, suite, shared) -> tuple[dict, dict[str, pd.DataFrame]]:
    """Evaluate direct totals on the exact regenerated control cohort and quote occurrences."""
    from src.models.monte_carlo import MonteCarloPredictor

    saved_totals, saved_quotes = _reference_tables(prepared)
    current_totals, current_quotes, features = _current_reference(shared)
    _assert_reference(saved_totals, saved_quotes, current_totals, current_quotes, shared.evidence["actuals"])
    features = features.sort_values(["game_date", "game_id", "player_id"], kind="stable").reset_index(drop=True)
    if features.duplicated(KEYS).any():
        raise BenchmarkError("Canonical eligible REB features have duplicate player-game keys")
    saved_keys = cast(pd.DataFrame, saved_totals[KEYS]).sort_values(KEYS).reset_index(drop=True)
    feature_keys = cast(pd.DataFrame, features[KEYS]).sort_values(KEYS).reset_index(drop=True)
    if not saved_keys.equals(feature_keys):
        raise BenchmarkError("Direct benchmark eligibility differs from complete control reference; no intersection accepted")
    prediction_features = (suite.all_feature_names if prepared.get("info", {}).get("feature_selection", {}).get("effective")
                           else prepared["features"])
    native = _prediction_frame(suite, features, prediction_features)
    sampler = MonteCarloPredictor(None, n_samples=prepared["config"].n_samples, random_state=prepared["config"].seed)
    rows, sample_map = [], {}
    for index, identity in features.iterrows():
        samples = np.maximum(sampler._inverse_transform_sample(np.asarray(QUANTILES), native.iloc[index].to_numpy(float)), 0)
        key = (str(identity["player_id"]), str(identity["game_id"]))
        sample_map[key] = samples
        sampled = np.quantile(samples, QUANTILES)
        rows.append({
            "player_id": key[0], "game_id": key[1], "game_date": identity["game_date"],
            **{f"native_q{int(q * 100)}": float(native.iloc[index][qcol]) for q, qcol in zip(QUANTILES, QCOLS, strict=True)},
            **{f"pred_q{int(q * 100)}": float(value) for q, value in zip(QUANTILES, sampled, strict=True)},
        })
    direct = pd.DataFrame(rows).merge(saved_totals[KEYS + ["actual", *PCOLS]], on=KEYS, how="left", suffixes=("", "_control"), validate="one_to_one")
    direct["score_eligible"] = np.isfinite(cast(pd.Series, pd.to_numeric(direct["actual"], errors="coerce")))
    scored_totals = direct[direct["score_eligible"]].copy()
    if scored_totals.empty:
        raise BenchmarkError("No outcome-complete common reference rows are available for scoring")
    quote_rows = []
    for occurrence, quote in enumerate(saved_quotes.to_dict("records")):
        key = (str(quote["player_id"]), str(quote["game_id"]))
        if key not in sample_map:
            raise BenchmarkError("Reference quote lacks an exact direct sample key")
        quote_rows.append({
            "occurrence": occurrence, "player_id": key[0], "game_id": key[1],
            "game_date": quote["game_date"], "bookmaker": quote["bookmaker"], "line": float(quote["line"]),
            "actual": float(quote["actual"]), "candidate_over_prob": float((sample_map[key] > float(quote["line"])).mean()),
            "control_over_prob": float(quote["over_prob"]), "market_over": float(quote["market_over"]),
        })
    matched_quotes = pd.DataFrame(quote_rows)
    candidate_score = cast(pd.DataFrame, scored_totals[["actual", *PCOLS]]).copy()
    control_score = pd.DataFrame({"actual": scored_totals["actual"]})
    for name in PCOLS:
        control_score[name] = scored_totals[f"{name}_control"]
    candidate_quotes = matched_quotes.rename(columns={"candidate_over_prob": "over_prob"})
    control_quotes = matched_quotes.rename(columns={"control_over_prob": "over_prob"})
    candidate_metrics, control_metrics = _scores(candidate_score), _scores(control_score)
    candidate_probability, control_probability = _scores(candidate_quotes), _scores(control_quotes)
    summary = {
        "state": "complete", "decision_grade": False,
        "reference_total_rows": len(direct), "matched_total_rows": len(scored_totals),
        "matched_quote_occurrences": len(matched_quotes),
        "candidate": {"totals": candidate_metrics, "quotes": candidate_probability},
        "control": {"totals": control_metrics, "quotes": control_probability},
        "candidate_minus_control": {
            "mean_pinball": candidate_metrics["mean_pinball"] - control_metrics["mean_pinball"],
            "median_mae": candidate_metrics["mae"] - control_metrics["mae"],
            "median_rmse": candidate_metrics["rmse"] - control_metrics["rmse"],
            "median_bias": candidate_metrics["bias"] - control_metrics["bias"],
            "brier": candidate_probability["brier"] - control_probability["brier"],
        },
        "limitations": [
            "Equal-information union is not identical per-head dimensionality.",
            "Calibration season was evaluation-only and was not used for fitting or model selection.",
            "Evaluation eligibility was not filtered using realized minutes.",
            "Predictive score changes do not establish profitability, ROI, promotion, or architecture superiority.",
        ],
    }
    direct = direct.rename(columns={
        **{name: f"candidate_sampled_{name.removeprefix('pred_')}" for name in PCOLS},
        **{f"{name}_control": f"control_sampled_{name.removeprefix('pred_')}" for name in PCOLS},
    })
    return summary, {"matched_totals.csv": direct, "matched_quotes.csv": matched_quotes}


def render_report(summary: dict) -> str:
    delta = summary["candidate_minus_control"]
    return "\n".join([
        "# NBA direct-total REB benchmark", "",
        "Standalone diagnostic benchmark only; no betting simulation or promotion decision.", "",
        f"- Matched total rows: {summary['matched_total_rows']}",
        f"- Matched quote occurrences: {summary['matched_quote_occurrences']}",
        f"- Candidate minus control mean pinball: {delta['mean_pinball']:.6f}",
        f"- Candidate minus control median MAE: {delta['median_mae']:.6f}",
        f"- Candidate minus control Brier: {delta['brier']:.6f}", "",
        "Lower is better for pinball, MAE, RMSE, and Brier. These discovery-window scores are not ROI evidence.", "",
        "## Limitations", *[f"- {item}" for item in summary["limitations"]], "",
    ])


def source_hashes(repo_root: Path) -> dict[str, str]:
    hashes = {}
    for name in SOURCE_FILES:
        path = repo_root / name
        if path.is_file():
            hashes[name] = file_hash(path)
    return hashes


def manifest_inventory(root: Path, *, exclude: set[str] | None = None) -> dict[str, dict[str, Any]]:
    excluded = exclude or set()
    return {path.name: {"sha256": file_hash(path), "bytes": path.stat().st_size}
            for path in sorted(root.iterdir()) if path.is_file() and path.name not in excluded}
