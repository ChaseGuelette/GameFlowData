"""Saved-evidence prepare, fit, and evaluate workflow for learned market blending."""

from __future__ import annotations

import json
import math
import os
import shutil
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.backtesting.backtest_harness import BacktestHarness
from src.models.nba_artifacts import canonical_sha256
from src.models.nba_learned_market_blend import (
    CLIP_MAX,
    CLIP_MIN,
    MODE,
    SUPPORTED_STATS,
    LearnedMarketBlendError,
    content_identity,
    finalize_artifact,
    fit_per_stat_weights,
    learned_market_probability_for_stat,
    load_artifact,
    probability_scores,
    sha256_file,
)
from src.models.nba_learned_market_blend import (
    SCHEMA_VERSION as MODEL_SCHEMA_VERSION,
)

PREPARATION_SCHEMA_VERSION = 1
EVALUATION_SCHEMA_VERSION = 1
QUOTE_KEY = ["player_id", "game_id", "game_date", "stat", "bookmaker", "line"]
PLAYER_GAME_STAT_KEY = ["player_id", "game_id", "game_date", "stat"]
TREATMENTS = ("raw", "usual_bl_reference", "market_only", MODE)


class LearnedMarketBlendWorkflowError(RuntimeError):
    """Raised when saved evidence cannot support the requested workflow."""


@dataclass(frozen=True)
class PrepareRequest:
    diagnostic_dir: Path
    sweep_dir: Path
    usual_bl_cell: str
    stats: tuple[str, ...]
    output_dir: Path


@dataclass(frozen=True)
class FitRequest:
    input_dir: Path
    fit_start: str
    fit_end: str
    shrinkage_strength: float
    output_dir: Path


@dataclass(frozen=True)
class EvaluateRequest:
    input_dir: Path
    blend_artifact: Path
    start: str
    end: str
    edge_thresholds: tuple[float, ...]
    starting_bankroll: float
    flat_bet_size: float
    output_dir: Path
    allowed_bets: tuple[tuple[str, str], ...] | None = None


@dataclass(frozen=True)
class PreparedBundle:
    root: Path
    manifest: dict[str, Any]
    opportunities: pd.DataFrame


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearnedMarketBlendWorkflowError(f"Cannot read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise LearnedMarketBlendWorkflowError(f"Expected JSON object: {path}")
    return value


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path,
            float_precision="round_trip",
            dtype={
                "player_id": "string",
                "game_id": "string",
                "game_date": "string",
                "stat": "string",
                "bookmaker": "string",
            },
        )
    except Exception as exc:
        raise LearnedMarketBlendWorkflowError(f"Cannot read CSV: {path}") from exc


def _numbers(series: Any, name: str) -> np.ndarray:
    if not isinstance(series, pd.Series):
        raise LearnedMarketBlendWorkflowError(f"{name} must be one unambiguous column")
    numeric = cast(pd.Series, pd.to_numeric(series, errors="coerce"))
    values = np.asarray(numeric, dtype=float)
    if not np.isfinite(values).all():
        raise LearnedMarketBlendWorkflowError(f"{name} contains missing or nonfinite values")
    return values


def _bools(series: Any) -> pd.Series:
    if not isinstance(series, pd.Series):
        raise LearnedMarketBlendWorkflowError("Boolean evidence must be one unambiguous column")
    if pd.api.types.is_bool_dtype(series):
        return series.astype(bool)
    mapped = series.astype(str).str.strip().str.lower().map({"true": True, "false": False})
    if mapped.isna().any():
        raise LearnedMarketBlendWorkflowError(f"Invalid boolean evidence in {series.name}")
    return mapped.astype(bool)


def _validate_stats(stats: Sequence[str]) -> tuple[str, ...]:
    result = tuple(stats)
    if not result or len(result) != len(set(result)) or any(stat not in SUPPORTED_STATS for stat in result):
        raise LearnedMarketBlendWorkflowError(f"Stats must be unique choices from {SUPPORTED_STATS}")
    return result


def _parse_date(value: object, name: str) -> date:
    try:
        if not isinstance(value, str):
            raise TypeError("Date is not a string")
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError("Noncanonical date")
        return parsed
    except (TypeError, ValueError) as exc:
        raise LearnedMarketBlendWorkflowError(f"{name} must be YYYY-MM-DD") from exc


def _validate_range(start: str, end: str) -> tuple[date, date]:
    first = _parse_date(start, "start")
    last = _parse_date(end, "end")
    if first > last:
        raise LearnedMarketBlendWorkflowError("Date range is reversed")
    return first, last


def _validate_output(output: Path, inputs: Iterable[Path]) -> Path:
    final = output.resolve(strict=False)
    if final.exists():
        raise LearnedMarketBlendWorkflowError(f"Output path already exists: {final}")
    for source in inputs:
        root = source.resolve(strict=True)
        if final == root or root in final.parents or final in root.parents:
            raise LearnedMarketBlendWorkflowError("Output must be isolated from all inputs")
    return final


def _receipt(path: Path, evidence: Mapping[str, Any] | None, label: str) -> dict[str, Any]:
    if not isinstance(evidence, Mapping) or not path.is_file():
        raise LearnedMarketBlendWorkflowError(f"Missing receipt/file for {label}")
    digest = sha256_file(path)
    size = path.stat().st_size
    if digest != evidence.get("sha256") or size != evidence.get("bytes", size):
        raise LearnedMarketBlendWorkflowError(f"Receipt mismatch for {label}")
    result: dict[str, Any] = {"sha256": digest, "bytes": size}
    if path.suffix == ".csv":
        frame = _read_csv(path)
        if evidence.get("rows") is not None and len(frame) != evidence.get("rows"):
            raise LearnedMarketBlendWorkflowError(f"Row receipt mismatch for {label}")
        if evidence.get("columns") is not None and list(frame.columns) != evidence.get("columns"):
            raise LearnedMarketBlendWorkflowError(f"Column receipt mismatch for {label}")
        result.update({"rows": len(frame), "columns": list(frame.columns)})
    return result


def _file_evidence(path: Path, *, rows: int | None = None, columns: list[str] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"sha256": sha256_file(path), "bytes": path.stat().st_size}
    if rows is not None:
        result["rows"] = rows
    if columns is not None:
        result["columns"] = columns
    return result


def _input_receipts(paths: Iterable[Path]) -> dict[str, Any]:
    return {str(path.resolve(strict=True)): _file_evidence(path) for path in paths}


def _recheck_inputs(receipts: Mapping[str, Any]) -> None:
    for path, evidence in receipts.items():
        if _file_evidence(Path(path)) != evidence:
            raise LearnedMarketBlendWorkflowError(f"Consumed input changed: {path}")


def _atomic_root(final: Path) -> Path:
    final.parent.mkdir(parents=True, exist_ok=True)
    root = final.parent / f".{final.name}.incomplete-{uuid.uuid4().hex}"
    root.mkdir()
    return root


def _publish(root: Path, final: Path) -> Path:
    if final.exists():
        raise LearnedMarketBlendWorkflowError(f"Output collision during publication: {final}")
    os.rename(root, final)
    return final


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _select_cell(manifest: dict[str, Any], cell_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    plan = manifest.get("plan")
    completed = manifest.get("completed_cells")
    if manifest.get("state") != "complete" or not isinstance(plan, dict) or not isinstance(completed, list):
        raise LearnedMarketBlendWorkflowError("Reference sweep is not complete")
    cells = plan.get("cells")
    if not isinstance(cells, list):
        raise LearnedMarketBlendWorkflowError("Reference sweep cell plan is missing")
    matches = [cell for cell in cells if isinstance(cell, dict) and cell.get("cell_id") == cell_id]
    receipts = [item for item in completed if isinstance(item, dict) and item.get("cell_id") == cell_id]
    if len(matches) != 1 or len(receipts) != 1:
        raise LearnedMarketBlendWorkflowError(f"Cell is not uniquely planned/completed: {cell_id}")
    return matches[0], receipts[0]


def _artifact_chronology(plan_artifact: Mapping[str, Any], model_identity: str) -> dict[str, Any]:
    if plan_artifact.get("identity") != model_identity or plan_artifact.get("identity_verified") is not True:
        raise LearnedMarketBlendWorkflowError("Sweep artifact identity is not verified or differs from diagnostic")
    model_dir = Path(str(plan_artifact.get("path", ""))).resolve(strict=True)
    manifest_path = model_dir / "artifact_manifest.json"
    artifact_manifest = _json(manifest_path)
    manifest_content = {key: value for key, value in artifact_manifest.items() if key != "manifest_identity"}
    if (
        artifact_manifest.get("manifest_identity") != model_identity
        or canonical_sha256(manifest_content) != model_identity
        or artifact_manifest.get("completion", {}).get("complete") is not True
    ):
        raise LearnedMarketBlendWorkflowError("Source model manifest identity/completion differs")
    chronology = (
        artifact_manifest.get("data_provenance", {})
        .get("training", {})
        .get("execution", {})
        .get("chronology", {})
    )
    calibration = chronology.get("calibration") if isinstance(chronology, dict) else None
    training = chronology.get("training") if isinstance(chronology, dict) else None
    if not isinstance(calibration, dict) or not isinstance(training, dict):
        raise LearnedMarketBlendWorkflowError("Source model chronology is unavailable")
    cutoff = calibration.get("end")
    if not isinstance(cutoff, str):
        raise LearnedMarketBlendWorkflowError("Source model calibration cutoff is unavailable")
    training_end = _parse_date(training.get("end"), "model training end")
    cutoff = max(training_end, _parse_date(cutoff, "model calibration end")).isoformat()
    return {
        "model_dir": str(model_dir),
        "artifact_manifest_sha256": sha256_file(manifest_path),
        "training": training,
        "calibration": calibration,
        "fit_population_cutoff": cutoff,
        "source_verified": chronology.get("source_verified"),
        "strict_fit_before_calibration": chronology.get("strict_fit_before_calibration"),
        "historical_policy_verified": plan_artifact.get("historical_policy_verified"),
        "model_load_verified": plan_artifact.get("model_load_verified"),
    }


def _verify_prepare_sources(request: PrepareRequest) -> dict[str, Any]:
    stats = _validate_stats(request.stats)
    diagnostic = request.diagnostic_dir.resolve(strict=True)
    sweep = request.sweep_dir.resolve(strict=True)
    final = _validate_output(request.output_dir, [diagnostic, sweep])

    diagnostic_manifest_path = diagnostic / "diagnostic_manifest.json"
    diagnostic_manifest = _json(diagnostic_manifest_path)
    required_diagnostic = {
        "state": "complete",
        "status": "report_complete",
        "baseline_reproduced": True,
    }
    if any(diagnostic_manifest.get(key) != value for key, value in required_diagnostic.items()):
        raise LearnedMarketBlendWorkflowError("Diagnostic bundle is not the completed reproduced source")
    model_identity = diagnostic_manifest.get("model_identity")
    if not isinstance(model_identity, str) or len(model_identity) != 64 or any(c not in "0123456789abcdef" for c in model_identity):
        raise LearnedMarketBlendWorkflowError("Diagnostic model identity is missing or malformed")
    files = diagnostic_manifest.get("files")
    required_files = {"components.csv", "totals.csv", "quote_probabilities.csv", "summary.json", "report.md"}
    if not isinstance(files, dict) or set(files) != required_files:
        raise LearnedMarketBlendWorkflowError("Diagnostic receipt inventory is incomplete or unexpected")
    diagnostic_receipts = {
        name: _receipt(diagnostic / name, files.get(name), f"diagnostic/{name}")
        for name in sorted(required_files)
    }

    sweep_manifest_path = sweep / "sweep_manifest.json"
    sweep_hash = sha256_file(sweep_manifest_path)
    if sweep_hash != diagnostic_manifest.get("reference_manifest_sha256"):
        raise LearnedMarketBlendWorkflowError("Diagnostic/reference sweep manifest hash differs")
    bound_sweep = Path(str(diagnostic_manifest.get("sweep_dir", ""))).resolve(strict=True)
    if bound_sweep != sweep:
        raise LearnedMarketBlendWorkflowError("Diagnostic is bound to a different sweep directory")
    sweep_manifest = _json(sweep_manifest_path)
    plan = sweep_manifest.get("plan")
    if sweep_manifest.get("state") != "complete" or not isinstance(plan, dict):
        raise LearnedMarketBlendWorkflowError("Reference sweep is incomplete")
    plan_artifact = plan.get("artifact")
    fixed = plan.get("fixed_config")
    predictor = plan.get("predictor_settings")
    if not isinstance(plan_artifact, dict) or not isinstance(fixed, dict) or not isinstance(predictor, dict):
        raise LearnedMarketBlendWorkflowError("Reference sweep plan contracts are incomplete")
    chronology = _artifact_chronology(plan_artifact, model_identity)
    final = _validate_output(request.output_dir, [diagnostic, sweep, Path(chronology["model_dir"])])
    if predictor.get("combined_calibration_offsets") is not None:
        raise LearnedMarketBlendWorkflowError("Global combined calibration offsets are forbidden")
    if any(diagnostic_manifest.get(key) != fixed.get(key) for key in ("seed", "n_samples", "start", "end")):
        raise LearnedMarketBlendWorkflowError("Diagnostic and reference forecast settings/window differ")

    usual_cell, usual_receipt = _select_cell(sweep_manifest, request.usual_bl_cell)
    if usual_cell.get("raw_control") is not False:
        raise LearnedMarketBlendWorkflowError("--usual-bl-cell must identify an ordinary BL cell")
    usual_tuple = {
        "tau": usual_cell.get("tau"),
        "z_max": usual_cell.get("z_max"),
        "max_weight": usual_cell.get("max_weight"),
    }
    for name, value in usual_tuple.items():
        if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise LearnedMarketBlendWorkflowError(f"Invalid usual-BL reference {name}")
    usual_tuple = cast(dict[str, float], usual_tuple)
    if usual_tuple["z_max"] <= 0 or usual_tuple["max_weight"] > 1:
        raise LearnedMarketBlendWorkflowError("Usual-BL reference z_max/maximum weight is invalid")
    raw_matches = [
        cell
        for cell in cast(list[dict[str, Any]], plan.get("cells", []))
        if cell.get("raw_control") is True
        and cell.get("edge_threshold") is not None
        and usual_cell.get("edge_threshold") is not None
        and float(cell["edge_threshold"]) == float(usual_cell["edge_threshold"])
    ]
    if len(raw_matches) != 1:
        raise LearnedMarketBlendWorkflowError("Matching raw reference cell is not unique")
    raw_cell, raw_receipt = _select_cell(sweep_manifest, str(raw_matches[0]["cell_id"]))

    loaded: dict[str, pd.DataFrame] = {}
    cell_receipts: dict[str, Any] = {}
    for label, cell, receipt in (
        ("raw", raw_cell, raw_receipt),
        ("usual_bl_reference", usual_cell, usual_receipt),
    ):
        cell_dir = sweep / "cells" / str(cell["cell_id"])
        receipt_files = receipt.get("files")
        if not isinstance(receipt_files, dict):
            raise LearnedMarketBlendWorkflowError(f"Missing completed-cell receipts for {label}")
        edge_path = cell_dir / "all_bookmaker_edges.csv"
        config_path = cell_dir / "config.json"
        edge_evidence = _receipt(edge_path, receipt_files.get("all_bookmaker_edges.csv"), f"{label}/edges")
        config_evidence = _receipt(config_path, receipt_files.get("config.json"), f"{label}/config")
        config = _json(config_path)
        if config.get("cell_id") != cell.get("cell_id") or config.get("input_identity") != sweep_manifest.get("input_identity"):
            raise LearnedMarketBlendWorkflowError(f"{label} config identity differs from sweep")
        if label == "usual_bl_reference":
            bl_config = config.get("bl_config")
            if not isinstance(bl_config, dict) or any(
                bl_config.get(name) != value for name, value in usual_tuple.items()
            ) or bl_config.get("min_prob") != CLIP_MIN or bl_config.get("max_prob") != CLIP_MAX:
                raise LearnedMarketBlendWorkflowError("Usual BL config tuple/clipping differs from planned cell")
        if label == "raw" and config.get("bl_config") is not None:
            raise LearnedMarketBlendWorkflowError("Raw reference unexpectedly has BL config")
        loaded[label] = _read_csv(edge_path)
        cell_receipts[label] = {
            "cell_id": cell["cell_id"],
            "cell": cell,
            "edges": edge_evidence,
            "config": config_evidence,
        }

    settings = {
        "model_identity": model_identity,
        "canonical_input_identity": diagnostic_manifest.get("canonical_input_identity"),
        "reference_input_identity": sweep_manifest.get("input_identity"),
        "predictor_settings": predictor,
        "fixed_config": {
            key: fixed.get(key)
            for key in (
                "start",
                "end",
                "seed",
                "n_samples",
                "bookmakers",
                "stats",
                "market_stats",
                "min_minutes_avg",
            )
        },
        "source_policy": plan.get("source_policy"),
        "historical_readiness": sweep_manifest.get("historical_readiness"),
        "model_chronology": chronology,
    }
    return {
        "stats": stats,
        "diagnostic": diagnostic,
        "sweep": sweep,
        "final": final,
        "diagnostic_manifest": diagnostic_manifest,
        "sweep_manifest": sweep_manifest,
        "diagnostic_manifest_sha256": sha256_file(diagnostic_manifest_path),
        "sweep_manifest_sha256": sweep_hash,
        "diagnostic_receipts": diagnostic_receipts,
        "cell_receipts": cell_receipts,
        "raw": loaded["raw"],
        "usual": loaded["usual_bl_reference"],
        "usual_bl_tuple": usual_tuple,
        "settings": settings,
    }


def _prepare_population(verified: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = verified["stats"]
    diagnostic = cast(Path, verified["diagnostic"])
    quotes = _read_csv(diagnostic / "quote_probabilities.csv")
    required_quote_columns = set(QUOTE_KEY + ["actual", "arm", "oracle_non_executable", "over_prob", "market_over"])
    if not required_quote_columns <= set(quotes.columns):
        raise LearnedMarketBlendWorkflowError("Diagnostic quote schema is incomplete")
    baseline = quotes.loc[
        (quotes["arm"].astype(str) == "baseline")
        & (~_bools(quotes["oracle_non_executable"]))
        & (quotes["stat"].isin(stats))
    ].copy()
    if baseline.duplicated(QUOTE_KEY).any():
        raise LearnedMarketBlendWorkflowError("Diagnostic baseline quote keys are ambiguous")

    totals = _read_csv(diagnostic / "totals.csv")
    required_totals = set(PLAYER_GAME_STAT_KEY + ["arm", "oracle_non_executable", "actual", "actual_minutes"])
    if not required_totals <= set(totals.columns):
        raise LearnedMarketBlendWorkflowError("Diagnostic totals schema lacks participation evidence")
    total = totals.loc[
        (totals["arm"].astype(str) == "baseline")
        & (~_bools(totals["oracle_non_executable"]))
        & (totals["stat"].isin(stats))
    ].copy()
    if total.duplicated(PLAYER_GAME_STAT_KEY).any():
        raise LearnedMarketBlendWorkflowError("Diagnostic baseline totals are ambiguous")

    frames: dict[str, pd.DataFrame] = {}
    for label in ("raw", "usual"):
        source = cast(pd.DataFrame, verified[label])
        required = set(
            QUOTE_KEY
            + [
                "quote_status",
                "sample_status",
                "over_prob",
                "under_prob",
                "implied_over",
                "implied_under",
                "over_odds",
                "under_odds",
                "over_edge",
                "under_edge",
                "quote_decision_policy",
            ]
        )
        if not required <= set(source.columns):
            raise LearnedMarketBlendWorkflowError(f"{label} all-candidate schema is incomplete")
        quoted = source.loc[
            (source["quote_status"].astype(str) == "quoted") & source["stat"].isin(stats)
        ].copy()
        if quoted.duplicated(QUOTE_KEY).any():
            raise LearnedMarketBlendWorkflowError(f"{label} quoted candidate keys are ambiguous")
        if quoted.duplicated(PLAYER_GAME_STAT_KEY).any():
            raise LearnedMarketBlendWorkflowError(
                f"{label} contains multiple authoritative books/lines per player/game/stat; routing is unsupported"
            )
        frames[label] = quoted

    raw_keys = set(map(tuple, frames["raw"][QUOTE_KEY].itertuples(index=False, name=None)))
    usual_keys = set(map(tuple, frames["usual"][QUOTE_KEY].itertuples(index=False, name=None)))
    diagnostic_keys = set(map(tuple, baseline[QUOTE_KEY].itertuples(index=False, name=None)))
    if raw_keys != usual_keys or raw_keys != diagnostic_keys:
        raise LearnedMarketBlendWorkflowError("Raw, usual-BL, and diagnostic quote populations differ")
    raw_prices = frames["raw"].set_index(QUOTE_KEY)[["over_odds", "under_odds"]].sort_index()
    usual_prices = frames["usual"].set_index(QUOTE_KEY)[["over_odds", "under_odds"]].sort_index()
    if not np.array_equal(raw_prices.to_numpy(), usual_prices.to_numpy(), equal_nan=True):
        raise LearnedMarketBlendWorkflowError("Raw and usual-BL paired executable prices differ")

    usual_columns = QUOTE_KEY + [
        "over_prob",
        "under_prob",
        "implied_over",
        "implied_under",
        "over_edge",
        "under_edge",
        "posterior_over",
        "posterior_under",
        "model_over",
        "model_under",
        "confidence",
        "bl_enabled",
    ]
    usual = frames["usual"].loc[:, [column for column in usual_columns if column in frames["usual"].columns]].copy()
    usual = usual.rename(columns={column: f"usual_bl_{column}" for column in usual.columns if column not in QUOTE_KEY})
    diagnostic_columns = QUOTE_KEY + ["actual", "over_prob", "market_over"]
    diagnostic_quotes = baseline[diagnostic_columns].rename(
        columns={
            "actual": "diagnostic_actual",
            "over_prob": "empirical_over_prob_unclipped",
            "market_over": "market_over_prob",
        }
    )
    participation = total[PLAYER_GAME_STAT_KEY + ["actual", "actual_minutes"]].rename(
        columns={"actual": "total_actual", "actual_minutes": "actual_minutes"}
    )
    joined = frames["raw"].merge(diagnostic_quotes, on=QUOTE_KEY, how="inner", validate="one_to_one")
    joined = joined.merge(usual, on=QUOTE_KEY, how="inner", validate="one_to_one")
    joined = joined.merge(participation, on=PLAYER_GAME_STAT_KEY, how="left", validate="one_to_one")

    raw_empirical = _numbers(joined["empirical_over_prob_unclipped"], "diagnostic empirical probability")
    raw_expected = np.clip(raw_empirical, 0.05, 0.90)
    if not np.allclose(raw_expected, _numbers(joined["over_prob"], "raw replay probability"), rtol=0, atol=1e-12):
        raise LearnedMarketBlendWorkflowError("Raw replay clamp differs from diagnostic empirical probability")
    if not np.allclose(
        _numbers(joined["market_over_prob"], "diagnostic market probability"),
        _numbers(joined["implied_over"], "raw implied probability"),
        rtol=0,
        atol=1e-12,
    ):
        raise LearnedMarketBlendWorkflowError("Diagnostic market probability differs from raw quote")
    if not np.allclose(
        _numbers(joined["market_over_prob"], "diagnostic market probability"),
        _numbers(joined["usual_bl_implied_over"], "usual BL implied probability"),
        rtol=0,
        atol=1e-12,
    ):
        raise LearnedMarketBlendWorkflowError("Diagnostic market probability differs from usual-BL quote")
    if "usual_bl_posterior_over" in joined and not np.allclose(
        _numbers(joined["usual_bl_over_prob"], "usual BL decision probability"),
        _numbers(joined["usual_bl_posterior_over"], "usual BL posterior probability"),
        rtol=0,
        atol=1e-12,
    ):
        raise LearnedMarketBlendWorkflowError("Usual-BL decision/posterior probabilities differ")

    counts: dict[str, int] = {"source_rows": len(joined)}
    line = np.asarray(pd.to_numeric(joined["line"], errors="coerce"), dtype=float)
    actual = np.asarray(pd.to_numeric(joined["diagnostic_actual"], errors="coerce"), dtype=float)
    total_actual = np.asarray(pd.to_numeric(joined["total_actual"], errors="coerce"), dtype=float)
    minutes = np.asarray(pd.to_numeric(joined["actual_minutes"], errors="coerce"), dtype=float)
    over_odds = np.asarray(pd.to_numeric(joined["over_odds"], errors="coerce"), dtype=float)
    under_odds = np.asarray(pd.to_numeric(joined["under_odds"], errors="coerce"), dtype=float)
    masks = {
        "invalid_line": ~np.isfinite(line) | (line <= 0) | (~np.isclose(np.mod(line, 1.0), 0.5, rtol=0, atol=1e-12)),
        "unknown_or_invalid_actual": ~np.isfinite(actual) | (actual < 0) | (~np.isclose(np.mod(actual, 1.0), 0.0, rtol=0, atol=1e-12)),
        "actual_contradiction": ~np.isfinite(total_actual) | (~np.isclose(actual, total_actual, rtol=0, atol=0, equal_nan=False)),
        "void_or_dnp": ~np.isfinite(minutes) | (minutes <= 0),
        "invalid_prices": ~np.isfinite(over_odds) | ~np.isfinite(under_odds) | (over_odds == 0) | (under_odds == 0),
    }
    keep = pd.Series(True, index=joined.index)
    for name, mask in masks.items():
        dropped = keep & mask
        counts[f"dropped_{name}"] = int(dropped.sum())
        keep &= ~mask
    result = joined.loc[keep].copy()
    if result.empty:
        raise LearnedMarketBlendWorkflowError("No eligible prepared opportunities remain")
    missing_stats = set(stats) - set(result["stat"].astype(str))
    if missing_stats:
        raise LearnedMarketBlendWorkflowError(f"Prepared population is missing requested stats: {sorted(missing_stats)}")
    if result.duplicated(PLAYER_GAME_STAT_KEY).any():
        raise LearnedMarketBlendWorkflowError("Prepared output is not one authoritative quote per player/game/stat")

    result["actual"] = _numbers(result["diagnostic_actual"], "actual").astype(int)
    result["actual_minutes"] = _numbers(result["actual_minutes"], "actual minutes")
    result["y_over"] = (_numbers(result["actual"], "actual") > _numbers(result["line"], "line")).astype(int)
    result["raw_over_prob"] = _numbers(result["over_prob"], "raw over")
    result["raw_under_prob"] = _numbers(result["under_prob"], "raw under")
    result["usual_bl_over_prob"] = _numbers(result["usual_bl_over_prob"], "usual BL over")
    result["usual_bl_under_prob"] = _numbers(result["usual_bl_under_prob"], "usual BL under")
    result["market_under_prob"] = 1.0 - _numbers(result["market_over_prob"], "market over")
    counts["eligible_rows"] = len(result)
    for stat in stats:
        counts[f"eligible_{stat}"] = int((result["stat"] == stat).sum())
    return result.sort_values(["game_date", "game_id", "player_id", "stat"], kind="stable").reset_index(drop=True), counts


def prepare_saved_inputs(request: PrepareRequest, *, dry_run: bool = False) -> dict[str, Any] | Path:
    """Validate and prepare the exact saved diagnostic/sweep population."""

    verified = _verify_prepare_sources(request)
    expected_hashes = {
        verified["diagnostic"] / "diagnostic_manifest.json": verified["diagnostic_manifest_sha256"],
        verified["sweep"] / "sweep_manifest.json": verified["sweep_manifest_sha256"],
        Path(verified["settings"]["model_chronology"]["model_dir"]) / "artifact_manifest.json": verified["settings"]["model_chronology"]["artifact_manifest_sha256"],
    }
    expected_hashes.update({verified["diagnostic"] / name: receipt["sha256"] for name, receipt in verified["diagnostic_receipts"].items()})
    for receipt in verified["cell_receipts"].values():
        cell_dir = verified["sweep"] / "cells" / receipt["cell_id"]
        expected_hashes[cell_dir / "all_bookmaker_edges.csv"] = receipt["edges"]["sha256"]
        expected_hashes[cell_dir / "config.json"] = receipt["config"]["sha256"]
    input_receipts = _input_receipts(expected_hashes)
    if any(input_receipts[str(path.resolve())]["sha256"] != digest for path, digest in expected_hashes.items()):
        raise LearnedMarketBlendWorkflowError("Consumed input changed during source verification")
    opportunities, accounting = _prepare_population(verified)
    quote_policies = sorted(opportunities["quote_decision_policy"].dropna().astype(str).unique().tolist())
    if len(quote_policies) != 1:
        raise LearnedMarketBlendWorkflowError("Prepared opportunities do not share one quote policy")
    dates = sorted(opportunities["game_date"].astype(str).unique().tolist())
    settings = verified["settings"]
    identity_contract = {
        "model_identity": settings["model_identity"],
        "predictor_settings": settings["predictor_settings"],
        "fixed_config": {key: value for key, value in settings["fixed_config"].items() if key not in ("start", "end")},
        "source_policy": {key: value for key, value in (settings["source_policy"] or {}).items() if key != "watermark"},
        "quote_policy": quote_policies[0],
        "requested_stats": list(verified["stats"]),
        "usual_bl_tuple": verified["usual_bl_tuple"],
        "probability_semantics": {
            "empirical_over": "original diagnostic empirical P(samples > line), unclipped",
            "raw_decision": "existing replay clip to [0.05, 0.90]",
            "usual_bl": "saved ordinary BL posterior from explicit reference cell",
            "market": "saved paired no-vig implied probability",
        },
    }
    summary = {
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "mode": MODE,
        "dry_run": dry_run,
        "diagnostic_dir": str(verified["diagnostic"]),
        "sweep_dir": str(verified["sweep"]),
        "output_dir": str(verified["final"]),
        "requested_stats": list(verified["stats"]),
        "rows": len(opportunities),
        "games": int(opportunities[["game_id", "game_date"]].drop_duplicates().shape[0]),
        "observed_start": dates[0],
        "observed_end": dates[-1],
        "quote_policy": quote_policies[0],
        "raw_cell_id": verified["cell_receipts"]["raw"]["cell_id"],
        "usual_bl_cell_id": verified["cell_receipts"]["usual_bl_reference"]["cell_id"],
        "usual_bl_tuple": verified["usual_bl_tuple"],
        "accounting": accounting,
        "identity_contract": identity_contract,
        "identity_contract_id": content_identity(identity_contract),
        "data_status": "saved_research_evidence_not_promotion",
        "bundle_provenance": {
            "source_policy": settings["source_policy"],
            "canonical_input_identity": settings["canonical_input_identity"],
            "reference_input_identity": settings["reference_input_identity"],
            "source_start": settings["fixed_config"]["start"],
            "source_end": settings["fixed_config"]["end"],
        },
    }
    if dry_run:
        return summary

    final = cast(Path, verified["final"])
    current = _verify_prepare_sources(request)
    if (
        current["diagnostic_manifest_sha256"] != verified["diagnostic_manifest_sha256"]
        or current["sweep_manifest_sha256"] != verified["sweep_manifest_sha256"]
        or current["cell_receipts"] != verified["cell_receipts"]
    ):
        raise LearnedMarketBlendWorkflowError("Consumed saved evidence changed during preparation")
    root = _atomic_root(final)
    try:
        opportunities_path = root / "opportunities.csv"
        opportunities.to_csv(opportunities_path, index=False)
        files = {
            "opportunities.csv": _file_evidence(
                opportunities_path, rows=len(opportunities), columns=list(opportunities.columns)
            )
        }
        manifest = {
            **summary,
            "dry_run": False,
            "state": "complete",
            "bl_config": None,
            "input_receipts": input_receipts,
            "source_manifests": {
                "diagnostic_manifest_sha256": verified["diagnostic_manifest_sha256"],
                "sweep_manifest_sha256": verified["sweep_manifest_sha256"],
            },
            "source_receipts": {
                "diagnostic": verified["diagnostic_receipts"],
                "cells": verified["cell_receipts"],
            },
            "model_chronology": settings["model_chronology"],
            "files": files,
            "limitations": [
                "Saved-window research evidence only; no arbitrary-date inference.",
                "Source model identity is explicit; engineering checks do not authorize an owner experiment.",
                "One existing authoritative quote per player/game/stat is required; no routing is invented.",
            ],
        }
        _write_json(root / "preparation_manifest.json", manifest)
        report = (
            "# Learned market blend preparation\n\n"
            "Status: **saved research evidence**; not independent validation or promotion.\n\n"
            f"- Source model identity: `{settings['model_identity']}`\n"
            f"- Eligible opportunities: {len(opportunities):,}\n"
            f"- Dates: {dates[0]} through {dates[-1]}\n"
            f"- Stats: {', '.join(verified['stats'])}\n"
            f"- Raw cell: `{summary['raw_cell_id']}`\n"
            f"- Usual BL cell: `{summary['usual_bl_cell_id']}`; parameters: {json.dumps(verified['usual_bl_tuple'], sort_keys=True)}\n"
            f"- Quote policy: `{quote_policies[0]}`\n\n"
            "The empirical fit probability is the original diagnostic P(samples > line), not the raw replay clamp "
            "and not an ordinary-BL posterior. Root diagnostic oracle metadata does not override the selected "
            "baseline rows, which are individually required to be executable.\n\n"
            "An owner experiment requires the intended model and a frozen chronological fit/evaluation policy.\n"
        )
        (root / "preparation_report.md").write_text(report, encoding="utf-8")
        manifest["files"]["preparation_report.md"] = _file_evidence(root / "preparation_report.md")
        _write_json(root / "preparation_manifest.json", manifest)
        stored = _json(root / "preparation_manifest.json")
        if stored.get("state") != "complete" or stored.get("files", {}).get("opportunities.csv", {}).get("rows") != len(opportunities):
            raise LearnedMarketBlendWorkflowError("Prepared bundle readback failed")
        _recheck_inputs(input_receipts)
        return _publish(root, final)
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise


def load_prepared_bundle(root: Path) -> PreparedBundle:
    bundle_root = root.resolve(strict=True)
    manifest_path = bundle_root / "preparation_manifest.json"
    manifest = _json(manifest_path)
    if manifest.get("schema_version") != PREPARATION_SCHEMA_VERSION or manifest.get("state") != "complete":
        raise LearnedMarketBlendWorkflowError("Prepared bundle is not complete")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise LearnedMarketBlendWorkflowError("Prepared file receipts are missing")
    opportunities_path = bundle_root / "opportunities.csv"
    evidence = _receipt(opportunities_path, files.get("opportunities.csv"), "prepared opportunities")
    opportunities = _read_csv(opportunities_path)
    if evidence.get("rows") != len(opportunities):
        raise LearnedMarketBlendWorkflowError("Prepared opportunity count differs")
    stats = _validate_stats(cast(list[str], manifest.get("requested_stats", [])))
    if set(opportunities["stat"].astype(str)) != set(stats):
        raise LearnedMarketBlendWorkflowError("Prepared stat coverage differs from manifest")
    if opportunities.duplicated(PLAYER_GAME_STAT_KEY).any():
        raise LearnedMarketBlendWorkflowError("Prepared opportunity identity is ambiguous")
    identity_contract = manifest.get("identity_contract")
    if not isinstance(identity_contract, dict) or content_identity(identity_contract) != manifest.get("identity_contract_id"):
        raise LearnedMarketBlendWorkflowError("Prepared identity contract mismatch")
    return PreparedBundle(bundle_root, manifest, opportunities)


def _date_subset(frame: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    first, last = _validate_range(start, end)
    dates = pd.to_datetime(frame["game_date"], errors="raise").dt.date
    return frame.loc[(dates >= first) & (dates <= last)].copy()


def _class_preflight(frame: pd.DataFrame, stats: Sequence[str]) -> dict[str, Any]:
    if frame.empty:
        raise LearnedMarketBlendWorkflowError("Requested fit range has no prepared rows")
    result: dict[str, Any] = {}
    for stat in stats:
        group = frame.loc[frame["stat"] == stat]
        classes = sorted(np.unique(_numbers(group["y_over"], "fit outcome")).tolist())
        result[stat] = {
            "rows": len(group),
            "games": int(group[["game_id", "game_date"]].drop_duplicates().shape[0]),
            "dates": sorted(group["game_date"].astype(str).unique().tolist()),
            "classes": classes,
        }
        if group.empty or classes != [0, 1]:
            raise LearnedMarketBlendWorkflowError(
                f"Requested stat {stat!r} has invalid fit population: rows={len(group)}, classes={classes}"
            )
    return result


def fit_saved_inputs(request: FitRequest, *, dry_run: bool = False) -> dict[str, Any] | Path:
    """Fit pooled/per-stat weights on a declared whole-date window."""

    input_receipts = _input_receipts([request.input_dir / "preparation_manifest.json", request.input_dir / "opportunities.csv"])
    prepared = load_prepared_bundle(request.input_dir)
    _recheck_inputs(input_receipts)
    final = _validate_output(request.output_dir, [prepared.root, Path(prepared.manifest["model_chronology"]["model_dir"])])
    first, _ = _validate_range(request.fit_start, request.fit_end)
    try:
        shrinkage = float(request.shrinkage_strength)
    except (TypeError, ValueError) as exc:
        raise LearnedMarketBlendWorkflowError("shrinkage_strength must be finite and nonnegative") from exc
    if not math.isfinite(shrinkage) or shrinkage < 0:
        raise LearnedMarketBlendWorkflowError("shrinkage_strength must be finite and nonnegative")
    cutoff = _parse_date(str(prepared.manifest.get("model_chronology", {}).get("fit_population_cutoff")), "source cutoff")
    if first <= cutoff:
        raise LearnedMarketBlendWorkflowError(
            f"Fit population must begin after source model training/calibration cutoff {cutoff.isoformat()}"
        )
    fit_frame = _date_subset(prepared.opportunities, request.fit_start, request.fit_end)
    stats = _validate_stats(cast(list[str], prepared.manifest["requested_stats"]))
    preflight = _class_preflight(fit_frame, stats)
    observed_dates = sorted(fit_frame["game_date"].astype(str).unique().tolist())
    summary = {
        "schema_version": MODEL_SCHEMA_VERSION,
        "mode": MODE,
        "dry_run": dry_run,
        "input_dir": str(prepared.root),
        "output_dir": str(final),
        "declared_fit_start": request.fit_start,
        "declared_fit_end": request.fit_end,
        "observed_fit_start": observed_dates[0],
        "observed_fit_end": observed_dates[-1],
        "rows": len(fit_frame),
        "games": int(fit_frame[["game_id", "game_date"]].drop_duplicates().shape[0]),
        "requested_stats": list(stats),
        "shrinkage_strength": shrinkage,
        "stat_populations": preflight,
        "source_model_cutoff": cutoff.isoformat(),
        "data_status": prepared.manifest.get("data_status"),
    }
    if dry_run:
        return summary

    try:
        fitted = fit_per_stat_weights(
            fit_frame["stat"].astype(str).tolist(),
            _numbers(fit_frame["empirical_over_prob_unclipped"], "fit empirical probability"),
            _numbers(fit_frame["market_over_prob"], "fit market probability"),
            _numbers(fit_frame["y_over"], "fit outcome"),
            stats,
            shrinkage,
        )
    except LearnedMarketBlendError as exc:
        raise LearnedMarketBlendWorkflowError(str(exc)) from exc

    fit_game_keys = sorted(fit_frame["game_id"].astype(str).unique().tolist())
    artifact_payload = {
        "schema_version": MODEL_SCHEMA_VERSION,
        "mode": MODE,
        "bl_config": None,
        "requested_stats": list(stats),
        "weights": fitted["weights"],
        "pooled_weight": fitted["pooled"].weight,
        "shrinkage_strength": shrinkage,
        "weight_bounds": [0.0, 1.0],
        "probability_clipping": {"min": CLIP_MIN, "max": CLIP_MAX},
        "formula": "sigmoid(logit(market_over)+weight[stat]*(logit(empirical_over)-logit(market_over)))",
        "under_semantics": "1 - learned over probability; positive half-point binary markets only",
        "identity_contract": prepared.manifest["identity_contract"],
        "identity_contract_id": prepared.manifest["identity_contract_id"],
        "base_model_identity": prepared.manifest["identity_contract"]["model_identity"],
        "model_chronology": prepared.manifest["model_chronology"],
        "source_readiness": {
            "historical_readiness": "unverified",
            "data_status": prepared.manifest.get("data_status"),
            "decision_grade": False,
        },
        "fit_window": {
            "declared_start": request.fit_start,
            "declared_end": request.fit_end,
            "observed_start": observed_dates[0],
            "observed_end": observed_dates[-1],
            "dates": observed_dates,
            "rows": len(fit_frame),
            "games": len(fit_game_keys),
            "game_keys_sha256": content_identity(fit_game_keys),
            "game_keys": fit_game_keys,
            "stat_populations": preflight,
        },
        "input_receipts": input_receipts,
        "optimizer": {
            "method": "scipy.optimize.minimize_scalar bounded plus explicit endpoints",
            "tie_policy": "objective ties within 1e-12 choose smaller model weight",
            "pooled": fitted["pooled"].to_dict(),
            "per_stat": {stat: fitted["per_stat"][stat].to_dict() for stat in stats},
        },
        "fit_label": "in-sample blend fit scores",
    }
    artifact = finalize_artifact(artifact_payload)

    score_rows: list[dict[str, Any]] = []
    for stat_label, group in [("pooled", fit_frame), *[(stat, fit_frame.loc[fit_frame["stat"] == stat]) for stat in stats]]:
        labels = _numbers(group["y_over"], f"{stat_label} outcomes")
        candidates = {
            "market_only": _numbers(group["market_over_prob"], f"{stat_label} market"),
            "empirical_model": _numbers(group["empirical_over_prob_unclipped"], f"{stat_label} empirical"),
        }
        learned = np.empty(len(group), dtype=float)
        for stat in stats:
            mask = group["stat"].astype(str).to_numpy() == stat
            if mask.any():
                learned[mask] = cast(
                    np.ndarray,
                    learned_market_probability_for_stat(
                        _numbers(group.loc[mask, "empirical_over_prob_unclipped"], f"{stat} empirical"),
                        _numbers(group.loc[mask, "market_over_prob"], f"{stat} market"),
                        stat,
                        artifact["weights"],
                    ),
                )
        candidates[MODE] = learned
        for treatment, probabilities in candidates.items():
            score_rows.append(
                {
                    "population": stat_label,
                    "treatment": treatment,
                    **probability_scores(probabilities, labels),
                    "label": "in-sample blend fit scores",
                }
            )
    fit_scores = pd.DataFrame(score_rows)

    root = _atomic_root(final)
    try:
        _write_json(root / "learned_blend.json", artifact)
        fit_scores.to_csv(root / "fit_scores.csv", index=False)
        report = (
            "# Learned market blend fit\n\n"
            "Scores below are **in-sample blend fit scores**, not validation and not ROI fitting.\n\n"
            f"- Fit dates: {observed_dates[0]} through {observed_dates[-1]} ({len(fit_frame):,} rows)\n"
            f"- Pooled weight: {artifact['pooled_weight']:.8f}\n"
            f"- Shrinkage strength: {shrinkage}\n"
            + "".join(f"- {stat.upper()} weight: {artifact['weights'][stat]:.8f}\n" for stat in stats)
            + "\nThe weights are frozen scalars. No tau, confidence, intercept, online fitting, or evaluation ROI entered the fit.\n"
        )
        (root / "fit_report.md").write_text(report, encoding="utf-8")
        manifest = {
            "schema_version": MODEL_SCHEMA_VERSION,
            "state": "complete",
            "mode": MODE,
            "artifact_id": artifact["artifact_id"],
            "input_dir": str(prepared.root),
            "input_receipts": input_receipts,
            "files": {
                "learned_blend.json": _file_evidence(root / "learned_blend.json"),
                "fit_scores.csv": _file_evidence(
                    root / "fit_scores.csv", rows=len(fit_scores), columns=list(fit_scores.columns)
                ),
                "fit_report.md": _file_evidence(root / "fit_report.md"),
            },
        }
        _write_json(root / "fit_manifest.json", manifest)
        stored = load_artifact(root / "learned_blend.json")
        if stored["artifact_id"] != artifact["artifact_id"]:
            raise LearnedMarketBlendWorkflowError("Fit artifact readback failed")
        _recheck_inputs(input_receipts)
        return _publish(root, final)
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _allowed_bets(value: tuple[tuple[str, str], ...] | None, stats: Sequence[str]) -> list[tuple[str, str]] | None:
    if value is None:
        return None
    result = list(value)
    if not result or len(result) != len(set(result)):
        raise LearnedMarketBlendWorkflowError("allowed_bets must be a unique nonempty list when supplied")
    for stat, side in result:
        if stat not in stats or side not in ("over", "under"):
            raise LearnedMarketBlendWorkflowError("allowed_bets entries must be requested-stat over/under pairs")
    return result


def _treatment_probabilities(frame: pd.DataFrame, treatment: str, weights: Mapping[str, float]) -> tuple[np.ndarray, np.ndarray]:
    if treatment == "raw":
        over = _numbers(frame["raw_over_prob"], "raw over probability")
        under = _numbers(frame["raw_under_prob"], "raw under probability")
    elif treatment == "usual_bl_reference":
        over = _numbers(frame["usual_bl_over_prob"], "usual BL over probability")
        under = _numbers(frame["usual_bl_under_prob"], "usual BL under probability")
    elif treatment == "market_only":
        over = _numbers(frame["market_over_prob"], "market over probability")
        under = 1.0 - over
    elif treatment == MODE:
        over = np.empty(len(frame), dtype=float)
        for stat in frame["stat"].astype(str).unique():
            mask = frame["stat"].astype(str).to_numpy() == stat
            over[mask] = cast(
                np.ndarray,
                learned_market_probability_for_stat(
                    _numbers(frame.loc[mask, "empirical_over_prob_unclipped"], f"{stat} empirical probability"),
                    _numbers(frame.loc[mask, "market_over_prob"], f"{stat} market probability"),
                    stat,
                    weights,
                ),
            )
        under = 1.0 - over
    else:
        raise LearnedMarketBlendWorkflowError(f"Unknown treatment: {treatment}")
    return over, under


def _score_rows(
    population: str,
    treatment: str,
    threshold: float,
    frame: pd.DataFrame,
    probabilities: np.ndarray,
    outcomes: np.ndarray,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stat_label, mask in [
        ("pooled", np.ones(len(frame), dtype=bool)),
        *[(stat, frame["stat"].astype(str).to_numpy() == stat) for stat in sorted(frame["stat"].astype(str).unique())],
    ]:
        if not mask.any():
            continue
        rows.append(
            {
                "population": population,
                "treatment": treatment,
                "edge_threshold": threshold,
                "stat": stat_label,
                **probability_scores(probabilities[mask], outcomes[mask]),
            }
        )
    return rows


def _simulate_cell(
    source: pd.DataFrame,
    treatment: str,
    threshold: float,
    starting_bankroll: float,
    flat_bet_size: float,
    allowed_bets: list[tuple[str, str]] | None,
    stats: Sequence[str],
    weights: Mapping[str, float],
    usual_bl_tuple: Mapping[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    edges = source.copy()
    over, under = _treatment_probabilities(edges, treatment, weights)
    edges["over_prob"] = over
    edges["under_prob"] = under
    edges["over_edge"] = over - _numbers(edges["implied_over"], "implied over probability")
    edges["under_edge"] = under - _numbers(edges["implied_under"], "implied under probability")
    edges["blend_mode"] = treatment
    edges["effective_weight"] = cast(pd.Series, edges["stat"]).map(dict(weights)) if treatment == MODE else np.nan
    edges["bl_config"] = json.dumps(dict(usual_bl_tuple), sort_keys=True) if treatment == "usual_bl_reference" else None

    harness = BacktestHarness(
        engine=None,
        feature_store=None,
        model_pipeline=None,
        predictor=None,
        stats=list(stats),
        bookmakers=sorted(edges["bookmaker"].dropna().astype(str).unique().tolist()),
        min_minutes_avg=10,
        starting_bankroll=starting_bankroll,
        flat_bet_size=flat_bet_size,
        edge_threshold=threshold,
        allowed_bets=allowed_bets,
    )
    selection_input = edges.drop(columns=["actual"], errors="ignore")
    selected = harness._filter_best_bets(selection_input)  # pyright: ignore[reportPrivateUsage]
    if not selected.empty:
        selected["game_date"] = pd.to_datetime(selected["game_date"], errors="raise").dt.date
        selected = selected.sort_values(["game_date", "game_id", "player_id"], kind="stable").reset_index(drop=True)
    actuals = cast(pd.DataFrame, source[["player_id", "game_id", "stat", "actual"]]).drop_duplicates().rename(
        columns={"actual": "actual_value"}
    )
    if actuals.duplicated(["player_id", "game_id", "stat"]).any():
        raise LearnedMarketBlendWorkflowError("Actual outcome map is ambiguous")
    simulator = harness._simulator  # pyright: ignore[reportPrivateUsage]
    for game_date in sorted(selected["game_date"].unique()) if not selected.empty else []:
        simulator.resolve_bets(actuals)
        simulator.evaluate_predictions(selected.loc[selected["game_date"] == game_date], cast(date, game_date))
    simulator.resolve_bets(actuals)
    bets = simulator.to_dataframe()
    bets["blend_mode"] = treatment
    bets["effective_weight"] = (
        bets["stat"].apply(lambda stat: float(weights[str(stat)]))
        if treatment == MODE and not bets.empty
        else np.nan
    )
    bets["bl_config"] = json.dumps(dict(usual_bl_tuple), sort_keys=True) if treatment == "usual_bl_reference" else None
    predictions = harness._merge_actuals(selected, actuals) if not selected.empty else selected.copy()  # pyright: ignore[reportPrivateUsage]
    metrics = harness._metrics_calc.calculate(predictions, bets, starting_bankroll=starting_bankroll).to_dict()  # pyright: ignore[reportPrivateUsage]
    betting = metrics["betting"]
    if not math.isclose(float(betting["total_staked"]), float(bets.loc[bets.get("outcome", pd.Series(dtype=object)) != "push", "stake"].sum()) if not bets.empty else 0.0, rel_tol=0, abs_tol=1e-9):
        raise LearnedMarketBlendWorkflowError("Canonical stake reconciliation failed")

    score_rows = _score_rows(
        "all_candidates",
        treatment,
        threshold,
        edges,
        over,
        _numbers(edges["y_over"], "candidate outcomes"),
    )
    if not predictions.empty:
        selected_side = predictions["selected_side"].astype(str).to_numpy()
        selected_probability = np.where(
            selected_side == "over",
            _numbers(predictions["over_prob"], "selected over probability"),
            _numbers(predictions["under_prob"], "selected under probability"),
        )
        selected_actual = _numbers(predictions["actual"], "selected actual")
        selected_line = _numbers(predictions["line"], "selected line")
        selected_outcome = np.where(selected_side == "over", selected_actual > selected_line, selected_actual < selected_line).astype(float)
        score_rows.extend(
            _score_rows("selected", treatment, threshold, predictions, selected_probability, selected_outcome)
        )
    if not bets.empty:
        resolved = bets.loc[bets["outcome"].isin(["win", "loss"])].copy()
        if not resolved.empty:
            score_rows.extend(
                _score_rows(
                    "funded",
                    treatment,
                    threshold,
                    resolved,
                    _numbers(resolved["model_prob"], "funded model probability"),
                    (resolved["outcome"].astype(str).to_numpy() == "win").astype(float),
                )
            )
    return edges, predictions, bets, metrics, score_rows


def evaluate_saved_inputs(request: EvaluateRequest, *, dry_run: bool = False) -> dict[str, Any] | Path:
    """Evaluate frozen weights before canonical selection and simulation."""

    input_receipts = _input_receipts([
        request.input_dir / "preparation_manifest.json", request.input_dir / "opportunities.csv", request.blend_artifact,
    ])
    prepared = load_prepared_bundle(request.input_dir)
    artifact_path = request.blend_artifact.resolve(strict=True)
    artifact = load_artifact(artifact_path)
    _recheck_inputs(input_receipts)
    final = _validate_output(request.output_dir, [prepared.root, artifact_path.parent, Path(prepared.manifest["model_chronology"]["model_dir"])])
    first, _ = _validate_range(request.start, request.end)
    thresholds = tuple(float(value) for value in request.edge_thresholds)
    if not thresholds or len(thresholds) != len(set(thresholds)) or any(not math.isfinite(value) or value < 0 for value in thresholds):
        raise LearnedMarketBlendWorkflowError("edge_thresholds must be unique finite nonnegative values")
    if not math.isfinite(request.starting_bankroll) or request.starting_bankroll <= 0:
        raise LearnedMarketBlendWorkflowError("starting_bankroll must be finite and positive")
    if not math.isfinite(request.flat_bet_size) or request.flat_bet_size <= 0:
        raise LearnedMarketBlendWorkflowError("flat_bet_size must be finite and positive")
    if artifact["identity_contract_id"] != prepared.manifest["identity_contract_id"] or artifact["identity_contract"] != prepared.manifest["identity_contract"]:
        raise LearnedMarketBlendWorkflowError("Blend artifact model/settings/quote identity differs from prepared input")
    stats = _validate_stats(cast(list[str], prepared.manifest["requested_stats"]))
    if set(artifact["requested_stats"]) != set(stats) or set(artifact["weights"]) != set(stats):
        raise LearnedMarketBlendWorkflowError("Blend artifact stat coverage differs from prepared input")
    fit_end = _parse_date(str(artifact["fit_window"]["observed_end"]), "artifact fit end")
    declared_fit_end = _parse_date(str(artifact["fit_window"]["declared_end"]), "artifact declared fit end")
    if first <= fit_end or first <= declared_fit_end:
        raise LearnedMarketBlendWorkflowError("Evaluation must start strictly after declared and observed fit dates")
    evaluation = _date_subset(prepared.opportunities, request.start, request.end)
    if evaluation.empty:
        raise LearnedMarketBlendWorkflowError("Requested evaluation range has no prepared rows")
    observed_dates = sorted(evaluation["game_date"].astype(str).unique().tolist())
    evaluation_games = set(evaluation["game_id"].astype(str))
    fit_games = artifact["fit_window"].get("game_keys")
    if not isinstance(fit_games, list) or content_identity(fit_games) != artifact["fit_window"].get("game_keys_sha256"):
        raise LearnedMarketBlendWorkflowError("Artifact fit game keys are unavailable or inconsistent")
    if evaluation_games & set(fit_games):
        raise LearnedMarketBlendWorkflowError("Fit and evaluation game identities overlap")
    allowed = _allowed_bets(request.allowed_bets, stats)
    summary = {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "mode": MODE,
        "dry_run": dry_run,
        "input_dir": str(prepared.root),
        "blend_artifact": str(artifact_path),
        "artifact_id": artifact["artifact_id"],
        "output_dir": str(final),
        "declared_start": request.start,
        "declared_end": request.end,
        "observed_start": observed_dates[0],
        "observed_end": observed_dates[-1],
        "rows": len(evaluation),
        "games": int(evaluation[["game_id", "game_date"]].drop_duplicates().shape[0]),
        "stats": list(stats),
        "treatments": list(TREATMENTS),
        "edge_thresholds": list(thresholds),
        "starting_bankroll": request.starting_bankroll,
        "flat_bet_size": request.flat_bet_size,
        "allowed_bets": [f"{stat}:{side}" for stat, side in allowed] if allowed else None,
        "weights": artifact["weights"],
        "data_status": prepared.manifest.get("data_status"),
    }
    if dry_run:
        return summary

    all_edges: list[pd.DataFrame] = []
    all_selected: list[pd.DataFrame] = []
    all_bets: list[pd.DataFrame] = []
    configs: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    score_rows: list[dict[str, Any]] = []
    for threshold in thresholds:
        for treatment in TREATMENTS:
            edges, selected, bets, metrics, scores = _simulate_cell(
                evaluation,
                treatment,
                threshold,
                request.starting_bankroll,
                request.flat_bet_size,
                allowed,
                stats,
                artifact["weights"],
                prepared.manifest["usual_bl_tuple"],
            )
            cell_id = content_identity(
                {
                    "artifact_id": artifact["artifact_id"],
                    "treatment": treatment,
                    "edge_threshold": threshold,
                    "start": request.start,
                    "end": request.end,
                    "allowed_bets": summary["allowed_bets"],
                }
            )
            for frame in (edges, selected, bets):
                frame["cell_id"] = cell_id
                frame["treatment"] = treatment
                frame["edge_threshold"] = threshold
            all_edges.append(edges)
            all_selected.append(selected)
            all_bets.append(bets)
            config = {
                "cell_id": cell_id,
                "treatment": treatment,
                "blend_mode": treatment,
                "blend_artifact_id": artifact["artifact_id"] if treatment == MODE else None,
                "effective_weights": artifact["weights"] if treatment == MODE else None,
                "bl_config": prepared.manifest["usual_bl_tuple"] if treatment == "usual_bl_reference" else None,
                "edge_threshold": threshold,
                "starting_bankroll": request.starting_bankroll,
                "flat_bet_size": request.flat_bet_size,
                "stats": list(stats),
                "allowed_bets": summary["allowed_bets"],
                "empty_reason": "no_bets" if bets.empty else None,
            }
            configs.append(config)
            metric_rows.append(
                {
                    "cell_id": cell_id,
                    "treatment": treatment,
                    "edge_threshold": threshold,
                    "effective_weights": artifact["weights"] if treatment == MODE else None,
                    "bl_config": prepared.manifest["usual_bl_tuple"] if treatment == "usual_bl_reference" else None,
                    **metrics,
                }
            )
            score_rows.extend({"cell_id": cell_id, **row} for row in scores)

    edges_output = pd.concat(all_edges, ignore_index=True) if all_edges else pd.DataFrame()
    selected_output = pd.concat(all_selected, ignore_index=True) if all_selected else pd.DataFrame()
    bets_output = pd.concat(all_bets, ignore_index=True) if all_bets else pd.DataFrame()
    scores_output = pd.DataFrame(score_rows)
    root = _atomic_root(final)
    try:
        tables = {
            "all_candidates.csv": edges_output,
            "selected.csv": selected_output,
            "bets.csv": bets_output,
            "probability_scores.csv": scores_output,
        }
        files: dict[str, Any] = {}
        for name, frame in tables.items():
            frame.to_csv(root / name, index=False)
            files[name] = _file_evidence(root / name, rows=len(frame), columns=list(frame.columns))
        _write_json(root / "cell_configs.json", configs)
        _write_json(root / "metrics.json", metric_rows)
        files["cell_configs.json"] = _file_evidence(root / "cell_configs.json")
        files["metrics.json"] = _file_evidence(root / "metrics.json")
        report_lines = [
            "# Learned market blend evaluation",
            "",
            "Status: **engineering acceptance/development evidence**, not promotion or an owner experiment.",
            "",
            f"- Evaluation dates: {observed_dates[0]} through {observed_dates[-1]}",
            f"- Common preselection opportunities: {len(evaluation):,}",
            f"- Frozen artifact: `{artifact['artifact_id']}`",
            f"- Weights: {json.dumps(artifact['weights'], sort_keys=True)}",
            "",
            "| Treatment | Edge | Bets | Stake | Profit | ROI |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for config, metrics in zip(configs, metric_rows, strict=True):
            betting = metrics["betting"]
            report_lines.append(
                f"| {config['treatment']} | {config['edge_threshold']:.4f} | {betting['total_bets']} | "
                f"{betting['total_staked']:.2f} | {betting['total_profit']:.2f} | {betting['roi']:.4%} |"
            )
        report_lines.extend(
            [
                "",
                "Probabilities are scored on the common preselection cohort and separately on selected/funded populations. ",
                "Selection uses the existing BacktestHarness rank-first behavior; allowed-direction rejection occurs in the existing simulator without fallback to a lower-ranked side. Market-only no-bet cells remain explicit.",
                "",
                "This command does not refit weights, generate forecasts, support combo markets, change ordinary BL, or claim generic sweep/live compatibility.",
            ]
        )
        (root / "evaluation_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
        files["evaluation_report.md"] = _file_evidence(root / "evaluation_report.md")
        manifest = {
            **summary,
            "dry_run": False,
            "state": "complete",
            "decision_grade": False,
            "identity_contract_id": prepared.manifest["identity_contract_id"],
            "fit_window": artifact["fit_window"],
            "input_receipts": input_receipts,
            "files": files,
            "cells": configs,
            "reconciliation": {
                "cell_count": len(configs),
                "expected_cell_count": len(TREATMENTS) * len(thresholds),
                "all_candidate_rows": len(edges_output),
                "selected_rows": len(selected_output),
                "bet_rows": len(bets_output),
                "probability_score_rows": len(scores_output),
            },
            "limitations": [
                "Positive half-point PTS/REB/AST only.",
                "Saved-window forecasts only; no new inference.",
                "Evaluation edge grids make the window development evidence unless settings were frozen beforehand.",
                "The generic NBA evidence CLI is not claimed compatible with this schema.",
            ],
        }
        _write_json(root / "evaluation_manifest.json", manifest)
        stored = _json(root / "evaluation_manifest.json")
        if stored.get("state") != "complete" or stored.get("reconciliation", {}).get("cell_count") != len(configs):
            raise LearnedMarketBlendWorkflowError("Evaluation manifest readback failed")
        for name, evidence in files.items():
            path = root / name
            if sha256_file(path) != evidence["sha256"]:
                raise LearnedMarketBlendWorkflowError(f"Evaluation output receipt mismatch: {name}")
        _recheck_inputs(input_receipts)
        return _publish(root, final)
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise
