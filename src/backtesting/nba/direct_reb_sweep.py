"""Offline saved-window direct-REB sweep and artifact-only analysis.

This module deliberately consumes the completed direct-total benchmark and the
receipt-verified original sweep.  It never opens a database, loads a model, or
reconstructs the control distribution from five quantiles.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.backtesting.backtest_harness import BacktestHarness, BacktestResult
from src.backtesting.bet_simulator import BetSimulator
from src.backtesting.nba.sweep_runner import export_cell
from src.backtesting.performance_metrics import MetricsCalculator
from src.models.black_litterman import BlackLittermanBlender, BLConfig
from src.models.monte_carlo import MonteCarloPredictor
from src.models.nba_artifacts import _rename_no_replace, create_exclusive_incomplete_dir, sha256_file

SCHEMA_VERSION = "nba_direct_reb_saved_window_sweep_v1"
ANALYSIS_VERSION = "nba_direct_reb_saved_window_analysis_v1"
MODELS = ("direct", "control")
QUANTILES = np.asarray([0.10, 0.25, 0.50, 0.75, 0.90])
KEYS = ["player_id", "game_id"]
QUOTE_KEYS = ["player_id", "game_id", "game_date", "bookmaker", "line"]
DEPENDENCIES = (
    "src/backtesting/nba/direct_reb_sweep.py",
    "src/backtesting/backtest_harness.py",
    "src/backtesting/bet_simulator.py",
    "src/backtesting/performance_metrics.py",
    "src/backtesting/nba/sweep_runner.py",
    "src/models/black_litterman.py",
    "src/models/monte_carlo.py",
)
BET_COLUMNS = [
    "player_id", "game_id", "game_date", "stat", "side", "line", "odds", "stake",
    "bookmaker", "selected_snapshot_time", "over_snapshot_time", "under_snapshot_time",
    "selected_market_last_update", "selected_bookmaker_last_update", "selected_decision_time",
    "selected_line", "selected_price", "selected_side", "selected_bookmaker",
    "quote_decision_policy", "over_market_last_update", "under_market_last_update",
    "over_bookmaker_last_update", "under_bookmaker_last_update", "over_bookmaker",
    "under_bookmaker", "model_prob", "implied_prob", "edge", "posterior_prob",
    "actual", "outcome", "profit",
]


class DirectRebSweepError(RuntimeError):
    """Safe public failure for this offline bundle."""


@dataclass(frozen=True)
class SweepRequest:
    benchmark_dir: Path
    output_dir: Path
    reference_sweep_dir: Path | None = None
    models: tuple[str, ...] = MODELS


@dataclass(frozen=True)
class Cell:
    model: str
    edge_threshold: float
    tau: float | None
    z_max: float | None
    max_weight: float | None
    source_cell_id: str

    @property
    def treatment(self) -> str:
        return "raw" if self.tau is None else "fixed_bl"

    @property
    def cell_id(self) -> str:
        payload = json.dumps(
            {
                "schema": SCHEMA_VERSION,
                "model": self.model,
                "edge_threshold": self.edge_threshold,
                "tau": self.tau,
                "z_max": self.z_max,
                "max_weight": self.max_weight,
                "source_cell_id": self.source_cell_id,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return "direct_reb_" + hashlib.sha256(payload.encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "model": self.model,
            "treatment": self.treatment,
            "edge_threshold": self.edge_threshold,
            "tau": self.tau,
            "z_max": self.z_max,
            "max_weight": self.max_weight,
            "source_cell_id": self.source_cell_id,
        }


@dataclass
class Prepared:
    request: SweepRequest
    benchmark_manifest: dict[str, Any]
    reference_manifest: dict[str, Any]
    benchmark_dir: Path
    reference_dir: Path
    totals: pd.DataFrame
    matched_quotes: pd.DataFrame
    raw_edges: pd.DataFrame
    bl_edges: pd.DataFrame
    cells: tuple[Cell, ...]
    source_receipts: dict[str, dict[str, Any]]
    source_hashes: dict[str, str]
    plan: dict[str, Any]


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DirectRebSweepError(f"Invalid JSON input: {path.name}") from exc
    if not isinstance(value, dict):
        raise DirectRebSweepError(f"JSON object required: {path.name}")
    return cast(dict[str, Any], value)


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str).encode()
    ).hexdigest()


def _write_json(path: Path, value: dict[str, Any], *, replace: bool = False) -> None:
    target = path.with_suffix(path.suffix + ".tmp") if replace else path
    mode = "w" if replace else "x"
    with target.open(mode, encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, indent=2, allow_nan=False, default=str)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    if replace:
        os.replace(target, path)


def _write_csv(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    with path.open("x", encoding="utf-8", newline="") as handle:
        frame.to_csv(handle, index=False)
        handle.flush()
        os.fsync(handle.fileno())
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = sum(1 for _ in reader)
    if header != list(frame.columns) or rows != len(frame):
        raise DirectRebSweepError(f"CSV readback failed: {path.name}")
    return {"sha256": sha256_file(path), "rows": rows, "columns": header}


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            path,
            dtype={"player_id": str, "game_id": str},
            float_precision="round_trip",
            low_memory=False,
        )
    except (OSError, pd.errors.ParserError) as exc:
        raise DirectRebSweepError(f"Invalid CSV input: {path.name}") from exc


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], frame.to_dict(orient="records"))


def _numeric_array(series: pd.Series) -> np.ndarray:
    return np.asarray(pd.to_numeric(series, errors="coerce"), dtype=np.float64)


def _numeric_sum(series: pd.Series) -> float:
    return float(np.nansum(_numeric_array(series)))


def _verify_receipt(root: Path, name: str, receipt: Any) -> Path:
    path = root / name
    if not isinstance(receipt, dict) or not path.is_file() or sha256_file(path) != receipt.get("sha256"):
        raise DirectRebSweepError(f"Source receipt mismatch: {name}")
    if "bytes" in receipt and path.stat().st_size != receipt["bytes"]:
        raise DirectRebSweepError(f"Source byte count mismatch: {name}")
    return path


def _resolved_output_isolated(output: Path, inputs: list[Path]) -> None:
    final = output.resolve(strict=False)
    incomplete = Path(f"{final}_incomplete")
    if final.exists() or incomplete.exists():
        raise DirectRebSweepError("Output or incomplete output already exists")
    for source in inputs:
        root = source.resolve(strict=True)
        if final == root or root in final.parents or final in root.parents:
            raise DirectRebSweepError("Output must be isolated from all inputs")


def _validate_models(models: tuple[str, ...]) -> tuple[str, ...]:
    if not models or len(models) != len(set(models)) or any(model not in MODELS for model in models):
        raise DirectRebSweepError("Models must be unique choices from: direct control")
    return models


def _reference_cell_sources(reference: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    plan = reference.get("plan")
    completed = reference.get("completed_cells")
    if reference.get("state") != "complete" or not isinstance(plan, dict) or not isinstance(completed, list):
        raise DirectRebSweepError("Reference sweep is not complete")
    cells = plan.get("cells")
    if not isinstance(cells, list) or len(cells) != 10 or len(completed) != 10:
        raise DirectRebSweepError("Reference sweep must contain the exact ten-cell grid")
    expected_edges = [0.03, 0.05, 0.07, 0.10, 0.12]
    raw = [cell for cell in cells if cell.get("raw_control") is True]
    bl = [cell for cell in cells if cell.get("raw_control") is False]
    if sorted(float(cell.get("edge_threshold")) for cell in raw) != expected_edges:
        raise DirectRebSweepError("Reference raw edge ladder differs from approved grid")
    if sorted(float(cell.get("edge_threshold")) for cell in bl) != expected_edges:
        raise DirectRebSweepError("Reference BL edge ladder differs from approved grid")
    for cell in raw:
        if any(cell.get(name) is not None for name in ("tau", "z_max", "max_weight")):
            raise DirectRebSweepError("Reference raw treatment is malformed")
    for cell in bl:
        if (cell.get("tau"), cell.get("z_max"), cell.get("max_weight")) != (0.5, 0.5, 0.5):
            raise DirectRebSweepError("Reference BL treatment differs from approved fixed settings")
    receipts = {item.get("cell_id"): item for item in completed if isinstance(item, dict)}
    if set(receipts) != {cell.get("cell_id") for cell in cells}:
        raise DirectRebSweepError("Reference completed-cell identities do not match the plan")
    source_ids: dict[str, str] = {}
    for treatment, group in (("raw", raw), ("fixed_bl", bl)):
        hashes = {
            receipts[cell["cell_id"]].get("files", {}).get("all_bookmaker_edges.csv", {}).get("sha256")
            for cell in group
        }
        if len(hashes) != 1 or None in hashes:
            raise DirectRebSweepError(f"Reference {treatment} edge receipts are not threshold-independent")
        source_ids[treatment] = cast(str, sorted(group, key=lambda item: item["edge_threshold"])[0]["cell_id"])
    return cast(list[dict[str, Any]], cells), source_ids


def _match_benchmark_quotes(source: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    quoted = source.loc[(source["stat"] == "reb") & (source["quote_status"] == "quoted")].copy()
    if len(quoted) != len(benchmark):
        raise DirectRebSweepError("Reference quoted REB count differs from benchmark quote count")
    for frame in (quoted, benchmark):
        frame["player_id"] = frame["player_id"].map(str)
        frame["game_id"] = frame["game_id"].map(str)
        frame["game_date"] = frame["game_date"].map(str)
        frame["bookmaker"] = frame["bookmaker"].map(str)
        frame["line"] = pd.to_numeric(frame["line"], errors="raise")
        frame["_dup"] = frame.groupby(QUOTE_KEYS, sort=False).cumcount()
    order = [*QUOTE_KEYS, "_dup"]
    left = benchmark.sort_values(order, kind="stable").reset_index(drop=True)
    right = quoted.sort_values(order, kind="stable").reset_index(drop=True)
    if not left[order].equals(right[order]):
        raise DirectRebSweepError("Reference quote multiset differs from benchmark quotes")
    control_probability_column = "model_over" if bool(right["bl_enabled"].fillna(False).any()) else "over_prob"
    for left_name, right_name in (
        ("control_over_prob", control_probability_column),
        ("market_over", "implied_over"),
    ):
        a = _numeric_array(cast(pd.Series, left[left_name]))
        b = _numeric_array(cast(pd.Series, right[right_name]))
        if not np.allclose(a, b, rtol=1e-10, atol=1e-10, equal_nan=False):
            raise DirectRebSweepError(f"Reference quote probability mismatch: {left_name}")
    conflict_fields = [name for name in right.columns if
                       name.startswith(("over_", "under_", "selected_", "source_")) or
                       name in {"quote_decision_policy", "bet_decision_time", "requested_snapshot_time",
                                "task_id", "selection_policy_version", "market_key"}]
    for _, group in right.groupby(QUOTE_KEYS, sort=False, dropna=False):
        if len(group) > 1 and any(name in group and group[name].nunique(dropna=False) > 1 for name in conflict_fields):
            raise DirectRebSweepError("Ambiguous duplicate quote prices/provenance")
    return right.drop(columns="_dup")


def prepare_sweep(request: SweepRequest) -> Prepared:
    """Validate all saved inputs and construct an immutable execution plan."""
    models = _validate_models(request.models)
    benchmark_dir = request.benchmark_dir.resolve(strict=True)
    benchmark_manifest_path = benchmark_dir / "benchmark_manifest.json"
    benchmark = _json(benchmark_manifest_path)
    synthetic = benchmark.get("synthetic_fixture") is True
    required = {
        "state": "complete", "status": "report_complete", "decision_grade": False,
        "architecture": "direct_total_reb_quantiles", "start": "2026-10-21", "end": "2026-11-02",
        "seed": 42, "n_samples": 5000,
    }
    if any(benchmark.get(name) != value for name, value in required.items()):
        raise DirectRebSweepError("Benchmark manifest does not match the approved completed run")
    files = benchmark.get("files")
    if not isinstance(files, dict):
        raise DirectRebSweepError("Benchmark file inventory is missing")
    for name in (
        "matched_totals.csv", "matched_quotes.csv", "effective_config.json",
        "reb_direct_total_model.joblib", "report.md", "summary.json",
    ):
        _verify_receipt(benchmark_dir, name, files.get(name))
    effective = _json(benchmark_dir / "effective_config.json")
    sampler = effective.get("sampler")
    if not isinstance(sampler, dict) or sampler != {
        "clip_min": 0,
        "implementation": "MonteCarloPredictor._inverse_transform_sample",
        "n_samples": 5000,
        "rounding": False,
        "seed": 42,
    }:
        raise DirectRebSweepError("Benchmark sampler contract differs from the canonical saved-output replay")
    default_reference = Path(cast(str, benchmark.get("sweep_dir", "")))
    reference_dir = (request.reference_sweep_dir or default_reference).resolve(strict=True)
    reference_manifest_path = reference_dir / "sweep_manifest.json"
    if sha256_file(reference_manifest_path) != benchmark.get("reference_manifest_sha256"):
        raise DirectRebSweepError("Reference sweep identity/hash differs from benchmark receipt")
    reference = _json(reference_manifest_path)
    _, source_ids = _reference_cell_sources(reference)
    fixed = reference["plan"].get("fixed_config", {})
    expected_fixed = {
        "start": "2026-10-21", "end": "2026-11-02", "starting_bankroll": 2000.0,
        "flat_bet_size": 10.0, "seed": 42, "n_samples": 5000, "min_minutes_avg": 10.0,
    }
    if any(fixed.get(name) != value for name, value in expected_fixed.items()):
        raise DirectRebSweepError("Reference fixed settings differ from the approved benchmark")
    bookmakers = fixed.get("bookmakers")
    if not isinstance(bookmakers, list) or not bookmakers:
        raise DirectRebSweepError("Reference bookmaker settings are missing")
    _resolved_output_isolated(request.output_dir, [benchmark_dir, reference_dir])

    totals = _read_csv(benchmark_dir / "matched_totals.csv")
    matched_quotes = _read_csv(benchmark_dir / "matched_quotes.csv")
    if not synthetic and (len(totals) != 1579 or len(matched_quotes) != 1033):
        raise DirectRebSweepError("Benchmark row counts differ from the approved receipts")
    if synthetic and (totals.empty or matched_quotes.empty):
        raise DirectRebSweepError("Synthetic fixture must exercise populated totals and quotes")
    required_totals = {
        "player_id", "game_id", "game_date", "actual", "score_eligible",
        *{f"native_q{q}" for q in (10, 25, 50, 75, 90)},
        *{f"candidate_sampled_q{q}" for q in (10, 25, 50, 75, 90)},
    }
    required_quotes = {*QUOTE_KEYS, "occurrence", "actual", "candidate_over_prob", "control_over_prob", "market_over"}
    if not required_totals <= set(totals) or not required_quotes <= set(matched_quotes):
        raise DirectRebSweepError("Benchmark CSV schema is incomplete")
    duplicates = bool(cast(pd.Series, totals.duplicated(KEYS)).any())
    all_eligible = bool(cast(pd.Series, totals["score_eligible"]).astype(bool).all())
    if duplicates or not all_eligible:
        raise DirectRebSweepError("Benchmark totals are duplicate or outcome-incomplete")
    actual = _numeric_array(cast(pd.Series, totals["actual"]))
    if not np.isfinite(actual).all():
        raise DirectRebSweepError("Benchmark contains unknown/nonfinite outcomes")

    receipts = {item["cell_id"]: item for item in reference["completed_cells"]}
    source_receipts: dict[str, dict[str, Any]] = {}
    edges: dict[str, pd.DataFrame] = {}
    for treatment, cell_id in source_ids.items():
        receipt = receipts[cell_id]["files"]["all_bookmaker_edges.csv"]
        path = _verify_receipt(reference_dir / "cells" / cell_id, "all_bookmaker_edges.csv", receipt)
        frame = _read_csv(path)
        if len(frame) != receipt.get("rows") or list(frame.columns) != receipt.get("columns"):
            raise DirectRebSweepError(f"Reference {treatment} edge schema/count mismatch")
        edges[treatment] = frame
        source_receipts[treatment] = {
            "cell_id": cell_id,
            "sha256": receipt["sha256"],
            "rows": receipt["rows"],
        }
    raw_quoted = _match_benchmark_quotes(edges["raw"], matched_quotes)
    bl_quoted = _match_benchmark_quotes(edges["fixed_bl"], matched_quotes)
    if not raw_quoted[QUOTE_KEYS].reset_index(drop=True).equals(bl_quoted[QUOTE_KEYS].reset_index(drop=True)):
        raise DirectRebSweepError("Raw and BL source quote identities differ")

    base_cells = reference["plan"]["cells"]
    cells = tuple(
        Cell(
            model=model,
            edge_threshold=float(item["edge_threshold"]),
            tau=None if item["raw_control"] else float(item["tau"]),
            z_max=None if item["raw_control"] else float(item["z_max"]),
            max_weight=None if item["raw_control"] else float(item["max_weight"]),
            source_cell_id=source_ids["raw" if item["raw_control"] else "fixed_bl"],
        )
        for model in models
        for item in base_cells
    )
    if len(cells) != 10 * len(models):
        raise DirectRebSweepError("Planned cell count is not ten per selected model")
    repo_root = Path(__file__).resolve().parents[3]
    source_hashes = {name: sha256_file(repo_root / name) for name in DEPENDENCIES}
    source_hashes["benchmark_manifest.json"] = sha256_file(benchmark_manifest_path)
    source_hashes["reference_sweep_manifest.json"] = sha256_file(reference_manifest_path)
    plan = {
        "schema_version": SCHEMA_VERSION,
        "decision_grade": False,
        "models": list(models),
        "cell_count": len(cells),
        "cells": [cell.to_dict() for cell in cells],
        "fixed_settings": {
            **expected_fixed,
            "bookmakers": bookmakers,
            "stats": ["reb"],
            "raw_probability_policy": "empirical over clipped to [0.05,0.90], under=1-over",
            "bl_probability_policy": "incumbent BlackLittermanBlender.blend_prediction",
        },
        "benchmark": {
            "path": str(benchmark_dir),
            "manifest_sha256": source_hashes["benchmark_manifest.json"],
            "model_identity": benchmark.get("model_identity"),
            "model_sha256": benchmark.get("model_sha256"),
            "canonical_input_identity": benchmark.get("canonical_input_identity"),
            "matched_total_rows": len(totals),
            "matched_quote_occurrences": len(matched_quotes),
            "synthetic_fixture": synthetic,
        },
        "reference_sweep": {
            "path": str(reference_dir),
            "manifest_sha256": source_hashes["reference_sweep_manifest.json"],
            "input_identity": reference.get("input_identity"),
            "source_receipts": source_receipts,
        },
        "source_hashes": source_hashes,
    }
    plan["plan_identity"] = _canonical_hash(plan)
    return Prepared(
        request=SweepRequest(benchmark_dir, request.output_dir.resolve(strict=False), reference_dir, models),
        benchmark_manifest=benchmark,
        reference_manifest=reference,
        benchmark_dir=benchmark_dir,
        reference_dir=reference_dir,
        totals=totals,
        matched_quotes=matched_quotes,
        raw_edges=edges["raw"],
        bl_edges=edges["fixed_bl"],
        cells=cells,
        source_receipts=source_receipts,
        source_hashes=source_hashes,
        plan=plan,
    )


def dry_run_summary(prepared: Prepared) -> dict[str, Any]:
    """Return side-effect-free validated planning output."""
    return {
        "schema_version": SCHEMA_VERSION,
        "decision_grade": False,
        "benchmark_dir": str(prepared.benchmark_dir),
        "reference_sweep_dir": str(prepared.reference_dir),
        "models": list(prepared.request.models),
        "matched_total_rows": len(prepared.totals),
        "matched_quote_occurrences": len(prepared.matched_quotes),
        "settings_per_model": 10,
        "cell_count": len(prepared.cells),
        "plan_identity": prepared.plan["plan_identity"],
        "fixed_settings": prepared.plan["fixed_settings"],
    }


def _reconstruct_direct(prepared: Prepared) -> tuple[pd.DataFrame, dict[tuple[str, str, str], np.ndarray]]:
    totals = prepared.totals.copy()
    totals["player_id"] = totals["player_id"].map(str)
    totals["game_id"] = totals["game_id"].map(str)
    totals["game_date"] = totals["game_date"].map(str)
    totals = totals.sort_values(["game_date", "game_id", "player_id"], kind="stable").reset_index(drop=True)
    sampler = MonteCarloPredictor(None, n_samples=5000, random_state=42)
    rows: list[dict[str, Any]] = []
    sample_map: dict[tuple[str, str, str], np.ndarray] = {}
    for row in totals.to_dict("records"):
        native = np.asarray([row[f"native_q{q}"] for q in (10, 25, 50, 75, 90)], dtype=float)
        if not np.isfinite(native).all() or (np.diff(native) < -1e-10).any():
            raise DirectRebSweepError("Direct native quantiles are nonfinite or nonmonotone")
        samples = np.maximum(sampler._inverse_transform_sample(QUANTILES, native), 0)
        sampled = np.quantile(samples, QUANTILES)
        saved = np.asarray([row[f"candidate_sampled_q{q}"] for q in (10, 25, 50, 75, 90)], dtype=float)
        if not np.allclose(sampled, saved, rtol=1e-12, atol=1e-12):
            raise DirectRebSweepError("Direct sample quantiles do not reproduce benchmark receipt")
        key = (str(row["player_id"]), str(row["game_id"]), "reb")
        sample_map[key] = samples
        rows.append({
            "player_id": key[0], "game_id": key[1], "stat": "reb", "game_date": row["game_date"],
            "pred_mean": float(np.mean(samples)), "pred_std": float(np.std(samples)),
            "pred_median": float(sampled[2]),
            **{f"pred_q{q}": float(value) for q, value in zip((10, 25, 50, 75, 90), sampled, strict=True)},
            **{f"native_q{q}": float(value) for q, value in zip((10, 25, 50, 75, 90), native, strict=True)},
        })
    predictions = pd.DataFrame(rows)
    lookup = {(r[0], r[1]): samples for (r0, r1, _), samples in sample_map.items() for r in [(r0, r1)]}
    for quote in prepared.matched_quotes.to_dict("records"):
        samples = lookup.get((str(quote["player_id"]), str(quote["game_id"])))
        if samples is None:
            raise DirectRebSweepError("Benchmark quote lacks reconstructed direct samples")
        probability = float((samples > float(quote["line"])).mean())
        if abs(probability - float(quote["candidate_over_prob"])) > (1.0 / 5000 + 1e-12):
            raise DirectRebSweepError("Direct over probability does not reproduce benchmark receipt")
    return predictions, sample_map


def _lines_from_raw(prepared: Prepared) -> pd.DataFrame:
    quoted = prepared.raw_edges.loc[
        (prepared.raw_edges["stat"] == "reb") & (prepared.raw_edges["quote_status"] == "quoted")
    ].copy()
    quoted["market_key"] = "player_rebounds"
    base = {
        "player_id", "game_id", "market_key", "line", "bookmaker", "over_odds", "under_odds",
    }
    metadata = [name for name in quoted.columns if name not in {
        "stat", "game_date", "player_name", "team_id", "game_time", "pred_mean", "pred_std",
        "pred_median", "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90",
        "sample_status", "quote_status", "over_prob", "under_prob", "implied_over", "implied_under",
        "over_edge", "under_edge", "model_over", "model_under", "market_over", "market_under",
        "confidence", "posterior_over", "posterior_under", "bl_enabled", "selected_side",
        "selected_line", "selected_price", "max_edge",
    }]
    columns = [name for name in quoted.columns if name in base or name in metadata]
    return quoted.loc[:, list(dict.fromkeys(columns))]


def _actuals(prepared: Prepared) -> pd.DataFrame:
    actuals = prepared.totals.loc[:, ["player_id", "game_id", "actual"]].copy()
    actuals["player_id"] = actuals["player_id"].map(str)
    actuals["game_id"] = actuals["game_id"].map(str)
    actuals["stat"] = "reb"
    return actuals.rename(columns={"actual": "actual_value"})


def _harness(prepared: Prepared, cell: Cell, *, calculate_edges: bool) -> BacktestHarness:
    blender = None
    if calculate_edges and cell.tau is not None:
        blender = BlackLittermanBlender(
            BLConfig(tau=cell.tau, z_max=cast(float, cell.z_max), max_weight=cast(float, cell.max_weight))
        )
    return BacktestHarness(
        engine=None,
        feature_store=None,
        model_pipeline=None,
        predictor=None,
        stats=["reb"],
        bookmakers=list(prepared.plan["fixed_settings"]["bookmakers"]),
        min_minutes_avg=10,
        starting_bankroll=2000.0,
        flat_bet_size=10.0,
        edge_threshold=cell.edge_threshold,
        bl_blender=blender,
    )


def _simulate(prepared: Prepared, cell: Cell, edges: pd.DataFrame) -> BacktestResult:
    harness = _harness(prepared, cell, calculate_edges=False)
    selected = harness._filter_best_bets(edges.copy())
    if not selected.empty:
        selected["game_date"] = pd.to_datetime(selected["game_date"], errors="raise").dt.date
        selected = selected.sort_values(["game_date", "game_id", "player_id"], kind="stable").reset_index(drop=True)
    actuals = _actuals(prepared)
    simulator: BetSimulator = harness._simulator
    for game_date in sorted(selected["game_date"].unique()) if not selected.empty else []:
        simulator.resolve_bets(actuals)
        simulator.evaluate_predictions(selected.loc[selected["game_date"] == game_date], cast(date, game_date))
    simulator.resolve_bets(actuals)
    bets = simulator.to_dataframe().reindex(columns=BET_COLUMNS)
    predictions = harness._merge_actuals(selected, actuals) if not selected.empty else selected.copy()
    metrics = harness._metrics_calc.calculate(predictions, bets, starting_bankroll=2000.0)
    schema_identity = _canonical_hash({
        "predictions.csv": list(predictions.columns),
        "bets.csv": list(bets.columns),
        "all_bookmaker_edges.csv": list(edges.columns),
    })
    config = {
        **cell.to_dict(),
        "schema_version": SCHEMA_VERSION,
        "decision_grade": False,
        "input_identity": prepared.plan["plan_identity"],
        "output_schema_identity": schema_identity,
        "starting_bankroll": 2000.0,
        "flat_bet_size": 10.0,
        "stats": ["reb"],
        "bookmakers": prepared.plan["fixed_settings"]["bookmakers"],
        "empty_reason": "no_bets" if bets.empty else None,
    }
    return BacktestResult(
        predictions_df=predictions,
        bets_df=bets,
        all_edges_df=edges,
        metrics=metrics,
        start_date=date(2026, 10, 21),
        end_date=date(2026, 11, 2),
        config=config,
    )


def evaluate_cell(
    prepared: Prepared,
    cell: Cell,
    direct_predictions: pd.DataFrame,
    direct_samples: dict[tuple[str, str, str], np.ndarray],
    lines: pd.DataFrame,
) -> BacktestResult:
    """Evaluate one saved-window cell through incumbent numerical owners."""
    if cell.model == "direct":
        harness = _harness(prepared, cell, calculate_edges=True)
        edges = harness._calculate_edges(direct_predictions.copy(), lines.copy(), direct_samples)
        edges["quote_status"] = np.where(edges["line"].notna(), "quoted", "no_quote")
        edges["market_key"] = "player_rebounds"
    else:
        source = prepared.raw_edges if cell.tau is None else prepared.bl_edges
        edges = source.loc[source["stat"] == "reb"].copy()
    return _simulate(prepared, cell, edges)


def _recheck_sources(prepared: Prepared) -> None:
    if sha256_file(prepared.benchmark_dir / "benchmark_manifest.json") != prepared.source_hashes["benchmark_manifest.json"]:
        raise DirectRebSweepError("Benchmark manifest mutated during execution")
    for name, receipt in prepared.benchmark_manifest["files"].items():
        _verify_receipt(prepared.benchmark_dir, name, receipt)
    if sha256_file(prepared.reference_dir / "sweep_manifest.json") != prepared.source_hashes["reference_sweep_manifest.json"]:
        raise DirectRebSweepError("Reference sweep manifest mutated during execution")
    for treatment, receipt in prepared.source_receipts.items():
        path = prepared.reference_dir / "cells" / receipt["cell_id"] / "all_bookmaker_edges.csv"
        if sha256_file(path) != receipt["sha256"]:
            raise DirectRebSweepError(f"Reference {treatment} edges mutated during execution")
    repo_root = Path(__file__).resolve().parents[3]
    for name in DEPENDENCIES:
        if sha256_file(repo_root / name) != prepared.source_hashes[name]:
            raise DirectRebSweepError(f"Runtime dependency mutated during execution: {name}")


def _verify_cell_receipt(root: Path, receipt: dict[str, Any]) -> None:
    cell_dir = root / "cells" / receipt["cell_id"]
    expected = {"config.json", "metrics.json", "predictions.csv", "bets.csv", "all_bookmaker_edges.csv"}
    if set(receipt.get("files", {})) != expected:
        raise DirectRebSweepError("Cell receipt file set is incomplete")
    for name, evidence in receipt["files"].items():
        path = cell_dir / name
        if not path.is_file() or sha256_file(path) != evidence.get("sha256"):
            raise DirectRebSweepError(f"Cell output hash mismatch: {receipt['cell_id']}/{name}")
        if name.endswith(".csv"):
            frame = _read_csv(path)
            if len(frame) != evidence.get("rows") or list(frame.columns) != evidence.get("columns"):
                raise DirectRebSweepError(f"Cell output count/schema mismatch: {receipt['cell_id']}/{name}")


def run_sweep(prepared: Prepared) -> Path:
    """Execute the complete selected model grid and publish only after readback."""
    current = prepare_sweep(prepared.request)
    if current.plan["plan_identity"] != prepared.plan["plan_identity"]:
        raise DirectRebSweepError("Prepared plan is stale")
    root, final = create_exclusive_incomplete_dir(
        prepared.request.output_dir.parent, prepared.request.output_dir.name
    )
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "state": "incomplete",
        "decision_grade": False,
        "plan": prepared.plan,
        "planned_cell_ids": [cell.cell_id for cell in prepared.cells],
        "completed_cells": [],
        "outputs": {},
        "failure": None,
    }
    stage = "initialization"
    try:
        _write_json(root / "direct_reb_sweep_manifest.json", manifest)
        stage = "direct_sample_reconstruction"
        predictions, samples = _reconstruct_direct(prepared)
        lines = _lines_from_raw(prepared)
        summaries: list[dict[str, Any]] = []
        receipts: list[dict[str, Any]] = []
        for index, cell in enumerate(prepared.cells, start=1):
            stage = f"cell:{cell.cell_id}"
            result = evaluate_cell(prepared, cell, predictions, samples, lines)
            receipt = export_cell(root, result)
            receipts.append(receipt)
            metrics = result.metrics.to_dict()
            summaries.append({
                **cell.to_dict(),
                "predictions": len(result.predictions_df),
                "candidate_edges": len(result.all_edges_df),
                **metrics["betting"],
                **metrics["risk"],
                "decision_grade": False,
            })
            manifest["completed_cells"] = receipts.copy()
            _write_json(root / "direct_reb_sweep_manifest.json", manifest, replace=True)
            print(
                f"[{index}/{len(prepared.cells)}] model={cell.model} edge={cell.edge_threshold:.2f} "
                f"treatment={cell.treatment} bets={result.metrics.total_bets} "
                f"profit=${result.metrics.total_profit:.2f} ROI={result.metrics.roi:.2%} "
                f"Sharpe={result.metrics.sharpe_ratio:.3f}",
                flush=True,
            )
        stage = "completion_verification"
        summary = pd.DataFrame(summaries)
        manifest["outputs"]["sweep_summary.csv"] = _write_csv(root / "sweep_summary.csv", summary)
        manifest["outputs"]["summary.json"] = None
        summary_json = {
            "schema_version": SCHEMA_VERSION,
            "decision_grade": False,
            "models": list(prepared.request.models),
            "cell_count": len(prepared.cells),
            "matched_total_rows": len(prepared.totals),
            "matched_quote_occurrences": len(prepared.matched_quotes),
            "limitations": [
                "Saved discovery window only; no arbitrary-date inference.",
                "Raw replay preserves incumbent over clamp/complement semantics.",
                "Benchmark Brier used unbounded empirical over probabilities; betting replay uses incumbent policy.",
                "Cells are alternative policies; profits must not be summed.",
                "No model promotion, production policy change, or CLV claim is authorized.",
            ],
        }
        _write_json(root / "summary.json", summary_json)
        manifest["outputs"]["summary.json"] = {"sha256": sha256_file(root / "summary.json")}
        if len(receipts) != len(prepared.cells) or {r["cell_id"] for r in receipts} != {c.cell_id for c in prepared.cells}:
            raise DirectRebSweepError("Completed cells differ from planned cells")
        for receipt in receipts:
            _verify_cell_receipt(root, receipt)
        _recheck_sources(prepared)
        manifest["state"] = "complete"
        _write_json(root / "direct_reb_sweep_manifest.json", manifest, replace=True)
        stored = _json(root / "direct_reb_sweep_manifest.json")
        if stored.get("state") != "complete" or len(stored.get("completed_cells", [])) != len(prepared.cells):
            raise DirectRebSweepError("Final manifest readback failed")
        _rename_no_replace(root, final)
        return final
    except BaseException as exc:
        manifest["state"] = "failed"
        manifest["failure"] = {"stage": stage, "type": type(exc).__name__}
        try:
            _write_json(root / "direct_reb_sweep_manifest.json", manifest, replace=True)
        except Exception:
            pass
        if isinstance(exc, KeyboardInterrupt | SystemExit):
            raise
        if isinstance(exc, DirectRebSweepError):
            raise
        raise DirectRebSweepError(f"Sweep failed at {stage}: {type(exc).__name__}") from exc


def _accounting(bets: pd.DataFrame) -> dict[str, float | int]:
    if bets.empty:
        return {"bets": 0, "wins": 0, "losses": 0, "pushes": 0, "staked": 0.0, "profit": 0.0, "roi": 0.0}
    numeric = bets[["actual", "line", "odds", "stake", "profit"]].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(numeric.to_numpy(dtype=np.float64)).all() or bool(cast(pd.Series, numeric["stake"] < 0).any()):
        raise DirectRebSweepError("Bet ledger contains nonfinite accounting values")
    reconstructed: list[tuple[str, float]] = []
    for row in _records(bets):
        side = row.get("side")
        if side not in ("over", "under") or row.get("stat") != "reb":
            raise DirectRebSweepError("Bet ledger contains unsupported stat/side")
        actual, line, odds, stake = map(float, (row["actual"], row["line"], row["odds"], row["stake"]))
        if actual == line:
            outcome, profit = "push", 0.0
        else:
            win = actual > line if side == "over" else actual < line
            outcome = "win" if win else "loss"
            profit = stake * (odds / 100 if odds > 0 else 100 / abs(odds)) if win else -stake
        reconstructed.append((outcome, profit))
    expected_outcomes = [item[0] for item in reconstructed]
    expected_profit = np.asarray([item[1] for item in reconstructed])
    if list(bets["outcome"].astype(str)) != expected_outcomes or not np.allclose(
        _numeric_array(cast(pd.Series, bets["profit"])), expected_profit, rtol=1e-12, atol=1e-10
    ):
        raise DirectRebSweepError("Bet payoff/outcome accounting does not reproduce")
    wins = expected_outcomes.count("win")
    losses = expected_outcomes.count("loss")
    pushes = expected_outcomes.count("push")
    staked = _numeric_sum(cast(pd.Series, bets.loc[bets["outcome"] != "push", "stake"]))
    profit = float(expected_profit.sum())
    return {
        "bets": len(bets), "wins": wins, "losses": losses, "pushes": pushes,
        "staked": staked, "profit": profit, "roi": profit / staked if staked else 0.0,
    }


def _load_verified_sweep(sweep_dir: Path) -> tuple[dict[str, Any], pd.DataFrame, dict[str, dict[str, pd.DataFrame]]]:
    root = sweep_dir.resolve(strict=True)
    manifest = _json(root / "direct_reb_sweep_manifest.json")
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("state") != "complete":
        raise DirectRebSweepError("Analysis requires a complete direct-REB sweep bundle")
    planned = manifest.get("planned_cell_ids")
    completed = manifest.get("completed_cells")
    if not isinstance(planned, list) or not isinstance(completed, list) or len(planned) != len(completed):
        raise DirectRebSweepError("Sweep planned/completed cell counts disagree")
    if set(planned) != {item.get("cell_id") for item in completed if isinstance(item, dict)}:
        raise DirectRebSweepError("Sweep planned/completed cell identities disagree")
    outputs = manifest.get("outputs", {})
    if set(outputs) != {"sweep_summary.csv", "summary.json"}:
        raise DirectRebSweepError("Sweep output receipt inventory is incomplete")
    for name, receipt in outputs.items():
        _verify_receipt(root, name, receipt)
    summary_receipt = outputs["sweep_summary.csv"]
    if sha256_file(root / "sweep_summary.csv") != summary_receipt.get("sha256"):
        raise DirectRebSweepError("Sweep summary receipt mismatch")
    summary = _read_csv(root / "sweep_summary.csv")
    if len(summary) != len(planned) or set(summary["cell_id"]) != set(planned):
        raise DirectRebSweepError("Sweep summary cell inventory mismatch")
    tables: dict[str, dict[str, pd.DataFrame]] = {}
    for receipt in completed:
        _verify_cell_receipt(root, receipt)
        cell_id = receipt["cell_id"]
        tables[cell_id] = {
            name: _read_csv(root / "cells" / cell_id / name)
            for name in ("bets.csv", "predictions.csv", "all_bookmaker_edges.csv")
        }
        accounting = _accounting(tables[cell_id]["bets.csv"])
        row = summary.loc[summary["cell_id"] == cell_id].iloc[0]
        for field, summary_field in (("bets", "total_bets"), ("staked", "total_staked"), ("profit", "total_profit"), ("roi", "roi")):
            if not math.isclose(float(accounting[field]), float(row[summary_field]), rel_tol=1e-10, abs_tol=1e-9):
                raise DirectRebSweepError(f"Summary/accounting mismatch: {cell_id}/{summary_field}")
        metrics = _json(root / "cells" / cell_id / "metrics.json")
        recalculated = MetricsCalculator().calculate(
            tables[cell_id]["predictions.csv"], tables[cell_id]["bets.csv"], starting_bankroll=2000.0
        ).to_dict()
        for section in ("betting", "risk"):
            for name, expected in recalculated[section].items():
                saved_value = float(metrics.get(section, {}).get(name, math.nan))
                summary_value = float(row.get(name, math.nan))
                if not (math.isclose(saved_value, float(expected), rel_tol=1e-10, abs_tol=1e-9)
                        and math.isclose(summary_value, float(expected), rel_tol=1e-10, abs_tol=1e-9)):
                    raise DirectRebSweepError(f"Metrics recomputation mismatch: {cell_id}/{name}")
    return manifest, summary, tables


def _group_summary(summary: pd.DataFrame, tables: dict[str, dict[str, pd.DataFrame]], field: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for config in _records(summary):
        bets = tables[config["cell_id"]]["bets.csv"]
        if bets.empty:
            continue
        for value, group in bets.groupby(field, dropna=False, sort=True):
            accounting = _accounting(group.reset_index(drop=True))
            rows.append({
                "cell_id": config["cell_id"], "model": config["model"],
                "treatment": config["treatment"], "edge_threshold": config["edge_threshold"],
                field: value, **accounting,
            })
    return pd.DataFrame(rows)


def _markdown_table(frame: pd.DataFrame) -> str:
    """Render a compact Markdown table without an optional tabulate dependency."""
    if frame.empty:
        return "No rows."
    columns = [str(name) for name in frame.columns]
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in frame.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def _paired_tables(summary: pd.DataFrame, tables: dict[str, dict[str, pd.DataFrame]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    paired: list[dict[str, Any]] = []
    decomposition: list[dict[str, Any]] = []
    settings = cast(pd.DataFrame, summary[["treatment", "edge_threshold"]].drop_duplicates())
    wager_key = ["player_id", "game_id", "stat", "side", "line", "odds", "bookmaker"]
    for setting in _records(settings):
        subset = summary.loc[
            (summary["treatment"] == setting["treatment"])
            & (summary["edge_threshold"] == setting["edge_threshold"])
        ]
        if set(subset["model"]) != {"direct", "control"}:
            continue
        records = {row["model"]: row for row in _records(subset)}
        direct, control = records["direct"], records["control"]
        paired.append({
            "treatment": setting["treatment"], "edge_threshold": setting["edge_threshold"],
            "direct_cell_id": direct["cell_id"], "control_cell_id": control["cell_id"],
            **{f"direct_{name}": direct[name] for name in ("total_bets", "total_profit", "roi", "sharpe_ratio", "max_drawdown")},
            **{f"control_{name}": control[name] for name in ("total_bets", "total_profit", "roi", "sharpe_ratio", "max_drawdown")},
            "delta_bets": direct["total_bets"] - control["total_bets"],
            "delta_profit": direct["total_profit"] - control["total_profit"],
            "delta_roi": direct["roi"] - control["roi"],
            "delta_sharpe": direct["sharpe_ratio"] - control["sharpe_ratio"],
            "delta_max_drawdown": direct["max_drawdown"] - control["max_drawdown"],
        })
        ledgers: dict[str, pd.DataFrame] = {}
        for model, record in records.items():
            ledger = tables[record["cell_id"]]["bets.csv"].copy()
            ledger["_occurrence"] = ledger.groupby(wager_key, dropna=False, sort=False).cumcount()
            ledgers[model] = ledger
        left = ledgers["direct"].merge(
            ledgers["control"], on=[*wager_key, "_occurrence"], how="outer", suffixes=("_direct", "_control"), indicator=True
        )
        for bucket, indicator in (("common", "both"), ("direct_only", "left_only"), ("control_only", "right_only")):
            group = left.loc[left["_merge"] == indicator]
            direct_profit = _numeric_sum(cast(pd.Series, group.get("profit_direct", pd.Series(dtype=float))))
            control_profit = _numeric_sum(cast(pd.Series, group.get("profit_control", pd.Series(dtype=float))))
            direct_stake = _numeric_sum(cast(pd.Series, group.get("stake_direct", pd.Series(dtype=float))))
            control_stake = _numeric_sum(cast(pd.Series, group.get("stake_control", pd.Series(dtype=float))))
            decomposition.append({
                "treatment": setting["treatment"], "edge_threshold": setting["edge_threshold"],
                "bucket": bucket, "wagers": len(group),
                "direct_profit": direct_profit, "control_profit": control_profit,
                "direct_stake": direct_stake, "control_stake": control_stake,
                "stake_difference": direct_stake - control_stake,
                "profit_difference": direct_profit - control_profit,
            })
    return pd.DataFrame(paired), pd.DataFrame(decomposition)


def analyze_sweep(sweep_dir: Path, output_dir: Path) -> Path:
    """Verify and analyze a completed direct-REB sweep without replay or fitting."""
    root = sweep_dir.resolve(strict=True)
    output = output_dir.resolve(strict=False)
    _resolved_output_isolated(output, [root])
    manifest, summary, tables = _load_verified_sweep(root)
    benchmark_root = Path(manifest["plan"]["benchmark"]["path"])
    benchmark_manifest_path = benchmark_root / "benchmark_manifest.json"
    if sha256_file(benchmark_manifest_path) != manifest["plan"]["benchmark"]["manifest_sha256"]:
        raise DirectRebSweepError("Analysis benchmark manifest differs from sweep receipt")
    benchmark_manifest = _json(benchmark_manifest_path)
    forecast_path = _verify_receipt(benchmark_root, "summary.json", benchmark_manifest["files"].get("summary.json"))
    benchmark_summary = _json(forecast_path)
    analysis_root, final = create_exclusive_incomplete_dir(output.parent, output.name)
    try:
        paired, decomposition = _paired_tables(summary, tables)
        side = _group_summary(summary, tables, "side")
        by_date = _group_summary(summary, tables, "game_date")
        by_book = _group_summary(summary, tables, "bookmaker")
        portfolio_rows = []
        for row in _records(summary):
            bets = tables[row["cell_id"]]["bets.csv"]
            identity_frame = cast(pd.DataFrame, bets[
                ["player_id", "game_id", "stat", "side", "line", "odds", "bookmaker", "stake"]
            ].fillna("").astype(str))
            identity = _canonical_hash(_records(identity_frame))
            portfolio_rows.append({"cell_id": row["cell_id"], "portfolio_identity": identity})
        portfolios = pd.DataFrame(portfolio_rows)
        portfolios["repeated_cells"] = portfolios.groupby("portfolio_identity")["cell_id"].transform("count")
        receipts = {
            "matched_cells.csv": _write_csv(analysis_root / "matched_cells.csv", paired),
            "wager_decomposition.csv": _write_csv(analysis_root / "wager_decomposition.csv", decomposition),
            "side_summary.csv": _write_csv(analysis_root / "side_summary.csv", side),
            "date_summary.csv": _write_csv(analysis_root / "date_summary.csv", by_date),
            "book_summary.csv": _write_csv(analysis_root / "book_summary.csv", by_book),
            "portfolio_identities.csv": _write_csv(analysis_root / "portfolio_identities.csv", portfolios),
        }

        raw = summary.loc[summary["treatment"] == "raw"].sort_values(["model", "edge_threshold"])
        fixed5 = summary.loc[summary["edge_threshold"] == 0.05].sort_values(["model", "treatment"])
        best = summary.sort_values("roi", ascending=False).iloc[0]
        analysis = {
            "schema_version": ANALYSIS_VERSION,
            "decision_grade": False,
            "source_sweep": str(root),
            "source_manifest_sha256": sha256_file(root / "direct_reb_sweep_manifest.json"),
            "models": sorted(summary["model"].unique().tolist()),
            "paired_comparisons_available": not paired.empty,
            "verified_cells": len(summary),
            "best_roi_cell": {
                "cell_id": best["cell_id"], "model": best["model"], "treatment": best["treatment"],
                "edge_threshold": float(best["edge_threshold"]), "bets": int(best["total_bets"]),
                "roi": float(best["roi"]), "exploratory_small_sample": int(best["total_bets"]) < 100,
            },
            "benchmark_forecast_metrics": benchmark_summary,
            "files": receipts,
            "limitations": [
                "Discovery-window saved-artifact analysis; no independent validation window.",
                "Grid peaks are not crowned as promotion winners.",
                "Competing cell profits are not additive and are never summed.",
                "Common/direct-only/control-only buckets are descriptive saved-wager decompositions.",
                "No CLV, ranker, production policy, or architecture decision is implied.",
            ],
        }
        _write_json(analysis_root / "analysis.json", analysis)
        report = [
            "# NBA direct-total REB saved-window sweep analysis", "",
            "Receipt-verified, artifact-only analysis. Decision grade: false.", "",
            f"- Verified cells: {len(summary)}",
            f"- Models: {', '.join(analysis['models'])}",
            f"- Paired comparisons available: {analysis['paired_comparisons_available']}",
            f"- Best observed ROI cell: `{best['cell_id']}` ({best['model']}, {best['treatment']}, edge {best['edge_threshold']:.2f}, {int(best['total_bets'])} bets, ROI {best['roi']:.2%})",
            "- Best observed cells below 100 bets are exploratory; no post-grid peak is promoted.", "",
            "## Raw ladder", "",
            _markdown_table(raw[["model", "edge_threshold", "total_bets", "total_profit", "roi", "sharpe_ratio", "max_drawdown"]]), "",
            "## Fixed 5% raw / BL comparison", "",
            _markdown_table(fixed5[["model", "treatment", "total_bets", "total_profit", "roi", "sharpe_ratio", "max_drawdown"]]), "",
            "## Paired direct minus control", "",
            (_markdown_table(paired) if not paired.empty else "Unavailable: this sweep contains only one model lane."), "",
            "## Forecast benchmark (separate evidence)", "",
            f"- Matched totals: {benchmark_summary.get('matched_total_rows')}",
            f"- Matched quote occurrences: {benchmark_summary.get('matched_quote_occurrences')}",
            f"- Candidate minus control mean pinball: {benchmark_summary.get('candidate_minus_control', {}).get('mean_pinball')}",
            f"- Candidate minus control Brier: {benchmark_summary.get('candidate_minus_control', {}).get('brier')}",
            "- Forecast scores and betting results share saved quote identity but remain distinct discovery-window evidence.", "",
            "## Limitations", "",
            *[f"- {item}" for item in analysis["limitations"]], "",
        ]
        (analysis_root / "report.md").write_text("\n".join(report), encoding="utf-8", newline="\n")
        analysis["files"]["analysis.json"] = {"sha256": sha256_file(analysis_root / "analysis.json")}
        analysis["files"]["report.md"] = {"sha256": sha256_file(analysis_root / "report.md")}
        _write_json(analysis_root / "analysis_manifest.json", {
            "schema_version": ANALYSIS_VERSION,
            "state": "complete",
            "decision_grade": False,
            "source_manifest_sha256": analysis["source_manifest_sha256"],
            "files": analysis["files"],
        })
        for name, receipt in analysis["files"].items():
            if sha256_file(analysis_root / name) != receipt["sha256"]:
                raise DirectRebSweepError(f"Analysis output hash readback failed: {name}")
        _rename_no_replace(analysis_root, final)
        return final
    except BaseException:
        try:
            _write_json(analysis_root / "analysis_failure.json", {"state": "failed"})
        except Exception:
            pass
        raise
