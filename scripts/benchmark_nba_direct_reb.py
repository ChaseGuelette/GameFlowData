"""Fit and matched-score a standalone direct-total NBA REB quantile benchmark."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--model-dir", required=True, help="Exact completed decomposition-control artifact")
    parser.add_argument("--sweep-dir", required=True, help="Exact completed local reference sweep")
    parser.add_argument("--diagnostic-dir", required=True, help="Exact completed reproduced error-stage diagnostic")
    parser.add_argument("--output-dir", required=True, help="New isolated benchmark directory; no overwrites")
    parser.add_argument("--local", action="store_true", help="Required: use only the local PostgreSQL mirror")
    parser.add_argument("--max-games", type=int, default=200, help="Cap evaluation games (1..200)")
    parser.add_argument("--max-player-games", type=int, default=2000, help="Cap evaluation feature rows")
    parser.add_argument("--max-outcome-rows", type=int, default=10000, help="Cap evaluation outcome rows")
    parser.add_argument("--max-outcome-games", type=int, default=200, help="Cap evaluation outcome games")
    parser.add_argument("--max-training-rows", type=int, default=100000,
                        help="Independent cap for each loaded train/cal population (1..150000)")
    parser.add_argument("--tune", action="store_true",
                        help="Opt in to fresh shared-parameter pinball tuning")
    parser.add_argument("--tuning-trials", type=int, default=None,
                        help="Shared Optuna trial budget (1..200; default 100 with --tune)")
    parser.add_argument("--reselect-features", action="store_true",
                        help="Opt in to per-quantile feature reselection on inner training rows")
    parser.add_argument("--dry-run", action="store_true",
                        help="Filesystem/config preflight only; no connection, model load, fit, or output")
    return parser


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def execute(prepared: dict) -> Path:
    from src.backtesting.nba.sweep_runner import (
        SweepFailure,
        acquire_shared_inputs,
        bootstrap_runtime,
    )
    from src.diagnostics.nba_direct_reb_benchmark import (
        BenchmarkError,
        evaluate_direct,
        file_hash,
        fit_direct_model,
        load_training_populations,
        manifest_inventory,
        render_report,
        source_hashes,
    )
    from src.diagnostics.nba_error_stages import (
        BoundedFeatureStore,
        install_read_only_guard,
        load_actual_minutes,
    )
    from src.models.nba_artifacts import (
        _rename_no_replace,
        create_exclusive_incomplete_dir,
        validate_artifact_bundle,
    )

    info, cfg, plan = prepared["info"], prepared["config"], prepared["plan"]
    output = Path(info["output_dir"])
    incomplete, final = create_exclusive_incomplete_dir(output.parent, output.name)
    repo_root = Path(__file__).resolve().parents[1]
    runtime = None
    stage = "runtime_bootstrap"
    manifest = {
        **info, "execution": True, "full_execution": True, "state": "incomplete", "status": "in_progress",
        "started_utc": datetime.now(UTC).isoformat(), "stage": stage,
        "source_hashes": source_hashes(repo_root),
        "benchmark_source": {
            "scripts/benchmark_nba_direct_reb.py": file_hash(Path(__file__)),
            "src/diagnostics/nba_direct_reb_benchmark.py": file_hash(
                repo_root / "src/diagnostics/nba_direct_reb_benchmark.py"),
        },
    }
    _write_json(incomplete / "benchmark_manifest.json", manifest)
    try:
        runtime = bootstrap_runtime(cfg, plan)
        engine = runtime.harness.engine
        guards = install_read_only_guard(engine)
        stage = "outcome_preflight"
        manifest["stage"] = stage
        _write_json(incomplete / "benchmark_manifest.json", manifest)
        # The cap is in player-games, not the harness's melted stat/combo rows.
        outcomes = load_actual_minutes(engine, cfg, info["max_outcome_rows"])
        if outcomes["game_id"].nunique() > info["max_outcome_games"]:
            raise BenchmarkError("Evaluation outcome games exceed --max-outcome-games")
        outcome_keys = set(zip(outcomes["player_id"].map(str), outcomes["game_id"].map(str), strict=True))
        manifest["outcome_player_games"] = len(outcome_keys)
        stage = "training_acquisition"
        manifest["stage"] = stage
        _write_json(incomplete / "benchmark_manifest.json", manifest)
        train, calibration, chronology = load_training_populations(engine, prepared)
        stage = "direct_fit"
        manifest["stage"] = stage
        _write_json(incomplete / "benchmark_manifest.json", manifest)
        suite, fit_receipt = fit_direct_model(prepared, train, calibration)
        actual_features = suite.all_feature_names
        feature_evidence = fit_receipt["feature_selection"]
        model_path = incomplete / info["model_filename"]
        suite.save(str(model_path))
        if any(value != 0.0 for value in suite.calibration_offsets.values()):
            raise BenchmarkError("Direct suite offsets were not cleared before save")
        _write_json(incomplete / "fit_calibration_scores.json", fit_receipt)
        if fit_receipt["tuning"]["effective"]:
            _write_json(incomplete / "tuning_results.json", fit_receipt["tuning"])
        _write_json(incomplete / "effective_config.json", {
            "model": suite.config.to_dict(), "features": actual_features,
            "features_per_quantile": feature_evidence["selected_per_quantile"],
            "feature_policy": feature_evidence["policy"],
            "tuning": fit_receipt["tuning"],
            "target": "actual_reb", "fit_rule": "actual_minutes >= 10; finite nonnegative actual_reb",
            "calibration_use": "evaluation_only", "sampler": {
                "implementation": "MonteCarloPredictor._inverse_transform_sample", "n_samples": cfg.n_samples,
                "seed": cfg.seed, "clip_min": 0, "rounding": False,
            },
        })
        stage = "canonical_acquisition"
        manifest["stage"] = stage
        _write_json(incomplete / "benchmark_manifest.json", manifest)
        runtime.harness.feature_store = BoundedFeatureStore(
            runtime.harness.feature_store, info["max_games"], info["max_player_games"],
        )
        try:
            shared = acquire_shared_inputs(cfg, plan, runtime)
        except SweepFailure as exc:
            if isinstance(exc.__context__, BenchmarkError):
                raise exc.__context__ from None
            raise
        canonical_outcomes = shared.evidence["actuals"]
        canonical_keys = set(zip(canonical_outcomes["player_id"].map(str),
                                 canonical_outcomes["game_id"].map(str), strict=True))
        if canonical_keys != outcome_keys:
            raise BenchmarkError("Canonical outcomes differ from bounded pre-fit player-game population")
        stage = "matched_evaluation"
        manifest["stage"] = stage
        _write_json(incomplete / "benchmark_manifest.json", manifest)
        summary, exports = evaluate_direct(prepared, suite, shared)
        shared.assert_unchanged(plan)
        stage = "source_readback"
        inspection = validate_artifact_bundle(cfg.model_dir, cfg.stats, allow_legacy=False)
        if inspection.identity != plan["artifact"]["identity"]:
            raise BenchmarkError("Source model changed during benchmark")
        if file_hash(Path(info["sweep_dir"]) / "sweep_manifest.json") != info["reference_manifest_sha256"]:
            raise BenchmarkError("Reference sweep manifest changed during benchmark")
        if file_hash(Path(info["diagnostic_dir"]) / "diagnostic_manifest.json") != info["diagnostic_manifest_sha256"]:
            raise BenchmarkError("Reference diagnostic manifest changed during benchmark")
        stage = "write_results"
        for name, frame in exports.items():
            frame.to_csv(incomplete / name, index=False)
        _write_json(incomplete / "summary.json", summary)
        (incomplete / "report.md").write_text(render_report(summary), encoding="utf-8")
        # Read back the scored exports before finalization.
        for name, frame in exports.items():
            observed = __import__("pandas").read_csv(
                incomplete / name, dtype={"player_id": str, "game_id": str}, low_memory=False,
            )
            if len(observed) != len(frame) or list(observed.columns) != list(frame.columns):
                raise BenchmarkError(f"Result readback mismatch: {name}")
        manifest.update(
            state="complete", status="report_complete", stage="complete",
            completed_utc=datetime.now(UTC).isoformat(), database_guards=guards,
            chronology=chronology, training_rows=len(train), calibration_rows=len(calibration),
            model_sha256=file_hash(model_path), canonical_input_identity=shared.identity,
            baseline_reproduced=True, source_artifact_unchanged=True, decision_grade=False,
            live_data_checked=True, model_deserialized=runtime.model_deserialized,
            effective_model_config=suite.config.to_dict(),
            feature_count=len(actual_features), feature_identity=feature_evidence["selected_union_identity"],
            feature_selection=feature_evidence, tuning=fit_receipt["tuning"],
        )
        manifest["files"] = manifest_inventory(incomplete, exclude={"benchmark_manifest.json"})
        _write_json(incomplete / "benchmark_manifest.json", manifest)
        _rename_no_replace(incomplete, final)
        return final
    except BaseException as exc:
        manifest.update(state="failed", status="execution_failed", stage=stage,
                        failed_stage=stage, error_type=type(exc).__name__)
        if isinstance(exc, BenchmarkError | SweepFailure):
            manifest["reason"] = str(exc)
        if incomplete.exists():
            _write_json(incomplete / "benchmark_manifest.json", manifest)
        raise
    finally:
        if runtime is not None:
            cast(Any, runtime.harness.engine).dispose()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from src.diagnostics.nba_direct_reb_benchmark import BenchmarkError, prepare_benchmark

    try:
        prepared = prepare_benchmark(
            args.model_dir, args.sweep_dir, args.diagnostic_dir, args.output_dir, local=args.local,
            max_games=args.max_games, max_player_games=args.max_player_games,
            max_outcome_rows=args.max_outcome_rows, max_outcome_games=args.max_outcome_games,
            max_training_rows=args.max_training_rows, tune=args.tune,
            tuning_trials=args.tuning_trials, reselect_features=args.reselect_features,
        )
    except (BenchmarkError, OSError, ValueError, KeyError) as exc:
        reason = str(exc) if isinstance(exc, BenchmarkError) else f"Preflight failed: {type(exc).__name__}"
        print(json.dumps({"state": "blocked", "reason": reason}), file=sys.stderr)
        return 2
    if args.dry_run:
        print(json.dumps(prepared["info"], indent=2, sort_keys=True, allow_nan=False))
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    from src.backtesting.nba.sweep_runner import SweepFailure
    try:
        output = execute(prepared)
    except KeyboardInterrupt:
        print("Benchmark interrupted; incomplete evidence retained.", file=sys.stderr)
        return 130
    except Exception as exc:
        reason = str(exc) if isinstance(exc, BenchmarkError | SweepFailure) else f"Benchmark failed: {type(exc).__name__}"
        print(json.dumps({"state": "failed", "reason": reason,
                          "note": "No retry or production change; inspect the incomplete manifest."}), file=sys.stderr)
        return 1
    print(json.dumps({"state": "complete", "output_dir": str(output), "decision_grade": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
