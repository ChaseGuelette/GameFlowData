"""Owner-run NBA error-stage diagnosis. --dry-run never loads models or queries DB."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--model-dir", required=True, help="Exact completed model used by the reference sweep")
    parser.add_argument("--sweep-dir", required=True, help="Completed local reference sweep; supplies dates/settings")
    parser.add_argument("--output-dir", required=True, help="New isolated diagnostic directory; no overwrites")
    parser.add_argument("--local", action="store_true", help="Required: use only the local PostgreSQL mirror")
    parser.add_argument("--max-games", type=int, default=200, help="Cap feature/reference games (1..200)")
    parser.add_argument("--max-outcome-games", type=int, default=200,
                        help="Cap games in the broader actual-outcome population (1..200)")
    parser.add_argument("--max-player-games", type=int, default=2000,
                        help="Cap source feature rows before prediction")
    parser.add_argument("--max-outcome-rows", type=int, default=10000,
                        help="Cap all non-DNP outcomes in the date window (1..10000), independently of feature rows")
    parser.add_argument("--dry-run", action="store_true", help="Filesystem/config preflight only; no outputs created")
    return parser


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def execute(prepared: dict) -> Path:
    from src.backtesting.nba.sweep_runner import SweepFailure, acquire_shared_inputs, bootstrap_runtime
    from src.diagnostics.nba_error_stages import (
        BoundedFeatureStore,
        DiagnosticError,
        analyze_acquired,
        file_hash,
        install_read_only_guard,
        load_actual_minutes,
        render_report,
    )
    from src.models.nba_artifacts import (
        _rename_no_replace,
        create_exclusive_incomplete_dir,
        validate_artifact_bundle,
    )

    cfg, plan, info = prepared["config"], prepared["plan"], prepared["info"]
    output = Path(info["output_dir"])
    incomplete, final = create_exclusive_incomplete_dir(output.parent, output.name)
    logger = logging.getLogger(__name__)
    runtime = None
    stage = "runtime_bootstrap"
    manifest = {**info, "execution": True, "status": "in_progress", "state": "incomplete",
                "started_utc": datetime.now(UTC).isoformat(),
                "diagnostic_source": {
                    "scripts/diagnose_nba_error_stages.py": file_hash(Path(__file__)),
                    "src/diagnostics/nba_error_stages.py": file_hash(
                        Path(__file__).resolve().parents[1] / "src/diagnostics/nba_error_stages.py"),
                }}
    _write_json(incomplete / "diagnostic_manifest.json", manifest)
    try:
        logger.info("Loading exact model; diagnostic window %s through %s", cfg.start, cfg.end)
        runtime = bootstrap_runtime(cfg, plan)
        manifest["model_deserialized"] = runtime.model_deserialized
        engine = runtime.harness.engine
        guards = install_read_only_guard(engine)
        stage = "actual_minutes"
        logger.info("Reading bounded actual minutes/stat outcomes with READ ONLY and 15-second query timeout")
        outcomes = load_actual_minutes(engine, cfg, info["max_outcome_rows"])
        logger.info("Loaded %s actual-outcome rows (cap %s); feature-row cap remains %s",
                    len(outcomes), info["max_outcome_rows"], info["max_player_games"])
        if outcomes["game_id"].nunique() > info["max_outcome_games"]:
            raise DiagnosticError(
                f"Actual-outcome game cap exceeded: {outcomes['game_id'].nunique()} games, "
                f"--max-outcome-games={info['max_outcome_games']}"
            )
        runtime.harness.feature_store = BoundedFeatureStore(
            runtime.harness.feature_store, info["max_games"], info["max_player_games"],
        )
        stage = "canonical_acquisition"
        logger.info("Acquiring canonical pregame features/quotes and reproducing the reference baseline")
        try:
            shared = acquire_shared_inputs(cfg, plan, runtime)
        except SweepFailure as exc:
            # Preserve our safe validation details, not arbitrary DB exceptions.
            if isinstance(exc.__context__, DiagnosticError):
                raise exc.__context__ from None
            raise
        stage = "stage_analysis"
        summary, exports = analyze_acquired(prepared, runtime, shared, outcomes)
        shared.assert_unchanged(plan)
        stage = "source_readback"
        inspection = validate_artifact_bundle(cfg.model_dir, cfg.stats, allow_legacy=False)
        if inspection.identity != plan["artifact"]["identity"]:
            raise DiagnosticError("Source model changed during diagnosis")
        reference = Path(info["sweep_dir"]) / "sweep_manifest.json"
        if file_hash(reference) != info["reference_manifest_sha256"]:
            raise DiagnosticError("Reference sweep manifest changed during diagnosis")
        stage = "write_results"
        for name, frame in exports.items():
            frame.to_csv(incomplete / name, index=False)
        _write_json(incomplete / "summary.json", summary)
        (incomplete / "report.md").write_text(render_report(summary), encoding="utf-8")
        manifest.update(status="report_complete", state="complete", live_data_checked=True,
                        completed_utc=datetime.now(UTC).isoformat(), database_guards=guards,
                        source_artifact_unchanged=True, baseline_reproduced=True,
                        actual_outcome_rows=len(outcomes), canonical_input_identity=shared.identity,
                        oracle_non_executable=True,
                        files={p.name: {"sha256": file_hash(p), "bytes": p.stat().st_size}
                               for p in incomplete.iterdir() if p.is_file() and p.name != "diagnostic_manifest.json"})
        _write_json(incomplete / "diagnostic_manifest.json", manifest)
        _rename_no_replace(incomplete, final)
        logger.info("Diagnostic complete: %s", final)
        for stat, value in summary["oracle_sensitivity"].items():
            logger.info("%s: %s (sensitivity, not proven causal attribution)", stat, value["triage_suggestion"])
        return final
    except BaseException as exc:
        manifest.update(state="failed", status="execution_failed", failed_stage=stage,
                        error_type=type(exc).__name__)
        if isinstance(exc, DiagnosticError | SweepFailure):
            manifest["reason"] = str(exc)
        if incomplete.exists():
            _write_json(incomplete / "diagnostic_manifest.json", manifest)
        raise
    finally:
        if runtime is not None:
            runtime.harness.engine.dispose()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from src.diagnostics.nba_error_stages import DiagnosticError, prepare_diagnostic

    try:
        prepared = prepare_diagnostic(
            args.model_dir, args.sweep_dir, args.output_dir, local=args.local,
            max_games=args.max_games, max_player_games=args.max_player_games,
            max_outcome_rows=args.max_outcome_rows,
            max_outcome_games=args.max_outcome_games,
        )
    except (DiagnosticError, OSError, ValueError, KeyError) as exc:
        reason = str(exc) if isinstance(exc, DiagnosticError) else f"Preflight failed: {type(exc).__name__}"
        print(json.dumps({"state": "blocked", "reason": reason}), file=sys.stderr)
        return 2
    if args.dry_run:
        print(json.dumps(prepared["info"], indent=2, sort_keys=True, allow_nan=False))
        return 0
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger().setLevel(logging.INFO)
    from src.backtesting.nba.sweep_runner import SweepFailure

    try:
        output = execute(prepared)
    except KeyboardInterrupt:
        print("Diagnostic interrupted; incomplete evidence retained.", file=sys.stderr)
        return 130
    except Exception as exc:
        reason = str(exc) if isinstance(exc, DiagnosticError | SweepFailure) else f"Diagnostic failed: {type(exc).__name__}"
        print(json.dumps({"state": "failed", "reason": reason,
                          "note": "No retry, training or production change; inspect the incomplete manifest."}), file=sys.stderr)
        return 1
    print(json.dumps({"state": "complete", "output_dir": str(output),
                      "decision_grade": False, "oracle_non_executable": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
