#!/usr/bin/env python3
"""
Promote Model to Production
============================
Copies a trained model and saves its runtime BL/paper policy for deployment.

Usage:
    # Promote the latest run
    python scripts/promote_model.py

    # Promote a specific run
    python scripts/promote_model.py run_20260210_095220

    # List available runs
    python scripts/promote_model.py --list

    # Validate a nested run with explicit BL settings, without modifying production
    python scripts/promote_model.py no_injury_baseline/nba_run_20261108_122129 --bl-tau .75 --bl-z-max .5 --bl-max-weight .75 --edge-threshold .07 --dry-run

Policies are saved next to the model as production.policy.json, not inside the
immutable artifact bundle. The forward policy remains best-opportunity-first,
UNDER-only, $10 flat stakes. Restart/redeploy consumers after promotion.
"""

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.models.nba_artifacts import validate_artifact_bundle  # noqa: E402
from src.models.nba_paper_policy import NBA_PAPER_POLICY, NBAPaperPolicy  # noqa: E402

ARTIFACTS_DIR = Path(__file__).resolve().parents[1] / "src" / "models" / "artifacts"
PRODUCTION_DIR = ARTIFACTS_DIR / "production"  # default; overridden by --name

REQUIRED_FILES = [
    "minutes_model.joblib",
    "pts_rate_model.joblib",
    "reb_rate_model.joblib",
    "ast_rate_model.joblib",
    "feature_config.joblib",
    "selected_features.json",
]


def list_runs():
    """List all available model runs."""
    runs = sorted([
        d for d in ARTIFACTS_DIR.iterdir()
        if d.is_dir()
        and d.name.startswith("nba_run_")
        and not d.name.endswith("_incomplete")
    ])

    if not runs:
        print("No completed model runs found.")
        return []

    print(f"\nAvailable model runs ({len(runs)}):\n")
    for run in runs:
        # Check if it has all required files
        complete = all((run / f).exists() for f in REQUIRED_FILES)
        status = "✓" if complete else "✗ (incomplete)"
        print(f"  {run.name}  {status}")

    print(f"\nLatest: {runs[-1].name}")
    return runs


def promote(run_name: str | None = None, target_name: str = "production", *,
            sweep_config: str | None = None, overrides: dict | None = None, dry_run: bool = False):
    """Promote a verified bundle and its policy; never infer the best sweep cell."""
    if Path(target_name).name != target_name or target_name in (".", ".."):
        raise ValueError("Target must be a single folder name")
    root = ARTIFACTS_DIR.resolve()
    target_dir = root / target_name
    if run_name is None:
        runs = sorted(d for d in root.iterdir() if d.is_dir() and d.name.startswith("nba_run_")
                      and not d.name.endswith("_incomplete"))
        if not runs:
            raise ValueError("No completed runs found; supply a nested artifact path explicitly")
        source = runs[-1]
    else:
        source = (root / run_name).resolve()
    if root not in source.parents or source == target_dir or source in target_dir.parents or target_dir in source.parents:
        raise ValueError("Source must be a separate model directory inside artifacts")
    missing = [name for name in REQUIRED_FILES if not (source / name).is_file()]
    if missing:
        raise ValueError(f"Missing model files: {', '.join(missing)}")
    artifact = validate_artifact_bundle(source, allow_legacy=True)
    settings = NBA_PAPER_POLICY.to_dict()
    if sweep_config:
        config = json.loads(Path(sweep_config).read_text(encoding="utf-8"))
        configured_source = Path(config["model_dir"])
        if not configured_source.is_absolute():
            configured_source = PROJECT_ROOT / configured_source
        if configured_source.resolve() != source:
            raise ValueError("Sweep config refers to a different model")
        if config.get("raw_control") or config.get("kelly_used") or config.get("flat_bet_size") != 10:
            raise ValueError("Choose a BL cell with $10 flat stakes, not raw/Kelly")
        if set(config.get("stats", [])) != {"pts", "reb", "ast"}:
            raise ValueError("Sweep must use PTS/REB/AST")
        settings.update(edge_threshold=config["edge_threshold"], bl_tau=config["tau"],
                        bl_z_max=config["z_max"], bl_max_weight=config["max_weight"])
        for key in ("min_odds", "max_odds", "n_samples"):
            settings[key] = config[key]
        settings["random_state"] = config["seed"]
    settings.update(overrides or {})
    policy = NBAPaperPolicy(**settings)
    identity_input = json.dumps({"model": artifact.identity, "settings": policy.to_dict(),
                                 "direction": "best_then_under"}, sort_keys=True).encode()
    evaluation_id = "nba-paper-" + hashlib.sha256(identity_input).hexdigest()[:24]
    record = {"schema_version": 1, "source_model": str(source.relative_to(root)),
              "model_identity": artifact.identity, "direction_policy": "best_then_under",
              "settings": policy.to_dict(), "evaluation_run_id": evaluation_id,
              "sweep_config": str(Path(sweep_config).resolve()) if sweep_config else None}
    policy_path = root / f"{target_name}.policy.json"
    print(json.dumps({"dry_run": dry_run, "target": str(target_dir),
                      "policy_file": str(policy_path), **record}, indent=2))
    if dry_run:
        return record

    # Stage and validate before touching production. Retain previous bytes for rollback.
    stage = Path(tempfile.mkdtemp(prefix=".promotion-", dir=root))
    staged_model = stage / "candidate"
    previous = stage / "previous"
    staged_policy = stage / "policy.json"
    had_policy = policy_path.exists()
    moved_previous = False
    installed = False
    try:
        shutil.copytree(source, staged_model)
        if validate_artifact_bundle(staged_model, allow_legacy=True).identity != artifact.identity:
            raise ValueError("Copied artifact identity changed")
        staged_policy.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        if had_policy:
            shutil.copy2(policy_path, stage / "previous.policy.json")
        if target_dir.exists():
            target_dir.rename(previous)
            moved_previous = True
        staged_model.rename(target_dir)
        installed = True
        staged_policy.replace(policy_path)
        if validate_artifact_bundle(target_dir, allow_legacy=True).identity != artifact.identity:
            raise ValueError("Promoted artifact identity changed")
        if json.loads(policy_path.read_text(encoding="utf-8")) != json.loads(json.dumps(record)):
            raise ValueError("Promoted policy readback mismatch")
    except Exception:
        if installed:
            shutil.rmtree(target_dir)
        if moved_previous:
            previous.rename(target_dir)
        if had_policy and (stage / "previous.policy.json").exists():
            shutil.copy2(stage / "previous.policy.json", policy_path)
        elif not had_policy:
            policy_path.unlink(missing_ok=True)
        shutil.rmtree(stage)
        raise
    if moved_previous or had_policy:
        print(f"Rollback backup: {stage}")
    else:
        shutil.rmtree(stage)
    print(f"Promoted model and policy to {target_name}. Restart/redeploy consumers to load settings.")
    print("No deployment, database writes, or paper bets were triggered.")
    return record


def main():
    parser = argparse.ArgumentParser(
        description="Promote a model run to production",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "run_name",
        nargs="?",
        help="Name of the run to promote (e.g., run_20260210_095220). Defaults to latest.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available model runs",
    )
    parser.add_argument(
        "--name",
        default="production",
        help="Target production folder name (default: production). Use 'production_playoffs' for playoff model.",
    )
    parser.add_argument("--sweep-config", help="Exact sweep cell config.json; imports its BL settings")
    parser.add_argument("--bl-tau", type=float)
    parser.add_argument("--bl-z-max", type=float)
    parser.add_argument("--bl-max-weight", type=float)
    parser.add_argument("--edge-threshold", type=float)
    parser.add_argument("--dry-run", action="store_true", help="Validate and print without writing")
    args = parser.parse_args()

    if args.list:
        list_runs()
    else:
        overrides = {key: value for key, value in {
            "bl_tau": args.bl_tau, "bl_z_max": args.bl_z_max,
            "bl_max_weight": args.bl_max_weight, "edge_threshold": args.edge_threshold,
        }.items() if value is not None}
        try:
            promote(args.run_name, target_name=args.name, sweep_config=args.sweep_config,
                    overrides=overrides, dry_run=args.dry_run)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            parser.exit(1, f"Promotion failed: {exc}\n")


if __name__ == "__main__":
    main()
