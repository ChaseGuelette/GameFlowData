#!/usr/bin/env python3
"""
Promote Model to Production
============================
Copies a trained model run to the production folder for deployment.

Usage:
    # Promote the latest run
    python scripts/promote_model.py

    # Promote a specific run
    python scripts/promote_model.py run_20260210_095220

    # List available runs
    python scripts/promote_model.py --list
"""

import argparse
import shutil
import sys
from pathlib import Path

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


def promote(run_name: str | None = None, target_name: str = "production"):
    """Promote a model run to a named production folder."""
    target_dir = ARTIFACTS_DIR / target_name

    runs = sorted([
        d for d in ARTIFACTS_DIR.iterdir()
        if d.is_dir()
        and d.name.startswith("nba_run_")
        and not d.name.endswith("_incomplete")
    ])

    if not runs:
        print("Error: No completed model runs found.")
        sys.exit(1)

    # Find the run to promote
    if run_name:
        source = ARTIFACTS_DIR / run_name
        if not source.exists():
            print(f"Error: Run not found: {run_name}")
            print("Use --list to see available runs.")
            sys.exit(1)
    else:
        source = runs[-1]
        print(f"No run specified, using latest: {source.name}")

    # Validate the run has required files
    missing = [f for f in REQUIRED_FILES if not (source / f).exists()]
    if missing:
        print(f"Error: Run {source.name} is missing required files:")
        for f in missing:
            print(f"  - {f}")
        sys.exit(1)

    # Clear existing target folder
    if target_dir.exists():
        print(f"Removing existing {target_name} model...")
        shutil.rmtree(target_dir)

    # Copy to target
    print(f"Copying {source.name} to {target_name}/...")
    shutil.copytree(source, target_dir)

    # Create a marker file with source info
    marker = target_dir / ".source"
    marker.write_text(source.name)

    print(f"\n✓ Model promoted to {target_name}!")
    print(f"  Source: {source.name}")
    print(f"  Target: src/models/artifacts/{target_name}/")
    print("\nNext steps:")
    print(f"  git add src/models/artifacts/{target_name}/")
    print(f"  git commit -m 'Promote {source.name} to {target_name}'")


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
    args = parser.parse_args()

    if args.list:
        list_runs()
    else:
        promote(args.run_name, target_name=args.name)


if __name__ == "__main__":
    main()
