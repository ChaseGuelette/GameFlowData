#!/usr/bin/env python3
"""
Manual Pipeline Trigger — Props + Edge Refresh
===============================================
Quick one-shot script to scrape fresh props and recalculate edges.
Run manually whenever you want updated dashboard data.

Usage:
    python run_now.py              # Props-only scrape + edge refresh
    python run_now.py --full       # Full lines scrape + edge refresh
    python run_now.py --inference  # Props-only scrape + full inference (slower)
    python run_now.py --dry-run    # Preview what would run without executing
"""

import argparse
import subprocess
import sys
import time

PROPS_CMD = [sys.executable, "src/orchestration/lines_job.py", "--live", "--props-only"]
FULL_LINES_CMD = [sys.executable, "src/orchestration/lines_job.py", "--live"]
EDGE_REFRESH_CMD = [sys.executable, "src/orchestration/edge_refresh_job.py"]
INFERENCE_CMD = [sys.executable, "src/orchestration/inference_job.py"]


def run_step(label: str, cmd: list[str], dry_run: bool = False) -> bool:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  $ {' '.join(cmd)}")
    print(f"{'='*60}\n")

    if dry_run:
        print("  [DRY RUN] Skipped\n")
        return True

    start = time.time()
    result = subprocess.run(cmd)
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"\n  Done in {elapsed:.1f}s\n")
        return True
    else:
        print(f"\n  FAILED (exit code {result.returncode}) after {elapsed:.1f}s\n")
        return False


def main():
    parser = argparse.ArgumentParser(description="Manual pipeline trigger")
    parser.add_argument("--full", action="store_true", help="Full lines scrape (game lines + props + injuries)")
    parser.add_argument("--inference", action="store_true", help="Full inference instead of edge refresh")
    parser.add_argument("--dry-run", action="store_true", help="Preview commands without executing")
    args = parser.parse_args()

    print("\n  GameFlowData — Manual Pipeline Run\n")

    start = time.time()

    # Step 1: Scrape props
    lines_cmd = FULL_LINES_CMD if args.full else PROPS_CMD
    lines_label = "Full Lines Scrape" if args.full else "Props-Only Scrape"
    if not run_step(f"Step 1: {lines_label}", lines_cmd, args.dry_run):
        print("Props scrape failed — aborting.")
        sys.exit(1)

    # Step 2: Edge refresh or full inference
    if args.inference:
        if not run_step("Step 2: Full Inference", INFERENCE_CMD, args.dry_run):
            print("Inference failed.")
            sys.exit(1)
    else:
        if not run_step("Step 2: Edge Refresh", EDGE_REFRESH_CMD, args.dry_run):
            print("Edge refresh failed.")
            sys.exit(1)

    total = time.time() - start
    print(f"{'='*60}")
    print(f"  All done in {total:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
