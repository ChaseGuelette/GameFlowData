#!/usr/bin/env python3
"""Run the MLB quote-clean audit suite.

This wrapper coordinates the post-sweep validation lane:

1. Optional dense CLV snapshot linking
2. Quote-clean dropout/timing audit
3. CLV analysis for one or more bets.csv files
4. CLV failure-mode diagnosis
5. Manifest + markdown summary

It intentionally does not run the backtest sweep itself. Keep the expensive
sweep explicit so decision-time policy comparisons stay preregistered and
reviewable.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class SuiteItem:
    label: str
    bets_csv: str
    clv_output_dir: str
    diagnosis_output_dir: str
    clv_returncode: int | None = None
    diagnosis_returncode: int | None = None
    decision_label: str | None = None
    phase1b_decision: str | None = None
    mean_clv_implied_prob: float | None = None
    mean_clv_ci_low: float | None = None
    edge_clv_spearman: float | None = None
    edge_clv_ci_low: float | None = None


def _sanitize_label(path: Path) -> str:
    if path.name == "bets.csv" and path.parent.name:
        return path.parent.name
    return path.stem.replace(".", "_")


def discover_bets_files(sweep_output_dir: Path) -> list[Path]:
    if (sweep_output_dir / "bets.csv").exists():
        return [sweep_output_dir / "bets.csv"]
    return sorted(sweep_output_dir.glob("config_*/bets.csv"))


def run_cmd(cmd: list[str], *, dry_run: bool = False) -> int:
    print("$ " + " ".join(cmd))
    if dry_run:
        return 0
    return subprocess.run(cmd, check=False).returncode


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_overall_summary(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return {}
    for row in rows:
        if row.get("group") == "overall":
            return row
    return rows[0] if rows else {}


def read_phase1b(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        with path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return None
    if not rows:
        return None
    return rows[0].get("decision")


def _float_or_none(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def write_manifest(items: list[SuiteItem], output_dir: Path, metadata: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "metadata": metadata,
        "items": [asdict(item) for item in items],
    }
    (output_dir / "suite_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    csv_path = output_dir / "suite_manifest.csv"
    if items:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(items[0]).keys()))
            writer.writeheader()
            for item in items:
                writer.writerow(asdict(item))
    else:
        csv_path.write_text("", encoding="utf-8")

    lines = [
        "# MLB Quote-Clean Audit Suite Summary",
        "",
        "## Metadata",
        "",
    ]
    for key, value in metadata.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Items", "", "| Label | CLV rc | Diagnosis rc | Decision | Phase1B | Mean CLV | Mean CI Low | Spearman CI Low |", "|---|---:|---:|---|---|---:|---:|---:|"])
    for item in items:
        lines.append(
            f"| {item.label} | {item.clv_returncode} | {item.diagnosis_returncode} | "
            f"{item.decision_label or ''} | {item.phase1b_decision or ''} | "
            f"{'' if item.mean_clv_implied_prob is None else f'{item.mean_clv_implied_prob:.6f}'} | "
            f"{'' if item.mean_clv_ci_low is None else f'{item.mean_clv_ci_low:.6f}'} | "
            f"{'' if item.edge_clv_ci_low is None else f'{item.edge_clv_ci_low:.6f}'} |"
        )
    (output_dir / "suite_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run MLB quote-clean audit + CLV + diagnosis suite")
    parser.add_argument("--sweep-output-dir", required=True, help="Sweep directory containing config_*/bets.csv")
    parser.add_argument("--output-dir", required=True, help="Suite output directory")
    parser.add_argument("--model-dir", required=True, help="MLB model artifact directory for dropout audit")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--stats", nargs="+", required=True)
    parser.add_argument("--quote-cutoff-time-et", default="13:30")
    parser.add_argument(
        "--quote-decision-policy",
        choices=["fixed_et", "skip_early_fixed_et", "relative_to_commence", "slate_or_tminus"],
        default="fixed_et",
    )
    parser.add_argument("--quote-relative-minutes", type=int, default=60)
    parser.add_argument(
        "--line-source",
        choices=["mlb_raw_player_props", "mlb_player_props_clv_snapshots"],
        default="mlb_player_props_clv_snapshots",
        help="Odds source for dropout audit line rebuild.",
    )
    parser.add_argument(
        "--snapshots-table",
        choices=["mlb_raw_player_props", "mlb_player_props_clv_snapshots"],
        default="mlb_player_props_clv_snapshots",
        help="Odds source for CLV analysis.",
    )
    parser.add_argument("--bets-csv", action="append", default=None, help="Specific bets.csv path; repeatable. Defaults to config_*/bets.csv discovery.")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--run-linker", action="store_true", help="Run scripts/link_mlb_clv_snapshots.py before audit/CLV.")
    parser.add_argument("--skip-dropout-audit", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=50)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    py = sys.executable
    sweep_dir = Path(args.sweep_output_dir)
    bets_files = [Path(p) for p in args.bets_csv] if args.bets_csv else discover_bets_files(sweep_dir)
    if not bets_files:
        raise SystemExit(f"No bets.csv files found under {sweep_dir}")

    if args.run_linker:
        rc = run_cmd([
            py,
            str(repo_root / "scripts" / "link_mlb_clv_snapshots.py"),
            "--report",
            str(output_dir / "mlb_clv_snapshot_link_report.md"),
        ], dry_run=args.dry_run)
        if rc != 0:
            raise SystemExit(rc)

    dropout_rc = None
    if not args.skip_dropout_audit:
        dropout_cmd = [
            py,
            str(repo_root / "scripts" / "audit_mlb_quote_clean_dropout.py"),
            "--model-dir", args.model_dir,
            "--start", args.start,
            "--end", args.end,
            "--stats", *args.stats,
            "--quote-cutoff-time-et", args.quote_cutoff_time_et,
            "--quote-decision-policy", args.quote_decision_policy,
            "--quote-relative-minutes", str(args.quote_relative_minutes),
            "--line-source", args.line_source,
            "--sweep-output-dir", str(sweep_dir),
            "--output-dir", str(output_dir / "dropout_audit"),
            "--batch-size", str(args.batch_size),
        ]
        if args.local:
            dropout_cmd.append("--local")
        dropout_rc = run_cmd(dropout_cmd, dry_run=args.dry_run)
        if dropout_rc != 0:
            print(f"WARNING: dropout audit exited {dropout_rc}; continuing to CLV items")

    items: list[SuiteItem] = []
    for bets_csv in bets_files:
        label = _sanitize_label(bets_csv)
        clv_dir = output_dir / "clv" / label
        diag_dir = output_dir / "diagnosis" / label
        item = SuiteItem(label=label, bets_csv=str(bets_csv), clv_output_dir=str(clv_dir), diagnosis_output_dir=str(diag_dir))

        clv_cmd = [
            py,
            str(repo_root / "scripts" / "analyze_mlb_batter_hits_clv.py"),
            "--bets-csv", str(bets_csv),
            "--output-dir", str(clv_dir),
            "--snapshots-table", args.snapshots_table,
            "--batch-size", str(args.batch_size),
        ]
        if args.local:
            clv_cmd.append("--local")
        item.clv_returncode = run_cmd(clv_cmd, dry_run=args.dry_run)

        diag_cmd = [
            py,
            str(repo_root / "scripts" / "diagnose_mlb_clv_failure_modes.py"),
            "--clv-output-dir", str(clv_dir),
            "--output-dir", str(diag_dir),
        ]
        item.diagnosis_returncode = run_cmd(diag_cmd, dry_run=args.dry_run)

        if not args.dry_run:
            diag = read_json(diag_dir / "clv_failure_modes.json")
            overall = read_overall_summary(clv_dir / "clv_summary.csv")
            item.decision_label = diag.get("decision_label")
            item.phase1b_decision = read_phase1b(clv_dir / "phase1b_decision.csv")
            item.mean_clv_implied_prob = _float_or_none(overall.get("mean_clv_implied_prob"))
            item.mean_clv_ci_low = _float_or_none(overall.get("mean_clv_ci_low"))
            item.edge_clv_spearman = _float_or_none(overall.get("edge_clv_spearman"))
            item.edge_clv_ci_low = _float_or_none(overall.get("edge_clv_ci_low"))
        items.append(item)

    metadata = {
        "sweep_output_dir": str(sweep_dir),
        "dropout_returncode": dropout_rc,
        "line_source": args.line_source,
        "snapshots_table": args.snapshots_table,
        "quote_decision_policy": args.quote_decision_policy,
        "quote_cutoff_time_et": args.quote_cutoff_time_et,
        "quote_relative_minutes": args.quote_relative_minutes,
    }
    write_manifest(items, output_dir, metadata)
    print(f"Wrote suite manifest to {output_dir / 'suite_manifest.json'}")
    print(f"Wrote suite summary to {output_dir / 'suite_summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
