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
from collections import Counter
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
    total_bets: int | None = None
    total_profit: float | None = None
    roi: float | None = None
    top_bookmaker: str | None = None
    top_bookmaker_share: float | None = None
    espnbet_bets: int | None = None
    espnbet_share: float | None = None
    failure_mode_top: str | None = None
    failure_reason_top: str | None = None
    gate_status: str | None = None
    policy_recommendation: str | None = None
    dense_table_adequate: str | None = None
    mean_clv_confirmed: str | None = None
    edge_ranking_confirmed: str | None = None
    espnbet_concentration_blocking: str | None = None
    flat_staking_allowed: str | None = None
    edge_sizing_allowed: str | None = None
    feature_expansion_allowed: str | None = None
    retraining_indicated: str | None = None
    book_routing_policy: str | None = None
    preferred_book_bets: int | None = None
    preferred_book_share: float | None = None
    espnbet_or_prophetx_bets: int | None = None
    espnbet_or_prophetx_share: float | None = None
    candidate_edges_rows: int | None = None
    preferred_candidates_share: float | None = None
    preferred_selected_share: float | None = None
    preferred_edge_survives_share: float | None = None
    timing_stability_status: str | None = None
    timing_required_horizons: list[str] | None = None
    timing_horizons_present: list[str] | None = None
    timing_horizon_coverage_pct: dict[str, float] | None = None


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




def read_bets_rollup(path: Path) -> dict:
    """Extract decision-grade sweep/bet metrics from bets.csv plus optional metrics.json."""
    rollup = {
        "total_bets": None,
        "total_profit": None,
        "roi": None,
        "top_bookmaker": None,
        "top_bookmaker_share": None,
        "espnbet_bets": None,
        "espnbet_share": None,
    }
    metrics = read_json(path.parent / "metrics.json")
    for src, dst in [
        ("total_bets", "total_bets"),
        ("bets", "total_bets"),
        ("total_profit", "total_profit"),
        ("profit", "total_profit"),
        ("roi", "roi"),
    ]:
        if src in metrics and metrics[src] is not None:
            rollup[dst] = metrics[src]
    if rollup["roi"] is None and "roi_pct" in metrics:
        try:
            rollup["roi"] = float(metrics["roi_pct"]) / 100.0
        except Exception:
            pass

    if not path.exists():
        return rollup
    try:
        with path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return rollup
    if rollup["total_bets"] is None:
        rollup["total_bets"] = len(rows)

    profit_cols = ["profit", "pnl", "actual_profit", "net_profit"]
    stake_cols = ["stake", "bet_size", "amount", "risk"]
    profit = 0.0
    stake = 0.0
    saw_profit = False
    for row in rows:
        for col in profit_cols:
            if row.get(col) not in (None, ""):
                try:
                    profit += float(row[col])
                    saw_profit = True
                    break
                except Exception:
                    pass
        for col in stake_cols:
            if row.get(col) not in (None, ""):
                try:
                    stake += float(row[col])
                    break
                except Exception:
                    pass
    if saw_profit and rollup["total_profit"] is None:
        rollup["total_profit"] = profit
    if stake > 0 and rollup["roi"] is None:
        rollup["roi"] = profit / stake

    book_col = next((c for c in ["bookmaker", "sportsbook", "book"] if rows and c in rows[0]), None)
    if book_col:
        counts = Counter(str(r.get(book_col, "")).strip() for r in rows if str(r.get(book_col, "")).strip())
        if counts:
            top_book, top_count = counts.most_common(1)[0]
            total = sum(counts.values())
            rollup["top_bookmaker"] = top_book
            rollup["top_bookmaker_share"] = top_count / total if total else None
            espn = sum(v for k, v in counts.items() if k.lower().replace(" ", "") in {"espnbet", "espnbets"})
            rollup["espnbet_bets"] = espn
            rollup["espnbet_share"] = espn / total if total else None
    return rollup


PREFERRED_BOOKS = {
    "draftkings", "fanduel", "betmgm", "caesars", "williamhill_us",
    "betrivers", "fanatics", "hardrockbet", "hardrockbet_oh",
}
CONCENTRATION_BOOKS = {"espnbet", "prophetx"}


def _clean_book(value) -> str:
    return str(value or "").strip().lower().replace(" ", "")


def read_book_routing_rollup(bets_csv: Path) -> dict:
    """Read saved bet/candidate files and return compact book-routing diagnostics."""
    out = {
        "book_routing_policy": None,
        "preferred_book_bets": None,
        "preferred_book_share": None,
        "espnbet_or_prophetx_bets": None,
        "espnbet_or_prophetx_share": None,
        "candidate_edges_rows": None,
        "preferred_candidates_share": None,
        "preferred_selected_share": None,
        "preferred_edge_survives_share": None,
    }
    if bets_csv.exists():
        try:
            with bets_csv.open(newline="", encoding="utf-8") as f:
                bets = list(csv.DictReader(f))
        except Exception:
            bets = []
        if bets:
            book_col = "selected_bookmaker" if "selected_bookmaker" in bets[0] else "bookmaker"
            books = [_clean_book(r.get(book_col)) for r in bets]
            total = len(books)
            preferred = sum(1 for b in books if b in PREFERRED_BOOKS)
            concentrated = sum(1 for b in books if b in CONCENTRATION_BOOKS)
            out["preferred_book_bets"] = preferred
            out["preferred_book_share"] = preferred / total if total else None
            out["espnbet_or_prophetx_bets"] = concentrated
            out["espnbet_or_prophetx_share"] = concentrated / total if total else None
            policies = [r.get("book_routing_policy") for r in bets if r.get("book_routing_policy")]
            if policies:
                out["book_routing_policy"] = Counter(policies).most_common(1)[0][0]

    candidate_path = bets_csv.parent / "bookmaker_candidate_edges.csv"
    if not candidate_path.exists():
        candidate_path = bets_csv.parent / "all_bookmaker_edges.csv"
    if candidate_path.exists():
        try:
            with candidate_path.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception:
            rows = []
        if rows:
            total = len(rows)
            out["candidate_edges_rows"] = total
            preferred = sum(1 for r in rows if str(r.get("preferred_book_candidate", "")).lower() == "true")
            selected = [r for r in rows if str(r.get("selected_by_policy", "")).lower() == "true"]
            selected_preferred = sum(1 for r in selected if str(r.get("preferred_book_candidate", "")).lower() == "true")
            preferred_survives = [
                r for r in rows
                if str(r.get("preferred_book_candidate", "")).lower() == "true"
                and (
                    str(r.get("over_clears_edge_threshold", "")).lower() == "true"
                    or str(r.get("under_clears_edge_threshold", "")).lower() == "true"
                    or str(r.get("clears_edge_threshold", "")).lower() == "true"
                )
            ]
            out["preferred_candidates_share"] = preferred / total if total else None
            out["preferred_selected_share"] = selected_preferred / len(selected) if selected else None
            out["preferred_edge_survives_share"] = len(preferred_survives) / preferred if preferred else None
            policies = [r.get("book_routing_policy") for r in rows if r.get("book_routing_policy")]
            if policies and not out["book_routing_policy"]:
                out["book_routing_policy"] = Counter(policies).most_common(1)[0][0]
    return out


def read_failure_rollup(path: Path) -> dict:
    data = read_json(path)
    if not data:
        return {}
    candidates = []
    for key in ["failure_modes", "reason_counts", "mode_counts", "bookmaker_clusters", "summary"]:
        value = data.get(key)
        if isinstance(value, dict):
            candidates.append(value)
    flat = data if isinstance(data, dict) else {}
    candidates.append(flat)
    out = {}
    for d in candidates:
        for key, val in d.items():
            if isinstance(val, int | float) and key not in {"total", "count", "n"}:
                out[str(key)] = val
            elif isinstance(val, dict) and "count" in val:
                try:
                    out[str(key)] = float(val["count"])
                except Exception:
                    pass
    if not out:
        return {}
    top_key, _ = max(out.items(), key=lambda kv: kv[1])
    return {"top": top_key, "counts": out}




def populate_validation_decisions(item: SuiteItem, dropout_summary: dict | None) -> None:
    """Populate concrete yes/no gate answers used by suite_summary.md."""
    dropout_decision = (dropout_summary or {}).get("decision")
    if dropout_decision == "FAIL":
        item.dense_table_adequate = "no"
    elif dropout_decision == "WARN":
        item.dense_table_adequate = "needs-linking-audit"
    else:
        item.dense_table_adequate = "yes"

    item.mean_clv_confirmed = (
        "yes" if item.mean_clv_ci_low is not None and item.mean_clv_ci_low > 0
        else "no" if item.mean_clv_ci_low is not None
        else "underpowered"
    )
    item.edge_ranking_confirmed = (
        "yes" if item.edge_clv_ci_low is not None and item.edge_clv_ci_low > 0
        else "no" if item.edge_clv_ci_low is not None
        else "underpowered"
    )
    item.espnbet_concentration_blocking = (
        "sensitivity-required" if item.espnbet_share is not None and item.espnbet_share > 0.50 else "no"
    )

    item.flat_staking_allowed = "yes" if item.mean_clv_confirmed == "yes" and item.dense_table_adequate == "yes" else "no"
    item.edge_sizing_allowed = "yes" if item.flat_staking_allowed == "yes" and item.edge_ranking_confirmed == "yes" else "no"
    item.feature_expansion_allowed = "yes" if item.edge_sizing_allowed == "yes" else "no"
    item.retraining_indicated = "after-data-fix" if item.dense_table_adequate != "yes" else "no"

    if item.dense_table_adequate != "yes":
        item.policy_recommendation = "needs_more_data"
    elif item.espnbet_concentration_blocking == "sensitivity-required":
        item.policy_recommendation = "needs_book_sensitivity"
    elif item.mean_clv_confirmed != "yes":
        item.policy_recommendation = "reject"
    elif item.edge_ranking_confirmed != "yes":
        item.policy_recommendation = "candidate_flat_only"
    else:
        item.policy_recommendation = "candidate_for_next_gate"


def determine_gate_status(item: SuiteItem, dropout_summary: dict | None) -> str:
    if item.clv_returncode != 0 or item.diagnosis_returncode != 0:
        return "FAIL: command failure"
    if dropout_summary and dropout_summary.get("decision") == "FAIL":
        return "FAIL: dropout timing"

    if item.mean_clv_ci_low is None or item.edge_clv_ci_low is None:
        return "WARN: missing CLV gates"
    if item.mean_clv_ci_low <= 0:
        return "FAIL: mean CLV CI low <= 0"
    if item.edge_clv_ci_low <= 0:
        return "FAIL: edge-ranking CI low <= 0"
    if item.espnbet_share is not None and item.espnbet_share > 0.50:
        return "WARN: ESPNBet concentration"
    return "PASS"


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

    dropout_summary = metadata.get("dropout_summary") or {}
    lines = [
        "# MLB Quote-Clean Audit Suite Summary",
        "",
        "## Decision",
        "",
    ]
    gate_counts = Counter((item.gate_status or "UNKNOWN").split(":", 1)[0] for item in items)
    overall_gate = "FAIL" if gate_counts.get("FAIL") else ("WARN" if gate_counts.get("WARN") else "PASS")
    lines.append(f"- Overall gate status: {overall_gate}")
    lines.append(f"- Dropout audit decision: {dropout_summary.get('decision', 'not_run')}")
    if dropout_summary.get("reason"):
        lines.append(f"- Dropout reason: {dropout_summary.get('reason')}")
    lines.extend(["", "## Metadata", ""])
    for key, value in metadata.items():
        if key != "dropout_summary":
            lines.append(f"- {key}: {value}")

    lines.extend([
        "",
        "## Validation Gate Report",
        "",
        "| Label | Gate | Policy Recommendation | ROI | Bets | Profit | Mean CLV CI Low | Spearman CI Low | Top Book | ESPNBet Share | Decision | Phase1B |",
        "|---|---|---|---:|---:|---:|---:|---:|---|---:|---|---|",
    ])
    for item in items:
        lines.append(
            f"| {item.label} | {item.gate_status or ''} | {item.policy_recommendation or ''} | "
            f"{'' if item.roi is None else f'{item.roi:.4f}'} | "
            f"{'' if item.total_bets is None else item.total_bets} | "
            f"{'' if item.total_profit is None else f'{item.total_profit:.2f}'} | "
            f"{'' if item.mean_clv_ci_low is None else f'{item.mean_clv_ci_low:.6f}'} | "
            f"{'' if item.edge_clv_ci_low is None else f'{item.edge_clv_ci_low:.6f}'} | "
            f"{item.top_bookmaker or ''} | "
            f"{'' if item.espnbet_share is None else f'{item.espnbet_share:.2%}'} | "
            f"{item.decision_label or ''} | {item.phase1b_decision or ''} |"
        )

    lines.extend(["", "## Dropout Buckets", ""])
    bucket_counts = dropout_summary.get("bucket_counts") if isinstance(dropout_summary, dict) else None
    if bucket_counts:
        lines.extend(["| Bucket | Count |", "|---|---:|"])
        for bucket, count in bucket_counts.items():
            lines.append(f"| {bucket} | {count} |")
    else:
        lines.append("Dropout bucket summary not available.")

    lines.extend(["", "## Concrete Gate Answers", ""])
    lines.extend([
        "| Label | Dense Table | Mean CLV | Edge Ranking | ESPNBet Blocking | Flat Staking | Edge/Kelly Sizing | Feature Expansion | Retraining |",
        "|---|---|---|---|---|---|---|---|---|",
    ])
    for item in items:
        lines.append(
            f"| {item.label} | {item.dense_table_adequate or ''} | {item.mean_clv_confirmed or ''} | "
            f"{item.edge_ranking_confirmed or ''} | {item.espnbet_concentration_blocking or ''} | "
            f"{item.flat_staking_allowed or ''} | {item.edge_sizing_allowed or ''} | "
            f"{item.feature_expansion_allowed or ''} | {item.retraining_indicated or ''} |"
        )

    lines.extend(["", "## Book Routing / Candidate Edge Signals", ""])
    lines.extend([
        "| Label | Routing Policy | Preferred Bet Share | ESPNBet+ProphetX Share | Candidate Rows | Preferred Candidate Share | Preferred Selected Share | Preferred Edge-Survival Share |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ])
    for item in items:
        lines.append(
            f"| {item.label} | {item.book_routing_policy or ''} | "
            f"{'' if item.preferred_book_share is None else f'{item.preferred_book_share:.2%}'} | "
            f"{'' if item.espnbet_or_prophetx_share is None else f'{item.espnbet_or_prophetx_share:.2%}'} | "
            f"{'' if item.candidate_edges_rows is None else item.candidate_edges_rows} | "
            f"{'' if item.preferred_candidates_share is None else f'{item.preferred_candidates_share:.2%}'} | "
            f"{'' if item.preferred_selected_share is None else f'{item.preferred_selected_share:.2%}'} | "
            f"{'' if item.preferred_edge_survives_share is None else f'{item.preferred_edge_survives_share:.2%}'} |"
        )

    lines.extend(["", "## Failure Mode / Concentration Signals", ""])
    lines.extend(["| Label | Top Failure Mode | Top Failure Reason | Top Bookmaker Share | ESPNBet Bets |", "|---|---|---|---:|---:|"])

    for item in items:
        lines.append(
            f"| {item.label} | {item.failure_mode_top or ''} | {item.failure_reason_top or ''} | "
            f"{'' if item.top_bookmaker_share is None else f'{item.top_bookmaker_share:.2%}'} | "
            f"{'' if item.espnbet_bets is None else item.espnbet_bets} |"
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


def build_audit_evidence_metadata(
    *,
    skip_dropout_audit: bool,
    dry_run: bool,
    dropout_returncode: int | None,
    dropout_summary_path: Path,
    dropout_summary: dict,
) -> dict:
    dropout_dir = dropout_summary_path.parent
    dropout_output_paths = [
        dropout_summary_path,
        dropout_dir / "audit_summary.md",
        dropout_dir / "dropout_summary_by_bucket.csv",
        dropout_dir / "dropout_rows.csv",
        dropout_dir / "selected_clean_quotes.csv",
        dropout_dir / "dropout_by_date.csv",
        dropout_dir / "dropout_by_game.csv",
        dropout_dir / "dropout_by_bookmaker.csv",
    ]
    full_audit_complete = (
        not skip_dropout_audit
        and not dry_run
        and dropout_returncode == 0
        and bool(dropout_summary)
    )
    return {
        "audit_mode": "clv_only" if skip_dropout_audit else "full",
        "dropout_audit_ran": not skip_dropout_audit and not dry_run,
        "dropout_returncode": dropout_returncode,
        "dropout_summary_path": str(dropout_summary_path),
        "dropout_output_paths": [str(path) for path in dropout_output_paths],
        "dropout_decision": dropout_summary.get("decision"),
        "full_audit_complete": full_audit_complete,
        "full_audit_passed": (
            full_audit_complete and dropout_summary.get("decision") == "PASS"
        ),
    }


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
            str(repo_root / "scripts" / "analyze_mlb_clv.py"),
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
            failure = read_failure_rollup(diag_dir / "clv_failure_modes.json")
            item.failure_mode_top = failure.get("top")
            timing_stability = diag.get("timing_stability", {})
            if isinstance(timing_stability, dict):
                item.timing_stability_status = timing_stability.get("status")
                item.timing_required_horizons = timing_stability.get("required_horizons")
                item.timing_horizons_present = timing_stability.get("horizons_present")
                item.timing_horizon_coverage_pct = timing_stability.get("coverage_pct")
        rollup = read_bets_rollup(bets_csv)
        item.total_bets = int(rollup["total_bets"]) if rollup.get("total_bets") is not None else None
        item.total_profit = _float_or_none(rollup.get("total_profit"))
        item.roi = _float_or_none(rollup.get("roi"))
        item.top_bookmaker = rollup.get("top_bookmaker")
        item.top_bookmaker_share = _float_or_none(rollup.get("top_bookmaker_share"))
        item.espnbet_bets = int(rollup["espnbet_bets"]) if rollup.get("espnbet_bets") is not None else None
        item.espnbet_share = _float_or_none(rollup.get("espnbet_share"))
        routing = read_book_routing_rollup(bets_csv)
        item.book_routing_policy = routing.get("book_routing_policy")
        item.preferred_book_bets = int(routing["preferred_book_bets"]) if routing.get("preferred_book_bets") is not None else None
        item.preferred_book_share = _float_or_none(routing.get("preferred_book_share"))
        item.espnbet_or_prophetx_bets = int(routing["espnbet_or_prophetx_bets"]) if routing.get("espnbet_or_prophetx_bets") is not None else None
        item.espnbet_or_prophetx_share = _float_or_none(routing.get("espnbet_or_prophetx_share"))
        item.candidate_edges_rows = int(routing["candidate_edges_rows"]) if routing.get("candidate_edges_rows") is not None else None
        item.preferred_candidates_share = _float_or_none(routing.get("preferred_candidates_share"))
        item.preferred_selected_share = _float_or_none(routing.get("preferred_selected_share"))
        item.preferred_edge_survives_share = _float_or_none(routing.get("preferred_edge_survives_share"))
        items.append(item)

    dropout_summary_path = output_dir / "dropout_audit" / "audit_summary.json"
    dropout_summary = read_json(dropout_summary_path) if not args.dry_run else {}
    for item in items:
        populate_validation_decisions(item, dropout_summary)
        item.gate_status = determine_gate_status(item, dropout_summary)

    metadata = {
        "sweep_output_dir": str(sweep_dir),
        **build_audit_evidence_metadata(
            skip_dropout_audit=args.skip_dropout_audit,
            dry_run=args.dry_run,
            dropout_returncode=dropout_rc,
            dropout_summary_path=dropout_summary_path,
            dropout_summary=dropout_summary,
        ),
        "line_source": args.line_source,
        "snapshots_table": args.snapshots_table,
        "quote_decision_policy": args.quote_decision_policy,
        "quote_cutoff_time_et": args.quote_cutoff_time_et,
        "quote_relative_minutes": args.quote_relative_minutes,
        "dropout_summary": dropout_summary,
    }
    write_manifest(items, output_dir, metadata)
    print(f"Wrote suite manifest to {output_dir / 'suite_manifest.json'}")
    print(f"Wrote suite summary to {output_dir / 'suite_summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
