#!/usr/bin/env python3
"""Classify MLB CLV diagnostic failure modes from existing CLV outputs.

This is a report-only layer over `scripts/analyze_mlb_batter_hits_clv.py` output.
It does not recompute CLV, retrain models, or change promotion math.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_INPUT_FILES = [
    "clv_summary.csv",
    "clv_by_plus_odds_band.csv",
    "clv_by_edge_bin.csv",
    "clv_by_bookmaker.csv",
    "clv_timing_stability.csv",
    "clv_matches.csv",
    "phase1b_decision.csv",
]

REQUIRED_SUMMARY_COLUMNS = {
    "group",
    "n",
    "n_scored",
    "n_same_book",
    "n_consensus_fallback",
    "n_unmatched",
    "mean_clv_implied_prob",
    "mean_clv_ci_low",
    "mean_clv_ci_high",
    "edge_clv_spearman",
    "edge_clv_ci_low",
    "n_blocks",
}

DECISION_PASS = "pass"
DECISION_MODEL = "fail_model_or_edge"
DECISION_DATA = "fail_data_or_timing"
DECISION_INCONCLUSIVE = "inconclusive_underpowered"
DECISION_INVALID = "invalid_missing_inputs"


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except Exception:
        return False


def _float(row: pd.Series | dict, key: str, default: float = float("nan")) -> float:
    try:
        val = row.get(key, default)
        return float(val)
    except Exception:
        return default


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _missing_inputs(clv_output_dir: Path) -> list[str]:
    return [name for name in REQUIRED_INPUT_FILES if not (clv_output_dir / name).exists()]


def _overall(summary: pd.DataFrame) -> pd.Series | None:
    if summary.empty:
        return None
    if "group" in summary.columns:
        overall = summary[summary["group"].astype(str).str.lower() == "overall"]
        if not overall.empty:
            return overall.iloc[0]
    return summary.iloc[0]


def _horizons_present(timing: pd.DataFrame) -> set[str]:
    if timing.empty:
        return set()
    for col in ["horizon", "timing_horizon", "group"]:
        if col in timing.columns:
            return {str(x).lower().replace(" ", "") for x in timing[col].dropna()}
    return set()


def _line_movement_mismatch(matches: pd.DataFrame) -> bool:
    if matches.empty or "line_movement_class" not in matches.columns:
        return False
    counts = matches["line_movement_class"].astype(str).value_counts()
    unfavorable = int(counts.get("unfavorable_line_move", 0))
    favorable = int(counts.get("favorable_line_move", 0))
    same = int(counts.get("same_line_odds_clv", 0))
    total = unfavorable + favorable + same
    return total >= 20 and unfavorable > favorable and unfavorable / total >= 0.30


def _data_quality_failure(matches: pd.DataFrame, overall: pd.Series) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    n = max(_float(overall, "n", 0.0), 1.0)
    unmatched = _float(overall, "n_unmatched", 0.0)
    if unmatched / n > 0.20:
        reasons.append(f"unmatched rate {unmatched / n:.1%} exceeds 20%")

    if not matches.empty:
        if "bet_snapshot_time" in matches.columns and matches["bet_snapshot_time"].isna().mean() > 0.05:
            reasons.append("more than 5% of bet timestamps are missing")
        if "close_snapshot_time" in matches.columns and "bet_snapshot_time" in matches.columns:
            close_ts = pd.to_datetime(matches["close_snapshot_time"], utc=True, errors="coerce")
            bet_ts = pd.to_datetime(matches["bet_snapshot_time"], utc=True, errors="coerce")
            valid = close_ts.notna() & bet_ts.notna()
            if valid.any() and (close_ts[valid] <= bet_ts[valid]).mean() > 0.05:
                reasons.append("more than 5% of close snapshots are at/before bet time")
        if "close_snapshot_time" in matches.columns and "commence_time" in matches.columns:
            close_ts = pd.to_datetime(matches["close_snapshot_time"], utc=True, errors="coerce")
            commence = pd.to_datetime(matches["commence_time"], utc=True, errors="coerce")
            valid = close_ts.notna() & commence.notna()
            if valid.any() and (close_ts[valid] >= commence[valid]).mean() > 0.05:
                reasons.append("more than 5% of close snapshots are at/after commence")
    return bool(reasons), reasons


def _band_failures(bands: pd.DataFrame) -> list[str]:
    if bands.empty:
        return []
    failures = []
    for _, row in bands.iterrows():
        group = str(row.get("group", row.get("plus_odds_band", "unknown")))
        n_scored = _float(row, "n_scored", _float(row, "n", 0.0))
        mean = _float(row, "mean_clv_implied_prob")
        ci_high = _float(row, "mean_clv_ci_high", float("nan"))
        if n_scored >= 20 and (_finite(mean) and mean < 0) and (not _finite(ci_high) or ci_high < 0):
            failures.append(group)
    return failures


def _bookmaker_failures(bookmakers: pd.DataFrame) -> tuple[bool, list[str]]:
    if bookmakers.empty:
        return False, []
    reasons = []
    if "n" in bookmakers.columns and bookmakers["n"].sum() > 0:
        top = bookmakers.sort_values("n", ascending=False).iloc[0]
        share = float(top["n"]) / float(bookmakers["n"].sum())
        if share > 0.60:
            reasons.append(f"bookmaker concentration: {top.get('group', 'unknown')} has {share:.1%} of rows")
    for _, row in bookmakers.iterrows():
        n_scored = _float(row, "n_scored", _float(row, "n", 0.0))
        mean = _float(row, "mean_clv_implied_prob")
        ci_high = _float(row, "mean_clv_ci_high", float("nan"))
        if n_scored >= 20 and _finite(mean) and mean < 0 and (not _finite(ci_high) or ci_high < 0):
            reasons.append(f"bookmaker {row.get('group', 'unknown')} has materially negative CLV")
    return bool(reasons), reasons


def _write_outputs(output_dir: Path, result: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "clv_failure_modes.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    lines = [
        "# MLB CLV Failure-Mode Diagnosis",
        "",
        f"Decision: **{result['decision_label']}**",
        "",
        "## Failure modes",
        "",
    ]
    if result["failure_modes"]:
        lines.extend(f"- `{mode}`" for mode in result["failure_modes"])
    else:
        lines.append("- None detected")
    lines.extend(["", "## Reasons", ""])
    if result["reasons"]:
        lines.extend(f"- {reason}" for reason in result["reasons"])
    else:
        lines.append("- CLV outputs satisfy the configured promotion diagnostics.")
    lines.extend(
        [
            "",
            "## Interpretation standard",
            "",
            "- CLV failure blocks promotion but does not automatically mean the model should be deleted.",
            "- Data/timing failures should be fixed before interpreting model edge quality.",
            "- Positive mean CLV with weak edge ranking does not validate production sizing/ranking.",
        ]
    )
    (output_dir / "clv_failure_modes.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def diagnose_clv_failure_modes(clv_output_dir: str | Path, output_dir: str | Path) -> dict:
    clv_output_dir = Path(clv_output_dir)
    output_dir = Path(output_dir)
    failure_modes: list[str] = []
    reasons: list[str] = []

    missing = _missing_inputs(clv_output_dir)
    if missing:
        result = {
            "decision_label": DECISION_INVALID,
            "failure_modes": ["data_quality_failure"],
            "reasons": [f"Missing required CLV input files: {missing}"],
            "inputs": {"clv_output_dir": str(clv_output_dir)},
        }
        _write_outputs(output_dir, result)
        return result

    summary = _read_csv(clv_output_dir / "clv_summary.csv")
    missing_cols = sorted(REQUIRED_SUMMARY_COLUMNS - set(summary.columns))
    if missing_cols:
        result = {
            "decision_label": DECISION_INVALID,
            "failure_modes": ["data_quality_failure"],
            "reasons": [f"clv_summary.csv missing required columns: {missing_cols}"],
            "inputs": {"clv_output_dir": str(clv_output_dir)},
        }
        _write_outputs(output_dir, result)
        return result

    overall = _overall(summary)
    if overall is None:
        result = {
            "decision_label": DECISION_INVALID,
            "failure_modes": ["data_quality_failure"],
            "reasons": ["clv_summary.csv has no rows"],
            "inputs": {"clv_output_dir": str(clv_output_dir)},
        }
        _write_outputs(output_dir, result)
        return result

    bands = _read_csv(clv_output_dir / "clv_by_plus_odds_band.csv")
    edge_bins = _read_csv(clv_output_dir / "clv_by_edge_bin.csv")
    bookmakers = _read_csv(clv_output_dir / "clv_by_bookmaker.csv")
    timing = _read_csv(clv_output_dir / "clv_timing_stability.csv")
    matches = _read_csv(clv_output_dir / "clv_matches.csv")
    phase1b = _read_csv(clv_output_dir / "phase1b_decision.csv")

    mean = _float(overall, "mean_clv_implied_prob")
    mean_low = _float(overall, "mean_clv_ci_low")
    mean_high = _float(overall, "mean_clv_ci_high")
    corr = _float(overall, "edge_clv_spearman")
    corr_low = _float(overall, "edge_clv_ci_low")
    n_scored = _float(overall, "n_scored", 0.0)
    n_blocks = _float(overall, "n_blocks", 0.0)
    n = max(_float(overall, "n", 0.0), 1.0)
    same_book = _float(overall, "n_same_book", 0.0)
    consensus = _float(overall, "n_consensus_fallback", 0.0)

    data_bad, data_reasons = _data_quality_failure(matches, overall)
    if data_bad:
        failure_modes.append("data_quality_failure")
        reasons.extend(data_reasons)

    if mean < 0:
        failure_modes.append("negative_mean_clv")
        reasons.append(f"Mean CLV is negative ({mean:+.6f}).")

    if mean >= 0 and (mean_low <= 0 or n_scored < 100 or n_blocks < 10):
        failure_modes.append("underpowered_or_inconclusive")
        reasons.append(
            f"Mean CLV is positive but underpowered/inconclusive: mean={mean:+.6f}, ci_low={mean_low:+.6f}, n_scored={n_scored:.0f}, n_blocks={n_blocks:.0f}."
        )

    if not _finite(corr) or corr <= 0 or ( _finite(corr_low) and corr_low <= 0):
        failure_modes.append("edge_ranking_failure")
        reasons.append(f"Spearman(edge, CLV) is not confirmed positive: corr={corr:+.6f}, ci_low={corr_low:+.6f}.")

    if same_book / n < 0.50 or consensus / n > 0.33:
        failure_modes.append("same_book_coverage_failure")
        reasons.append(f"Same-book coverage is weak or consensus fallback is high: same_book={same_book / n:.1%}, consensus={consensus / n:.1%}.")

    horizons = _horizons_present(timing)
    required_horizons = {"+15m", "+30m", "+60m", "15m", "30m", "60m"}
    has_15 = bool(horizons & {"+15m", "15m", "+15", "15"})
    has_30 = bool(horizons & {"+30m", "30m", "+30", "30"})
    has_60 = bool(horizons & {"+60m", "60m", "+60", "60"})
    if not (has_15 and has_30 and has_60):
        failure_modes.append("timing_stability_missing")
        reasons.append("Timing stability horizons are missing or sparse; expected +15/+30/+60 minute diagnostics.")

    bookmaker_bad, bookmaker_reasons = _bookmaker_failures(bookmakers)
    if bookmaker_bad:
        failure_modes.append("bookmaker_cluster_failure")
        reasons.extend(bookmaker_reasons)

    band_failures = _band_failures(bands)
    if band_failures:
        failure_modes.append("odds_band_failure")
        reasons.append(f"Odds bands with materially negative CLV: {band_failures}")

    if _line_movement_mismatch(matches):
        failure_modes.append("line_movement_mismatch")
        reasons.append("Unfavorable line movement dominates favorable movement among scored line-movement rows.")

    # De-duplicate while preserving order.
    failure_modes = list(dict.fromkeys(failure_modes))

    data_failures = {"data_quality_failure", "same_book_coverage_failure"}
    model_failures = {"negative_mean_clv", "edge_ranking_failure", "bookmaker_cluster_failure", "odds_band_failure", "line_movement_mismatch"}
    inconclusive_only = {"underpowered_or_inconclusive", "timing_stability_missing"}

    if any(mode in data_failures for mode in failure_modes):
        decision = DECISION_DATA
    elif any(mode in model_failures for mode in failure_modes):
        decision = DECISION_MODEL
    elif failure_modes and set(failure_modes).issubset(inconclusive_only):
        decision = DECISION_INCONCLUSIVE
    else:
        decision = DECISION_PASS

    result = {
        "decision_label": decision,
        "failure_modes": failure_modes,
        "reasons": reasons,
        "inputs": {"clv_output_dir": str(clv_output_dir)},
        "overall": {
            "n": _float(overall, "n", 0.0),
            "n_scored": n_scored,
            "mean_clv_implied_prob": mean,
            "mean_clv_ci_low": mean_low,
            "mean_clv_ci_high": mean_high,
            "edge_clv_spearman": corr,
            "edge_clv_ci_low": corr_low,
            "n_blocks": n_blocks,
            "same_book_share": same_book / n,
            "consensus_fallback_share": consensus / n,
        },
        "phase1b_decision": phase1b.to_dict("records")[:3],
        "edge_bins_present": not edge_bins.empty,
        "timing_horizons_present": sorted(horizons),
    }
    _write_outputs(output_dir, result)
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose CLV failure modes from analyze_mlb_batter_hits_clv.py outputs")
    parser.add_argument("--clv-output-dir", required=True, help="Directory containing CLV output CSV files")
    parser.add_argument("--output-dir", required=True, help="Directory for failure-mode report outputs")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = diagnose_clv_failure_modes(args.clv_output_dir, args.output_dir)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
