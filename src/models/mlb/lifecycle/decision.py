"""Conservative report-only promotion decisions for MLB lifecycle evidence."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.models.mlb.lifecycle.config import ResolvedLifecycleConfig


@dataclass(frozen=True)
class DecisionRecord:
    classification: str
    posture: str
    reasons: list[str]
    evidence: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "posture": self.posture,
            "reasons": list(self.reasons),
            "evidence": dict(self.evidence),
        }


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _float(value: object) -> float | None:
    if not isinstance(value, str | int | float) or isinstance(value, bool):
        return None
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int | None:
    number = _float(value)
    return int(number) if number is not None else None


def _bool(value: object) -> bool | None:
    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "1", "pass", "passed"}:
        return True
    if normalized in {"false", "no", "0", "fail", "failed"}:
        return False
    return None


def _file_evidence(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"path": str(path), "exists": False, "sha256": None}
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "exists": True, "sha256": digest.hexdigest()}


def _timing_csv_valid(path: Path) -> bool:
    rows = _rows(path)
    aliases = (
        {"+15m", "15m", "+15", "15"},
        {"+30m", "30m", "+30", "30"},
        {"+60m", "60m", "+60", "60"},
    )
    for horizon_aliases in aliases:
        scored = False
        for row in rows:
            observed = str(row.get("horizon") or "").lower().replace(" ", "")
            value = row.get("horizon_clv_implied_prob")
            if observed not in horizon_aliases or value in {None, ""}:
                continue
            try:
                scored = math.isfinite(float(value))
            except (TypeError, ValueError):
                scored = False
            if scored:
                break
        if not scored:
            return False
    return True


def _suite_evidence(
    csv_path: Path, json_path: Path
) -> tuple[list[dict[str, Any]], dict[str, Any], bool]:
    csv_rows = _rows(csv_path)
    metadata: dict[str, Any] = {}
    if not json_path.exists():
        return list(csv_rows), metadata, False
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return list(csv_rows), metadata, False
    if isinstance(payload, dict) and isinstance(payload.get("metadata"), dict):
        metadata = payload["metadata"]
    items = payload.get("items", []) if isinstance(payload, dict) else []
    json_items = [item for item in items if isinstance(item, dict)]
    if csv_rows:
        csv_labels = {str(row.get("label") or "") for row in csv_rows}
        json_labels = {str(item.get("label") or "") for item in json_items}
        return list(csv_rows), metadata, bool(json_items) and csv_labels == json_labels
    return json_items, metadata, bool(json_items)


def _posture(purpose: str, confirmed: bool, full_audit: bool) -> str:
    if purpose == "discovery":
        return "hypothesis_only"
    if purpose == "independent_validation":
        return "flat_paper_candidate" if confirmed else "live_blocked"
    return "live_ready" if confirmed and full_audit else "live_blocked"


def _edge_from_label(label: str) -> float | None:
    match = re.search(r"(?:^|_)edge(-?\d+(?:\.\d+)?)", label)
    return _float(match.group(1)) if match else None


def _matching_sweep_row(label: str, sweep: list[dict[str, str]]) -> dict[str, str] | None:
    """Match the audit label to the exact ordered sweep config, not edge alone."""
    match = re.match(r"config_(\d+)(?:_|$)", label)
    if not match:
        return None
    index = int(match.group(1)) - 1
    if index < 0 or index >= len(sweep):
        return None
    row = sweep[index]
    label_edge = _edge_from_label(label)
    row_edge = _float(row.get("edge_threshold"))
    if label_edge is not None and row_edge is not None and abs(label_edge - row_edge) > 1e-12:
        return None
    kelly_match = re.search(r"(?:^|_)kelly(-?\d+(?:\.\d+)?)", label)
    label_kelly = _float(kelly_match.group(1)) if kelly_match else None
    row_kelly = _float(row.get("kelly_fraction"))
    if label_kelly is not None and row_kelly is not None and abs(label_kelly - row_kelly) > 1e-12:
        return None
    tau_match = re.search(r"(?:^|_)tau(-?\d+(?:\.\d+)?)", label)
    label_tau = _float(tau_match.group(1)) if tau_match else None
    row_tau = _float(row.get("tau"))
    if "no_BL" in label and row_tau is not None:
        return None
    if label_tau is not None and (row_tau is None or abs(label_tau - row_tau) > 1e-12):
        return None
    return row


def _independent_window_evidence(
    resolved: ResolvedLifecycleConfig, sweep_summary_csv: Path
) -> tuple[bool, list[str], dict[str, Any]]:
    results_path = sweep_summary_csv.with_name("sweep_results.json")
    if not results_path.exists():
        return False, ["sweep_results.json is missing"], {}
    try:
        payload = json.loads(results_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False, ["sweep_results.json is unreadable"], {}
    metadata = payload.get("sweep_metadata", {}) if isinstance(payload, dict) else {}
    contract = metadata.get("promotion_contract", {}) if isinstance(metadata, dict) else {}
    quote_clean = contract.get("quote_clean", {}) if isinstance(contract, dict) else {}
    checks = {
        "start_date": str(metadata.get("start_date")) == str(resolved.evaluation.start),
        "end_date": str(metadata.get("end_date")) == str(resolved.evaluation.end),
        "promotion_grade": contract.get("promotion_grade") is True,
        "quote_clean": quote_clean.get("enabled") is True,
        "line_source": str(quote_clean.get("line_source")) == str(resolved.quotes.line_source),
        "decision_policy": str(quote_clean.get("decision_policy"))
        == str(resolved.quotes.decision_policy),
        "relative_minutes": _int(quote_clean.get("relative_minutes"))
        == resolved.quotes.relative_minutes,
    }
    failures = [
        f"independent evidence mismatch: {name}" for name, passed in checks.items() if not passed
    ]
    return not failures, failures, {"path": str(results_path), "checks": checks}


def _failed_gate(row: dict[str, Any]) -> bool:
    gate = str(row.get("gate_status", "")).strip().lower()
    decision = str(row.get("decision_label", "")).strip().lower()
    return gate.startswith(("fail", "block", "reject", "exclude")) or any(
        token in decision for token in ("fail", "reject", "exclude", "block")
    )


def evaluate_decision(
    resolved: ResolvedLifecycleConfig,
    *,
    sweep_summary_csv: Path,
    suite_manifest_csv: Path,
    suite_manifest_json: Path,
    ranking_root: Path,
) -> DecisionRecord:
    sweep = _rows(sweep_summary_csv)
    suite, suite_metadata, suite_labels_verified = _suite_evidence(
        suite_manifest_csv, suite_manifest_json
    )
    ranker_files = sorted(ranking_root.glob("**/ranking_score_summary.csv")) if ranking_root.exists() else []
    ranker_by_label = {path.parent.name: _rows(path) for path in ranker_files}
    independent_verified, independent_reasons, independent_detail = _independent_window_evidence(
        resolved, sweep_summary_csv
    )
    finalist = resolved.purpose == "finalist_certification"
    require_independent = resolved.decision.require_independent_window or finalist
    require_positive_clv = resolved.decision.require_positive_mean_clv_ci_low or finalist
    require_positive_ranker = resolved.decision.require_positive_ranker_ci_low or finalist
    require_monotonicity = resolved.decision.require_edge_bucket_monotonicity or finalist

    evidence: dict[str, Any] = {
        "sweep_rows": len(sweep),
        "suite_rows": len(suite),
        "ranking_summary_files": len(ranker_files),
        "audit_mode": resolved.audit.mode,
        "purpose": resolved.purpose,
        "independent_window_verified": independent_verified,
        "independent_window": independent_detail,
        "audit_attestation": suite_metadata,
        "suite_labels_verified": suite_labels_verified,
    }
    global_missing: list[str] = []
    global_failures: list[str] = []
    if not sweep:
        global_missing.append("Sweep summary is missing or empty")
    if not suite:
        global_missing.append("CLV audit manifest is missing or empty")
    elif not suite_labels_verified:
        global_missing.append("CLV audit CSV/JSON candidate labels do not match")
    if require_positive_ranker and not ranker_files:
        global_missing.append("Ranker diagnostics are missing")
    if resolved.purpose == "finalist_certification" and resolved.audit.mode != "full":
        global_missing.append("Finalist certification requires audit.mode=full")
    if finalist:
        if suite_metadata.get("audit_mode") != "full":
            global_missing.append("Persisted full-audit attestation is missing")
        elif suite_metadata.get("full_audit_complete") is not True:
            global_missing.append("Persisted full/dropout audit is incomplete")
        elif (
            suite_metadata.get("dropout_audit_ran") is not True
            or suite_metadata.get("dropout_returncode") != 0
        ):
            global_missing.append("Successful full/dropout audit execution is not attested")
        audit_root = suite_manifest_json.parent
        dropout_summary_path = audit_root / "dropout_audit" / "audit_summary.json"
        dropout_output_paths = [
            dropout_summary_path,
            audit_root / "dropout_audit" / "audit_summary.md",
            audit_root / "dropout_audit" / "dropout_summary_by_bucket.csv",
            audit_root / "dropout_audit" / "dropout_rows.csv",
            audit_root / "dropout_audit" / "selected_clean_quotes.csv",
            audit_root / "dropout_audit" / "dropout_by_date.csv",
            audit_root / "dropout_audit" / "dropout_by_game.csv",
            audit_root / "dropout_audit" / "dropout_by_bookmaker.csv",
        ]
        dropout_summary: dict[str, Any] = {}
        if dropout_summary_path.is_file():
            try:
                loaded_summary = json.loads(dropout_summary_path.read_text(encoding="utf-8"))
                if isinstance(loaded_summary, dict):
                    dropout_summary = loaded_summary
            except (OSError, json.JSONDecodeError):
                pass
        if not dropout_summary:
            global_missing.append("Persisted dropout audit summary file is missing or unreadable")
        if any(not path.is_file() for path in dropout_output_paths):
            global_missing.append("Persisted full dropout output bundle is incomplete")
        if suite_metadata.get("dropout_summary_path") != str(dropout_summary_path):
            global_missing.append("Dropout audit summary path attestation does not match")
        if suite_metadata.get("dropout_output_paths") != [
            str(path) for path in dropout_output_paths
        ]:
            global_missing.append("Dropout output-bundle attestation does not match")
        dropout_decision = str(dropout_summary.get("decision") or "").upper()
        if dropout_decision != str(suite_metadata.get("dropout_decision") or "").upper():
            global_missing.append("Dropout audit decision attestation does not match")
        if dropout_decision == "FAIL":
            global_failures.append("Persisted dropout audit failed")
        elif dropout_decision != "PASS" or suite_metadata.get("full_audit_passed") is not True:
            global_missing.append("Persisted dropout audit PASS evidence is unavailable")
        evidence["dropout_audit"] = {
            "verified": not any("dropout" in reason.lower() for reason in global_missing),
            "decision": dropout_decision or None,
            "returncode": suite_metadata.get("dropout_returncode"),
            "summary": _file_evidence(dropout_summary_path),
            "outputs": [_file_evidence(path) for path in dropout_output_paths],
        }
    if require_independent and not independent_verified:
        global_missing.extend(independent_reasons)

    candidate_checks: list[dict[str, Any]] = []
    minimum_bets = int(resolved.audit.minimum_bets or 0)
    for index, suite_row in enumerate(suite):
        label = str(suite_row.get("label") or f"candidate_{index + 1}")
        edge = _edge_from_label(label)
        sweep_row = _matching_sweep_row(label, sweep)
        ranker_rows = ranker_by_label.get(label, [])
        failures: list[str] = []
        missing: list[str] = []

        bets = _int(suite_row.get("total_bets"))
        roi = _float(suite_row.get("roi"))
        clv_ci = _float(suite_row.get("mean_clv_ci_low"))
        drawdown = _float(sweep_row.get("max_drawdown")) if sweep_row else None
        ranker_ci_available = any(_float(row.get("ci_low")) is not None for row in ranker_rows)
        qualifying_ranker_rows = [
            row
            for row in ranker_rows
            if _bool(row.get("pass")) is True
            and (ci_low := _float(row.get("ci_low"))) is not None
            and ci_low > 0
        ]
        ranker_ci_values = [_float(row.get("ci_low")) for row in qualifying_ranker_rows]
        best_ranker_ci = max(
            (value for value in ranker_ci_values if value is not None), default=None
        )
        passing_ranker = bool(qualifying_ranker_rows)
        monotonic = any(
            _bool(row.get("monotonic_bins")) is True for row in qualifying_ranker_rows
        )
        dense_adequate = _bool(suite_row.get("dense_table_adequate"))
        timing_stability_status = str(
            suite_row.get("timing_stability_status") or ""
        ).strip().upper()
        timing_path = suite_manifest_json.parent / "clv" / label / "clv_timing_stability.csv"
        diagnosis_path = (
            suite_manifest_json.parent / "diagnosis" / label / "clv_failure_modes.json"
        )
        timing_diagnosis_status = ""
        timing_diagnosis_valid = False
        if diagnosis_path.is_file():
            try:
                diagnosis_payload = json.loads(diagnosis_path.read_text(encoding="utf-8"))
                timing_payload = (
                    diagnosis_payload.get("timing_stability", {})
                    if isinstance(diagnosis_payload, dict)
                    else {}
                )
                if isinstance(timing_payload, dict):
                    timing_diagnosis_status = str(timing_payload.get("status") or "").upper()
                    required_horizons = {"+15m", "+30m", "+60m"}
                    coverage = timing_payload.get("coverage_pct")
                    timing_diagnosis_valid = (
                        set(timing_payload.get("required_horizons") or []) == required_horizons
                        and isinstance(coverage, dict)
                        and all(
                            not isinstance(coverage.get(horizon), bool)
                            and isinstance(coverage.get(horizon), int | float)
                            and coverage[horizon] > 0
                            for horizon in required_horizons
                        )
                    )
            except (OSError, json.JSONDecodeError):
                pass

        if bets is None:
            missing.append("total_bets unavailable")
        elif bets < minimum_bets:
            missing.append(f"sample {bets} < {minimum_bets}")
        if resolved.decision.require_positive_roi:
            if roi is None:
                missing.append("ROI unavailable")
            elif roi <= 0:
                failures.append(f"ROI not positive ({roi:.4f})")
        if drawdown is None:
            missing.append("matching sweep drawdown unavailable")
        elif drawdown > resolved.decision.max_drawdown:
            failures.append(
                f"drawdown {drawdown:.4f} > {resolved.decision.max_drawdown:.4f}"
            )
        if require_positive_clv:
            if clv_ci is None:
                missing.append("mean CLV CI low unavailable")
            elif clv_ci <= 0:
                failures.append(f"mean CLV CI low not positive ({clv_ci:.6f})")
        if _failed_gate(suite_row):
            failures.append("CLV audit explicitly failed or blocked")
        if resolved.purpose == "finalist_certification":
            if dense_adequate is None:
                missing.append("dense quote coverage/timing evidence unavailable")
            elif not dense_adequate:
                failures.append("dense quote coverage/timing evidence failed")
            if not timing_stability_status:
                missing.append("timing-stability evidence unavailable")
            elif timing_stability_status != "PASS":
                failures.append("timing-stability evidence failed")
            if not timing_path.is_file() or not diagnosis_path.is_file():
                missing.append("persisted timing-stability files unavailable")
            elif not _timing_csv_valid(timing_path) or not timing_diagnosis_valid:
                missing.append("persisted timing-stability evidence is malformed")
            elif timing_diagnosis_status != timing_stability_status:
                missing.append("timing-stability attestation does not match diagnosis")
        if require_positive_ranker:
            if not ranker_rows:
                missing.append("matching ranker diagnostics unavailable")
            elif not ranker_ci_available:
                missing.append("ranker CI low unavailable")
            elif not passing_ranker:
                failures.append("no passing ranker with positive CI low")
        if require_monotonicity:
            if not ranker_rows:
                missing.append("ranker monotonicity unavailable")
            elif not monotonic:
                failures.append("ranker CLV bins are not monotonic")

        candidate_checks.append(
            {
                "label": label,
                "edge_threshold": edge,
                "total_bets": bets,
                "roi": roi,
                "max_drawdown": drawdown,
                "mean_clv_ci_low": clv_ci,
                "best_ranker_ci_low": best_ranker_ci,
                "edge_bucket_monotonic": monotonic if ranker_rows else None,
                "dense_table_adequate": dense_adequate,
                "timing_stability_status": timing_stability_status or None,
                "timing_stability": {
                    "verified": (
                        timing_path.is_file()
                        and diagnosis_path.is_file()
                        and timing_diagnosis_status == timing_stability_status == "PASS"
                    ),
                    "status": timing_stability_status or None,
                    "timing_csv": _file_evidence(timing_path),
                    "diagnosis_json": _file_evidence(diagnosis_path),
                },
                "failures": failures,
                "missing": missing,
                "passes": not failures and not missing,
            }
        )

    evidence["candidate_checks"] = candidate_checks
    passing = [candidate for candidate in candidate_checks if candidate["passes"]]
    evidence["passing_candidates"] = [candidate["label"] for candidate in passing]

    if passing and not global_missing and not global_failures:
        classification = "Confirm"
        reasons = [f"Passing candidate: {candidate['label']}" for candidate in passing]
    elif global_failures or any(candidate["failures"] for candidate in candidate_checks):
        classification = "Exclude"
        reasons = [*global_failures]
        reasons.extend(
            f"{candidate['label']}: {reason}"
            for candidate in candidate_checks
            for reason in candidate["failures"]
        )
    elif global_missing or any(candidate["missing"] for candidate in candidate_checks):
        classification = "Shelf"
        reasons = [*global_missing]
        reasons.extend(
            f"{candidate['label']}: {reason}"
            for candidate in candidate_checks
            for reason in candidate["missing"]
        )
    else:
        classification = "Exclude"
        reasons = [
            f"{candidate['label']}: {reason}"
            for candidate in candidate_checks
            for reason in candidate["failures"]
        ] or ["No candidate passed the configured evidence gates"]

    posture = _posture(
        resolved.purpose,
        classification == "Confirm",
        resolved.audit.mode == "full",
    )
    return DecisionRecord(classification, posture, reasons, evidence)
