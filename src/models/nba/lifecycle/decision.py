"""Pure report-only truth table. No profitability or config switch can promote."""
from __future__ import annotations

from src.models.nba_artifacts import canonical_sha256

REQUIRED = ("validate", "train_or_attach", "artifact_identity", "sweep", "audit")


def build_decision(*, purpose: str, stages: dict, requested_checks: list[str]) -> dict:
    required = [stages.get(name, {}) for name in REQUIRED]
    audit = stages.get("audit", {}).get("output", {})
    statuses = audit.get("requested_check_statuses", {})
    invalid = any(item.get("status") == "failed" for item in required)
    invalid |= any(item.get("status") == "FAIL" for item in audit.get("issues", []))
    invalid |= "FAIL" in statuses.values() or audit.get("report_exit_code") == 1
    invalid |= audit.get("exit_code") not in (None, 0, 2)
    complete = all(item.get("status") == "completed" and item.get("input_identity")
                   and item.get("output_identity") for item in required)
    complete = bool(complete and audit.get("export_verified") is True and audit.get("exit_code") == 0
                    and audit.get("report_exit_code") == 0 and audit.get("report_state") == "complete"
                    and type(requested_checks) is list and requested_checks
                    and "integrity" in requested_checks
                    and len(set(requested_checks)) == len(requested_checks)
                    and set(statuses) == set(requested_checks)
                    and statuses.get("integrity") == "PASS"
                    and all(value == "PASS" for value in statuses.values()))
    if invalid:
        classification = "blocked_invalid_evidence"
    elif not complete:
        classification = "blocked_incomplete_lifecycle"
    elif purpose == "discovery":
        classification = "controlled_mechanics_complete"
    else:
        classification = "blocked_historical_integration_pending"
    return {"schema_version": 1, "classification": classification, "purpose": purpose,
            "report_only": True, "historical_readiness": "unverified", "decision_grade": False,
            "live_posture": "blocked", "promotion_authorized": False, "training_launch_authorized": False,
            "policy": "unresolved_until_slice8", "stage_identity": canonical_sha256(stages),
            "required_stages": list(REQUIRED), "requested_checks": requested_checks}


def decision_markdown(decision: dict) -> str:
    return ("NBA lifecycle (report only)\n\n" + decision["classification"]
            + "\n\nHistorical readiness: unverified. Decision grade: false. Live posture: blocked.\n"
            + "Promotion and training launch are not authorized. Source policy remains unresolved until Slice 8.\n")
