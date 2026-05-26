"""Promotion-evidence metadata contracts for MLB quote-clean sweeps.

This module is intentionally report-only. It labels whether a sweep artifact is
promotion-grade evidence without blocking legacy/debug workflows by default.
"""

from __future__ import annotations

from typing import Any

from src.backtesting.mlb.sweep_config import QuoteCleanConfig

DENSE_CLV_LINE_SOURCE = "mlb_player_props_clv_snapshots"


def build_promotion_contract_metadata(
    quote_clean: QuoteCleanConfig,
    *,
    dense_clv_linked_coverage_audit_note: str | None = None,
) -> dict[str, Any]:
    """Return report-only metadata describing sweep evidence quality.

    Legacy non-quote-clean mode remains runnable but is labeled hypothesis-only.
    Quote-clean mode is promotion-grade only as replay evidence; downstream CLV,
    dropout, and ranking gates still decide whether a model/config is promotable.
    Dense CLV snapshots require an explicit linked-coverage audit note because
    unlinked game/player rows can silently change the replay population.
    """
    warnings: list[str] = []
    promotion_grade = quote_clean.enabled
    evidence_label = "promotion_grade_quote_clean" if promotion_grade else "hypothesis_only"

    if not quote_clean.enabled:
        warnings.append(
            "Legacy line mode aggregates snapshots and is hypothesis-only; use --quote-clean for promotion evidence."
        )

    dense_clv_audit_required = False
    if quote_clean.enabled and quote_clean.line_source == DENSE_CLV_LINE_SOURCE:
        dense_clv_audit_required = not bool(dense_clv_linked_coverage_audit_note)
        if dense_clv_audit_required:
            warnings.append(
                "Dense CLV line source requires an explicit linked coverage audit note for game_id/player_id coverage."
            )

    return {
        "promotion_grade": promotion_grade,
        "evidence_label": evidence_label,
        "quote_clean": {
            "enabled": quote_clean.enabled,
            "cutoff_time_et": quote_clean.cutoff_time_et,
            "decision_policy": quote_clean.decision_policy,
            "relative_minutes": quote_clean.relative_minutes,
            "line_source": quote_clean.line_source,
        },
        "line_source": quote_clean.line_source,
        "quote_decision_policy": quote_clean.decision_policy,
        "dense_clv_linked_coverage_audit_required": dense_clv_audit_required,
        "dense_clv_linked_coverage_audit_note": dense_clv_linked_coverage_audit_note,
        "warnings": warnings,
    }
