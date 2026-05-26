from __future__ import annotations

from src.backtesting.mlb.promotion_contracts import build_promotion_contract_metadata
from src.backtesting.mlb.sweep_config import QuoteCleanConfig


def test_legacy_mode_is_allowed_but_labeled_hypothesis_only():
    metadata = build_promotion_contract_metadata(QuoteCleanConfig(enabled=False))

    assert metadata["promotion_grade"] is False
    assert metadata["evidence_label"] == "hypothesis_only"
    assert metadata["quote_clean"]["enabled"] is False
    assert any("Legacy line mode" in warning for warning in metadata["warnings"])


def test_promotion_grade_metadata_records_quote_clean_line_source_and_decision_policy():
    metadata = build_promotion_contract_metadata(
        QuoteCleanConfig(
            enabled=True,
            cutoff_time_et="13:30",
            decision_policy="slate_or_tminus",
            relative_minutes=60,
            line_source="mlb_raw_player_props",
        )
    )

    assert metadata["promotion_grade"] is True
    assert metadata["evidence_label"] == "promotion_grade_quote_clean"
    assert metadata["quote_clean"] == {
        "enabled": True,
        "cutoff_time_et": "13:30",
        "decision_policy": "slate_or_tminus",
        "relative_minutes": 60,
        "line_source": "mlb_raw_player_props",
    }
    assert metadata["line_source"] == "mlb_raw_player_props"
    assert metadata["quote_decision_policy"] == "slate_or_tminus"
    assert metadata["warnings"] == []


def test_dense_clv_line_source_requires_linked_coverage_audit_note_in_metadata():
    metadata = build_promotion_contract_metadata(
        QuoteCleanConfig(
            enabled=True,
            decision_policy="slate_or_tminus",
            line_source="mlb_player_props_clv_snapshots",
        )
    )

    assert metadata["promotion_grade"] is True
    assert metadata["dense_clv_linked_coverage_audit_required"] is True
    assert metadata["dense_clv_linked_coverage_audit_note"] is None
    assert any("linked coverage audit" in warning for warning in metadata["warnings"])

    with_note = build_promotion_contract_metadata(
        QuoteCleanConfig(
            enabled=True,
            decision_policy="slate_or_tminus",
            line_source="mlb_player_props_clv_snapshots",
        ),
        dense_clv_linked_coverage_audit_note="linked game_id/player_id coverage checked in audit_suite",
    )

    assert with_note["dense_clv_linked_coverage_audit_required"] is False
    assert with_note["dense_clv_linked_coverage_audit_note"] == "linked game_id/player_id coverage checked in audit_suite"
    assert with_note["warnings"] == []
