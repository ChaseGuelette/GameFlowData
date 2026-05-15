from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.diagnose_mlb_clv_failure_modes import diagnose_clv_failure_modes


def _write_required_clv_dir(path: Path, *, summary: dict | None = None, matches: pd.DataFrame | None = None) -> None:
    path.mkdir(parents=True, exist_ok=True)
    base_summary = {
        "group": "overall",
        "n": 150,
        "n_scored": 140,
        "n_same_book": 125,
        "n_consensus_fallback": 10,
        "n_unmatched": 15,
        "mean_clv_implied_prob": 0.025,
        "mean_clv_ci_low": 0.004,
        "mean_clv_ci_high": 0.045,
        "edge_clv_spearman": 0.24,
        "edge_clv_ci_low": 0.03,
        "edge_clv_ci_high": 0.42,
        "n_blocks": 18,
    }
    if summary:
        base_summary.update(summary)
    pd.DataFrame([base_summary]).to_csv(path / "clv_summary.csv", index=False)
    pd.DataFrame([
        {"group": "+100_to_+149", "n": 80, "n_scored": 76, "mean_clv_implied_prob": 0.02, "mean_clv_ci_low": 0.001, "mean_clv_ci_high": 0.04},
        {"group": "-110_to_+99", "n": 70, "n_scored": 64, "mean_clv_implied_prob": 0.03, "mean_clv_ci_low": 0.002, "mean_clv_ci_high": 0.05},
    ]).to_csv(path / "clv_by_plus_odds_band.csv", index=False)
    pd.DataFrame([
        {"group": "0.05-0.08", "n": 75, "n_scored": 70, "mean_clv_implied_prob": 0.02},
        {"group": "0.08+", "n": 75, "n_scored": 70, "mean_clv_implied_prob": 0.03},
    ]).to_csv(path / "clv_by_edge_bin.csv", index=False)
    pd.DataFrame([
        {"group": "espnbet", "n": 70, "n_scored": 65, "mean_clv_implied_prob": 0.02, "mean_clv_ci_low": 0.001, "mean_clv_ci_high": 0.04},
        {"group": "draftkings", "n": 80, "n_scored": 75, "mean_clv_implied_prob": 0.03, "mean_clv_ci_low": 0.002, "mean_clv_ci_high": 0.05},
    ]).to_csv(path / "clv_by_bookmaker.csv", index=False)
    pd.DataFrame([
        {"horizon": "+15m", "n_scored": 100, "mean_clv_implied_prob": 0.01},
        {"horizon": "+30m", "n_scored": 100, "mean_clv_implied_prob": 0.015},
        {"horizon": "+60m", "n_scored": 100, "mean_clv_implied_prob": 0.02},
    ]).to_csv(path / "clv_timing_stability.csv", index=False)
    if matches is None:
        matches = pd.DataFrame([
            {"clv_source": "same_book_close", "line_movement_class": "same_line_odds_clv", "close_snapshot_time": "2026-04-13T22:00:00Z", "bet_snapshot_time": "2026-04-13T17:30:00Z"}
        ] * 120)
    matches.to_csv(path / "clv_matches.csv", index=False)
    pd.DataFrame([{"decision": "phase2_allowed", "phase2_allowed": True}]).to_csv(path / "phase1b_decision.csv", index=False)


def test_minimal_positive_case_passes(tmp_path: Path) -> None:
    _write_required_clv_dir(tmp_path)

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "pass"
    assert result["failure_modes"] == []
    assert (tmp_path / "out" / "clv_failure_modes.json").exists()
    assert (tmp_path / "out" / "clv_failure_modes.md").exists()


def test_negative_mean_clv_fails_model_or_edge(tmp_path: Path) -> None:
    _write_required_clv_dir(tmp_path, summary={"mean_clv_implied_prob": -0.01, "mean_clv_ci_high": -0.001})

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "fail_model_or_edge"
    assert "negative_mean_clv" in result["failure_modes"]


def test_positive_mean_but_ci_crosses_zero_is_underpowered(tmp_path: Path) -> None:
    _write_required_clv_dir(tmp_path, summary={"mean_clv_implied_prob": 0.01, "mean_clv_ci_low": -0.002})

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "inconclusive_underpowered"
    assert "underpowered_or_inconclusive" in result["failure_modes"]


def test_edge_ranking_failure_blocks_promotion(tmp_path: Path) -> None:
    _write_required_clv_dir(tmp_path, summary={"edge_clv_spearman": -0.12, "edge_clv_ci_low": -0.4})

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "fail_model_or_edge"
    assert "edge_ranking_failure" in result["failure_modes"]


def test_consensus_fallback_dominance_is_data_failure(tmp_path: Path) -> None:
    _write_required_clv_dir(tmp_path, summary={"n_same_book": 20, "n_consensus_fallback": 100, "n_unmatched": 30})

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "fail_data_or_timing"
    assert "same_book_coverage_failure" in result["failure_modes"]


def test_missing_input_files_are_invalid(tmp_path: Path) -> None:
    tmp_path.mkdir(exist_ok=True)
    pd.DataFrame().to_csv(tmp_path / "clv_summary.csv", index=False)

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "invalid_missing_inputs"
    assert "data_quality_failure" in result["failure_modes"]


def test_missing_required_columns_are_invalid(tmp_path: Path) -> None:
    _write_required_clv_dir(tmp_path)
    pd.DataFrame([{"group": "overall", "n": 10}]).to_csv(tmp_path / "clv_summary.csv", index=False)

    result = diagnose_clv_failure_modes(tmp_path, tmp_path / "out")

    assert result["decision_label"] == "invalid_missing_inputs"
    assert "data_quality_failure" in result["failure_modes"]
