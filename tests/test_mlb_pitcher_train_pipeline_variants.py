"""Regression tests for MLB pitcher K feature-family controls."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.models.mlb import mlb_train_pipeline as pipeline


class _DummyEngine:
    pass


class _DummySelector:
    def __init__(self, *args, **kwargs):
        pass

    def select_features_per_quantile(self, df, target_col, candidates, model_name):
        assert target_col == "actual_so"
        assert model_name == "Pitcher K"
        return {
            0.1: list(candidates[:2]),
            0.5: list(candidates[1:3]),
            0.9: list(candidates[-2:]),
        }


def _make_orchestrator(monkeypatch, tmp_path, **kwargs):
    monkeypatch.setattr(pipeline, "get_engine", lambda local=False: _DummyEngine())
    monkeypatch.setattr(pipeline, "MLBFeatureStore", lambda engine: object())
    monkeypatch.setattr(pipeline, "PitcherTrainingLoader", lambda feature_store: object())
    return pipeline.MLBTrainingOrchestrator(
        base_artifacts_dir=str(tmp_path),
        **kwargs,
    )


def _minimal_pitcher_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "game_id": [1, 2, 3],
            "player_id": [10, 11, 12],
            "actual_so": [5, 7, 4],
            "actual_ip": [5.0, 6.0, 4.2],
            "pitcher_avg_so_l5": [5.1, 6.2, 4.4],
            "pitcher_avg_ip_l5": [5.2, 6.1, 4.8],
            "pitcher_min_ip_l5": [4.0, 5.0, 3.2],
            "pitcher_max_ip_l5": [7.0, 7.2, 6.0],
            "pitcher_pct_starts_under_5_ip_l10": [0.2, 0.1, 0.4],
            "manager_starter_short_hook_rate_l30": [0.3, 0.2, 0.5],
            "pitcher_fastball_velo_delta_l3_vs_szn": [0.1, -0.2, 0.0],
            "team_bullpen_pitches_last_3d": [88.0, 74.0, 102.0],
            "pitcher_left_last_start_early_flag": [0.0, 0.0, 1.0],
            "projected_lineup_k_pct": [0.22, 0.24, 0.20],
        }
    )


def test_parser_accepts_pitcher_force_feature_controls() -> None:
    parser = pipeline.build_arg_parser()
    args = parser.parse_args([
        "--force-include-families", "workload-leash,team_hook",
        "--force-exclude-families", "opponent_contact",
        "--force-include-features", "pitcher_avg_ip_l5",
        "--force-exclude-features", "projected_lineup_k_pct",
    ])

    assert args.force_include_families == ["workload-leash,team_hook"]
    assert args.force_exclude_families == ["opponent_contact"]
    assert args.force_include_features == ["pitcher_avg_ip_l5"]
    assert args.force_exclude_features == ["projected_lineup_k_pct"]


def test_orchestrator_normalizes_pitcher_force_feature_controls(monkeypatch, tmp_path) -> None:
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["workload-leash,team_hook"],
        force_exclude_families=["opponent_contact"],
        force_include_features=["pitcher_avg_ip_l5"],
        force_exclude_features=["projected_lineup_k_pct"],
    )

    assert orchestrator.force_include_families == ["workload_leash", "team_hook"]
    assert orchestrator.force_exclude_families == ["opponent_contact"]
    assert orchestrator.force_include_features == ["pitcher_avg_ip_l5"]
    assert orchestrator.force_exclude_features == ["projected_lineup_k_pct"]


def test_feature_selection_prepends_forced_family_features_and_keeps_lockout(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(pipeline, "ImprovedFeatureSelector", _DummySelector)
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["workload_leash"],
    )

    selected = orchestrator._run_feature_selection(_minimal_pitcher_df())

    for features in selected.values():
        assert features[:3] == ["pitcher_avg_ip_l5", "pitcher_min_ip_l5", "pitcher_max_ip_l5"]
        assert "projected_lineup_k_pct" not in features


def test_force_exclude_features_remove_pitcher_candidates(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(pipeline, "ImprovedFeatureSelector", _DummySelector)
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_exclude_features=["pitcher_avg_so_l5"],
    )

    selected = orchestrator._run_feature_selection(_minimal_pitcher_df())

    for features in selected.values():
        assert "pitcher_avg_so_l5" not in features


def test_exact_forced_include_can_override_phase3a_lockout(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(pipeline, "ImprovedFeatureSelector", _DummySelector)
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_features=["projected_lineup_k_pct"],
    )

    selected = orchestrator._run_feature_selection(_minimal_pitcher_df())

    for features in selected.values():
        assert features[0] == "projected_lineup_k_pct"


def test_missing_forced_pitcher_feature_fails_loudly(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(pipeline, "ImprovedFeatureSelector", _DummySelector)
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_features=["missing_pitcher_feature"],
    )

    with pytest.raises(ValueError, match="Forced feature.*missing"):
        orchestrator._run_feature_selection(_minimal_pitcher_df())


def test_pitcher_force_feature_metadata_written_to_run_config(monkeypatch, tmp_path) -> None:
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["workload_leash"],
        force_exclude_families=["opponent_contact"],
        force_include_features=["pitcher_avg_ip_l5"],
        force_exclude_features=["projected_lineup_k_pct"],
    )

    orchestrator._save_run_config([2024, 2025], 2026, "2026-04-12")

    config = json.loads((orchestrator.run_dir / "run_config.json").read_text())
    assert config["force_include_families"] == ["workload_leash"]
    assert config["force_exclude_families"] == ["opponent_contact"]
    assert config["force_include_features"] == ["pitcher_avg_ip_l5"]
    assert config["force_exclude_features"] == ["projected_lineup_k_pct"]
    assert config["force_feature_experiment"] is True
