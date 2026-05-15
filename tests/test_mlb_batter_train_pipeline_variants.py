"""Regression tests for MLB batter clean retrain variant wiring."""

import json

from src.models.mlb import mlb_batter_train_pipeline as pipeline


class _DummyEngine:
    pass


def _make_orchestrator(monkeypatch, tmp_path, exclude_prop_line=False):
    monkeypatch.setattr(pipeline, "get_engine", lambda local=False: _DummyEngine())
    return pipeline.MLBBatterTrainingOrchestrator(
        stat="hits",
        base_artifacts_dir=str(tmp_path),
        exclude_prop_line=exclude_prop_line,
    )


def test_no_prop_line_variant_excludes_all_prop_line_candidates(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(monkeypatch, tmp_path, exclude_prop_line=True)

    candidates = orchestrator._numeric_model_feature_candidates(
        {
            "game_id": "int64",
            "actual": "int64",
            "batter_avg_h_l5": "float64",
            "prop_line_batter_hits": "float64",
            "prop_line_batter_rbis": "float64",
        }
    )

    assert candidates == ["batter_avg_h_l5"]


def test_with_prop_line_variant_keeps_prop_line_candidates(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(monkeypatch, tmp_path, exclude_prop_line=False)

    candidates = orchestrator._numeric_model_feature_candidates(
        {
            "game_id": "int64",
            "actual": "int64",
            "batter_avg_h_l5": "float64",
            "prop_line_batter_hits": "float64",
        }
    )

    assert candidates == ["batter_avg_h_l5", "prop_line_batter_hits"]


def test_variant_metadata_preregisters_comparison_rule(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(monkeypatch, tmp_path, exclude_prop_line=True)

    orchestrator._save_run_config([2023, 2024], 2025, "2025-07-01")

    config = json.loads((orchestrator.run_dir / "run_config.json").read_text())
    assert config["exclude_prop_line"] is True
    assert config["variant"] == "no_prop_line"
    assert "quote_clean_replay" in config["pre_registered_comparison_rule"]
    assert "CLV" in config["pre_registered_comparison_rule"]
