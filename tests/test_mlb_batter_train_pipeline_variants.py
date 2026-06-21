"""Regression tests for MLB batter clean retrain variant wiring."""

import json

import pytest

from src.models.mlb import mlb_batter_train_pipeline as pipeline
from src.models.mlb.features.contracts import (
    BATTER_FORCE_FEATURE_FAMILIES,
    features_for_batter_families,
    normalize_feature_family_names,
    normalize_feature_names,
)


class _DummyEngine:
    pass


def _make_orchestrator(monkeypatch, tmp_path, exclude_prop_line=False, **kwargs):
    monkeypatch.setattr(pipeline, "get_engine", lambda local=False: _DummyEngine())
    return pipeline.MLBBatterTrainingOrchestrator(
        stat="hits",
        base_artifacts_dir=str(tmp_path),
        exclude_prop_line=exclude_prop_line,
        **kwargs,
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


def test_batter_force_feature_family_registry_contains_core_families():
    assert "contact_quality" in BATTER_FORCE_FEATURE_FAMILIES
    assert "matchup_pitcher" in BATTER_FORCE_FEATURE_FAMILIES
    assert "bullpen" in BATTER_FORCE_FEATURE_FAMILIES
    assert "environment" in BATTER_FORCE_FEATURE_FAMILIES
    assert "opportunity" in BATTER_FORCE_FEATURE_FAMILIES
    assert "market" in BATTER_FORCE_FEATURE_FAMILIES


def test_feature_family_expansion_preserves_order_and_dedupes():
    expanded = features_for_batter_families(["market", "environment"])
    assert expanded[:2] == ["prop_line_batter_hits", "line_total"]
    assert "park_hits_factor" in expanded
    assert len(expanded) == len(set(expanded))


def test_normalize_feature_family_names_accepts_commas_and_hyphens():
    assert normalize_feature_family_names(["contact-quality,matchup_pitcher"]) == [
        "contact_quality",
        "matchup_pitcher",
    ]


def test_normalize_feature_family_names_rejects_unknown_family():
    with pytest.raises(ValueError, match="Unknown batter feature family"):
        normalize_feature_family_names(["made_up_family"])


def test_normalize_feature_names_accepts_comma_lists():
    assert normalize_feature_names(["a,b", "c"]) == ["a", "b", "c"]


def test_orchestrator_normalizes_force_feature_controls(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["contact-quality,matchup_pitcher"],
        force_exclude_families=["market"],
        force_include_features=["batter_xba_l10"],
        force_exclude_features=["prop_line_batter_hits"],
    )

    assert orchestrator.force_include_families == ["contact_quality", "matchup_pitcher"]
    assert orchestrator.force_exclude_families == ["market"]
    assert orchestrator.force_include_features == ["batter_xba_l10"]
    assert orchestrator.force_exclude_features == ["prop_line_batter_hits"]


def test_force_exclude_family_removes_candidates(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_exclude_families=["contact_quality"],
    )

    candidates = orchestrator._numeric_model_feature_candidates(
        {
            "actual": "int64",
            "batter_avg_h_l5": "float64",
            "batter_xba_l10": "float64",
            "batter_barrel_pct_l10": "float64",
            "prop_line_batter_hits": "float64",
        }
    )

    assert "batter_avg_h_l5" in candidates
    assert "prop_line_batter_hits" in candidates
    assert "batter_xba_l10" not in candidates
    assert "batter_barrel_pct_l10" not in candidates


def test_force_include_family_returns_required_features(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["market"],
    )

    candidates, required = orchestrator._resolve_selector_candidates_and_required_features(
        {
            "actual": "int64",
            "batter_avg_h_l5": "float64",
            "prop_line_batter_hits": "float64",
            "line_total": "float64",
        }
    )

    assert "batter_avg_h_l5" in candidates
    assert "prop_line_batter_hits" not in candidates
    assert "line_total" not in candidates
    assert required == ["prop_line_batter_hits", "line_total"]


def test_include_exclude_conflict_fails_loudly(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_features=["batter_xba_l10"],
        force_exclude_features=["batter_xba_l10"],
    )

    with pytest.raises(ValueError, match="both included and excluded"):
        orchestrator._resolve_selector_candidates_and_required_features(
            {"actual": "int64", "batter_xba_l10": "float64"}
        )


def test_missing_forced_feature_fails_loudly(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_features=["missing_feature"],
    )

    with pytest.raises(ValueError, match="Forced feature.*missing"):
        orchestrator._resolve_selector_candidates_and_required_features(
            {"actual": "int64", "batter_avg_h_l5": "float64"}
        )


def test_ab_feature_resolution_can_skip_forced_includes(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["opportunity"],
    )

    candidates, required = orchestrator._resolve_selector_candidates_and_required_features(
        {
            "actual": "int64",
            "lineup_position": "float64",
            "projected_ab": "float64",
            "batter_avg_ab_l5": "float64",
        },
        extra_excluded={"projected_ab"},
        apply_forced_includes=False,
    )

    assert "lineup_position" in candidates
    assert "batter_avg_ab_l5" in candidates
    assert "projected_ab" not in candidates
    assert required == []


def test_required_features_precede_selector_features_and_dedupe():
    assert pipeline.MLBBatterTrainingOrchestrator._merge_required_and_selected_features(
        ["b", "a"],
        ["a", "c"],
    ) == ["b", "a", "c"]


def test_force_feature_metadata_written_to_run_config(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["contact_quality"],
        force_exclude_families=["market"],
        force_include_features=["batter_xba_l10"],
        force_exclude_features=["prop_line_batter_hits"],
    )

    orchestrator._save_run_config([2024, 2025], 2026, "2026-04-12")

    config = json.loads((orchestrator.run_dir / "run_config.json").read_text())
    assert config["force_include_families"] == ["contact_quality"]
    assert config["force_exclude_families"] == ["market"]
    assert config["force_include_features"] == ["batter_xba_l10"]
    assert config["force_exclude_features"] == ["prop_line_batter_hits"]
    assert config["force_feature_experiment"] is True


def test_batter_model_manifest_written_from_shared_artifact_helper(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(monkeypatch, tmp_path)

    orchestrator._save_model_manifest(git_hash="abc123")

    manifest = json.loads((orchestrator.run_dir / "model_manifest.json").read_text())
    assert manifest["stat_key"] == "batter_hits"
    assert manifest["model_type"] == "binomial"
    assert manifest["profile_name"] == "batter_hits"
    assert "batter_hits_binomial_meta.json" in manifest["artifact_files"]
    assert manifest["training_metadata_file"] == "training_metadata.json"


def test_parser_accepts_force_feature_controls():
    parser = pipeline.build_arg_parser()
    args = parser.parse_args([
        "--stat", "hits",
        "--force-include-families", "contact_quality,matchup_pitcher",
        "--force-exclude-families", "market",
        "--force-include-features", "batter_xba_l10",
        "--force-exclude-features", "prop_line_batter_hits",
    ])

    assert args.force_include_families == ["contact_quality,matchup_pitcher"]
    assert args.force_exclude_families == ["market"]
    assert args.force_include_features == ["batter_xba_l10"]
    assert args.force_exclude_features == ["prop_line_batter_hits"]
