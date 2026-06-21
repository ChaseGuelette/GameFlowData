from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.models.mlb.training.artifacts import (
    build_run_directory,
    finalize_incomplete_run_directory,
    write_calibration_report,
    write_feature_experiment_metadata,
    write_feature_manifest,
    write_model_manifest,
    write_run_config,
    write_training_metadata,
)


def test_build_run_directory_creates_incomplete_directory_with_final_name(tmp_path: Path) -> None:
    timestamp = datetime(2026, 6, 21, 16, 30, 5)

    run_dir, final_name = build_run_directory(
        tmp_path,
        prefix="mlb_run_batter_hits",
        timestamp=timestamp,
        suffix="no_prop_line",
    )

    assert final_name == "mlb_run_batter_hits_20260621_163005_no_prop_line"
    assert run_dir.name == f"{final_name}_incomplete"
    assert run_dir.is_dir()


def test_finalize_incomplete_run_directory_renames_and_returns_final_path(tmp_path: Path) -> None:
    run_dir, final_name = build_run_directory(tmp_path, prefix="mlb_run", timestamp=datetime(2026, 6, 21, 1, 2, 3))
    (run_dir / "sentinel.txt").write_text("ok", encoding="utf-8")

    final_dir = finalize_incomplete_run_directory(run_dir, final_name)

    assert final_dir.name == final_name
    assert (final_dir / "sentinel.txt").read_text(encoding="utf-8") == "ok"
    assert not run_dir.exists()


def test_artifact_json_writers_preserve_expected_shapes(tmp_path: Path) -> None:
    write_run_config(tmp_path, {"stat": "hits", "cal_end_date": "2026-04-12"})
    write_feature_manifest(tmp_path, {0.1: ["a"], "binary": ["b"]})
    write_feature_experiment_metadata(tmp_path, {"required_features": ["a"]})
    write_calibration_report(tmp_path, {"batter_hits": {"mean_ratio": 1.0}})
    write_training_metadata(tmp_path, {"git_hash": "abc", "train_rows": 10})

    assert json.loads((tmp_path / "run_config.json").read_text())["stat"] == "hits"
    assert json.loads((tmp_path / "feature_manifest.json").read_text()) == {"0.1": ["a"], "binary": ["b"]}
    assert json.loads((tmp_path / "feature_experiment_metadata.json").read_text())["required_features"] == ["a"]
    assert json.loads((tmp_path / "calibration_report_combined.json").read_text())["batter_hits"]["mean_ratio"] == 1.0
    assert json.loads((tmp_path / "training_metadata.json").read_text())["train_rows"] == 10


def test_model_manifest_records_cross_artifact_pointers(tmp_path: Path) -> None:
    write_model_manifest(
        tmp_path,
        stat_key="pitcher_strikeouts",
        model_type="quantile",
        profile_name="pitcher_strikeouts",
        created_at="2026-06-21T16:30:05",
        git_hash="abc123",
        artifact_files=["pitcher_k_model.joblib"],
        compatibility_loader="src.models.mlb.mlb_model_suite.MLBModelSuite",
    )

    manifest = json.loads((tmp_path / "model_manifest.json").read_text())
    assert manifest == {
        "schema_version": 1,
        "stat_key": "pitcher_strikeouts",
        "model_type": "quantile",
        "profile_name": "pitcher_strikeouts",
        "created_at": "2026-06-21T16:30:05",
        "git_hash": "abc123",
        "artifact_files": ["pitcher_k_model.joblib"],
        "feature_manifest_file": "feature_manifest.json",
        "calibration_report_file": "calibration_report_combined.json",
        "training_metadata_file": "training_metadata.json",
        "compatibility_loader": "src.models.mlb.mlb_model_suite.MLBModelSuite",
    }
