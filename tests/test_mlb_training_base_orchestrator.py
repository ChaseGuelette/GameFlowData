from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.models.mlb import mlb_batter_train_pipeline, mlb_train_pipeline
from src.models.mlb.training.base_orchestrator import BaseMLBTrainingOrchestrator


class _DummyOrchestrator(BaseMLBTrainingOrchestrator):
    def __init__(self, base_artifacts_dir: str | Path):
        super().__init__(
            base_artifacts_dir=base_artifacts_dir,
            artifact_prefix="dummy_run",
            timestamp=datetime(2026, 6, 21, 17, 30, 0),
        )


def test_base_orchestrator_owns_run_directory_lifecycle(tmp_path: Path) -> None:
    orchestrator = _DummyOrchestrator(tmp_path)

    assert orchestrator.run_dir.name == "dummy_run_20260621_173000_incomplete"
    assert orchestrator.run_dir.is_dir()

    (orchestrator.run_dir / "sentinel.txt").write_text("ok", encoding="utf-8")
    final_dir = orchestrator._finalize_run_directory()

    assert final_dir == tmp_path / "dummy_run_20260621_173000"
    assert orchestrator.run_dir == final_dir
    assert (final_dir / "sentinel.txt").read_text(encoding="utf-8") == "ok"


def test_base_orchestrator_provides_shared_artifact_writers(tmp_path: Path) -> None:
    orchestrator = _DummyOrchestrator(tmp_path)

    orchestrator._write_run_config({"stat": "dummy"})
    orchestrator._write_feature_manifest({0.1: ["a"]})
    orchestrator._write_calibration_report({"dummy": {"gap": 0.01}})
    orchestrator._write_training_metadata({"train_rows": 3})
    orchestrator._write_model_manifest(
        stat_key="dummy",
        model_type="quantile",
        profile_name="dummy",
        git_hash="abc123",
        artifact_files=["dummy.joblib"],
    )

    assert json.loads((orchestrator.run_dir / "run_config.json").read_text())["stat"] == "dummy"
    assert json.loads((orchestrator.run_dir / "feature_manifest.json").read_text()) == {"0.1": ["a"]}
    assert json.loads((orchestrator.run_dir / "training_metadata.json").read_text())["train_rows"] == 3
    manifest = json.loads((orchestrator.run_dir / "model_manifest.json").read_text())
    assert manifest["stat_key"] == "dummy"
    assert manifest["created_at"] == "2026-06-21T17:30:00"


def test_existing_pitcher_and_batter_orchestrators_use_shared_base() -> None:
    assert issubclass(mlb_train_pipeline.MLBTrainingOrchestrator, BaseMLBTrainingOrchestrator)
    assert issubclass(mlb_batter_train_pipeline.MLBBatterTrainingOrchestrator, BaseMLBTrainingOrchestrator)
