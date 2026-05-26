from __future__ import annotations

import json
from pathlib import Path

from scripts.audit_mlb_model_artifacts import SuiteSnapshot, inspect_stat_artifacts, run_audit


def test_inspect_stat_artifacts_reads_batter_feature_count(tmp_path: Path):
    model_dir = tmp_path / "production"
    model_dir.mkdir()
    (model_dir / "batter_hits_xgblss_booster.json").write_text("{}", encoding="utf-8")
    (model_dir / "batter_hits_negbin_meta.json").write_text(
        json.dumps({"feature_names": ["prop_line_batter_hits", "batter_woba_szn"]}),
        encoding="utf-8",
    )

    info = inspect_stat_artifacts(model_dir, "batter_hits")

    assert info["model_type"] == "negbin"
    assert info["core_artifacts"]["batter_hits_xgblss_booster.json"] is True
    assert info["feature_counts"] == {"count": 2}


def test_run_audit_fails_when_required_stat_not_loaded(tmp_path: Path):
    def resolver(path: Path) -> Path:
        return path

    def suite_loader(_path: Path) -> SuiteSnapshot:
        return SuiteSnapshot(available_stats=["pitcher_strikeouts"], predictor_classes={"pitcher_strikeouts": "Fake"})

    summary, failures = run_audit(
        tmp_path,
        ["pitcher_strikeouts", "batter_hits"],
        resolver=resolver,
        suite_loader=suite_loader,
    )

    assert summary["status"] == "fail"
    assert any("Missing required loaded model stats" in failure for failure in failures)


def test_run_audit_requires_explicit_batter_hrr_validation(tmp_path: Path):
    (tmp_path / "batter_hrr_xgblss_booster.json").write_text("{}", encoding="utf-8")
    (tmp_path / "batter_hrr_negbin_meta.json").write_text(
        json.dumps({"feature_names": ["batter_woba_szn"]}),
        encoding="utf-8",
    )

    def resolver(path: Path) -> Path:
        return path

    def suite_loader(_path: Path) -> SuiteSnapshot:
        return SuiteSnapshot(available_stats=["batter_hrr"], predictor_classes={"batter_hrr": "Fake"})

    summary, failures = run_audit(
        tmp_path,
        ["batter_hrr"],
        resolver=resolver,
        suite_loader=suite_loader,
    )

    assert summary["status"] == "fail"
    assert any("not marked validated" in failure for failure in failures)

    summary, failures = run_audit(
        tmp_path,
        ["batter_hrr"],
        validated_optional_stats={"batter_hrr"},
        resolver=resolver,
        suite_loader=suite_loader,
    )

    assert failures == []
    assert summary["status"] == "ok"
