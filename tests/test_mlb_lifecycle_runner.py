from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from src.models.mlb.lifecycle.config import resolve_lifecycle_config
from src.models.mlb.lifecycle.runner import (
    LifecycleRunner,
    _verify_audit_outputs,
    _verify_sweep_outputs,
    build_artifact_identity,
)


def _config(tmp_path: Path) -> Path:
    artifact = tmp_path / "artifact"
    sweep = tmp_path / "sweep"
    artifact.mkdir(exist_ok=True)
    sweep.mkdir(exist_ok=True)
    for name in (
        "batter_rbis_xgblss_booster.json",
        "batter_rbis_negbin_meta.json",
    ):
        (artifact / name).write_text("{}", encoding="utf-8")
    (artifact / "run_config.json").write_text(
        json.dumps(
            {
                "stat": "rbis",
                "model_type": "negbin",
                "train_seasons": [2024, 2025],
                "cal_season": 2026,
                "cal_end_date": "2026-04-12",
                "exclude_prop_line": True,
                "variant": "no_prop_line",
                "force_include_families": [],
                "force_include_features": [],
            }
        ),
        encoding="utf-8",
    )
    (artifact / "model_manifest.json").write_text(
        json.dumps(
            {
                "stat_key": "batter_rbis",
                "profile_name": "batter_rbis",
                "model_type": "negative_binomial",
            }
        ),
        encoding="utf-8",
    )
    sweep_summary = sweep / "sweep_summary.csv"
    if not sweep_summary.exists():
        sweep_summary.write_text(
            "edge_threshold,total_bets,roi,max_drawdown\n0.1,120,0.08,0.12\n",
            encoding="utf-8",
        )
    sweep_config = sweep / "config_01_no_BL_edge0.1_kelly0.125"
    sweep_config.mkdir(exist_ok=True)
    (sweep_config / "metrics.json").write_text(
        json.dumps({"total_bets": 120, "roi": 0.08}), encoding="utf-8"
    )
    (sweep_config / "bets.csv").write_text(
        "player_id,profit,stake\n1,8,100\n", encoding="utf-8"
    )
    sweep_results = sweep / "sweep_results.json"
    if not sweep_results.exists():
        sweep_results.write_text(
            json.dumps(
                {
                    "sweep_metadata": {
                        "start_date": "2026-05-18",
                        "end_date": "2026-06-21",
                        "promotion_contract": {
                            "promotion_grade": True,
                            "quote_clean": {
                                "enabled": True,
                                "line_source": "mlb_player_props_clv_snapshots",
                                "decision_policy": "slate_or_tminus",
                                "relative_minutes": 60,
                            },
                        },
                    },
                    "results": [
                        {
                            "config": {
                                "tau": None,
                                "edge_threshold": 0.1,
                                "kelly_fraction": 0.125,
                            },
                            "metrics": {"total_bets": 120, "roi": 0.08},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
    cfg = {
        "experiment": {
            "name": "runner_test",
            "profile": "batter_rbis",
            "purpose": "independent_validation",
            "output_root": str(tmp_path / "run"),
        },
        "model": {
            "base": "no_prop_line",
            "artifact_dir": str(artifact),
            "sweep_dir": str(sweep),
            "feature_controls": {"mode": "include", "families": [], "features": []},
        },
        "training": {"seasons": [2024, 2025], "calibration_season": 2026, "calibration_end": "2026-04-12"},
        "evaluation": {"start": "2026-05-18", "end": "2026-06-21", "flat_bet": 100},
        "quotes": {"clean": True},
        "audit": {"minimum_bets": 100, "bootstrap_samples": 100, "mode": "clv_only"},
        "decision": {"max_drawdown": 0.25},
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    resolved = resolve_lifecycle_config(path)
    cfg["model"]["sweep_artifact_identity_sha256"] = build_artifact_identity(
        resolved, artifact, dry_run=False
    )["identity_sha256"]
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return path


def _write_clv_only_audit(output: Path) -> str:
    label = "config_01_no_BL_edge0.1_kelly0.125"
    clv = output / "clv" / label
    clv.mkdir(parents=True, exist_ok=True)
    (clv / "clv_matches.csv").write_text("edge,clv\n0.1,0.02\n", encoding="utf-8")
    (clv / "clv_timing_stability.csv").write_text(
        "horizon,horizon_clv_implied_prob\n+15m,0.01\n+30m,0.02\n+60m,0.03\n",
        encoding="utf-8",
    )
    diagnosis = output / "diagnosis" / label
    diagnosis.mkdir(parents=True, exist_ok=True)
    (diagnosis / "clv_failure_modes.json").write_text(
        json.dumps(
            {
                "decision_label": "pass",
                "timing_stability": {
                    "status": "PASS",
                    "required_horizons": ["+15m", "+30m", "+60m"],
                    "horizons_present": ["+15m", "+30m", "+60m"],
                    "coverage_pct": {"+15m": 100, "+30m": 100, "+60m": 100},
                },
            }
        ),
        encoding="utf-8",
    )
    item = {
        "label": label,
        "clv_returncode": 0,
        "diagnosis_returncode": 0,
        "total_bets": 120,
        "roi": 0.08,
        "mean_clv_ci_low": 0.01,
        "gate_status": "PASS",
        "decision_label": "pass",
        "timing_stability_status": "PASS",
        "timing_required_horizons": ["+15m", "+30m", "+60m"],
    }
    (output / "suite_manifest.csv").write_text(
        "label,total_bets,roi,mean_clv_ci_low,gate_status,decision_label,timing_stability_status\n"
        f"{label},120,0.08,0.01,PASS,pass,PASS\n",
        encoding="utf-8",
    )
    (output / "suite_manifest.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "audit_mode": "clv_only",
                    "full_audit_complete": False,
                    "full_audit_passed": False,
                },
                "items": [item],
            }
        ),
        encoding="utf-8",
    )
    (output / "suite_summary.md").write_text("# audit\n", encoding="utf-8")
    return label


def test_dry_run_never_calls_subprocess_and_writes_manifests(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    runner = LifecycleRunner(_config(tmp_path), dry_run=True, run_command=lambda argv: calls.append(argv) or 0)
    result = runner.run()
    assert calls == []
    root = Path(result["run_root"])
    assert (root / "resolved_config.yaml").exists()
    assert (root / "artifact_identity.json").exists()
    assert (root / "commands.json").exists()
    decision = json.loads((root / "promotion_decision.json").read_text(encoding="utf-8"))
    assert decision["posture"] == "live_blocked"


def test_dry_run_uses_isolated_output_root(tmp_path: Path) -> None:
    config = _config(tmp_path)
    real_root = tmp_path / "run"
    real_root.mkdir()
    status_path = real_root / "stage_status.json"
    status_path.write_text(json.dumps({"audit": {"status": "completed"}}), encoding="utf-8")

    result = LifecycleRunner(config, dry_run=True).run()

    assert Path(result["run_root"]) != real_root
    assert json.loads(status_path.read_text(encoding="utf-8")) == {
        "audit": {"status": "completed"}
    }


def test_attached_sweep_requires_matching_artifact_identity(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"]["sweep_artifact_identity_sha256"] = "deadbeef"
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(RuntimeError, match="Attached sweep artifact identity mismatch"):
        LifecycleRunner(config, dry_run=True).run()


def test_artifact_identity_rejects_exposed_metadata_mismatch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"]["feature_controls"]["families"] = ["platoon"]
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    resolved = resolve_lifecycle_config(config)
    artifact = resolved.artifact_dir
    (artifact / "model_manifest.json").write_text(
        json.dumps({"stat_key": "batter_hits"}), encoding="utf-8"
    )
    (artifact / "training_metadata.json").write_text(
        json.dumps({"cal_end_date": "2026-04-01"}), encoding="utf-8"
    )
    (artifact / "run_config.json").write_text(
        json.dumps(
            {
                "stat": "rbis",
                "model_type": "classifier",
                "train_seasons": [2023, 2024],
                "cal_season": 2025,
                "cal_end_date": "2026-04-01",
                "exclude_prop_line": False,
                "variant": "with_prop_line",
                "force_include_families": [],
                "force_include_features": [],
            }
        ),
        encoding="utf-8",
    )

    identity = build_artifact_identity(resolved, artifact, dry_run=False)

    assert identity["status"] == "FAIL"
    assert any("stat mismatch" in reason for reason in identity["reasons"])
    assert any("model type mismatch" in reason for reason in identity["reasons"])
    assert any("calibration cutoff mismatch" in reason for reason in identity["reasons"])
    assert any("calibration season mismatch" in reason for reason in identity["reasons"])
    assert any("training seasons mismatch" in reason for reason in identity["reasons"])
    assert any("model base mismatch" in reason for reason in identity["reasons"])
    assert any("forced-include family mismatch" in reason for reason in identity["reasons"])


def test_artifact_identity_requires_profile_and_calibration_cutoff_evidence(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    resolved = resolve_lifecycle_config(config)
    artifact = resolved.artifact_dir
    (artifact / "model_manifest.json").unlink()
    (artifact / "run_config.json").write_text(
        json.dumps(
            {
                "model_type": "negbin",
                "train_seasons": [2024, 2025],
                "cal_season": 2026,
                "exclude_prop_line": True,
                "variant": "no_prop_line",
                "force_include_families": [],
                "force_include_features": [],
                "force_exclude_families": [],
                "force_exclude_features": [],
            }
        ),
        encoding="utf-8",
    )

    identity = build_artifact_identity(resolved, artifact, dry_run=False)

    assert identity["status"] == "FAIL"
    assert "Artifact profile evidence is unavailable" in identity["reasons"]
    assert "Artifact calibration cutoff evidence is unavailable" in identity["reasons"]


def test_artifact_identity_does_not_treat_stat_as_profile_evidence(tmp_path: Path) -> None:
    config = _config(tmp_path)
    resolved = resolve_lifecycle_config(config)
    artifact = resolved.artifact_dir
    (artifact / "model_manifest.json").unlink()
    run_config = json.loads((artifact / "run_config.json").read_text(encoding="utf-8"))
    assert run_config["stat"] == "rbis"

    identity = build_artifact_identity(resolved, artifact, dry_run=False)

    assert identity["status"] == "FAIL"
    assert "Artifact profile evidence is unavailable" in identity["reasons"]


def test_artifact_identity_rejects_extra_and_opposite_feature_controls(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"]["feature_controls"]["families"] = ["platoon"]
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    resolved = resolve_lifecycle_config(config)
    artifact = resolved.artifact_dir
    run_config = json.loads((artifact / "run_config.json").read_text(encoding="utf-8"))
    run_config["force_include_families"] = ["platoon", "contact_quality"]
    run_config["force_exclude_families"] = ["weather"]
    run_config["force_exclude_features"] = []
    (artifact / "run_config.json").write_text(json.dumps(run_config), encoding="utf-8")

    identity = build_artifact_identity(resolved, artifact, dry_run=False)

    assert identity["status"] == "FAIL"
    assert any("forced-include family mismatch" in reason for reason in identity["reasons"])
    assert any("opposite forced-exclude family mismatch" in reason for reason in identity["reasons"])


def test_prop_line_exception_only_allows_exclusion(tmp_path: Path) -> None:
    config = _config(tmp_path)
    resolved = resolve_lifecycle_config(config)
    artifact = resolved.artifact_dir
    run_config = json.loads((artifact / "run_config.json").read_text(encoding="utf-8"))
    run_config["force_include_features"] = [resolved.profile_obj.prop_line_feature]
    (artifact / "run_config.json").write_text(json.dumps(run_config), encoding="utf-8")

    identity = build_artifact_identity(resolved, artifact, dry_run=False)

    assert identity["status"] == "FAIL"
    assert any("forced-include feature mismatch" in reason for reason in identity["reasons"])


def test_from_stage_recovers_recorded_nonattached_artifact(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"].pop("artifact_dir")
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    initial = LifecycleRunner(config)
    artifact = tmp_path / "run/artifacts/mlb_run_batter_rbis_complete"
    artifact.mkdir(parents=True)
    for name in ("batter_rbis_xgblss_booster.json", "batter_rbis_negbin_meta.json"):
        (artifact / name).write_text("{}", encoding="utf-8")
    (artifact / "run_config.json").write_text(
        json.dumps(
            {
                "stat": "rbis",
                "model_type": "negbin",
                "train_seasons": [2024, 2025],
                "cal_season": 2026,
                "cal_end_date": "2026-04-12",
                "exclude_prop_line": True,
                "variant": "no_prop_line",
                "force_include_families": [],
                "force_include_features": [],
            }
        ),
        encoding="utf-8",
    )
    (artifact / "model_manifest.json").write_text(
        json.dumps(
            {
                "stat_key": "batter_rbis",
                "profile_name": "batter_rbis",
                "model_type": "negative_binomial",
            }
        ),
        encoding="utf-8",
    )
    initial.run_root.mkdir(parents=True, exist_ok=True)
    (initial.run_root / "stage_status.json").write_text(
        json.dumps(
            {
                "train_or_attach": {
                    "status": "completed",
                    "artifact_dir": str(artifact),
                    "input_identity": initial.config_identity,
                }
            }
        ),
        encoding="utf-8",
    )

    result = LifecycleRunner(config, dry_run=True, from_stage="artifact_identity").run()

    identity = json.loads(
        (Path(result["run_root"]) / "artifact_identity.json").read_text(encoding="utf-8")
    )
    assert identity["artifact_dir"] == str(artifact)
    assert identity["status"] == "PASS"


def test_resume_never_reuses_incomplete_artifact(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"].pop("artifact_dir")
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    incomplete = tmp_path / "mlb_run_batter_rbis_incomplete"
    incomplete.mkdir()
    run_root = tmp_path / "run"
    run_root.mkdir(exist_ok=True)
    (run_root / "stage_status.json").write_text(
        json.dumps(
            {
                "train_or_attach": {
                    "status": "completed",
                    "artifact_dir": str(incomplete),
                    "input_identity": LifecycleRunner(config).config_identity,
                }
            }
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake(argv: list[str]) -> int:
        calls.append(argv)
        if "mlb_batter_train_pipeline.py" in argv[1]:
            output = Path(argv[argv.index("--output-dir") + 1])
            artifact = output / "mlb_run_batter_rbis_complete"
            artifact.mkdir(parents=True)
            for name in (
                "batter_rbis_xgblss_booster.json",
                "batter_rbis_negbin_meta.json",
            ):
                (artifact / name).write_text("{}", encoding="utf-8")
            (artifact / "run_config.json").write_text(
                json.dumps(
                    {
                        "stat": "rbis",
                        "model_type": "negbin",
                        "train_seasons": [2024, 2025],
                        "cal_season": 2026,
                        "cal_end_date": "2026-04-12",
                        "exclude_prop_line": True,
                        "variant": "no_prop_line",
                        "force_include_families": [],
                        "force_include_features": [],
                    }
                ),
                encoding="utf-8",
            )
            (artifact / "model_manifest.json").write_text(
                json.dumps(
                    {
                        "stat_key": "batter_rbis",
                        "profile_name": "batter_rbis",
                        "model_type": "negative_binomial",
                    }
                ),
                encoding="utf-8",
            )
        if "run_mlb_quote_clean_audit_suite.py" in argv[1]:
            return 1
        return 0

    with pytest.raises(RuntimeError, match="stage 'audit' failed"):
        LifecycleRunner(config, dry_run=False, from_stage="train_or_attach", run_command=fake).run()
    assert any("mlb_batter_train_pipeline.py" in command[1] for command in calls)


def test_attach_resume_runs_audit_and_ranker_without_train_or_sweep(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake(argv: list[str]) -> int:
        calls.append(argv)
        if "run_mlb_quote_clean_audit_suite.py" in argv[1]:
            output = Path(argv[argv.index("--output-dir") + 1])
            label = "config_01_no_BL_edge0.1_kelly0.125"
            clv = output / f"clv/{label}"
            clv.mkdir(parents=True, exist_ok=True)
            (clv / "clv_matches.csv").write_text("edge,clv\n0.1,0.02\n", encoding="utf-8")
            (clv / "clv_timing_stability.csv").write_text(
                "horizon,horizon_clv_implied_prob\n+15m,0.01\n+30m,0.02\n+60m,0.03\n",
                encoding="utf-8",
            )
            diagnosis = output / f"diagnosis/{label}"
            diagnosis.mkdir(parents=True, exist_ok=True)
            (diagnosis / "clv_failure_modes.json").write_text(
                json.dumps(
                    {
                        "decision_label": "pass",
                        "timing_stability": {
                            "status": "PASS",
                            "required_horizons": ["+15m", "+30m", "+60m"],
                            "horizons_present": ["+15m", "+30m", "+60m"],
                            "coverage_pct": {"+15m": 100, "+30m": 100, "+60m": 100},
                        },
                    }
                ),
                encoding="utf-8",
            )
            (output / "suite_manifest.csv").write_text(
                "label,total_bets,roi,mean_clv_ci_low,gate_status,decision_label,timing_stability_status\n"
                f"{label},120,0.08,0.01,PASS,pass,PASS\n",
                encoding="utf-8",
            )
            (output / "suite_manifest.json").write_text(
                json.dumps(
                    {
                        "metadata": {
                            "audit_mode": "clv_only",
                            "full_audit_complete": False,
                            "full_audit_passed": False,
                        },
                        "items": [
                            {
                                "label": label,
                                "clv_returncode": 0,
                                "diagnosis_returncode": 0,
                                "total_bets": 120,
                                "roi": 0.08,
                                "mean_clv_ci_low": 0.01,
                                "gate_status": "PASS",
                                "decision_label": "pass",
                                "timing_stability_status": "PASS",
                                "timing_required_horizons": ["+15m", "+30m", "+60m"],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (output / "suite_summary.md").write_text("# audit\n", encoding="utf-8")
        if "analyze_mlb_clv_ranking_diagnostics.py" in argv[1]:
            output = Path(argv[argv.index("--output-dir") + 1])
            output.mkdir(parents=True, exist_ok=True)
            (output / "ranking_score_summary.csv").write_text(
                "score_name,ci_low,monotonic_bins,pass\nraw_edge,0.01,True,True\n",
                encoding="utf-8",
            )
        return 0

    runner = LifecycleRunner(_config(tmp_path), from_stage="audit", run_command=fake)
    result = runner.run()
    assert all("mlb_batter_train_pipeline.py" not in command[1] for command in calls)
    assert all("run_mlb_sweep.py" not in command[1] for command in calls)
    assert any("run_mlb_quote_clean_audit_suite.py" in command[1] for command in calls)
    assert any("analyze_mlb_clv_ranking_diagnostics.py" in command[1] for command in calls)
    assert result["decision"]["classification"] == "Confirm"
    assert result["decision"]["posture"] == "flat_paper_candidate"

    calls.clear()
    LifecycleRunner(_config(tmp_path), from_stage="audit", run_command=fake).run()
    assert calls == []

    timing_path = (
        tmp_path
        / "run/audit/clv/config_01_no_BL_edge0.1_kelly0.125/clv_timing_stability.csv"
    )
    timing_path.write_text(
        "horizon,horizon_clv_implied_prob\n+15m,0.011\n+30m,0.02\n+60m,0.03\n",
        encoding="utf-8",
    )
    LifecycleRunner(_config(tmp_path), from_stage="audit", run_command=fake).run()
    assert all("run_mlb_quote_clean_audit_suite.py" not in command[1] for command in calls)
    assert any("analyze_mlb_clv_ranking_diagnostics.py" in command[1] for command in calls)

    calls.clear()
    LifecycleRunner(
        _config(tmp_path), from_stage="audit", force_stage="audit", run_command=fake
    ).run()
    assert any("run_mlb_quote_clean_audit_suite.py" in command[1] for command in calls)
    assert any("analyze_mlb_clv_ranking_diagnostics.py" in command[1] for command in calls)

    calls.clear()
    (tmp_path / "sweep/sweep_summary.csv").write_text(
        "edge_threshold,total_bets,roi,max_drawdown\n0.1,121,0.08,0.12\n",
        encoding="utf-8",
    )
    LifecycleRunner(_config(tmp_path), from_stage="audit", run_command=fake).run()
    assert any("run_mlb_quote_clean_audit_suite.py" in command[1] for command in calls)
    assert any("analyze_mlb_clv_ranking_diagnostics.py" in command[1] for command in calls)

    calls.clear()
    (tmp_path / "run/audit/suite_manifest.csv").unlink()
    LifecycleRunner(_config(tmp_path), from_stage="audit", run_command=fake).run()
    assert any("run_mlb_quote_clean_audit_suite.py" in command[1] for command in calls)

    calls.clear()
    timing_path = (
        tmp_path
        / "run/audit/clv/config_01_no_BL_edge0.1_kelly0.125/clv_timing_stability.csv"
    )
    timing_path.unlink()
    with pytest.raises(RuntimeError, match="audit.*required output"):
        LifecycleRunner(_config(tmp_path), from_stage="decision", run_command=fake).run()
    assert calls == []


def test_zero_exit_without_audit_outputs_marks_stage_failed(tmp_path: Path) -> None:
    runner = LifecycleRunner(
        _config(tmp_path),
        from_stage="audit",
        run_command=lambda _argv: 0,
    )

    with pytest.raises(RuntimeError, match="audit.*required output"):
        runner.run()

    assert runner.statuses["audit"]["status"] == "failed"


def test_zero_exit_without_training_artifact_marks_stage_failed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"].pop("artifact_dir")
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    runner = LifecycleRunner(config, run_command=lambda _argv: 0)

    with pytest.raises(RuntimeError, match="train_or_attach.*required output"):
        runner.run()

    assert runner.statuses["train_or_attach"]["status"] == "failed"


def test_zero_exit_without_sweep_outputs_marks_stage_failed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["model"].pop("sweep_dir")
    raw["model"].pop("sweep_artifact_identity_sha256")
    config.write_text(yaml.safe_dump(raw), encoding="utf-8")
    runner = LifecycleRunner(config, run_command=lambda _argv: 0)

    with pytest.raises(RuntimeError, match="sweep.*required output"):
        runner.run()

    assert runner.statuses["sweep"]["status"] == "failed"


def test_zero_exit_without_ranker_summary_marks_stage_failed(tmp_path: Path) -> None:
    def fake(argv: list[str]) -> int:
        if "run_mlb_quote_clean_audit_suite.py" in argv[1]:
            _write_clv_only_audit(Path(argv[argv.index("--output-dir") + 1]))
        return 0

    runner = LifecycleRunner(_config(tmp_path), from_stage="audit", run_command=fake)

    with pytest.raises(RuntimeError, match="ranker.*required output"):
        runner.run()

    assert runner.statuses["ranker"]["status"] == "failed"


def test_sweep_output_contract_uses_positive_json_bet_count(tmp_path: Path) -> None:
    _config(tmp_path)
    metrics_path = tmp_path / "sweep/config_01_no_BL_edge0.1_kelly0.125/metrics.json"
    metrics_path.write_text(json.dumps({"total_bets": 0, "roi": 0.08}), encoding="utf-8")
    (metrics_path.parent / "bets.csv").unlink()

    with pytest.raises(RuntimeError, match="bets.csv"):
        _verify_sweep_outputs(tmp_path / "sweep")


def test_sweep_output_contract_rejects_config_directory_mismatch(tmp_path: Path) -> None:
    _config(tmp_path)
    results_path = tmp_path / "sweep/sweep_results.json"
    payload = json.loads(results_path.read_text(encoding="utf-8"))
    payload["results"][0]["config"]["edge_threshold"] = 0.2
    results_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="config mismatch|config directory mismatch"):
        _verify_sweep_outputs(tmp_path / "sweep")


def test_audit_output_contract_rejects_unscored_required_timing_horizon(tmp_path: Path) -> None:
    audit_dir = tmp_path / "audit"
    label = _write_clv_only_audit(audit_dir)
    timing_path = audit_dir / "clv" / label / "clv_timing_stability.csv"
    timing_path.write_text(
        "horizon,horizon_clv_implied_prob\n+15m,\n+30m,0.02\n+60m,0.03\n",
        encoding="utf-8",
    )
    bets = tmp_path / label / "bets.csv"
    bets.parent.mkdir()
    bets.write_text("player_id\n1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"timing.*\+15m"):
        _verify_audit_outputs(
            audit_dir,
            [bets],
            full_audit=False,
            require_timing_stability=True,
        )


def test_full_audit_output_contract_requires_dropout_bundle(tmp_path: Path) -> None:
    audit_dir = tmp_path / "audit"
    _write_clv_only_audit(audit_dir)
    manifest_path = audit_dir / "suite_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["metadata"].update(
        {
            "audit_mode": "full",
            "dropout_audit_ran": True,
            "dropout_returncode": 0,
            "dropout_summary_path": str(audit_dir / "dropout_audit" / "audit_summary.json"),
            "dropout_output_paths": [
                str(audit_dir / "dropout_audit" / "audit_summary.json"),
                str(audit_dir / "dropout_audit" / "audit_summary.md"),
                str(audit_dir / "dropout_audit" / "dropout_summary_by_bucket.csv"),
                str(audit_dir / "dropout_audit" / "dropout_rows.csv"),
                str(audit_dir / "dropout_audit" / "selected_clean_quotes.csv"),
                str(audit_dir / "dropout_audit" / "dropout_by_date.csv"),
                str(audit_dir / "dropout_audit" / "dropout_by_game.csv"),
                str(audit_dir / "dropout_audit" / "dropout_by_bookmaker.csv"),
            ],
            "full_audit_complete": True,
            "full_audit_passed": True,
            "dropout_decision": "PASS",
        }
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    bets = tmp_path / "config_01_no_BL_edge0.1_kelly0.125" / "bets.csv"
    bets.parent.mkdir()
    bets.write_text("player_id\n1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="dropout_audit.*audit_summary"):
        _verify_audit_outputs(audit_dir, [bets], full_audit=True)

    dropout = audit_dir / "dropout_audit"
    dropout.mkdir()
    (dropout / "audit_summary.json").write_text(
        json.dumps({"decision": "PASS"}), encoding="utf-8"
    )
    (dropout / "dropout_summary_by_bucket.csv").write_text(
        "dropout_bucket,count,pct\nclean_quote_available,1,1.0\n", encoding="utf-8"
    )
    (dropout / "dropout_rows.csv").write_text(
        "player_id,dropout_bucket\n1,clean_quote_available\n", encoding="utf-8"
    )
    (dropout / "selected_clean_quotes.csv").write_text(
        "player_id,selected_snapshot_time\n1,2026-05-18T17:00:00Z\n", encoding="utf-8"
    )
    (dropout / "audit_summary.md").write_text("# Dropout audit\n", encoding="utf-8")
    (dropout / "dropout_by_date.csv").write_text("date,count\n2026-05-18,1\n", encoding="utf-8")
    (dropout / "dropout_by_game.csv").write_text("game_id,count\n1,1\n", encoding="utf-8")
    (dropout / "dropout_by_bookmaker.csv").write_text(
        "bookmaker,count\ndraftkings,1\n", encoding="utf-8"
    )

    evidence = _verify_audit_outputs(audit_dir, [bets], full_audit=True)
    assert evidence["full_audit_complete"] is True
    assert evidence["dropout_decision"] == "PASS"
    assert evidence["dropout_output_paths"] == manifest["metadata"]["dropout_output_paths"]
