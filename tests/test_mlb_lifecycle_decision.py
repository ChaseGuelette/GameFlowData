from __future__ import annotations

import json
from pathlib import Path

import yaml

from src.models.mlb.lifecycle.config import resolve_lifecycle_config
from src.models.mlb.lifecycle.decision import evaluate_decision, recommend_staking


def _resolved(
    tmp_path: Path,
    purpose: str = "finalist_certification",
    audit_mode: str = "full",
    *,
    disable_safety_gates: bool = False,
):
    raw = {
        "experiment": {"name": "decision", "profile": "batter_rbis", "purpose": purpose, "output_root": str(tmp_path / "run")},
        "model": {"base": "no_prop_line"},
        "training": {"seasons": [2024, 2025], "calibration_season": 2026, "calibration_end": "2026-04-12"},
        "evaluation": {"start": "2026-05-18", "end": "2026-06-21"},
        "quotes": {"clean": True},
        "audit": {
            "minimum_bets": 100,
            "mode": audit_mode,
            "selection": {
                "policy": "explicit",
                "max_configs": 1,
                "include_no_bl_control": True,
                "rank_by": "sharpe_ratio",
                "configs": [
                    {
                        "tau": None,
                        "z_max": 1.0,
                        "max_weight": 0.5,
                        "edge_threshold": 0.1,
                        "kelly_fraction": 0.125,
                    }
                ],
            },
        },
        "decision": {
            "max_drawdown": 0.25,
            "require_positive_mean_clv_ci_low": not disable_safety_gates,
            "require_positive_ranker_ci_low": not disable_safety_gates,
            "require_edge_bucket_monotonicity": not disable_safety_gates,
            "require_independent_window": not disable_safety_gates,
        },
    }
    path = tmp_path / "decision.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return resolve_lifecycle_config(path)


def _evidence(tmp_path: Path, *, roi: float = 0.08, clv: float = 0.01, rank_ci: float = 0.01):
    label = "config_01_no_BL_edge0.1_kelly0.125"
    sweep = tmp_path / "sweep.csv"
    suite = tmp_path / "suite.csv"
    rank = tmp_path / f"rank/{label}"
    rank.mkdir(parents=True)
    sweep.write_text(
        "tau,edge_threshold,kelly_fraction,total_bets,roi,max_drawdown\n"
        f",0.1,0.125,120,{roi},0.12\n",
        encoding="utf-8",
    )
    sweep.with_name("sweep_results.json").write_text(
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
                }
            }
        ),
        encoding="utf-8",
    )
    suite.write_text(
        "label,total_bets,roi,mean_clv_ci_low,gate_status,decision_label,dense_table_adequate,timing_stability_status\n"
        f"{label},120,{roi},{clv},PASS,pass,yes,PASS\n",
        encoding="utf-8",
    )
    audit_root = suite.parent
    dropout_dir = audit_root / "dropout_audit"
    dropout_dir.mkdir()
    dropout_summary = dropout_dir / "audit_summary.json"
    dropout_summary.write_text(json.dumps({"decision": "PASS"}), encoding="utf-8")
    (dropout_dir / "dropout_summary_by_bucket.csv").write_text(
        "dropout_bucket,count,pct\nclean_quote_available,120,1.0\n", encoding="utf-8"
    )
    (dropout_dir / "dropout_rows.csv").write_text(
        "player_id,dropout_bucket\n1,clean_quote_available\n", encoding="utf-8"
    )
    (dropout_dir / "selected_clean_quotes.csv").write_text(
        "player_id,selected_snapshot_time\n1,2026-05-18T17:00:00Z\n", encoding="utf-8"
    )
    (dropout_dir / "audit_summary.md").write_text("# Dropout audit\n", encoding="utf-8")
    (dropout_dir / "dropout_by_date.csv").write_text(
        "date,count\n2026-05-18,1\n", encoding="utf-8"
    )
    (dropout_dir / "dropout_by_game.csv").write_text("game_id,count\n1,1\n", encoding="utf-8")
    (dropout_dir / "dropout_by_bookmaker.csv").write_text(
        "bookmaker,count\ndraftkings,1\n", encoding="utf-8"
    )
    timing_dir = audit_root / "clv" / label
    timing_dir.mkdir(parents=True)
    (timing_dir / "clv_timing_stability.csv").write_text(
        "horizon,horizon_clv_implied_prob\n+15m,0.01\n+30m,0.02\n+60m,0.03\n",
        encoding="utf-8",
    )
    diagnosis_dir = audit_root / "diagnosis" / label
    diagnosis_dir.mkdir(parents=True)
    (diagnosis_dir / "clv_failure_modes.json").write_text(
        json.dumps(
            {
                "timing_stability": {
                    "status": "PASS",
                    "required_horizons": ["+15m", "+30m", "+60m"],
                    "coverage_pct": {"+15m": 100, "+30m": 100, "+60m": 100},
                }
            }
        ),
        encoding="utf-8",
    )
    dropout_output_paths = [
        dropout_summary,
        dropout_dir / "audit_summary.md",
        dropout_dir / "dropout_summary_by_bucket.csv",
        dropout_dir / "dropout_rows.csv",
        dropout_dir / "selected_clean_quotes.csv",
        dropout_dir / "dropout_by_date.csv",
        dropout_dir / "dropout_by_game.csv",
        dropout_dir / "dropout_by_bookmaker.csv",
    ]
    suite.with_suffix(".json").write_text(
        json.dumps(
            {
                "metadata": {
                    "audit_mode": "full",
                    "dropout_audit_ran": True,
                    "dropout_returncode": 0,
                    "dropout_summary_path": str(dropout_summary),
                    "dropout_output_paths": [str(path) for path in dropout_output_paths],
                    "dropout_decision": "PASS",
                    "full_audit_complete": True,
                    "full_audit_passed": True,
                },
                "items": [
                    {
                        "label": label,
                        "total_bets": 120,
                        "roi": roi,
                        "mean_clv_ci_low": clv,
                        "gate_status": "PASS",
                        "decision_label": "pass",
                        "dense_table_adequate": "yes",
                        "timing_stability_status": "PASS",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (rank / "ranking_score_summary.csv").write_text(
        f"score_name,ci_low,monotonic_bins,pass\nraw_edge,{rank_ci},True,True\n",
        encoding="utf-8",
    )
    return sweep, suite, tmp_path / "rank"


def test_confirm_and_live_ready_require_full_finalist_evidence(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )
    assert decision.classification == "Confirm"
    assert decision.posture == "live_ready"
    assert decision.evidence["dropout_audit"]["verified"] is True
    assert decision.evidence["dropout_audit"]["summary"]["sha256"]
    timing = decision.evidence["candidate_checks"][0]["timing_stability"]
    assert timing["verified"] is True
    assert timing["timing_csv"]["sha256"]
    assert timing["diagnosis_json"]["sha256"]


def test_finalist_missing_full_audit_attestation_shelves(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    suite.with_suffix(".json").unlink()

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Shelf"
    assert decision.posture == "live_blocked"


def test_finalist_failed_dropout_evidence_excludes(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    manifest_path = suite.with_suffix(".json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["metadata"]["dropout_decision"] = "FAIL"
    manifest["metadata"]["full_audit_passed"] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (suite.parent / "dropout_audit" / "audit_summary.json").write_text(
        json.dumps({"decision": "FAIL"}), encoding="utf-8"
    )

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=manifest_path,
        ranking_root=rank,
    )

    assert decision.classification == "Exclude"
    assert decision.posture == "live_blocked"


def test_finalist_failed_timing_stability_excludes(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    suite.write_text(
        suite.read_text(encoding="utf-8").replace(",PASS\n", ",FAIL\n"),
        encoding="utf-8",
    )

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Exclude"
    assert decision.posture == "live_blocked"


def test_failure_excludes_and_missing_evidence_shelves(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path, roi=-0.01)
    excluded = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )
    assert excluded.classification == "Exclude"
    assert excluded.posture == "live_blocked"

    shelf = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=tmp_path / "none.csv",
        suite_manifest_csv=tmp_path / "none2.csv",
        suite_manifest_json=tmp_path / "none.json",
        ranking_root=tmp_path / "none",
    )
    assert shelf.classification == "Shelf"
    assert shelf.posture == "live_blocked"


def test_duplicate_edges_use_exact_config_index_for_drawdown(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    sweep.write_text(
        "tau,edge_threshold,kelly_fraction,total_bets,roi,max_drawdown\n"
        ",0.1,0.125,120,0.08,0.90\n"
        ",0.1,0.125,120,0.08,0.12\n",
        encoding="utf-8",
    )
    label = "config_02_no_BL_edge0.1_kelly0.125"
    suite.write_text(
        "label,total_bets,roi,mean_clv_ci_low,gate_status,decision_label,dense_table_adequate,timing_stability_status\n"
        f"{label},120,0.08,0.01,PASS,pass,yes,PASS\n",
        encoding="utf-8",
    )
    manifest_path = suite.with_suffix(".json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    original_label = manifest["items"][0]["label"]
    manifest["items"][0]["label"] = label
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (suite.parent / "clv" / original_label).rename(suite.parent / "clv" / label)
    (suite.parent / "diagnosis" / original_label).rename(suite.parent / "diagnosis" / label)
    target = rank / label
    target.mkdir(parents=True)
    (target / "ranking_score_summary.csv").write_text(
        "score_name,ci_low,monotonic_bins,pass\nraw_edge,0.01,True,True\n",
        encoding="utf-8",
    )
    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )
    assert decision.classification == "Confirm"
    candidate = decision.evidence["candidate_checks"][0]
    assert candidate["max_drawdown"] == 0.12


def test_unverified_independent_window_blocks_confirmation_and_live(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    results_path = sweep.with_name("sweep_results.json")
    payload = json.loads(results_path.read_text(encoding="utf-8"))
    payload["sweep_metadata"]["promotion_contract"]["quote_clean"]["enabled"] = False
    results_path.write_text(json.dumps(payload), encoding="utf-8")

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Shelf"
    assert decision.posture == "live_blocked"
    assert decision.evidence["independent_window_verified"] is False


def test_fail_prefixed_audit_status_excludes_candidate(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    label = "config_01_no_BL_edge0.1_kelly0.125"
    suite.write_text(
        "label,total_bets,roi,mean_clv_ci_low,gate_status,decision_label,dense_table_adequate\n"
        f"{label},120,0.08,0.01,FAIL: command failure,fail,yes\n",
        encoding="utf-8",
    )

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Exclude"
    assert decision.posture == "live_blocked"


def test_finalist_cannot_disable_live_safety_gates(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path, clv=-0.01, rank_ci=-0.01)
    results_path = sweep.with_name("sweep_results.json")
    payload = json.loads(results_path.read_text(encoding="utf-8"))
    payload["sweep_metadata"]["promotion_contract"]["quote_clean"]["enabled"] = False
    results_path.write_text(json.dumps(payload), encoding="utf-8")
    ranking_path = next(rank.glob("**/ranking_score_summary.csv"))
    ranking_path.write_text(
        "score_name,ci_low,monotonic_bins,pass\nraw_edge,-0.01,False,False\n",
        encoding="utf-8",
    )

    decision = evaluate_decision(
        _resolved(tmp_path, disable_safety_gates=True),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification != "Confirm"
    assert decision.posture == "live_blocked"


def test_finalist_requires_persisted_dropout_summary_file(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    (suite.parent / "dropout_audit" / "audit_summary.json").unlink()

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Shelf"
    assert any("dropout audit summary" in reason.lower() for reason in decision.reasons)


def test_finalist_requires_persisted_timing_files(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    next((suite.parent / "clv").glob("*/clv_timing_stability.csv")).unlink()

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Shelf"
    assert any("persisted timing-stability" in reason.lower() for reason in decision.reasons)


def test_finalist_rejects_unscored_required_timing_horizon(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    timing_path = next((suite.parent / "clv").glob("*/clv_timing_stability.csv"))
    timing_path.write_text(
        "horizon,horizon_clv_implied_prob\n+15m,\n+30m,0.01\n+60m,0.01\n",
        encoding="utf-8",
    )

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification != "Confirm"
    assert decision.posture == "live_blocked"
    assert any("timing-stability evidence is malformed" in reason.lower() for reason in decision.reasons)


def test_decision_requires_matching_audit_csv_and_json_labels(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    manifest_path = suite.with_suffix(".json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["items"][0]["label"] = "unrelated_candidate"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    decision = evaluate_decision(
        _resolved(tmp_path),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=manifest_path,
        ranking_root=rank,
    )

    assert decision.classification == "Shelf"
    assert decision.evidence["suite_labels_verified"] is False


def test_positive_ranker_ci_and_pass_must_be_on_same_row(tmp_path: Path) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    ranking_path = next(rank.glob("**/ranking_score_summary.csv"))
    ranking_path.write_text(
        "score_name,ci_low,monotonic_bins,pass\n"
        "positive_but_failed,0.02,True,False\n"
        "passing_but_negative,-0.01,True,True\n",
        encoding="utf-8",
    )

    decision = evaluate_decision(
        _resolved(tmp_path, disable_safety_gates=True),
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    assert decision.classification == "Exclude"
    assert decision.posture == "live_blocked"


def test_staking_recommendation_maps_confirm_independent_to_flat_paper(
    tmp_path: Path,
) -> None:
    sweep, suite, rank = _evidence(tmp_path)
    resolved = _resolved(tmp_path, purpose="independent_validation", audit_mode="clv_only")
    decision = evaluate_decision(
        resolved,
        sweep_summary_csv=sweep,
        suite_manifest_csv=suite,
        suite_manifest_json=suite.with_suffix(".json"),
        ranking_root=rank,
    )

    recommendation = recommend_staking(resolved, decision, dry_run=False)

    assert decision.classification == "Confirm"
    assert recommendation.recommendation == "flat_paper"
    assert recommendation.report_only is True
    assert recommendation.live_action_performed is False
    assert recommendation.kelly_action_performed is False
