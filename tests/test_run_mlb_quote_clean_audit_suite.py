from __future__ import annotations

import sys
from pathlib import Path

from scripts.run_mlb_quote_clean_audit_suite import (
    SuiteItem,
    _sanitize_label,
    determine_gate_status,
    discover_bets_files,
    read_bets_rollup,
    write_manifest,
)


def test_discover_bets_files_prefers_root_bets_csv(tmp_path: Path) -> None:
    root_bets = tmp_path / "bets.csv"
    root_bets.write_text("player_id\n", encoding="utf-8")
    cfg = tmp_path / "config_a"
    cfg.mkdir()
    (cfg / "bets.csv").write_text("player_id\n", encoding="utf-8")

    assert discover_bets_files(tmp_path) == [root_bets]


def test_discover_bets_files_finds_config_bets_csv(tmp_path: Path) -> None:
    cfg_b = tmp_path / "config_b"
    cfg_a = tmp_path / "config_a"
    cfg_b.mkdir()
    cfg_a.mkdir()
    b = cfg_b / "bets.csv"
    a = cfg_a / "bets.csv"
    b.write_text("player_id\n", encoding="utf-8")
    a.write_text("player_id\n", encoding="utf-8")

    assert discover_bets_files(tmp_path) == [a, b]


def test_sanitize_label_uses_config_dir_for_bets_csv() -> None:
    assert _sanitize_label(Path("backtest_results/sweep/config_edge005/bets.csv")) == "config_edge005"


def test_read_bets_rollup_extracts_roi_profit_and_bookmaker_concentration(tmp_path: Path) -> None:
    bets = tmp_path / "bets.csv"
    bets.write_text(
        "bookmaker,profit,stake\nESPNBet,2,10\nDraftKings,-1,10\nESPNBet,3,10\n",
        encoding="utf-8",
    )

    rollup = read_bets_rollup(bets)

    assert rollup["total_bets"] == 3
    assert rollup["total_profit"] == 4.0
    assert rollup["roi"] == 4 / 30
    assert rollup["top_bookmaker"] == "ESPNBet"
    assert rollup["espnbet_bets"] == 2


def test_write_manifest_includes_validation_gate_report_and_dropout_buckets(tmp_path: Path) -> None:
    item = SuiteItem(
        label="config_03",
        bets_csv="bets.csv",
        clv_output_dir="clv/config_03",
        diagnosis_output_dir="diagnosis/config_03",
        clv_returncode=0,
        diagnosis_returncode=0,
        mean_clv_ci_low=0.001,
        edge_clv_ci_low=0.002,
        total_bets=25,
        total_profit=3.5,
        roi=0.14,
        top_bookmaker="ESPNBet",
        espnbet_share=0.4,
        gate_status="PASS",
    )
    write_manifest(
        [item],
        tmp_path,
        {
            "sweep_output_dir": "sweep",
            "dropout_summary": {
                "decision": "PASS",
                "reason": "ok",
                "bucket_counts": {"clean_quote_available": 10},
            },
        },
    )

    summary = (tmp_path / "suite_summary.md").read_text(encoding="utf-8")
    assert "## Validation Gate Report" in summary
    assert "config_03" in summary
    assert "## Dropout Buckets" in summary
    assert "clean_quote_available" in summary


def test_determine_gate_status_fails_edge_ranking_ci_low() -> None:
    item = SuiteItem(
        label="cfg",
        bets_csv="bets.csv",
        clv_output_dir="clv",
        diagnosis_output_dir="diag",
        clv_returncode=0,
        diagnosis_returncode=0,
        mean_clv_ci_low=0.01,
        edge_clv_ci_low=-0.01,
    )

    assert determine_gate_status(item, {"decision": "PASS"}).startswith("FAIL: edge-ranking")


def test_dry_run_clv_command_uses_generic_analyzer(tmp_path: Path, monkeypatch) -> None:
    from scripts import run_mlb_quote_clean_audit_suite as suite

    sweep_dir = tmp_path / "sweep"
    sweep_dir.mkdir()
    bets_csv = sweep_dir / "bets.csv"
    bets_csv.write_text("bookmaker,profit,stake\nDraftKings,1,10\n", encoding="utf-8")
    output_dir = tmp_path / "out"
    calls: list[list[str]] = []

    def fake_run_cmd(cmd, *, dry_run=False):
        calls.append([str(part) for part in cmd])
        return 0

    monkeypatch.setattr(suite, "run_cmd", fake_run_cmd)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_mlb_quote_clean_audit_suite.py",
            "--sweep-output-dir", str(sweep_dir),
            "--output-dir", str(output_dir),
            "--model-dir", "dummy-model",
            "--start", "2026-04-01",
            "--end", "2026-04-02",
            "--stats", "pitcher_strikeouts",
            "--skip-dropout-audit",
            "--dry-run",
        ],
    )

    assert suite.main() == 0
    clv_calls = [cmd for cmd in calls if any("analyze_mlb" in part for part in cmd)]
    assert clv_calls
    assert any(part.endswith("analyze_mlb_clv.py") for part in clv_calls[0])
    assert not any(part.endswith("analyze_mlb_batter_hits_clv.py") for part in clv_calls[0])
