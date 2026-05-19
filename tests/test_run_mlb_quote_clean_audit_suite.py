from __future__ import annotations

from pathlib import Path

from scripts.run_mlb_quote_clean_audit_suite import _sanitize_label, discover_bets_files


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
