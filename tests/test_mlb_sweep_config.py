"""Tests for MLB sweep typed config and CLI parsing helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src.backtesting.mlb.sweep_config import (
    QuoteCleanConfig,
    SweepCliConfig,
    SweepConfig,
    build_arg_parser,
    build_sweep_grid,
    parse_sweep_cli_config,
    parse_tau_values,
)


def test_parse_tau_values_preserves_none_baseline():
    assert parse_tau_values(["none", "0.03", "0.10"]) == [None, 0.03, 0.10]


def test_sweep_config_label_shows_flat_stake_instead_of_kelly():
    config = SweepConfig(
        tau=None,
        edge_threshold=0.15,
        kelly_fraction=0.125,
        flat_bet_size=100.0,
    )

    assert config.label == "no_BL | edge=0.15 | flat=$100"
    assert config.to_dict()["flat_bet_size"] == 100.0


def test_build_sweep_grid_applies_flat_bet_to_every_config():
    configs = build_sweep_grid(
        tau_values=[None, 0.05],
        edge_thresholds=[0.08],
        kelly_fractions=[0.125],
        z_max_values=[0.25, 1.0],
        max_weight_values=[0.50],
        flat_bet_size=25.0,
    )

    assert [c.tau for c in configs] == [None, 0.05, 0.05]
    assert all(c.flat_bet_size == 25.0 for c in configs)


def test_parse_sweep_cli_config_records_dates_quote_clean_and_output_dir():
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--start",
            "2025-07-01",
            "--end",
            "2025-07-07",
            "--tau",
            "none",
            "0.05",
            "--edge",
            "0.08",
            "--kelly",
            "0.125",
            "--flat",
            "100",
            "--quote-clean",
            "--quote-cutoff-time-et",
            "17:30",
            "--quote-decision-policy",
            "slate_or_tminus",
            "--quote-relative-minutes",
            "45",
            "--line-source",
            "mlb_player_props_clv_snapshots",
            "--dense-clv-linked-coverage-audit-note",
            "audit_suite verified linked game_id/player_id coverage",
            "--output-dir",
            "backtest_results/test_run",
        ]
    )

    config = parse_sweep_cli_config(args)

    assert isinstance(config, SweepCliConfig)
    assert config.start_date == date(2025, 7, 1)
    assert config.end_date == date(2025, 7, 7)
    assert config.tau_values == [None, 0.05]
    assert config.output_dir == Path("backtest_results/test_run")
    assert config.quote_clean == QuoteCleanConfig(
        enabled=True,
        cutoff_time_et="17:30",
        decision_policy="slate_or_tminus",
        relative_minutes=45,
        line_source="mlb_player_props_clv_snapshots",
    )
    assert config.dense_clv_linked_coverage_audit_note == "audit_suite verified linked game_id/player_id coverage"
    assert len(config.sweep_grid) == 2
    assert all(c.flat_bet_size == 100.0 for c in config.sweep_grid)


def test_parse_sweep_cli_config_makes_combined_mode_explicit():
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--start",
            "2025-07-01",
            "--end",
            "2025-07-07",
            "--combined",
            "--direction",
            "under",
            "--stats",
            "batter_hits",
            "pitcher_strikeouts",
        ]
    )

    config = parse_sweep_cli_config(args)

    assert config.combined is True
    assert config.direction == "under"
    assert config.stats == ["batter_hits", "pitcher_strikeouts"]
    assert config.cli_allowed_bets == {("batter_hits", "under"), ("pitcher_strikeouts", "under")}


def test_build_arg_parser_rejects_invalid_line_source_before_db_work():
    parser = build_arg_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--start",
                "2025-07-01",
                "--end",
                "2025-07-07",
                "--line-source",
                "bad_table",
            ]
        )
