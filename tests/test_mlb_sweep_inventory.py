"""Structural inventory guards for the MLB sweep god-module migration.

These tests do not prove betting correctness. They guard migration ownership:
responsibilities extracted from run_mlb_sweep.py should not quietly drift back
into the runner while the behavior tests continue to pass.
"""

from __future__ import annotations

import ast
from pathlib import Path

RUNNER_PATH = Path("src/backtesting/mlb/run_mlb_sweep.py")
QUOTE_POLICY_PATH = Path("src/backtesting/mlb/quote_decision_policy.py")
QUOTE_LINE_SERVICE_PATH = Path("src/backtesting/mlb/quote_clean_line_service.py")
DATA_LOADER_PATH = Path("src/backtesting/mlb/backtest_data_loader.py")
PREDICTION_CACHE_PATH = Path("src/backtesting/mlb/prediction_cache.py")
SWEEP_RESULTS_PATH = Path("src/backtesting/mlb/sweep_results.py")
EDGE_ENGINE_PATH = Path("src/backtesting/mlb/edge_engine.py")
MATCHUP_CACHE_PATH = Path("src/backtesting/mlb/matchup_cache.py")
SWEEP_EXECUTION_PATH = Path("src/backtesting/mlb/sweep_execution.py")
SWEEP_BOOTSTRAP_PATH = Path("src/backtesting/mlb/sweep_bootstrap.py")


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_source(path: Path, function_name: str) -> str:
    source = _source(path)
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            segment = ast.get_source_segment(source, node)
            assert segment is not None
            return segment
    raise AssertionError(f"{function_name} not found in {path}")


def _non_comment_loc(path: Path) -> int:
    return sum(
        1
        for line in _source(path).splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def test_quote_decision_policy_module_owns_expected_public_helpers():
    source = _source(QUOTE_POLICY_PATH)
    tree = ast.parse(source)
    functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}

    assert {
        "build_fixed_cutoff_ts",
        "build_slate_decision_ts",
        "decision_time_for_game",
    }.issubset(functions)


def test_run_mlb_sweep_quote_decision_helpers_are_only_compatibility_wrappers():
    wrappers = {
        "_build_quote_clean_cutoff_ts": "build_fixed_cutoff_ts",
        "_build_slate_decision_ts": "build_slate_decision_ts",
        "_game_decision_time": "decision_time_for_game",
    }

    for wrapper_name, delegate_name in wrappers.items():
        body = _function_source(RUNNER_PATH, wrapper_name)
        assert delegate_name in body
        assert "Compatibility wrapper" in body
        assert "pd.to_datetime" not in body
        assert "ZoneInfo" not in body
        assert "datetime_time" not in body


def test_run_mlb_sweep_no_longer_imports_quote_policy_implementation_dependencies():
    source = _source(RUNNER_PATH)

    assert "from zoneinfo import ZoneInfo" not in source
    assert "from datetime import time as datetime_time" not in source
    assert "build_fixed_cutoff_ts" in source
    assert "build_slate_decision_ts" in source
    assert "decision_time_for_game" in source


def test_quote_clean_line_service_owns_shared_line_selection_seam():
    runner_source = _source(RUNNER_PATH)
    service_source = _source(QUOTE_LINE_SERVICE_PATH)

    assert "fetch_lines_for_date" in runner_source
    assert "fetch_lines_at_decision_time" not in runner_source
    assert "fetch_lines_at_decision_time" in service_source
    assert "from src.backtesting.mlb.line_selection import fetch_lines_at_decision_time" in service_source


def test_run_mlb_sweep_uses_typed_cli_config_after_parse_boundary():
    source = _source(RUNNER_PATH)

    assert "parse_sweep_cli_config(args)" in source
    assert "args." not in source


def test_backtest_data_loader_owns_schedule_and_actuals_sql_boundaries():
    runner_source = _source(RUNNER_PATH)
    loader_source = _source(DATA_LOADER_PATH)

    assert "fetch_game_dates" in runner_source
    assert "fetch_actuals_by_date" in runner_source
    assert "fetch_games_for_date" in runner_source
    assert "from sqlalchemy import text" not in runner_source
    assert "FROM mlb_game_schedule" not in runner_source
    assert "did_not_play IS NOT TRUE" not in runner_source

    assert "from sqlalchemy import text" in loader_source
    assert "FROM mlb_game_schedule" in loader_source
    assert "did_not_play IS NOT TRUE" in loader_source


def test_prediction_cache_owns_date_prediction_and_feature_store_prediction_loop():
    runner_source = _source(RUNNER_PATH)
    cache_source = _source(PREDICTION_CACHE_PATH)

    assert "build_predictions_for_date" in runner_source
    assert "class DatePrediction" not in runner_source
    assert "BATTER_STAT_FS_MAP" not in runner_source
    assert "get_player_game_features" not in runner_source
    assert "get_features_for_date" not in runner_source

    assert "class DatePrediction" in cache_source
    assert "BATTER_STAT_FS_MAP" in cache_source
    assert "get_player_game_features" in cache_source
    assert "get_features_for_date" in cache_source


def test_sweep_results_owns_output_serialization_and_comparison_table():
    runner_source = _source(RUNNER_PATH)
    results_source = _source(SWEEP_RESULTS_PATH)

    assert "save_results(" in runner_source
    assert "print_comparison_table(" in runner_source
    assert "class SweepResult" not in runner_source
    assert "sweep_results.json" not in runner_source
    assert "sweep_summary.csv" not in runner_source
    assert "bets.csv" not in runner_source
    assert "predictions.csv" not in runner_source

    assert "class SweepResult" in results_source
    assert "def save_results" in results_source
    assert "def print_comparison_table" in results_source
    assert "sweep_results.json" in results_source
    assert "sweep_summary.csv" in results_source


def test_edge_engine_owns_edge_and_base_probability_calculation():
    runner_source = _source(RUNNER_PATH)
    edge_source = _source(EDGE_ENGINE_PATH)

    assert "compute_edges_for_config" in runner_source
    assert "precompute_mlb_base_probs" in runner_source
    assert "def compute_edges_for_config" not in runner_source
    assert "def precompute_mlb_base_probs" not in runner_source
    assert "float((samples > line_val).mean())" not in runner_source
    assert "np.where(over_arr > 0" not in runner_source
    assert "posterior_logit" not in runner_source

    assert "def compute_edges_for_config" in edge_source
    assert "def precompute_mlb_base_probs" in edge_source
    assert "def build_config_edge_frame" in edge_source
    assert "float((samples > line_val).mean())" in edge_source
    assert "posterior_logit" in edge_source
    assert "norm.cdf" not in edge_source
    assert "scipy.stats" not in edge_source


def test_matchup_cache_owns_season_level_precompute():
    runner_source = _source(RUNNER_PATH)
    matchup_source = _source(MATCHUP_CACHE_PATH)

    assert "build_matchup_cache" in runner_source
    assert "compute_opposing_starter_bulk" not in runner_source
    assert "compute_platoon_splits_bulk" not in runner_source
    assert "Precomputing matchup features" not in runner_source

    assert "def build_matchup_cache" in matchup_source
    assert "compute_opposing_starter_bulk" in matchup_source
    assert "compute_platoon_splits_bulk" in matchup_source
    assert "Precomputing matchup features" in matchup_source


def test_sweep_execution_owns_per_config_simulation_orchestration():
    runner_source = _source(RUNNER_PATH)
    execution_source = _source(SWEEP_EXECUTION_PATH)

    assert "run_single_config_fast_mlb" in runner_source
    assert "run_combined_config" in runner_source
    assert "def run_single_config_fast_mlb" not in runner_source
    assert "def run_single_config" not in runner_source
    assert "def run_combined_config" not in runner_source
    assert "BetSimulator(" not in runner_source
    assert "MetricsCalculator" not in runner_source
    assert "_resolve_bets_from_lookup" not in runner_source

    assert "def run_single_config_fast_mlb" in execution_source
    assert "def run_single_config" in execution_source
    assert "def run_combined_config" in execution_source
    assert "BetSimulator(" in execution_source
    assert "MetricsCalculator" in execution_source
    assert "_resolve_bets_from_lookup" in execution_source


def test_sweep_bootstrap_owns_model_dir_and_runtime_construction():
    runner_source = _source(RUNNER_PATH)
    bootstrap_source = _source(SWEEP_BOOTSTRAP_PATH)

    assert "initialize_sweep_runtime" in runner_source
    assert "def find_latest_model_dir" not in runner_source
    assert "get_engine(" not in runner_source
    assert "MLBFeatureStore(engine)" not in runner_source
    assert "MLBModelSuite.from_directory" not in runner_source
    assert "MLBBatterFeatureStore(engine)" not in runner_source

    assert "def find_latest_model_dir" in bootstrap_source
    assert "def initialize_sweep_runtime" in bootstrap_source
    assert "from src.db.client import get_engine" in bootstrap_source
    assert "MLBFeatureStore" in bootstrap_source
    assert "MLBModelSuite" in bootstrap_source
    assert "from_directory" in bootstrap_source
    assert "MLBBatterFeatureStore" in bootstrap_source


def test_final_run_mlb_sweep_runner_shape_target():
    source = _source(RUNNER_PATH)

    assert _non_comment_loc(RUNNER_PATH) < 450
    assert "pd.read_sql" not in source
    assert "sqlalchemy import text" not in source
