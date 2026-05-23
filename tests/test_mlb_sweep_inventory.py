"""Structural inventory guards for the MLB sweep god-module migration.

These tests do not prove betting correctness. They guard migration ownership:
responsibilities extracted from run_mlb_sweep.py should not quietly drift back
into the runner while the behavior tests continue to pass.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

RUNNER_PATH = Path("src/backtesting/mlb/run_mlb_sweep.py")
QUOTE_POLICY_PATH = Path("src/backtesting/mlb/quote_decision_policy.py")
QUOTE_LINE_SERVICE_PATH = Path("src/backtesting/mlb/quote_clean_line_service.py")


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


@pytest.mark.xfail(reason="Final thin-runner target; enable after later extraction phases remove remaining responsibilities.")
def test_final_run_mlb_sweep_runner_shape_target():
    source = _source(RUNNER_PATH)

    assert _non_comment_loc(RUNNER_PATH) < 450
    assert "pd.read_sql" not in source
    assert "sqlalchemy import text" not in source
