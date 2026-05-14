from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "analyze_mlb_batter_hits_residuals.py"


def load_module():
    spec = importlib.util.spec_from_file_location("analyze_mlb_batter_hits_residuals", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_sample_status_thresholds():
    m = load_module()
    assert m.sample_status(29, 100) == "masked_small_n"
    assert m.sample_status(30, 100) == "exploratory"
    assert m.sample_status(99, 100) == "exploratory"
    assert m.sample_status(100, 100) == "decision_eligible"


def test_push_excluded_from_hit_rate_and_zero_profit_roi():
    m = load_module()
    df = pd.DataFrame(
        {
            "game_date": ["2026-04-01", "2026-04-01", "2026-04-02"],
            "outcome": ["win", "loss", "push"],
            "profit": [90.0, -100.0, 0.0],
            "stake": [100.0, 100.0, 100.0],
        }
    )
    metrics = m.compute_bet_metrics(df, flat_stake=100.0)
    assert metrics["pushes"] == 1
    assert metrics["hit_rate"] == 0.5
    assert metrics["profit"] == -10.0
    assert metrics["roi"] == -10.0 / 300.0


def test_block_bootstrap_ci_sanity_on_known_distribution():
    m = load_module()
    df = pd.DataFrame(
        {
            "game_date": ["2026-04-01"] * 2 + ["2026-04-02"] * 2 + ["2026-04-03"] * 2,
            "outcome": ["win", "loss", "win", "loss", "win", "loss"],
            "profit": [100.0, -100.0, 100.0, -100.0, 100.0, -100.0],
            "stake": [100.0] * 6,
        }
    )
    result = m.block_bootstrap_ci(df, lambda x: m.compute_bet_metrics(x)["roi"], n_resamples=200, seed=7)
    assert result["method"] == "block_by_game_date"
    assert result["n_blocks"] == 3
    assert result["estimate"] == 0.0
    assert result["ci_low"] <= 0.0 <= result["ci_high"]


def test_adaptive_edge_bins_merge_sparse_bins():
    m = load_module()
    df = pd.DataFrame({"edge": [0.081, 0.083, 0.101, 0.121, 0.151, 0.181, 0.231]})
    labels = m.assign_adaptive_edge_bins(df, min_bin_size=3)
    assert len(labels) == len(df)
    counts = labels.value_counts()
    assert counts.min() >= 3


def test_high_edge_stat_tests_return_material_flags():
    m = load_module()
    df = pd.DataFrame(
        {
            "outcome": ["win"] * 8 + ["loss"] * 8,
            "edge": [0.20] * 8 + [0.16] * 8,
            "odds": [-110] * 16,
            "model_prob": [0.70] * 8 + [0.55] * 8,
            "implied_prob": [0.50] * 16,
            "bookmaker": ["a"] * 8 + ["b"] * 8,
            "line_bucket": ["0.5"] * 8 + ["1.5"] * 8,
            "time_bucket": ["early"] * 8 + ["late"] * 8,
        }
    )
    tests = m.high_edge_win_loss_tests(df)
    by_dim = {row["dimension"]: row for row in tests}
    assert by_dim["model_prob"]["test"] == "mannwhitneyu"
    assert by_dim["bookmaker"]["test"] in {"chi2", "fisher_exact"}
    assert by_dim["bookmaker"]["material"] is True


def test_clv_detection_true_proxy_and_unavailable():
    m = load_module()
    true_df = pd.DataFrame({"odds_at_bet": [-110, 120], "odds_at_close": [-130, 100], "edge": [0.1, 0.2]})
    true_out = m.compute_clv_table(true_df)
    assert true_out is not None
    assert set(true_out["clv_type"]) == {"true_odds_clv_cents"}

    proxy_df = pd.DataFrame(
        {"odds_at_bet": [-110, 120], "consensus_close_implied_prob": [0.55, 0.40], "edge": [0.1, 0.2]}
    )
    proxy_out = m.compute_clv_table(proxy_df)
    assert proxy_out is not None
    assert set(proxy_out["clv_type"]) == {"implied_clv_proxy"}

    unavailable_df = pd.DataFrame({"odds": [-110, 120], "edge": [0.1, 0.2]})
    assert m.compute_clv_table(unavailable_df) is None


def test_drift_detection_rules():
    m = load_module()
    decaying = pd.DataFrame(
        {
            "week_index": [1, 2, 3, 4, 5, 6],
            "roi": [0.30, 0.20, 0.10, 0.00, -0.10, -0.20],
        }
    )
    result = m.detect_drift(decaying)
    assert result["spearman_r"] < 0
    assert result["decay_detected"] is True
    assert result["decay_watchlist"] is True

    watch = pd.DataFrame({"week_index": [1, 2, 3, 4], "roi": [0.3, 0.1, 0.2, -0.1]})
    result = m.detect_drift(watch)
    assert result["decay_detected"] is False
    assert result["decay_watchlist"] is True

    stable = pd.DataFrame({"week_index": [1, 2, 3, 4], "roi": [0.10, 0.11, 0.09, 0.10]})
    result = m.detect_drift(stable)
    assert result["decay_detected"] is False


def test_output_schema_constants_include_required_columns():
    m = load_module()
    required = {"group", "n", "n_blocks", "sample_status", "bootstrap_method"}
    assert required.issubset(set(m.METRIC_SCHEMA_BASE))
    assert {"dimension", "test", "p_value", "material", "win_median", "loss_median"}.issubset(set(m.WIN_LOSS_SCHEMA))
