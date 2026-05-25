from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import importlib.util
import sys

import numpy as np
import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "analyze_mlb_clv_ranking_diagnostics.py"


def load_module():
    spec = importlib.util.spec_from_file_location("analyze_mlb_clv_ranking_diagnostics", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_logit_clipping_produces_finite_scores():
    m = load_module()
    df = pd.DataFrame(
        {
            "edge": [0.1] * 4,
            "model_prob": [0.0, 1.0, 0.5, 0.9999999],
            "implied_prob": [0.0, 0.0, 0.0, 1.0],
            "clv_implied_prob": [0.1, 0.2, 0.3, 0.4],
        }
    )

    scores = m._build_base_scores(df)
    logit_scores = scores["logit_edge"]
    assert np.isfinite(logit_scores).all()


def test_implied_prob_prefers_implied_prob_and_falls_back_to_bet_implied_prob():
    m = load_module()
    df = pd.DataFrame(
        {
            "edge": [0.05, 0.10],
            "model_prob": [0.55, 0.65],
            "implied_prob": [np.nan, np.nan],
            "bet_implied_prob": [0.52, 0.64],
            "clv_implied_prob": [0.10, 0.20],
        }
    )

    scores = m._build_base_scores(df)
    assert scores["implied_prob"].tolist() == [0.52, 0.64]


def test_perfect_monotonic_score_passes_and_reversed_fails():
    m = load_module()
    n = 160
    score = np.arange(1, n + 1, dtype=float)
    df = pd.DataFrame(
        {
            "edge": score,
            "model_prob": np.full(n, 0.55),
            "implied_prob": np.full(n, 0.5),
            "clv_implied_prob": score / 200.0,
        }
    )

    passthrough = m.summarize_score(
        df,
        "raw_edge",
        df["edge"],
        bootstrap_samples=200,
        ci_level=0.95,
        min_n=100,
        random_seed=42,
    )
    reverse = m.summarize_score(
        df,
        "raw_edge",
        -df["edge"],
        bootstrap_samples=200,
        ci_level=0.95,
        min_n=100,
        random_seed=42,
    )

    assert passthrough["pass"] is True
    assert reverse["pass"] is False


def test_missing_optional_scores_are_not_required_and_are_skipped():
    m = load_module()
    df = pd.DataFrame(
        {
            "edge": [0.1, 0.2, 0.3],
            "model_prob": [0.55, 0.60, 0.45],
            "implied_prob": [0.50, 0.48, 0.46],
            "clv_implied_prob": [0.1, 0.2, 0.3],
        }
    )

    scores = m.build_score_registry(df)
    default_scores = m.filter_scores_for_set(scores, "default")
    all_scores = m.filter_scores_for_set(scores, "all")

    assert "line_score" not in all_scores
    assert "odds_at_bet_score" not in all_scores
    assert list(default_scores.keys()) == [
        "raw_edge",
        "abs_edge",
        "model_prob",
        "implied_prob",
        "logit_edge",
        "model_prob_x_abs_edge",
        "edge_zscore",
    ]


def test_candidate_edge_aggregation_from_bet_id_roundtrips_into_scores(tmp_path):
    m = load_module()
    clv_df = pd.DataFrame(
        {
            "bet_id": [101, 102, 103],
            "edge": [0.12, -0.08, 0.03],
            "model_prob": [0.55, 0.61, 0.49],
            "implied_prob": [0.50, 0.50, 0.50],
            "clv_implied_prob": [0.11, 0.17, 0.09],
        }
    )

    candidate_df = pd.DataFrame(
        {
            "bet_id": [101, 101, 102, 103, 103],
            "candidate_edge": [0.20, 0.15, 0.04, -0.01, 0.06],
            "bookmaker": ["A", "B", "A", "A", "C"],
        }
    )

    tmp_dir = tmp_path / "tmp_candidate_artifacts"
    tmp_dir.mkdir()
    candidate_csv = tmp_dir / "candidate.csv"
    candidate_df.to_csv(candidate_csv, index=False)

    candidate_scores = m.load_candidate_features(clv_df, candidate_csv)

    expected_best = pd.Series([0.20, 0.04, 0.06], index=clv_df.index)
    expected_mean = pd.Series([0.175, 0.04, 0.025], index=clv_df.index)
    expected_survival = pd.Series([2, 1, 1], index=clv_df.index)
    expected_reference_prob = pd.Series([0.375, 0.57, 0.465], index=clv_df.index)
    expected_execution_prob = pd.Series([0.43, 0.69, 0.46], index=clv_df.index)
    expected_model_alpha = pd.Series([0.175, 0.04, 0.025], index=clv_df.index)
    expected_execution_alpha = pd.Series([-0.055, -0.12, 0.005], index=clv_df.index)

    pd.testing.assert_series_equal(candidate_scores["candidate_best_edge"].round(6), expected_best)
    pd.testing.assert_series_equal(candidate_scores["candidate_mean_edge"].round(6), expected_mean)
    pd.testing.assert_series_equal(candidate_scores["candidate_edge_survival_count"], expected_survival)
    pd.testing.assert_series_equal(candidate_scores["reference_prob"].round(6), expected_reference_prob)
    pd.testing.assert_series_equal(candidate_scores["execution_prob"].round(6), expected_execution_prob)
    pd.testing.assert_series_equal(candidate_scores["model_alpha"].round(6), expected_model_alpha)
    pd.testing.assert_series_equal(candidate_scores["execution_alpha"].round(6), expected_execution_alpha)
    assert float(candidate_scores["alpha_reconstruction_error"].abs().max()) < 1e-12


def test_alpha_2d_buckets_and_filter_replay_scorecard():
    m = load_module()
    df = pd.DataFrame(
        {
            "game_date": ["2026-05-01"] * 6,
            "model_alpha": [-0.02, 0.01, 0.03, -0.01, 0.02, 0.05],
            "execution_alpha": [-0.01, 0.0, 0.006, 0.02, 0.004, 0.015],
            "clv_implied_prob": [-0.01, 0.0, 0.02, 0.01, 0.005, 0.03],
            "profit": [-100, -50, 120, 80, 20, 150],
            "stake": [100] * 6,
        }
    )

    buckets = pd.DataFrame(m.build_alpha_2d_buckets(df, bootstrap_samples=25, ci_level=0.95, random_seed=42))
    assert {"model_alpha_bucket", "execution_alpha_bucket", "bucket_n", "mean_clv", "roi", "max_drawdown"}.issubset(buckets.columns)
    assert "positive" in set(buckets["execution_alpha_bucket"])
    assert int(buckets["bucket_n"].sum()) == 6

    replay = pd.DataFrame(m.build_filter_replay(df, bootstrap_samples=25, ci_level=0.95, random_seed=42))
    rules = set(replay["filter_rule"])
    assert "all" in rules
    assert "execution_alpha>=0.005" in rules
    assert "model_alpha>=0_and_execution_alpha>=0.005" in rules
    positive_exec = replay.loc[replay["filter_rule"] == "execution_alpha>=0.005"].iloc[0]
    assert int(positive_exec["n"]) == 3
    assert positive_exec["roi"] > 0
    assert positive_exec["mean_clv"] > 0


def test_cli_run_writes_all_required_outputs(tmp_path):
    m = load_module()
    clv_path = tmp_path / "clv_matches.csv"
    out_dir = tmp_path / "out"
    pd.DataFrame(
        {
            "edge": [0.1, 0.2, 0.3, 0.4, 0.5, -0.1, -0.2, -0.3, 0.7, 0.6],
            "model_prob": [0.55, 0.58, 0.62, 0.64, 0.68, 0.52, 0.51, 0.49, 0.72, 0.74],
            "implied_prob": [0.50] * 10,
            "clv_implied_prob": [0.01, 0.02, 0.03, 0.04, 0.05, 0.01, 0.01, 0.01, 0.06, 0.07],
        }
    ).to_csv(clv_path, index=False)

    args = Namespace(
        clv_matches_csv=str(clv_path),
        output_dir=str(out_dir),
        candidate_edges_csv=None,
        score_set="default",
        bootstrap_samples=50,
        ci_level=0.95,
        min_n=3,
        random_seed=42,
    )

    out = m.run(args)

    assert Path(out["summary_csv"]).is_file()
    assert Path(out["bins_csv"]).is_file()
    assert Path(out["slice_csv"]).is_file()
    assert Path(out["scorecard_csv"]).is_file()
    assert Path(out["edge_decomposition_rows_csv"]).is_file()
    assert Path(out["edge_decomposition_2d_buckets_csv"]).is_file()
    assert Path(out["edge_decomposition_filter_replay_csv"]).is_file()
    assert Path(out["recommendation_md"]).is_file()
    assert (out_dir / "ranking_score_summary.csv").exists()
    assert (out_dir / "ranking_score_bins.csv").exists()
    assert (out_dir / "ranking_score_slice_summary.csv").exists()
    assert (out_dir / "ranking_scorecard_summary.csv").exists()
    assert (out_dir / "edge_decomposition_rows.csv").exists()
    assert (out_dir / "edge_decomposition_2d_buckets.csv").exists()
    assert (out_dir / "edge_decomposition_filter_replay.csv").exists()
    assert (out_dir / "ranking_score_recommendation.md").exists()
