"""
CLI tool for comparing predictions from two model directories side-by-side.

Loads two model artifacts (e.g., old archived vs new production), generates
predictions for the same set of players on a given date, and prints a
comparison table. No DB writes.

Examples:
  python src/tools/compare_models.py \
    --model-a src/models/artifacts/production_archived_20260305 \
    --model-b src/models/artifacts/production \
    --date 2026-03-05

  python src/tools/compare_models.py \
    --model-a src/models/artifacts/production_archived_20260305 \
    --model-b src/models/artifacts/production \
    --date 2026-03-05 \
    --player "Luka Doncic" \
    --stats pts reb ast
"""

import argparse
import sys
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

# Force UTF-8 output on Windows to handle player names with diacriticals
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.daily_runner import DailyPredictionRunner
from src.models.feature_store import FeatureStore
from src.models.monte_carlo import (
    MonteCarloPredictor,
    load_combined_calibration_offsets,
    load_copula_params,
)
from src.models.quantile_trainer import PlayerPropsModelPipeline


def load_model(model_dir: str, feature_store: FeatureStore) -> MonteCarloPredictor:
    """Load a model pipeline + MC predictor from a directory."""
    pipeline = PlayerPropsModelPipeline.load_all(model_dir, feature_store)
    copula_params = load_copula_params(model_dir)
    calibration_offsets = load_combined_calibration_offsets(model_dir)

    predictor = MonteCarloPredictor(
        model_pipeline=pipeline,
        n_samples=10000,
        random_state=42,
        copula_params=copula_params,
        combined_calibration_offsets=calibration_offsets,
    )

    return predictor


def describe_model(model_dir: str) -> str:
    """Return a short description of a model based on its artifacts."""
    p = Path(model_dir)
    parts = []
    if (p / "copula_params.json").exists():
        parts.append("copula")
    else:
        parts.append("legacy-sampling")
    if (p / "combined_calibration_offsets.json").exists():
        parts.append("conformal-offsets")
    if (p / "CALIBRATION_FAILED.txt").exists():
        parts.append("CALIBRATION_FAILED")
    if (p / "calibration_report_combined.json").exists():
        parts.append("calibrated")
    return f"{p.name} ({', '.join(parts)})"


def odds_to_prob(odds: float) -> float | None:
    if pd.isna(odds):
        return None
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def main():
    parser = argparse.ArgumentParser(
        description="Compare predictions from two model directories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--model-a", required=True, help="Path to model A (e.g., old/archived)")
    parser.add_argument("--model-b", required=True, help="Path to model B (e.g., new/production)")
    parser.add_argument("--date", type=str, default=str(date.today() - timedelta(days=1)),
                        help="Target date (YYYY-MM-DD, default: yesterday)")
    parser.add_argument("--player", type=str, help="Filter to specific player (partial match)")
    parser.add_argument("--stats", nargs="+", default=["pts", "reb", "ast"],
                        help="Stats to compare (default: pts reb ast)")

    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    print(f"\n{'='*70}")
    print(f"  Model Comparison - {target_date}")
    print(f"{'='*70}")

    # Init shared resources
    engine = get_engine()
    feature_store = FeatureStore(engine)

    # Load both models
    print(f"\n  Model A: {describe_model(args.model_a)}")
    predictor_a = load_model(args.model_a, feature_store)
    pipeline_a = predictor_a.pipeline

    print(f"  Model B: {describe_model(args.model_b)}")
    predictor_b = load_model(args.model_b, feature_store)

    # Use Model A's pipeline to discover games/players/features (shared)
    runner = DailyPredictionRunner(engine, feature_store, pipeline_a, predictor_a)

    print(f"\n  Discovering games and players for {target_date}...")
    games = runner._get_games_for_date(target_date)
    if not games:
        print("  No games found for this date.")
        return

    players = runner._get_players_for_games(games, target_date)
    players = runner._filter_injured_players(players, target_date)
    print(f"  Found {len(games)} games, {len(players)} active players")

    if not players:
        print("  No players found.")
        return

    features_df = runner._build_features_df(players, target_date)
    if features_df.empty:
        print("  No features could be built.")
        return

    print(f"  Built features for {len(features_df)} players")

    # Generate predictions from both models
    print("\n  Running Model A predictions...")
    preds_a, samples_a = predictor_a.predict_batch_for_date(features_df, stats=args.stats)
    df_a = pd.DataFrame(preds_a)

    print("  Running Model B predictions...")
    preds_b, samples_b = predictor_b.predict_batch_for_date(features_df, stats=args.stats)
    df_b = pd.DataFrame(preds_b)

    if df_a.empty or df_b.empty:
        print("  One or both models produced no predictions.")
        return

    # Merge predictions
    merge_keys = ["player_id", "game_id", "stat"]
    keep_cols_a = merge_keys + ["player_name", "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90", "pred_mean"]
    keep_cols_b = merge_keys + ["pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90", "pred_mean"]

    merged = df_a[keep_cols_a].merge(
        df_b[keep_cols_b], on=merge_keys, suffixes=("_a", "_b")
    )
    merged["q50_diff"] = merged["pred_q50_b"] - merged["pred_q50_a"]
    merged["q50_abs_diff"] = merged["q50_diff"].abs()

    # Fetch prop lines for market context
    lines_df = runner._get_current_lines(games, args.stats)
    if not lines_df.empty:
        market_to_stat = {"player_points": "pts", "player_rebounds": "reb", "player_assists": "ast"}
        lines_df = lines_df.copy()
        lines_df["stat"] = lines_df["market_key"].map(market_to_stat)
        lines_subset = lines_df[["player_id", "game_id", "stat", "line", "over_odds", "under_odds"]].copy()
        merged = merged.merge(lines_subset, on=merge_keys, how="left")
    else:
        merged["line"] = np.nan
        merged["over_odds"] = np.nan
        merged["under_odds"] = np.nan

    # Compute which model is closer to market line
    has_line = merged["line"].notna()
    merged["dist_a"] = (merged["pred_q50_a"] - merged["line"]).abs()
    merged["dist_b"] = (merged["pred_q50_b"] - merged["line"]).abs()
    merged["closer"] = np.where(
        ~has_line, "",
        np.where(merged["dist_a"] < merged["dist_b"], "A",
                 np.where(merged["dist_b"] < merged["dist_a"], "B", "tie"))
    )

    stat_labels = {"pts": "PTS", "reb": "REB", "ast": "AST"}

    # -- Section 1: Summary --
    print(f"\n{'-'*70}")
    print("  SUMMARY")
    print(f"{'-'*70}")
    print(f"  Players compared: {merged['player_id'].nunique()}")
    print(f"  Total prediction pairs: {len(merged)}")
    print()
    print(f"  {'Stat':>6s}  {'Mean |Q50 diff|':>16s}  {'Max |Q50 diff|':>15s}  {'A closer':>9s}  {'B closer':>9s}")
    print(f"  {'-'*60}")
    for stat in args.stats:
        stat_rows = merged[merged["stat"] == stat]
        if stat_rows.empty:
            continue
        mean_diff = stat_rows["q50_abs_diff"].mean()
        max_diff = stat_rows["q50_abs_diff"].max()
        with_line = stat_rows[stat_rows["line"].notna()]
        a_closer = (with_line["closer"] == "A").sum() if len(with_line) > 0 else 0
        b_closer = (with_line["closer"] == "B").sum() if len(with_line) > 0 else 0
        print(f"  {stat_labels.get(stat, stat):>6s}  {mean_diff:>16.2f}  {max_diff:>15.2f}  {a_closer:>9d}  {b_closer:>9d}")

    # --Section 2: Player Detail (when --player specified) --
    if args.player:
        # Strip diacriticals for fuzzy matching (e.g., "Doncic" matches "Dončić")
        def strip_accents(s: str) -> str:
            return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

        player_filter = strip_accents(args.player.lower())
        player_rows = merged[
            merged["player_name"].apply(lambda n: player_filter in strip_accents((n or "").lower()))
        ]

        if player_rows.empty:
            print(f"\n  No predictions found matching '{args.player}'")
        else:
            matched_name = player_rows.iloc[0]["player_name"]
            print(f"\n{'-'*70}")
            print(f"  PLAYER DETAIL: {matched_name}")
            print(f"{'-'*70}")
            print()
            print(f"  {'Stat':>4s}  {'':>5s}  {'Q10':>6s}  {'Q25':>6s}  {'Q50':>6s}  {'Q75':>6s}  "
                  f"{'Q90':>6s}  {'Mean':>6s}  {'Line':>6s}  {'Closer':>6s}")
            print(f"  {'-'*68}")

            for _, row in player_rows.iterrows():
                stat = stat_labels.get(row["stat"], row["stat"])
                line_str = f"{row['line']:.1f}" if pd.notna(row["line"]) else "  -"
                closer_str = row["closer"] if row["closer"] else "-"

                print(f"  {stat:>4s}  {'A':>5s}  {row['pred_q10_a']:6.1f}  {row['pred_q25_a']:6.1f}  "
                      f"{row['pred_q50_a']:6.1f}  {row['pred_q75_a']:6.1f}  {row['pred_q90_a']:6.1f}  "
                      f"{row['pred_mean_a']:6.1f}  {line_str:>6s}  {closer_str:>6s}")
                print(f"  {'':>4s}  {'B':>5s}  {row['pred_q10_b']:6.1f}  {row['pred_q25_b']:6.1f}  "
                      f"{row['pred_q50_b']:6.1f}  {row['pred_q75_b']:6.1f}  {row['pred_q90_b']:6.1f}  "
                      f"{row['pred_mean_b']:6.1f}")
                diff = row["q50_diff"]
                sign = "+" if diff >= 0 else ""
                print(f"  {'':>4s}  {'diff':>5s}  {'':>6s}  {'':>6s}  {sign}{diff:.1f}{'':>5s}")
                print()

    # --Section 3: Top Differences --
    print(f"{'-'*70}")
    print("  TOP 10 LARGEST Q50 DIFFERENCES")
    print(f"{'-'*70}")
    top = merged.nlargest(10, "q50_abs_diff")
    print()
    print(f"  {'Player':<22s} {'Stat':>4s} {'Q50_A':>6s} {'Q50_B':>6s} {'Diff':>7s} {'Line':>6s} {'Closer':>6s}")
    print(f"  {'-'*58}")
    for _, row in top.iterrows():
        name = (row["player_name"] or "???")[:21]
        stat = stat_labels.get(row["stat"], row["stat"])
        diff = row["q50_diff"]
        sign = "+" if diff >= 0 else ""
        line_str = f"{row['line']:.1f}" if pd.notna(row["line"]) else "-"
        closer_str = row["closer"] if row["closer"] else "-"
        print(f"  {name:<22s} {stat:>4s} {row['pred_q50_a']:6.1f} {row['pred_q50_b']:6.1f} "
              f"{sign}{diff:6.1f} {line_str:>6s} {closer_str:>6s}")

    # --Section 4: Market Accuracy Summary --
    with_line = merged[merged["line"].notna()]
    if len(with_line) > 0:
        print(f"\n{'-'*70}")
        print("  MARKET ACCURACY (which model's Q50 is closer to the line)")
        print(f"{'-'*70}")
        a_wins = (with_line["closer"] == "A").sum()
        b_wins = (with_line["closer"] == "B").sum()
        ties = (with_line["closer"] == "tie").sum()
        total = len(with_line)
        print(f"\n  Model A closer: {a_wins}/{total} ({a_wins/total*100:.1f}%)")
        print(f"  Model B closer: {b_wins}/{total} ({b_wins/total*100:.1f}%)")
        print(f"  Ties:           {ties}/{total}")
        print(f"\n  Mean distance to line - A: {with_line['dist_a'].mean():.2f}  B: {with_line['dist_b'].mean():.2f}")

    print()


if __name__ == "__main__":
    main()
