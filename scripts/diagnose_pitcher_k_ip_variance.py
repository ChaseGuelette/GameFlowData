"""
Test 2: IP Variance Attribution on High-Confidence Pitcher K Misses.

Reads pitcher_K bets from a backtest sweep output (bets.csv), joins against
mlb_player_game_stats_pitching from the local DB for actual IP, and decomposes
losses into "IP-driven" vs "rate-driven" failure modes.

Usage:
    python scripts/diagnose_pitcher_k_ip_variance.py --bets-csv backtest_results/sweep_XXXX/config_YY/bets.csv
    python scripts/diagnose_pitcher_k_ip_variance.py --sweep-dir backtest_results/sweep_XXXX
        (auto-picks the best-ROI config's bets.csv)
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.db.client import get_engine


def load_bets(bets_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(bets_csv)
    # Filter to pitcher_strikeouts only
    df = df[df["stat"] == "pitcher_strikeouts"].copy()
    print(f"Loaded {len(df)} pitcher_K bets from {bets_csv}")
    return df


def find_best_config_bets(sweep_dir: Path) -> Path:
    """Find the config subdirectory with the highest ROI and return its bets.csv."""
    best_roi = -999
    best_path = None
    for config_dir in sorted(sweep_dir.iterdir()):
        metrics_file = config_dir / "metrics.json"
        bets_file = config_dir / "bets.csv"
        if not metrics_file.exists() or not bets_file.exists():
            continue
        with open(metrics_file) as f:
            metrics = json.load(f)
        roi = metrics.get("roi_pct", metrics.get("roi", -999))
        if roi > best_roi:
            best_roi = roi
            best_path = bets_file
    if best_path is None:
        raise FileNotFoundError(f"No valid config dirs found in {sweep_dir}")
    print(f"Best config ROI: {best_roi:.1f}% -> {best_path.parent.name}")
    return best_path


def fetch_pitching_stats(engine, player_ids: list, game_dates: list) -> pd.DataFrame:
    """Fetch actual IP for the given player/date combos from local DB."""
    query = text("""
        SELECT player_id, game_date,
               ip,
               so
        FROM mlb_player_game_stats_pitching
        WHERE player_id = ANY(:pids)
          AND game_date = ANY(:dates)
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"pids": player_ids, "dates": game_dates})
        rows = result.fetchall()
    df = pd.DataFrame(rows, columns=["player_id", "game_date", "ip", "strikeouts_actual"])
    df.rename(columns={"ip": "innings_pitched"}, inplace=True)
    df["innings_pitched"] = df["innings_pitched"].astype(float)
    df["strikeouts_actual"] = df["strikeouts_actual"].astype(float)
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    return df


def analyze_ip_variance(bets: pd.DataFrame, pitching: pd.DataFrame) -> None:
    """Core analysis: decompose losses into IP-driven vs rate-driven."""

    # Merge
    bets["game_date_dt"] = pd.to_datetime(bets["game_date"]).dt.date
    merged = bets.merge(
        pitching,
        left_on=["player_id", "game_date_dt"],
        right_on=["player_id", "game_date"],
        how="left",
    )

    matched = merged["innings_pitched"].notna().sum()
    print(f"\nJoined {matched}/{len(bets)} bets to pitching stats "
          f"({len(bets) - matched} missing)")

    merged = merged[merged["innings_pitched"].notna()].copy()
    if merged.empty:
        print("ERROR: No pitching stats found for any bets. Check table/dates.")
        return

    # High-confidence: model_prob > 0.60
    hc = merged[merged["model_prob"] > 0.60].copy()
    print(f"\nHigh-confidence bets (model_prob > 60%): {len(hc)}")
    # Normalize outcome to uppercase
    hc["outcome"] = hc["outcome"].str.upper()

    print(f"  Wins: {(hc['outcome'] == 'WIN').sum()}, "
          f"Losses: {(hc['outcome'] == 'LOSS').sum()}, "
          f"Pushes: {(hc['outcome'] == 'PUSH').sum()}")

    losses = hc[hc["outcome"] == "LOSS"].copy()
    wins = hc[hc["outcome"] == "WIN"].copy()

    if losses.empty:
        print("No high-confidence losses to analyze.")
        return

    # --- IP Bucket Analysis ---
    ip_buckets = pd.cut(
        losses["innings_pitched"],
        bins=[0, 4.0, 5.0, 6.0, 7.0, 20.0],
        labels=["≤4 IP", "4-5 IP", "5-6 IP", "6-7 IP", "7+ IP"],
        right=True,
    )

    print(f"\n{'='*60}")
    print("IP DISTRIBUTION: HIGH-CONFIDENCE LOSSES vs WINS")
    print(f"{'='*60}")

    loss_dist = ip_buckets.value_counts(normalize=True).sort_index()
    print(f"\nLosses by IP bucket (n={len(losses)}):")
    for bucket, pct in loss_dist.items():
        count = ip_buckets.value_counts()[bucket]
        print(f"  {bucket}: {count:3d} ({pct:5.1%})")

    win_ip_buckets = pd.cut(
        wins["innings_pitched"],
        bins=[0, 4.0, 5.0, 6.0, 7.0, 20.0],
        labels=["≤4 IP", "4-5 IP", "5-6 IP", "6-7 IP", "7+ IP"],
        right=True,
    )
    win_dist = win_ip_buckets.value_counts(normalize=True).sort_index()
    print(f"\nWins by IP bucket (n={len(wins)}):")
    for bucket, pct in win_dist.items():
        count = win_ip_buckets.value_counts()[bucket]
        print(f"  {bucket}: {count:3d} ({pct:5.1%})")

    # --- Over vs Under decomposition ---
    print(f"\n{'='*60}")
    print("OVER vs UNDER BET LOSSES BY IP BUCKET")
    print(f"{'='*60}")

    for side in ["over", "under"]:
        side_losses = losses[losses["side"] == side]
        if side_losses.empty:
            print(f"\n  {side.upper()}: no losses")
            continue
        side_ip = pd.cut(
            side_losses["innings_pitched"],
            bins=[0, 4.0, 5.0, 6.0, 7.0, 20.0],
            labels=["≤4 IP", "4-5 IP", "5-6 IP", "6-7 IP", "7+ IP"],
            right=True,
        )
        print(f"\n  {side.upper()} losses (n={len(side_losses)}):")
        for bucket in ["≤4 IP", "4-5 IP", "5-6 IP", "6-7 IP", "7+ IP"]:
            count = (side_ip == bucket).sum()
            pct = count / len(side_losses)
            print(f"    {bucket}: {count:3d} ({pct:5.1%})")

    # --- Rate Decomposition ---
    print(f"\n{'='*60}")
    print("RATE vs IP DECOMPOSITION (OVER bets only)")
    print(f"{'='*60}")

    over_losses = losses[losses["side"] == "over"].copy()
    if not over_losses.empty:
        over_losses["actual_k_per_ip"] = (
            over_losses["strikeouts_actual"] / over_losses["innings_pitched"].replace(0, np.nan)
        )
        # What K/IP rate would have been needed to hit the over?
        # For over bet on line L, need actual > L, so needed_k = line + 0.5 (for standard lines)
        over_losses["needed_k"] = over_losses["line"] + 0.5
        over_losses["needed_k_per_ip"] = (
            over_losses["needed_k"] / over_losses["innings_pitched"].replace(0, np.nan)
        )

        # Classification:
        # "IP-driven" = K/IP rate was reasonable (≥ 0.8 K/IP) but innings were short (≤5 IP)
        # "Rate-driven" = Had enough innings (>5 IP) but K rate was low
        # "Both" = short outing AND low rate
        ip_short = over_losses["innings_pitched"] <= 5.0
        rate_low = over_losses["actual_k_per_ip"] < 0.8  # ~league avg is ~1.0 K/IP

        over_losses["failure_mode"] = "unknown"
        over_losses.loc[ip_short & ~rate_low, "failure_mode"] = "IP-driven"
        over_losses.loc[~ip_short & rate_low, "failure_mode"] = "Rate-driven"
        over_losses.loc[ip_short & rate_low, "failure_mode"] = "Both"
        over_losses.loc[~ip_short & ~rate_low, "failure_mode"] = "Unlucky (rate+IP fine)"

        mode_counts = over_losses["failure_mode"].value_counts()
        print(f"\n  Over bet losses decomposition (n={len(over_losses)}):")
        for mode, count in mode_counts.items():
            pct = count / len(over_losses)
            print(f"    {mode}: {count:3d} ({pct:5.1%})")

        # Detailed stats
        print(f"\n  Mean actual IP on losses: {over_losses['innings_pitched'].mean():.1f}")
        print(f"  Mean actual K/IP on losses: {over_losses['actual_k_per_ip'].mean():.2f}")
        print(f"  Mean line: {over_losses['line'].mean():.1f}")
        print(f"  Mean actual K: {over_losses['actual'].mean():.1f}")

    # --- Under bet analysis ---
    print(f"\n{'='*60}")
    print("UNDER BET LOSSES: IP ANALYSIS")
    print(f"{'='*60}")

    under_losses = losses[losses["side"] == "under"].copy()
    if not under_losses.empty:
        print(f"\n  Under bet losses (n={len(under_losses)}):")
        print(f"  Mean actual IP: {under_losses['innings_pitched'].mean():.1f}")
        print(f"  Mean actual K: {under_losses['actual'].mean():.1f}")
        print(f"  Mean line: {under_losses['line'].mean():.1f}")

        deep_outings = under_losses[under_losses["innings_pitched"] >= 7.0]
        print(f"  Deep outings (7+ IP): {len(deep_outings)} ({len(deep_outings)/len(under_losses):.0%})")
    else:
        print("\n  No under bet losses.")

    # --- Summary ---
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    short_ip_losses = (losses["innings_pitched"] <= 5.0).sum()
    total_losses = len(losses)
    print(f"  High-confidence losses with ≤5 IP: {short_ip_losses}/{total_losses} "
          f"({short_ip_losses/total_losses:.0%})")
    print(f"  → {'IP decomposition LIKELY USEFUL' if short_ip_losses/total_losses > 0.5 else 'IP decomposition may NOT be the primary fix'}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bets-csv", type=str, help="Path to a specific bets.csv")
    parser.add_argument("--sweep-dir", type=str, help="Path to sweep dir (auto-picks best config)")
    parser.add_argument("--local", action="store_true", default=True,
                        help="Use local DB (default: True)")
    args = parser.parse_args()

    if args.bets_csv:
        bets_csv = Path(args.bets_csv)
    elif args.sweep_dir:
        bets_csv = find_best_config_bets(Path(args.sweep_dir))
    else:
        print("ERROR: Provide --bets-csv or --sweep-dir")
        sys.exit(1)

    bets = load_bets(bets_csv)
    if bets.empty:
        print("No pitcher_K bets found in the CSV.")
        return

    engine = get_engine(local=True)
    player_ids = bets["player_id"].unique().tolist()
    game_dates = pd.to_datetime(bets["game_date"]).dt.date.unique().tolist()

    pitching = fetch_pitching_stats(engine, player_ids, game_dates)
    print(f"Fetched {len(pitching)} pitching stat rows from local DB")

    analyze_ip_variance(bets, pitching)


if __name__ == "__main__":
    main()
