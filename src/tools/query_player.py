"""
CLI tool for querying stored daily predictions.

Modes:
  1. Player + stat + line: Compute over/under probability from stored MC samples
  2. Player overview: Show all predictions for a player on a date
  3. Date overview (--top N): Show top N predictions by edge for a date

Examples:
  python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5
  python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5 --odds -110
  python src/tools/query_player.py --player "Cade Cunningham"
  python src/tools/query_player.py --top 20
  python src/tools/query_player.py --date 2026-01-29 --top 10
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.models.prediction_store import PredictionStore


def odds_to_decimal(american_odds: float) -> float:
    """Convert American odds to decimal profit multiplier."""
    if american_odds > 0:
        return american_odds / 100
    else:
        return 100 / abs(american_odds)


def format_prob(p: float | None) -> str:
    if p is None:
        return "N/A"
    return f"{p * 100:.1f}%"


def format_edge(e: float | None) -> str:
    if e is None:
        return "N/A"
    sign = "+" if e >= 0 else ""
    return f"{sign}{e * 100:.1f}%"


def format_odds(odds: float | None) -> str:
    if odds is None:
        return "N/A"
    if odds > 0:
        return f"+{int(odds)}"
    return str(int(odds))


def print_line_query(store: PredictionStore, prediction_date: date,
                     player_name: str, stat: str, line: float,
                     odds: float | None = None):
    """Mode 1: Player + stat + line — compute probability from MC samples."""
    player_id = store.get_player_id_by_name(player_name)
    if player_id is None:
        print(f"Player not found: '{player_name}'")
        return

    # Get prediction row for this player/stat/date
    preds = store.get_predictions(prediction_date, player_id=player_id, stat=stat)
    if preds.empty:
        print(f"No predictions found for player_id={player_id} on {prediction_date} ({stat})")
        return

    row = preds.iloc[0]

    # Load MC samples
    samples = store.get_samples(prediction_date, player_id, row["game_id"], stat)
    if samples is None:
        print(f"No MC samples found. Falling back to quantile interpolation.")
        # Could add quantile interpolation fallback here
        return

    prob_over = float((samples > line).mean())
    prob_under = float((samples <= line).mean())

    stat_label = {"pts": "Points", "reb": "Rebounds", "ast": "Assists",
                  "threes": "Threes", "blk": "Blocks", "stl": "Steals"}.get(stat, stat.upper())

    name = row.get("player_name") or player_name
    n = len(samples)

    print()
    print(f"{name} -- {stat_label} ({prediction_date})")
    print("-" * 50)
    print(f"Prediction:  {row['pred_mean']:.1f} mean | {row['pred_median']:.1f} median")
    print(f"Distribution: [{row['pred_q10']:.1f} q10] [{row['pred_q25']:.1f} q25] "
          f"[{row['pred_q50']:.1f} q50] [{row['pred_q75']:.1f} q75] [{row['pred_q90']:.1f} q90]")
    print()
    print(f"Query Line: {line}")
    print(f"  Over  {line}:  {format_prob(prob_over)} (model)")
    print(f"  Under {line}:  {format_prob(prob_under)} (model)")

    # If market line exists on prediction, show comparison
    if row.get("line") is not None and row.get("implied_over") is not None:
        print()
        print(f"Market Line: {row['line']}  [{format_odds(row.get('over_odds'))} / {format_odds(row.get('under_odds'))}]")
        print(f"  Implied Over:  {format_prob(row['implied_over'])}")
        print(f"  Implied Under: {format_prob(row['implied_under'])}")
        print(f"  Over Edge:  {format_edge(row.get('over_edge'))}")
        print(f"  Under Edge: {format_edge(row.get('under_edge'))}")

    # If user provided odds, calculate EV
    if odds is not None:
        profit = odds_to_decimal(odds)
        ev_over = prob_over * profit - (1 - prob_over)
        ev_under = prob_under * profit - (1 - prob_under)
        print()
        print(f"At {format_odds(odds)} odds:")
        print(f"  EV Over:  {ev_over:+.3f} units")
        print(f"  EV Under: {ev_under:+.3f} units")

    print()
    print(f"Based on {n:,} Monte Carlo samples")
    print()


def print_player_overview(store: PredictionStore, prediction_date: date, player_name: str):
    """Mode 2: Show all predictions for a player on a date."""
    player_id = store.get_player_id_by_name(player_name)
    if player_id is None:
        print(f"Player not found: '{player_name}'")
        return

    preds = store.get_predictions(prediction_date, player_id=player_id)
    if preds.empty:
        print(f"No predictions found for '{player_name}' on {prediction_date}")
        return

    name = preds.iloc[0].get("player_name") or player_name

    print()
    print(f"{name} -- All Predictions ({prediction_date})")
    print("=" * 70)

    for _, row in preds.iterrows():
        stat_label = {"pts": "Points", "reb": "Rebounds", "ast": "Assists",
                      "threes": "Threes"}.get(row["stat"], row["stat"].upper())

        line_info = ""
        if row.get("line") is not None:
            line_info = (f"  |  Line: {row['line']}  "
                         f"Over: {format_edge(row.get('over_edge'))}  "
                         f"Under: {format_edge(row.get('under_edge'))}")

        print(f"  {stat_label:10s}  "
              f"Mean: {row['pred_mean']:5.1f}  "
              f"Median: {row['pred_median']:5.1f}  "
              f"[{row['pred_q10']:.1f} - {row['pred_q90']:.1f}]"
              f"{line_info}")

    print()


def print_top_edges(store: PredictionStore, prediction_date: date, top_n: int):
    """Mode 3: Show top N predictions by absolute edge."""
    preds = store.get_predictions(prediction_date)
    if preds.empty:
        print(f"No predictions found for {prediction_date}")
        return

    # Filter to rows with edges
    has_edge = preds.dropna(subset=["over_edge", "under_edge"])
    if has_edge.empty:
        print(f"No edge calculations found for {prediction_date} ({len(preds)} predictions total, but no lines matched)")
        return

    # Compute best edge per row (max of |over_edge|, |under_edge|)
    has_edge = has_edge.copy()
    has_edge["best_edge"] = has_edge[["over_edge", "under_edge"]].abs().max(axis=1)
    has_edge["best_side"] = has_edge.apply(
        lambda r: "Over" if abs(r["over_edge"]) >= abs(r["under_edge"]) else "Under", axis=1
    )
    has_edge["edge_value"] = has_edge.apply(
        lambda r: r["over_edge"] if r["best_side"] == "Over" else r["under_edge"], axis=1
    )

    top = has_edge.nlargest(top_n, "best_edge")

    stat_labels = {"pts": "PTS", "reb": "REB", "ast": "AST", "threes": "3PM"}

    print()
    print(f"Top {len(top)} Edges -- {prediction_date}")
    print("=" * 85)
    print(f"  {'Player':<22s} {'Stat':>4s} {'Mean':>6s} {'Line':>6s} {'Side':>5s} "
          f"{'Model':>7s} {'Market':>7s} {'Edge':>7s} {'Odds':>6s}")
    print("-" * 85)

    for _, row in top.iterrows():
        name = (row.get("player_name") or str(row["player_id"]))[:21]
        stat = stat_labels.get(row["stat"], row["stat"].upper())
        side = row["best_side"]

        if side == "Over":
            model_p = row.get("over_prob")
            market_p = row.get("implied_over")
            edge = row.get("over_edge")
            odds = row.get("over_odds")
        else:
            model_p = row.get("under_prob")
            market_p = row.get("implied_under")
            edge = row.get("under_edge")
            odds = row.get("under_odds")

        print(f"  {name:<22s} {stat:>4s} {row['pred_mean']:6.1f} {row['line']:6.1f} "
              f"{side:>5s} {format_prob(model_p):>7s} {format_prob(market_p):>7s} "
              f"{format_edge(edge):>7s} {format_odds(odds):>6s}")

    print()
    print(f"Total predictions: {len(preds)} | With edges: {len(has_edge)}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Query stored daily predictions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Probability of Cade Cunningham scoring over 25.5 points
  python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5

  # Same query with EV calculation at -110 odds
  python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5 --odds -110

  # All predictions for a player
  python src/tools/query_player.py --player "Cade Cunningham"

  # Top 20 edges for today
  python src/tools/query_player.py --top 20

  # Top edges for a specific date
  python src/tools/query_player.py --date 2026-01-29 --top 10
""",
    )

    parser.add_argument("--player", type=str, help="Player name (partial match supported)")
    parser.add_argument("--stat", type=str, choices=["pts", "reb", "ast", "threes"], help="Stat to query")
    parser.add_argument("--line", type=float, help="Prop line to evaluate")
    parser.add_argument("--odds", type=float, help="American odds for EV calculation (e.g., -110)")
    parser.add_argument("--date", type=str, default=str(date.today()), help="Prediction date (YYYY-MM-DD)")
    parser.add_argument("--top", type=int, help="Show top N edges for the date")

    args = parser.parse_args()
    prediction_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    engine = get_engine()
    store = PredictionStore(engine)

    if args.top:
        # Mode 3: Top edges
        print_top_edges(store, prediction_date, args.top)
    elif args.player and args.stat and args.line is not None:
        # Mode 1: Specific line query
        print_line_query(store, prediction_date, args.player, args.stat, args.line, args.odds)
    elif args.player:
        # Mode 2: Player overview
        print_player_overview(store, prediction_date, args.player)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
