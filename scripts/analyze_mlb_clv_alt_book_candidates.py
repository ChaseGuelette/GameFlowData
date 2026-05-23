#!/usr/bin/env python3
"""Compare selected MLB bet book/edge to alternative bookmaker candidates.

This answers the practical question: if the quote-clean policy selected ESPNBet
or ProphetX because they showed the largest edge, would the same bet still clear
the edge threshold at sharper/reference books such as DraftKings, FanDuel, BetMGM,
Caesars/Betrivers, etc.?

Inputs are existing CLV audit artifacts, so this script does not query the DB,
retrain models, or rerun a sweep. It reads:
- clv_matches.csv: placed bets with selected book, selected odds, model_prob, edge
- raw_snapshots_used.csv: all candidate bookmaker odds fetched for CLV analysis

For each placed bet, it reconstructs same-player/game/stat/line candidate UNDER
prices available at or before the bet decision timestamp, then reports whether a
preferred/sharper book candidate existed and whether the edge survived there.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import pandas as pd

DEFAULT_ALT_BOOKS = (
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "betrivers",
    "fanatics",
    "hardrockbet",
    "hardrockbet_oh",
)


def american_to_implied_prob(odds: float) -> float:
    if pd.isna(odds):
        return float("nan")
    odds = float(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def clean_book(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower().replace(" ", "")


def parse_edge_threshold(label: str, fallback: float | None = None) -> float | None:
    match = re.search(r"edge([0-9]+(?:\.[0-9]+)?)", label)
    if match:
        return float(match.group(1))
    return fallback


def fmt_pct(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.1%}"


def fmt_float(value, digits: int = 4) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):+.{digits}f}"


def odds_gap_bucket(gap: float | None) -> str:
    if gap is None or pd.isna(gap):
        return "missing_alt"
    gap = abs(float(gap))
    if gap <= 5:
        return "within_5_cents"
    if gap <= 10:
        return "within_10_cents"
    if gap <= 20:
        return "within_20_cents"
    return "worse_by_20_plus_cents"


def load_bets(path: Path) -> pd.DataFrame:
    bets = pd.read_csv(path)
    required = ["bet_id", "player_id", "game_id", "market_key", "line_at_bet", "odds_at_bet", "model_prob", "edge", "bet_snapshot_time"]
    missing = [c for c in required if c not in bets.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    bets = bets.copy()
    for col in ["bet_id", "player_id", "game_id"]:
        bets[col] = pd.to_numeric(bets[col], errors="coerce").astype("Int64")
    for col in ["line_at_bet", "odds_at_bet", "model_prob", "edge"]:
        bets[col] = pd.to_numeric(bets[col], errors="coerce")
    bets["bet_snapshot_time"] = pd.to_datetime(bets["bet_snapshot_time"], utc=True, errors="coerce")
    bets["selected_book_key"] = bets.get("bookmaker_at_bet", bets.get("bookmaker", "")).map(clean_book)
    return bets


def load_candidate_quotes(path: Path, bets: pd.DataFrame) -> pd.DataFrame:
    usecols = [
        "player_id",
        "game_id",
        "bookmaker",
        "market_key",
        "line",
        "outcome_label",
        "odds_american",
        "snapshot_time",
    ]
    raw = pd.read_csv(path, usecols=usecols)
    raw = raw[raw["outcome_label"].astype(str).str.lower().isin({"over", "under"})].copy()
    raw["bookmaker_key"] = raw["bookmaker"].map(clean_book)
    raw["outcome_key"] = raw["outcome_label"].astype(str).str.lower()
    raw["snapshot_time"] = pd.to_datetime(raw["snapshot_time"], utc=True, errors="coerce")
    for col in ["player_id", "game_id"]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce").astype("Int64")
    raw["line"] = pd.to_numeric(raw["line"], errors="coerce")
    raw["odds_american"] = pd.to_numeric(raw["odds_american"], errors="coerce")

    keys = bets[["player_id", "game_id", "market_key", "line_at_bet"]].drop_duplicates().rename(columns={"line_at_bet": "line"})
    raw = raw.merge(keys, on=["player_id", "game_id", "market_key", "line"], how="inner")
    return raw


def build_candidate_rows(
    bets: pd.DataFrame,
    raw: pd.DataFrame,
    *,
    alt_books: tuple[str, ...],
    edge_threshold: float | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    alt_set = set(alt_books)
    rows: list[dict] = []
    all_candidate_rows: list[dict] = []

    raw_groups = {
        key: group.sort_values("snapshot_time")
        for key, group in raw.groupby(["player_id", "game_id", "market_key", "line"], dropna=False)
    }

    for bet in bets.itertuples(index=False):
        key = (bet.player_id, bet.game_id, bet.market_key, bet.line_at_bet)
        candidates = raw_groups.get(key)
        base = {
            "bet_id": int(bet.bet_id),
            "player_id": int(bet.player_id),
            "game_id": int(bet.game_id),
            "market_key": bet.market_key,
            "line": float(bet.line_at_bet),
            "selected_book": bet.selected_book_key,
            "selected_odds": float(bet.odds_at_bet),
            "selected_edge": float(bet.edge),
            "model_prob": float(bet.model_prob),
            "bet_snapshot_time": bet.bet_snapshot_time,
            "edge_threshold": edge_threshold,
        }
        if candidates is None or candidates.empty or pd.isna(bet.bet_snapshot_time):
            rows.append({**base, "alt_available": False, "reason": "no_candidate_rows"})
            continue

        available = candidates[candidates["snapshot_time"] <= bet.bet_snapshot_time].copy()
        if available.empty:
            rows.append({**base, "alt_available": False, "reason": "no_pre_bet_candidates"})
            continue

        latest_long = (
            available.sort_values("snapshot_time")
            .groupby(["bookmaker_key", "outcome_key"], as_index=False, dropna=False)
            .tail(1)
            .copy()
        )
        latest = (
            latest_long.pivot_table(
                index="bookmaker_key",
                columns="outcome_key",
                values=["odds_american", "snapshot_time"],
                aggfunc="last",
            )
            .reset_index()
        )
        latest.columns = ["_".join([str(x) for x in col if str(x)]) if isinstance(col, tuple) else str(col) for col in latest.columns]
        rename_map = {
            "bookmaker_key": "bookmaker_key",
            "odds_american_over": "over_odds",
            "odds_american_under": "under_odds",
            "snapshot_time_over": "over_snapshot_time",
            "snapshot_time_under": "under_snapshot_time",
        }
        latest = latest.rename(columns=rename_map)
        if "over_odds" not in latest.columns or "under_odds" not in latest.columns:
            rows.append({**base, "alt_available": False, "reason": "no_paired_candidate_quotes"})
            continue
        latest = latest[latest["over_odds"].notna() & latest["under_odds"].notna()].copy()
        if latest.empty:
            rows.append({**base, "alt_available": False, "reason": "no_paired_candidate_quotes"})
            continue
        latest["over_implied_prob"] = latest["over_odds"].map(american_to_implied_prob)
        latest["under_implied_prob_raw"] = latest["under_odds"].map(american_to_implied_prob)
        latest["alt_implied_prob"] = latest["under_implied_prob_raw"] / (latest["under_implied_prob_raw"] + latest["over_implied_prob"])
        latest["alt_edge"] = float(bet.model_prob) - latest["alt_implied_prob"]
        latest["alt_is_preferred"] = latest["bookmaker_key"].isin(alt_set)
        if edge_threshold is not None:
            latest["alt_edge_survives"] = latest["alt_edge"] >= edge_threshold
        else:
            latest["alt_edge_survives"] = pd.NA

        for c in latest.itertuples(index=False):
            all_candidate_rows.append(
                {
                    **base,
                    "candidate_book": c.bookmaker_key,
                    "candidate_odds": c.under_odds,
                    "candidate_over_odds": c.over_odds,
                    "candidate_snapshot_time": max(c.under_snapshot_time, c.over_snapshot_time),
                    "candidate_edge": c.alt_edge,
                    "candidate_is_preferred": bool(c.alt_is_preferred),
                    "candidate_edge_survives": None if edge_threshold is None else bool(c.alt_edge_survives),
                    "odds_gap_vs_selected": float(bet.odds_at_bet) - float(c.under_odds),
                    "edge_gap_vs_selected": float(bet.edge) - float(c.alt_edge),
                }
            )

        preferred = latest[latest["bookmaker_key"].isin(alt_set)].copy()
        if preferred.empty:
            rows.append({**base, "alt_available": False, "reason": "no_preferred_book_candidate"})
            continue

        # Pick the best available preferred book by edge/price. This models:
        # "What if we refused ESPNBet/ProphetX and took the best sharper-book quote?"
        best = preferred.sort_values(["alt_edge", "under_odds"], ascending=False).iloc[0]
        odds_gap = float(bet.odds_at_bet) - float(best["under_odds"])
        edge_gap = float(bet.edge) - float(best["alt_edge"])
        rows.append(
            {
                **base,
                "alt_available": True,
                "reason": "preferred_book_available",
                "alt_book": best["bookmaker_key"],
                "alt_odds": float(best["under_odds"]),
                "alt_over_odds": float(best["over_odds"]),
                "alt_snapshot_time": max(best["under_snapshot_time"], best["over_snapshot_time"]),
                "alt_edge": float(best["alt_edge"]),
                "alt_edge_survives": None if edge_threshold is None else bool(best["alt_edge"] >= edge_threshold),
                "alt_edge_within_005": bool(float(best["alt_edge"]) >= float(bet.edge) - 0.005),
                "alt_edge_within_010": bool(float(best["alt_edge"]) >= float(bet.edge) - 0.010),
                "odds_gap_vs_selected": odds_gap,
                "edge_gap_vs_selected": edge_gap,
                "odds_gap_bucket": odds_gap_bucket(odds_gap),
            }
        )

    return pd.DataFrame(rows), pd.DataFrame(all_candidate_rows)


def summarize(rows: pd.DataFrame) -> dict:
    total = len(rows)
    available = rows[rows["alt_available"] == True].copy()  # noqa: E712
    selected_bad_books = rows[rows["selected_book"].isin({"espnbet", "prophetx"})]
    selected_bad_with_alt = selected_bad_books[selected_bad_books["alt_available"]]
    out = {
        "total_bets": total,
        "preferred_alt_available": int(len(available)),
        "preferred_alt_available_share": len(available) / total if total else None,
        "selected_espnbet_or_prophetx": int(len(selected_bad_books)),
        "selected_espnbet_or_prophetx_share": len(selected_bad_books) / total if total else None,
        "selected_espnbet_or_prophetx_with_alt": int(len(selected_bad_with_alt)),
        "selected_espnbet_or_prophetx_with_alt_share": len(selected_bad_with_alt) / len(selected_bad_books) if len(selected_bad_books) else None,
    }
    if not available.empty:
        out.update(
            {
                "alt_edge_survives": int(available["alt_edge_survives"].fillna(False).sum()) if "alt_edge_survives" in available else None,
                "alt_edge_survives_share": float(available["alt_edge_survives"].fillna(False).mean()) if "alt_edge_survives" in available else None,
                "alt_edge_within_005_share": float(available["alt_edge_within_005"].mean()),
                "alt_edge_within_010_share": float(available["alt_edge_within_010"].mean()),
                "median_odds_gap_vs_selected": float(available["odds_gap_vs_selected"].median()),
                "mean_odds_gap_vs_selected": float(available["odds_gap_vs_selected"].mean()),
                "median_edge_gap_vs_selected": float(available["edge_gap_vs_selected"].median()),
                "mean_edge_gap_vs_selected": float(available["edge_gap_vs_selected"].mean()),
            }
        )
    return out


def write_markdown(path: Path, config_label: str, summary: dict, rows: pd.DataFrame, alt_books: tuple[str, ...]) -> None:
    lines = [
        f"# MLB Alternative Book Candidate Report — {config_label}",
        "",
        "This report checks whether selected ESPNBet/ProphetX-style edges survive if the bet is forced onto preferred/reference books.",
        "",
        f"Preferred books: {', '.join(alt_books)}",
        "",
        "## Summary",
        "",
    ]
    for key, value in summary.items():
        if key.endswith("share"):
            value_s = fmt_pct(value)
        elif isinstance(value, float):
            value_s = fmt_float(value, 4)
        else:
            value_s = str(value)
        lines.append(f"- {key}: {value_s}")

    if not rows.empty and "odds_gap_bucket" in rows.columns:
        lines.extend(["", "## Odds gap buckets for available preferred-book alternatives", ""])
        bucket = rows[rows["alt_available"] == True]["odds_gap_bucket"].value_counts().rename_axis("bucket").reset_index(name="count")  # noqa: E712
        total = int(bucket["count"].sum()) if not bucket.empty else 0
        lines.extend(["| Bucket | Count | Share |", "|---|---:|---:|"])
        for b in bucket.itertuples(index=False):
            lines.append(f"| {b.bucket} | {int(b.count)} | {fmt_pct(int(b.count) / total if total else None)} |")

    if not rows.empty:
        focus = rows[(rows["selected_book"].isin({"espnbet", "prophetx"})) & (rows["alt_available"] == True)].copy()  # noqa: E712
        if not focus.empty:
            lines.extend(["", "## Selected ESPNBet/ProphetX bets with preferred alternatives", ""])
            lines.extend(["| Selected | Alt | N | Alt edge survives | Median odds gap | Median edge gap |", "|---|---|---:|---:|---:|---:|"])
            grouped = focus.groupby(["selected_book", "alt_book"], dropna=False)
            for (sel, alt), g in grouped:
                survives = g["alt_edge_survives"].fillna(False).mean() if "alt_edge_survives" in g else float("nan")
                lines.append(
                    f"| {sel} | {alt} | {len(g)} | {fmt_pct(survives)} | {float(g['odds_gap_vs_selected'].median()):+.1f} | {float(g['edge_gap_vs_selected'].median()):+.4f} |"
                )

    lines.extend(
        [
            "",
            "## Interpretation notes",
            "",
            "- `odds_gap_vs_selected = selected_odds - alt_odds`; +5 means selected book was five cents better than the preferred alternative.",
            "- `alt_edge_survives` means the preferred alternative still clears the config edge threshold.",
            "- This is a quote-policy diagnostic, not a new backtest result. If promising, rerun the sweep with a predeclared preferred-book policy.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict:
    config_dir = Path(args.clv_config_dir)
    config_label = args.config_label or config_dir.name
    bets_path = config_dir / "clv_matches.csv"
    raw_path = config_dir / "raw_snapshots_used.csv"
    if not bets_path.exists():
        raise FileNotFoundError(bets_path)
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)

    threshold = args.edge_threshold if args.edge_threshold is not None else parse_edge_threshold(config_label)
    alt_books = tuple(clean_book(b) for b in args.alt_books)
    bets = load_bets(bets_path)
    raw = load_candidate_quotes(raw_path, bets)
    rows, candidate_rows = build_candidate_rows(bets, raw, alt_books=alt_books, edge_threshold=threshold)
    summary = summarize(rows)

    output_dir = Path(args.output_dir) if args.output_dir else config_dir / "alt_book_candidates"
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = output_dir / "alt_book_candidate_summary.csv"
    candidates_path = output_dir / "all_candidate_book_edges.csv"
    summary_path = output_dir / "alt_book_candidate_report.md"
    jsonish_path = output_dir / "summary_key_values.csv"
    rows.to_csv(rows_path, index=False)
    candidate_rows.to_csv(candidates_path, index=False)
    with jsonish_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        for key, value in summary.items():
            writer.writerow([key, value])
    write_markdown(summary_path, config_label, summary, rows, alt_books)
    return {"rows": len(rows), "output_dir": str(output_dir), "summary": summary}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare selected MLB CLV bets to alternative preferred bookmaker candidate lines")
    parser.add_argument("--clv-config-dir", required=True, help="Path to clv/<config> dir containing clv_matches.csv and raw_snapshots_used.csv")
    parser.add_argument("--config-label", default=None)
    parser.add_argument("--edge-threshold", type=float, default=None, help="Override edge threshold; otherwise parsed from config label like edge0.15")
    parser.add_argument("--alt-books", nargs="+", default=list(DEFAULT_ALT_BOOKS), help="Preferred/reference books to test")
    parser.add_argument("--output-dir", default=None)
    return parser.parse_args()


if __name__ == "__main__":
    result = run(parse_args())
    print(f"Wrote alt-book candidate report for {result['rows']} bets")
    print(result["output_dir"])
    for key, value in result["summary"].items():
        print(f"{key}: {value}")
