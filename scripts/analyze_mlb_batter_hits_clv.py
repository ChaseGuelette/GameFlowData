#!/usr/bin/env python3
"""Phase 1B CLV diagnostics for MLB batter_hits saved backtest bets.

This script joins saved `bets.csv` rows to historical bookmaker odds snapshots,
attempting same-book close first and consensus close as a labeled fallback.
It does not retrain models or change model probabilities.
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, time as datetime_time
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import text

try:
    from scipy.stats import spearmanr
except Exception:  # pragma: no cover
    spearmanr = None

sys.path.append(str(Path(__file__).resolve().parents[1]))

_MIN_PROB = 1e-9
_MAX_PROB = 1.0 - 1e-9

STAT_TO_MARKET_KEY = {
    "batter_hrr": "batter_hits_runs_rbis",
}

EXCLUDED_BOOKMAKERS = (
    "novig",
    "betonlineag",
    "dabble_us_dfs",
    "betr_us_dfs",
    "pick6",
    "prizepicks",
    "underdog",
)


def american_to_implied_prob(odds: float) -> float:
    if pd.isna(odds):
        return float("nan")
    odds = float(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def implied_prob_to_american(prob: float) -> float:
    if pd.isna(prob):
        return float("nan")
    prob = min(max(float(prob), _MIN_PROB), _MAX_PROB)
    if prob < 0.5:
        return (100.0 / prob) - 100.0
    return -100.0 * prob / (1.0 - prob)


def plus_odds_band(odds: float) -> str:
    odds = float(odds)
    if odds <= 99:
        return "-110_to_+99"
    if odds <= 149:
        return "+100_to_+149"
    return "+150_plus"


def normalize_bets(bets: pd.DataFrame) -> pd.DataFrame:
    out = bets.copy().reset_index(drop=True)
    if "bet_id" not in out.columns:
        out.insert(0, "bet_id", np.arange(len(out)))
    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"]).dt.date.astype(str)
    for col in ["player_id", "game_id"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    for col in ["line", "odds", "edge", "model_prob", "implied_prob"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["side"] = out["side"].astype(str).str.lower()
    out["bookmaker"] = out["bookmaker"].astype(str).str.lower()
    out["market_key"] = out["stat"].map(STAT_TO_MARKET_KEY).fillna(out["stat"])
    if "bet_snapshot_time" not in out.columns:
        for candidate in ["selected_snapshot_time", "under_snapshot_time", "over_snapshot_time", "snapshot_time"]:
            if candidate in out.columns:
                out["bet_snapshot_time"] = out[candidate]
                break
    if "bet_snapshot_time" in out.columns:
        out["bet_snapshot_time"] = pd.to_datetime(out["bet_snapshot_time"], utc=True, errors="coerce")
    return out


def apply_assumed_bet_time_et(bets: pd.DataFrame, cutoff_time_et: str | None) -> pd.DataFrame:
    if not cutoff_time_et:
        return bets
    try:
        hour_s, minute_s = cutoff_time_et.split(":", 1)
        cutoff_t = datetime_time(hour=int(hour_s), minute=int(minute_s))
    except Exception as exc:
        raise ValueError(f"Invalid assumed bet time {cutoff_time_et!r}; expected HH:MM") from exc
    out = bets.copy()
    out["bet_snapshot_time"] = [
        datetime.combine(pd.to_datetime(gd).date(), cutoff_t, tzinfo=ZoneInfo("America/New_York"))
        for gd in out["game_date"]
    ]
    out["bet_snapshot_time"] = pd.to_datetime(out["bet_snapshot_time"], utc=True)
    return out


def normalize_snapshots(snapshots: pd.DataFrame) -> pd.DataFrame:
    out = snapshots.copy()
    if out.empty:
        return out
    out["bookmaker"] = out["bookmaker"].astype(str).str.lower()
    out["market_key"] = out["market_key"].replace({v: k for k, v in STAT_TO_MARKET_KEY.items()})
    out["outcome_label"] = out["outcome_label"].astype(str).str.lower()
    for col in ["player_id", "game_id"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    for col in ["line", "odds_american"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["snapshot_time", "commence_time", "game_time_utc"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], utc=True, errors="coerce")
    if "commence_time" not in out.columns and "game_time_utc" in out.columns:
        out["commence_time"] = out["game_time_utc"]
    return out


def _latest_before(df: pd.DataFrame, cutoff) -> pd.Series | None:
    if df.empty:
        return None
    x = df.copy()
    if cutoff is not None and not pd.isna(cutoff):
        x = x[x["snapshot_time"] <= cutoff]
    if x.empty:
        return None
    return x.sort_values("snapshot_time").iloc[-1]


def _snapshot_near_plus15(df: pd.DataFrame, bet_time) -> pd.Series | None:
    if df.empty or pd.isna(bet_time):
        return None
    target = bet_time + pd.Timedelta(minutes=15)
    x = df[(df["snapshot_time"] >= bet_time) & (df["snapshot_time"] <= target)]
    if x.empty:
        return None
    return x.sort_values("snapshot_time").iloc[-1]


def _line_movement_class(side: str, bet_line: float, close_line: float) -> str:
    if pd.isna(close_line) or pd.isna(bet_line):
        return "unmatched"
    if math.isclose(float(bet_line), float(close_line), rel_tol=0.0, abs_tol=1e-9):
        return "same_line_odds_clv"
    if side == "under":
        return "favorable_line_move" if close_line > bet_line else "unfavorable_line_move"
    return "favorable_line_move" if close_line < bet_line else "unfavorable_line_move"


def _base_unmatched_row(bet: pd.Series, reason: str) -> dict:
    row = bet.to_dict()
    row.update(
        {
            "clv_source": "unmatched",
            "unmatched_reason": reason,
            "bookmaker_at_bet": bet.get("bookmaker"),
            "bookmaker_at_close": pd.NA,
            "line_at_bet": bet.get("line"),
            "odds_at_bet": bet.get("odds"),
            "bet_implied_prob": american_to_implied_prob(bet.get("odds")),
            "line_at_close": np.nan,
            "odds_at_close": np.nan,
            "close_implied_prob": np.nan,
            "close_snapshot_time": pd.NaT,
            "open_odds": np.nan,
            "open_snapshot_time": pd.NaT,
            "line_movement_class": "unmatched",
            "same_book_clv_cents": np.nan,
            "clv_implied_prob": np.nan,
            "plus15_odds": np.nan,
            "plus15_snapshot_time": pd.NaT,
            "plus15_clv_implied_prob": np.nan,
            "plus_odds_band": plus_odds_band(bet.get("odds")),
        }
    )
    return row


def _consensus_row(candidates: pd.DataFrame) -> dict | None:
    if candidates.empty:
        return None
    latest = candidates.sort_values("snapshot_time").groupby("bookmaker", as_index=False).tail(1)
    if latest.empty:
        return None
    implied = latest["odds_american"].map(american_to_implied_prob)
    return {
        "bookmaker": "consensus",
        "line": float(latest["line"].mode().iloc[0]) if not latest["line"].mode().empty else float(latest["line"].iloc[0]),
        "odds_american": float(latest["odds_american"].mean()),
        "consensus_implied_prob": float(implied.mean()),
        "snapshot_time": latest["snapshot_time"].max(),
    }


def build_clv_matches(bets: pd.DataFrame, snapshots: pd.DataFrame) -> pd.DataFrame:
    bets_n = normalize_bets(bets)
    snaps = normalize_snapshots(snapshots)
    rows: list[dict] = []

    if snaps.empty:
        return pd.DataFrame([_base_unmatched_row(b, "no_snapshots") for _, b in bets_n.iterrows()])

    for _, bet in bets_n.iterrows():
        side = str(bet["side"]).lower()
        market_key = bet.get("market_key", bet.get("stat"))
        base_mask = (
            (snaps["player_id"] == bet["player_id"])
            & (snaps["game_id"] == bet["game_id"])
            & (snaps["market_key"] == market_key)
            & (snaps["outcome_label"] == side)
        )
        pool = snaps[base_mask]
        if pool.empty:
            rows.append(_base_unmatched_row(bet, "no_player_market_snapshots"))
            continue

        commence = pool["commence_time"].dropna().min() if "commence_time" in pool.columns else pd.NaT
        cutoff = commence if not pd.isna(commence) else None

        same_book = pool[pool["bookmaker"] == bet["bookmaker"]]
        same_book_same_line = same_book[np.isclose(same_book["line"].astype(float), float(bet["line"]))]
        close = _latest_before(same_book_same_line, cutoff)
        clv_source = "same_book_close" if close is not None else None

        # If same book exists but not same line at close, preserve line movement class but do not score odds CLV.
        if close is None:
            same_book_any_line = _latest_before(same_book, cutoff)
            if same_book_any_line is not None:
                close = same_book_any_line
                clv_source = "same_book_close"
            else:
                same_line_pool = pool[np.isclose(pool["line"].astype(float), float(bet["line"]))]
                if cutoff is not None:
                    same_line_pool = same_line_pool[same_line_pool["snapshot_time"] <= cutoff]
                consensus = _consensus_row(same_line_pool)
                if consensus is not None:
                    close = pd.Series(consensus)
                    clv_source = "consensus_close_fallback"

        if close is None:
            rows.append(_base_unmatched_row(bet, "no_close_match"))
            continue

        close_line = float(close["line"])
        close_odds = float(close["odds_american"])
        bet_odds = float(bet["odds"])
        bet_imp = american_to_implied_prob(bet_odds)
        close_imp = float(close.get("consensus_implied_prob", american_to_implied_prob(close_odds)))
        movement = _line_movement_class(side, float(bet["line"]), close_line)
        same_line = movement == "same_line_odds_clv"

        open_row = None
        open_pool = same_book_same_line if not same_book_same_line.empty else pool[np.isclose(pool["line"].astype(float), float(bet["line"]))]
        if not open_pool.empty:
            open_row = open_pool.sort_values("snapshot_time").iloc[0]

        plus15_row = None
        if "bet_snapshot_time" in bets_n.columns and not pd.isna(bet.get("bet_snapshot_time")):
            plus15_pool = same_book_same_line if not same_book_same_line.empty else pd.DataFrame()
            plus15_row = _snapshot_near_plus15(plus15_pool, bet.get("bet_snapshot_time"))

        out = bet.to_dict()
        out.update(
            {
                "clv_source": clv_source,
                "unmatched_reason": "",
                "bookmaker_at_bet": bet.get("bookmaker"),
                "bookmaker_at_close": "consensus" if clv_source == "consensus_close_fallback" else close.get("bookmaker"),
                "line_at_bet": float(bet["line"]),
                "odds_at_bet": bet_odds,
                "bet_implied_prob": bet_imp,
                "line_at_close": close_line,
                "odds_at_close": close_odds,
                "close_implied_prob": close_imp,
                "close_snapshot_time": close.get("snapshot_time"),
                "open_odds": float(open_row["odds_american"]) if open_row is not None else np.nan,
                "open_snapshot_time": open_row["snapshot_time"] if open_row is not None else pd.NaT,
                "line_movement_class": movement,
                "same_book_clv_cents": (bet_odds - close_odds) if same_line and clv_source == "same_book_close" else np.nan,
                "clv_implied_prob": (close_imp - bet_imp) if same_line else np.nan,
                "plus15_odds": float(plus15_row["odds_american"]) if plus15_row is not None else np.nan,
                "plus15_snapshot_time": plus15_row["snapshot_time"] if plus15_row is not None else pd.NaT,
                "plus15_clv_implied_prob": (american_to_implied_prob(float(plus15_row["odds_american"])) - bet_imp) if plus15_row is not None else np.nan,
                "plus_odds_band": plus_odds_band(bet_odds),
            }
        )
        rows.append(out)

    return pd.DataFrame(rows)


def _block_col(df: pd.DataFrame) -> tuple[str, str]:
    if "game_date" in df.columns:
        return "game_date", "block_by_game_date"
    if "game_id" in df.columns:
        return "game_id", "block_by_game_id"
    raise ValueError("Phase 1B decision bootstrap requires game_date or game_id")


def block_bootstrap_ci(df: pd.DataFrame, metric_fn: Callable[[pd.DataFrame], float], n_resamples: int = 1000, ci_level: float = 0.95, seed: int = 17) -> dict:
    clean = df.copy()
    if clean.empty:
        return {"estimate": np.nan, "ci_low": np.nan, "ci_high": np.nan, "n_blocks": 0, "bootstrap_method": "empty"}
    block_col, method = _block_col(clean)
    blocks = list(clean[block_col].dropna().unique())
    if not blocks:
        return {"estimate": np.nan, "ci_low": np.nan, "ci_high": np.nan, "n_blocks": 0, "bootstrap_method": "empty"}
    grouped = {b: clean[clean[block_col] == b] for b in blocks}
    rng = np.random.default_rng(seed)
    vals = []
    for _ in range(n_resamples):
        sampled = rng.choice(blocks, size=len(blocks), replace=True)
        sample = pd.concat([grouped[b] for b in sampled], ignore_index=True)
        vals.append(metric_fn(sample))
    vals = np.asarray(vals, dtype=float)
    vals = vals[np.isfinite(vals)]
    alpha = (1.0 - ci_level) / 2.0
    estimate = metric_fn(clean)
    if len(vals) == 0:
        return {"estimate": estimate, "ci_low": np.nan, "ci_high": np.nan, "n_blocks": len(blocks), "bootstrap_method": method}
    return {
        "estimate": estimate,
        "ci_low": float(np.quantile(vals, alpha)),
        "ci_high": float(np.quantile(vals, 1.0 - alpha)),
        "n_blocks": len(blocks),
        "bootstrap_method": method,
    }


def spearman_edge_clv(df: pd.DataFrame) -> float:
    x = df[["edge", "clv_implied_prob"]].dropna()
    if len(x) < 3 or x["edge"].nunique() < 2 or x["clv_implied_prob"].nunique() < 2:
        return float("nan")
    if spearmanr is None:
        return float(x["edge"].corr(x["clv_implied_prob"], method="spearman"))
    return float(spearmanr(x["edge"], x["clv_implied_prob"]).statistic)


def summarize_group(df: pd.DataFrame, group: str, n_resamples: int, ci_level: float) -> dict:
    scored = df[df["clv_implied_prob"].notna()].copy()
    mean_ci = block_bootstrap_ci(scored, lambda x: float(x["clv_implied_prob"].mean()), n_resamples=n_resamples, ci_level=ci_level)
    corr_ci = block_bootstrap_ci(scored, spearman_edge_clv, n_resamples=n_resamples, ci_level=ci_level)
    return {
        "group": group,
        "n": int(len(df)),
        "n_scored": int(len(scored)),
        "n_same_book": int((df["clv_source"] == "same_book_close").sum()) if "clv_source" in df.columns else 0,
        "n_consensus_fallback": int((df["clv_source"] == "consensus_close_fallback").sum()) if "clv_source" in df.columns else 0,
        "n_unmatched": int((df["clv_source"] == "unmatched").sum()) if "clv_source" in df.columns else 0,
        "mean_clv_implied_prob": mean_ci["estimate"],
        "mean_clv_ci_low": mean_ci["ci_low"],
        "mean_clv_ci_high": mean_ci["ci_high"],
        "edge_clv_spearman": corr_ci["estimate"],
        "edge_clv_ci_low": corr_ci["ci_low"],
        "edge_clv_ci_high": corr_ci["ci_high"],
        "n_blocks": mean_ci["n_blocks"],
        "bootstrap_method": mean_ci["bootstrap_method"],
    }


def summarize_by(df: pd.DataFrame, column: str, n_resamples: int, ci_level: float) -> pd.DataFrame:
    rows = []
    for key, group_df in df.groupby(column, dropna=False):
        rows.append(summarize_group(group_df, str(key), n_resamples, ci_level))
    return pd.DataFrame(rows)


def decide_phase1b(mean_clv_ci_low: float, mean_clv: float, edge_corr_ci_low: float, edge_corr: float, failing_bands: list[str]) -> dict:
    mean_confirmed = pd.notna(mean_clv) and pd.notna(mean_clv_ci_low) and mean_clv > 0 and mean_clv_ci_low > 0
    corr_confirmed = pd.notna(edge_corr) and pd.notna(edge_corr_ci_low) and edge_corr > 0 and edge_corr_ci_low > 0
    if failing_bands:
        return {
            "decision": "restrict_plus_odds_band",
            "phase2_allowed": False,
            "reason": f"Aggregate CLV may be positive, but {', '.join(failing_bands)} has negative CLV with CI excluding zero.",
            "next_step": "Rerun/review under-only benchmark excluding the failing plus-odds band before feature work.",
        }
    if mean_confirmed and corr_confirmed:
        return {
            "decision": "phase2_allowed",
            "phase2_allowed": True,
            "reason": "Mean bet-to-close CLV and edge-to-CLV monotonicity both pass block-bootstrap gates.",
            "next_step": "Proceed to Phase 2 feature-family selection from Phase 1 residual diagnostics.",
        }
    if mean_confirmed and not corr_confirmed:
        return {
            "decision": "ranking_quality_unconfirmed",
            "phase2_allowed": False,
            "reason": "Mean CLV is positive, but predicted edge does not have confirmed positive rank correlation with CLV.",
            "next_step": "Review edge thresholding / BL selection policy and require another confirmation window.",
        }
    return {
        "decision": "stop_feature_expansion",
        "phase2_allowed": False,
        "reason": "Mean CLV and/or edge-to-CLV correlation is non-positive or statistically inconclusive.",
        "next_step": "Treat ROI as unconfirmed variance/market-selection artifact; collect +200 under-only bets or +30 calendar days, then rerun Phase 1B.",
    }


def fetch_snapshots_for_bets(bets: pd.DataFrame, local: bool = False, batch_size: int = 50) -> pd.DataFrame:
    from src.db.client import get_engine

    bets_n = normalize_bets(bets)
    game_ids = [int(x) for x in sorted(bets_n["game_id"].dropna().unique())]
    player_ids = [int(x) for x in sorted(bets_n["player_id"].dropna().unique())]
    market_keys = sorted(bets_n["market_key"].dropna().unique())
    if not game_ids or not player_ids or not market_keys:
        return pd.DataFrame()

    engine = get_engine(local=local)
    rows = []
    excluded = tuple(EXCLUDED_BOOKMAKERS)
    for i in range(0, len(game_ids), batch_size):
        gid_batch = game_ids[i : i + batch_size]
        params = {f"gid_{j}": gid for j, gid in enumerate(gid_batch)}
        params.update({f"pid_{j}": pid for j, pid in enumerate(player_ids)})
        params.update({f"mk_{j}": mk for j, mk in enumerate(market_keys)})
        params.update({f"ex_{j}": ex for j, ex in enumerate(excluded)})
        gid_ph = ", ".join(f":gid_{j}" for j in range(len(gid_batch)))
        pid_ph = ", ".join(f":pid_{j}" for j in range(len(player_ids)))
        mk_ph = ", ".join(f":mk_{j}" for j in range(len(market_keys)))
        ex_ph = ", ".join(f":ex_{j}" for j in range(len(excluded)))
        sql = text(f"""
            SELECT
                p.player_id,
                p.game_id,
                p.bookmaker,
                p.market_key,
                p.line,
                p.outcome_label,
                p.odds_american,
                p.snapshot_time,
                p.commence_time,
                s.game_time_utc
            FROM mlb_raw_player_props p
            LEFT JOIN mlb_game_schedule s ON s.game_id = p.game_id
            WHERE p.game_id IN ({gid_ph})
              AND p.player_id IN ({pid_ph})
              AND p.market_key IN ({mk_ph})
              AND p.bookmaker NOT IN ({ex_ph})
              AND p.player_id IS NOT NULL
              AND p.snapshot_time IS NOT NULL
        """)
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '180000'"))
            part = pd.read_sql(sql, conn, params=params)
        rows.append(part)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def write_markdown_summary(path: Path, summary: pd.DataFrame, band_summary: pd.DataFrame, decision: dict, plus15_available: bool) -> None:
    overall = summary.iloc[0].to_dict() if not summary.empty else {}
    lines = [
        "# MLB Batter Hits Phase 1B CLV Summary",
        "",
        f"Decision: **{decision['decision']}**",
        f"Phase 2 allowed: **{decision['phase2_allowed']}**",
        "",
        f"Reason: {decision['reason']}",
        f"Next step: {decision['next_step']}",
        "",
        "## Overall CLV gates",
        "",
        f"- Bets: {overall.get('n', 0)}; scored CLV bets: {overall.get('n_scored', 0)}",
        f"- Same-book matches: {overall.get('n_same_book', 0)}; consensus fallbacks: {overall.get('n_consensus_fallback', 0)}; unmatched: {overall.get('n_unmatched', 0)}",
        f"- Mean implied-prob CLV: {overall.get('mean_clv_implied_prob', np.nan):+.6f} [{overall.get('mean_clv_ci_low', np.nan):+.6f}, {overall.get('mean_clv_ci_high', np.nan):+.6f}]",
        f"- Spearman(edge, CLV): {overall.get('edge_clv_spearman', np.nan):+.6f} [{overall.get('edge_clv_ci_low', np.nan):+.6f}, {overall.get('edge_clv_ci_high', np.nan):+.6f}]",
        f"- Bootstrap: {overall.get('bootstrap_method', 'unknown')}; n_blocks={overall.get('n_blocks', 0)}",
        "",
        "## Plus-odds bands",
        "",
    ]
    if band_summary.empty:
        lines.append("No plus-odds band summary available.")
    else:
        cols = ["group", "n", "n_scored", "mean_clv_implied_prob", "mean_clv_ci_low", "mean_clv_ci_high", "edge_clv_spearman"]
        available_cols = [c for c in cols if c in band_summary.columns]
        lines.append("```text")
        lines.append(band_summary[available_cols].to_string(index=False))
        lines.append("```")
    lines.extend(
        [
            "",
            "## Timing stability",
            "",
            "- +15 minute CLV was computed." if plus15_available else "- +15 minute CLV was not decision-grade because saved bets lacked bet/selected snapshot timestamps or no +15 snapshots were matchable.",
            "",
            "## Relevant prior lessons/invariants",
            "",
            "- No model retraining or global recalibration was performed.",
            "- Feature expansion remains blocked unless CLV gates pass.",
            "- Saved legacy-line sweeps remain hypothesis-generating until quote timing / CLV validates them.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> dict:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    bets = pd.read_csv(args.bets_csv)
    bets = normalize_bets(bets)
    bets = apply_assumed_bet_time_et(bets, args.assume_bet_time_et)

    if args.snapshots_csv:
        snapshots = pd.read_csv(args.snapshots_csv)
    else:
        snapshots = fetch_snapshots_for_bets(bets, local=args.local, batch_size=args.batch_size)
        snapshots.to_csv(output_dir / "raw_snapshots_used.csv", index=False)

    matches = build_clv_matches(bets, snapshots)
    matches.to_csv(output_dir / "clv_matches.csv", index=False)

    scored = matches[matches["clv_implied_prob"].notna()].copy()
    summary = pd.DataFrame([summarize_group(matches, "overall", args.bootstrap_samples, args.ci_level)])
    summary.to_csv(output_dir / "clv_summary.csv", index=False)

    band_summary = summarize_by(matches, "plus_odds_band", args.bootstrap_samples, args.ci_level)
    band_summary.to_csv(output_dir / "clv_by_plus_odds_band.csv", index=False)

    edge_df = matches.copy()
    edge_df["edge_bin"] = pd.cut(edge_df["edge"], bins=[0, 0.15, 0.18, 0.22, np.inf], labels=["<0.15", "0.15-0.18", "0.18-0.22", "0.22+"])
    summarize_by(edge_df, "edge_bin", args.bootstrap_samples, args.ci_level).to_csv(output_dir / "clv_by_edge_bin.csv", index=False)
    summarize_by(matches, "bookmaker_at_bet", args.bootstrap_samples, args.ci_level).to_csv(output_dir / "clv_by_bookmaker.csv", index=False)

    timing = matches[["bet_id", "game_date", "player_id", "game_id", "bookmaker_at_bet", "line_at_bet", "odds_at_bet", "plus15_odds", "plus15_snapshot_time", "plus15_clv_implied_prob", "clv_implied_prob"]].copy()
    timing.to_csv(output_dir / "clv_timing_stability.csv", index=False)

    failing_bands = []
    for _, row in band_summary.iterrows():
        if row["group"] in {"-110_to_+99", "+100_to_+149", "+150_plus"}:
            if pd.notna(row.get("mean_clv_ci_high")) and row.get("mean_clv_ci_high") < 0:
                failing_bands.append(row["group"])

    overall = summary.iloc[0]
    decision = decide_phase1b(
        mean_clv_ci_low=overall.get("mean_clv_ci_low", np.nan),
        mean_clv=overall.get("mean_clv_implied_prob", np.nan),
        edge_corr_ci_low=overall.get("edge_clv_ci_low", np.nan),
        edge_corr=overall.get("edge_clv_spearman", np.nan),
        failing_bands=failing_bands,
    )
    pd.DataFrame([decision]).to_csv(output_dir / "phase1b_decision.csv", index=False)
    write_markdown_summary(
        output_dir / "phase1b_clv_summary.md",
        summary,
        band_summary,
        decision,
        plus15_available=bool(matches["plus15_clv_implied_prob"].notna().any()),
    )
    return {"output_dir": str(output_dir), "decision": decision}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 1B CLV diagnostics for MLB batter_hits saved bets")
    parser.add_argument("--bets-csv", required=True, help="Saved bets.csv for the benchmark config")
    parser.add_argument("--snapshots-csv", default=None, help="Optional pre-fetched raw snapshots CSV for tests/offline runs")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--local", action="store_true", help="Use LOCAL_DATABASE_URL via project get_engine(local=True)")
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--ci-level", type=float, default=0.95)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--assume-bet-time-et", default=None, help="Optional HH:MM ET bet-time assumption for quote-clean replay, e.g. 13:30")
    return parser.parse_args()


if __name__ == "__main__":
    result = run(parse_args())
    print(f"Wrote Phase 1B CLV diagnostics to {result['output_dir']}")
    print(f"Decision: {result['decision']['decision']}")
