"""Diagnose MLB Pitcher K Phase 3A regression vs Phase 2.

This script is intentionally read-only. It consumes saved sweep outputs, optionally
regenerates quantiles/feature values through the existing model + feature-store path,
and writes reviewable analysis artifacts under docs/analysis.
"""
from __future__ import annotations

import json
import math
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO))

PHASE2_ARTIFACT = REPO / "src/models/mlb/artifacts/mlb_run_20260513_111207"
PHASE3A_TUNED_ARTIFACT = REPO / "src/models/mlb/artifacts/mlb_run_20260513_160159"
PHASE2_BL_SWEEP = REPO / "backtest_results/mlb_sweep_pitcher_k_phase2_bl_under_focused_20260413_20260510"
PHASE2_RAW_SWEEP = REPO / "backtest_results/mlb_sweep_pitcher_k_phase2_raw_under_20260413_20260510"
PHASE3A_TUNED_UNDER_SWEEP = REPO / "backtest_results/mlb_sweep_20260513_161315"
PHASE3A_TUNED_BOTH_SWEEP = REPO / "backtest_results/mlb_sweep_20260513_161322"
PHASE3A_RAW_SWEEP = REPO / "backtest_results/mlb_sweep_pitcher_k_phase3a_raw_under_20260413_20260510"
PHASE3A_UNTUNED_BL_SWEEP = REPO / "backtest_results/mlb_sweep_pitcher_k_phase3a_bl_under_20260413_20260510"

OUT = REPO / "docs/analysis/mlb_phase3a_agreement_20260513"
PLOTS = OUT / "plots"

TARGET_BL = {"tau": 0.90, "z_max": 0.25, "max_weight": 0.80, "edge_threshold": 0.02}
RAW_EDGE = {"tau": None, "z_max": 0.25, "max_weight": 0.50, "edge_threshold": 0.05}
KEY = ["game_date", "player_id", "game_id", "stat", "line", "bookmaker"]
LINEUP_FEATURES = [
    "projected_lineup_k_pct",
    "opp_team_k_pct_l10",
    "projected_lineup_whiff_pct",
    "opp_team_whiff_pct_l10",
    "projected_lineup_contact_rate",
    "opp_team_contact_rate_l10",
    "projected_lineup_chase_pct",
    "opp_team_chase_pct_l10",
    "projected_lineup_top3_k_pct",
    "projected_lineup_mid3_k_pct",
    "projected_lineup_bot3_k_pct",
    "projected_lineup_k_concentration",
    "pct_opp_lineup_same_hand",
    "projected_lineup_hand_k_delta",
]


def _float_eq(a: Any, b: Any, tol: float = 1e-9) -> bool:
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None or (isinstance(b, float) and math.isnan(b))
    if b is None or (isinstance(b, float) and math.isnan(b)):
        return False
    return abs(float(a) - float(b)) <= tol


def load_metrics(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    flat = {}
    if "betting" in data:
        flat.update(data["betting"])
    if "risk" in data:
        flat.update(data["risk"])
    if "config" in data:
        flat["config"] = data["config"]
    return flat


def find_config_dir(sweep: Path, *, tau: float | None, z_max: float, max_weight: float, edge_threshold: float) -> Path:
    matches = []
    for d in sorted(sweep.iterdir()):
        if not d.is_dir() or not d.name.startswith("config_"):
            continue
        mf = d / "metrics.json"
        if not mf.exists():
            continue
        raw = json.loads(mf.read_text())
        cfg = raw.get("config", {})
        if (
            _float_eq(cfg.get("tau"), tau)
            and _float_eq(cfg.get("z_max", 1.0), z_max)
            and _float_eq(cfg.get("max_weight", 0.50), max_weight)
            and _float_eq(cfg.get("edge_threshold"), edge_threshold)
        ):
            matches.append(d)
    if not matches:
        raise FileNotFoundError(f"No config in {sweep} for tau={tau} z={z_max} mw={max_weight} edge={edge_threshold}")
    return matches[0]


def read_config(sweep: Path, cfg: dict[str, Any]) -> tuple[Path, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    d = find_config_dir(sweep, **cfg)
    bets = pd.read_csv(d / "bets.csv") if (d / "bets.csv").exists() else pd.DataFrame()
    preds = pd.read_csv(d / "predictions.csv") if (d / "predictions.csv").exists() else pd.DataFrame()
    metrics = load_metrics(d / "metrics.json")
    for df in [bets, preds]:
        if not df.empty and "game_date" in df.columns:
            df["game_date"] = pd.to_datetime(df["game_date"]).dt.strftime("%Y-%m-%d")
    return d, bets, preds, metrics


def summarize_metrics(label: str, metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "label": label,
        "total_bets": int(metrics.get("total_bets", 0)),
        "wins": int(metrics.get("wins", 0)),
        "losses": int(metrics.get("losses", 0)),
        "pushes": int(metrics.get("pushes", 0)),
        "hit_rate": float(metrics.get("hit_rate", 0.0)),
        "roi": float(metrics.get("roi", 0.0)),
        "total_profit": float(metrics.get("total_profit", 0.0)),
        "total_staked": float(metrics.get("total_staked", 0.0)),
        "sharpe_ratio": float(metrics.get("sharpe_ratio", 0.0)),
        "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
    }


def merge_primary(p2_bets: pd.DataFrame, p2_preds: pd.DataFrame, p3_bets: pd.DataFrame, p3_preds: pd.DataFrame) -> pd.DataFrame:
    p2b = p2_bets.copy()
    p2b = p2b[p2b["side"] == "under"].copy()
    p2p_cols = KEY + [c for c in ["under_prob", "under_edge", "over_prob", "over_edge", "implied_under", "implied_over", "market_under", "market_over", "model_over", "z_raw", "model_logit", "market_logit"] if c in p2_preds.columns]
    p3p_cols = KEY + [c for c in ["under_prob", "under_edge", "over_prob", "over_edge", "implied_under", "implied_over", "market_under", "market_over", "model_over", "z_raw", "model_logit", "market_logit"] if c in p3_preds.columns]
    p2p = p2_preds[p2p_cols].drop_duplicates(KEY).add_prefix("p2_")
    p3p = p3_preds[p3p_cols].drop_duplicates(KEY).add_prefix("p3_")
    p2p = p2p.rename(columns={f"p2_{k}": k for k in KEY})
    p3p = p3p.rename(columns={f"p3_{k}": k for k in KEY})

    merged = p2b.merge(p2p, on=KEY, how="left").merge(p3p, on=KEY, how="left")

    p3_key = p3_bets[p3_bets["side"] == "under"][KEY].drop_duplicates().assign(p3_under_bet=True)
    p3_any = p3_bets[KEY + ["side", "edge", "model_prob", "profit", "outcome"]].copy()
    p3_any = p3_any.sort_values(KEY + ["edge"], ascending=[True]*len(KEY) + [False]).drop_duplicates(KEY)
    p3_any = p3_any.rename(columns={"side": "p3_bet_side", "edge": "p3_bet_edge", "model_prob": "p3_bet_model_prob", "profit": "p3_bet_profit", "outcome": "p3_bet_outcome"})
    merged = merged.merge(p3_key, on=KEY, how="left").merge(p3_any, on=KEY, how="left")
    merged["p3_under_bet"] = merged["p3_under_bet"].fillna(False)

    merged["edge_drop"] = merged["edge"] - merged["p3_under_edge"]
    merged["edge_ratio"] = merged["p3_under_edge"] / merged["edge"].replace(0, np.nan)
    threshold = TARGET_BL["edge_threshold"]

    def bucket(r):
        if pd.isna(r.get("p3_under_edge")):
            return "missing_phase3a_prediction"
        if r.get("p3_over_edge", -999) >= threshold and r.get("p3_over_edge", -999) > r.get("p3_under_edge", -999):
            return "flipped_or_direction_invalidated"
        if r["p3_under_edge"] >= threshold:
            if (r["edge_drop"] < 0.02) or (r["edge_ratio"] >= 0.80):
                return "same_side_similar_edge"
            return "same_side_lower_edge_still_cleared"
        return "same_side_edge_dropped_below_threshold"

    merged["bucket"] = merged.apply(bucket, axis=1)
    merged["p2_win"] = merged["outcome"].eq("win")
    return merged


def summarize_buckets(df: pd.DataFrame, group_col: str = "bucket") -> pd.DataFrame:
    rows = []
    for name, g in df.groupby(group_col, dropna=False):
        rows.append({
            group_col: name,
            "count": len(g),
            "p2_wins": int((g["outcome"] == "win").sum()),
            "p2_losses": int((g["outcome"] == "loss").sum()),
            "p2_hit_rate": float((g["outcome"] == "win").mean()) if len(g) else np.nan,
            "p2_profit": float(g["profit"].sum()),
            "p2_staked": float(g["stake"].sum()),
            "p2_roi": float(g["profit"].sum() / g["stake"].sum()) if g["stake"].sum() else np.nan,
            "avg_p2_edge": float(g["edge"].mean()),
            "avg_p3_under_edge": float(g["p3_under_edge"].mean()),
            "avg_edge_drop": float(g["edge_drop"].mean()),
            "median_edge_drop": float(g["edge_drop"].median()),
            "avg_p2_under_prob": float(g["model_prob"].mean()),
            "avg_p3_under_prob": float(g["p3_under_prob"].mean()),
        })
    return pd.DataFrame(rows).sort_values("count", ascending=False)


def analyze_added_bets(p2_bets: pd.DataFrame, p3_bets: pd.DataFrame) -> pd.DataFrame:
    p2_keys = set(map(tuple, p2_bets[p2_bets["side"] == "under"][KEY].drop_duplicates().values.tolist()))
    p3u = p3_bets[p3_bets["side"] == "under"].copy()
    p3u["is_added_vs_phase2"] = [tuple(x) not in p2_keys for x in p3u[KEY].values.tolist()]
    return p3u[p3u["is_added_vs_phase2"]].copy()


def try_regenerate_quantiles_and_features(keys_df: pd.DataFrame, artifacts: dict[str, Path], use_local: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """Regenerate quantile predictions and Phase 3A feature values for diagnostic rows.

    Returns (quantiles_df, features_df, error_message). On failure, both dfs are empty.
    """
    try:
        from sqlalchemy import text
        from src.db.client import get_engine
        from src.models.mlb.mlb_feature_store import MLBFeatureStore
        from src.models.mlb.mlb_model_suite import MLBModelSuite
        from src.backtesting.mlb.run_mlb_sweep import run_shared_phases

        engine = get_engine(local=use_local)
        fs = MLBFeatureStore(engine)
        start = pd.to_datetime(keys_df["game_date"]).min().date()
        end = pd.to_datetime(keys_df["game_date"]).max().date()

        q_rows = []
        for label, artifact in artifacts.items():
            suite = MLBModelSuite.from_directory(artifact, n_samples=5000, random_state=42)
            game_dates, date_predictions, _date_lines, _date_actuals = run_shared_phases(
                engine=engine,
                pitcher_feature_store=fs,
                batter_feature_store=None,
                suite=suite,
                start_date=start,
                end_date=end,
                stats=["pitcher_strikeouts"],
                quote_clean_cutoff_time_et=None,
            )
            for gd, preds in date_predictions.items():
                for pred in preds:
                    q_rows.append({
                        "model_label": label,
                        "game_date": str(gd),
                        "player_id": int(pred.player_id),
                        "game_id": int(pred.game_id),
                        "stat": pred.stat,
                        "team_id": int(pred.team_id),
                        "opponent_id": int(pred.opponent_id),
                        "pred_mean": float(pred.pred_mean),
                        "pred_median": float(pred.pred_median),
                        "pred_q10": float(pred.pred_q10),
                        "pred_q25": float(pred.pred_q25),
                        "pred_q50": float(pred.pred_q50),
                        "pred_q75": float(pred.pred_q75),
                        "pred_q90": float(pred.pred_q90),
                    })
        qdf = pd.DataFrame(q_rows)
        wanted = keys_df[["game_date", "player_id", "game_id", "stat"]].drop_duplicates()
        qdf = qdf.merge(wanted, on=["game_date", "player_id", "game_id", "stat"], how="inner")

        # Build schedule context for Phase 3A feature extraction.
        game_ids = sorted(set(int(x) for x in wanted["game_id"].dropna().unique()))
        schedule = pd.DataFrame()
        if game_ids:
            params = {f"gid_{i}": gid for i, gid in enumerate(game_ids)}
            placeholders = ", ".join(f":gid_{i}" for i in range(len(game_ids)))
            schedule = pd.read_sql(text(f"""
                SELECT game_id, game_date, home_team_id, away_team_id,
                       probable_pitcher_home_id, probable_pitcher_away_id, venue_id, season
                FROM mlb_game_schedule
                WHERE game_id IN ({placeholders})
            """), engine, params=params)
        sched_by_gid = {int(r.game_id): r for _, r in schedule.iterrows()}

        f_rows = []
        # Use qdf Phase3A rows because they contain team/opponent ids from the actual prediction path.
        p3q = qdf[qdf["model_label"] == "phase3a_tuned"].drop_duplicates(["game_date", "player_id", "game_id", "stat"])
        for _, r in p3q.iterrows():
            gid = int(r["game_id"])
            sched = sched_by_gid.get(gid)
            if sched is None:
                continue
            team_id = int(r["team_id"])
            is_home = bool(team_id == int(sched.home_team_id))
            try:
                feat = fs.get_player_game_features(
                    player_id=int(r["player_id"]),
                    game_id=gid,
                    game_date=str(r["game_date"]),
                    team_id=team_id,
                    opp_team_id=int(r["opponent_id"]),
                    venue_id=int(sched.venue_id) if pd.notna(sched.venue_id) else 0,
                    season=int(sched.season) if pd.notna(sched.season) else pd.to_datetime(r["game_date"]).year,
                    is_home=is_home,
                )
                row = {"game_date": r["game_date"], "player_id": int(r["player_id"]), "game_id": gid, "stat": r["stat"]}
                if feat:
                    for c in LINEUP_FEATURES:
                        row[c] = feat.get(c, np.nan)
                    row["lineup_minus_team_k_pct"] = row.get("projected_lineup_k_pct", np.nan) - row.get("opp_team_k_pct_l10", np.nan)
                f_rows.append(row)
            except Exception:
                continue
        fdf = pd.DataFrame(f_rows)
        return qdf, fdf, None
    except Exception:
        return pd.DataFrame(), pd.DataFrame(), traceback.format_exc()


def add_quantile_columns(primary: pd.DataFrame, qdf: pd.DataFrame) -> pd.DataFrame:
    if qdf.empty:
        return primary
    p2q = qdf[qdf["model_label"] == "phase2"].drop(columns=["model_label"]).add_prefix("p2q_")
    p3q = qdf[qdf["model_label"] == "phase3a_tuned"].drop(columns=["model_label"]).add_prefix("p3q_")
    for k in ["game_date", "player_id", "game_id", "stat"]:
        p2q = p2q.rename(columns={f"p2q_{k}": k})
        p3q = p3q.rename(columns={f"p3q_{k}": k})
    out = primary.merge(p2q, on=["game_date", "player_id", "game_id", "stat"], how="left")
    out = out.merge(p3q, on=["game_date", "player_id", "game_id", "stat"], how="left")
    for q in ["q10", "q25", "q50", "q75", "q90"]:
        a = f"p2q_pred_{q}"
        b = f"p3q_pred_{q}"
        if a in out.columns and b in out.columns:
            out[f"{q}_p2_dist_to_line"] = (out[a] - out["line"]).abs()
            out[f"{q}_p3_dist_to_line"] = (out[b] - out["line"]).abs()
            out[f"{q}_shift_toward_line"] = out[f"{q}_p2_dist_to_line"] - out[f"{q}_p3_dist_to_line"]
            out[f"{q}_raw_shift_p3_minus_p2"] = out[b] - out[a]
    return out


def summarize_quantile_shift(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for subset_name, g in [("all_phase2_bl_bets", df), ("phase2_bl_winners", df[df["outcome"] == "win"]), ("phase2_bl_losers", df[df["outcome"] == "loss"] )]:
        for q in ["q10", "q25", "q50", "q75", "q90"]:
            col = f"{q}_shift_toward_line"
            if col not in g.columns:
                continue
            rows.append({
                "subset": subset_name,
                "quantile": q,
                "count_with_q": int(g[col].notna().sum()),
                "mean_shift_toward_line": float(g[col].mean()),
                "median_shift_toward_line": float(g[col].median()),
                "pct_moved_toward_line": float((g[col] > 0).mean()),
                "mean_p2_dist_to_line": float(g[f"{q}_p2_dist_to_line"].mean()),
                "mean_p3_dist_to_line": float(g[f"{q}_p3_dist_to_line"].mean()),
            })
    return pd.DataFrame(rows)


def add_features_to_added(added: pd.DataFrame, fdf: pd.DataFrame) -> pd.DataFrame:
    if fdf.empty or added.empty:
        return added
    out = added.merge(fdf, on=["game_date", "player_id", "game_id", "stat"], how="left")
    if "lineup_minus_team_k_pct" in out.columns:
        abs_delta = out["lineup_minus_team_k_pct"].abs()
        valid = abs_delta.dropna()
        if len(valid) >= 3:
            q66 = valid.quantile(2/3)
            q33 = valid.quantile(1/3)
            out["lineup_delta_bucket"] = np.where(abs_delta >= q66, "high_delta", np.where(abs_delta <= q33, "low_delta", "mid_delta"))
        else:
            out["lineup_delta_bucket"] = "unknown"
    return out


def summarize_added(added: pd.DataFrame) -> pd.DataFrame:
    if added.empty:
        return pd.DataFrame()
    group_col = "lineup_delta_bucket" if "lineup_delta_bucket" in added.columns else "is_added_vs_phase2"
    rows = []
    for name, g in added.groupby(group_col, dropna=False):
        rows.append({
            group_col: name,
            "count": len(g),
            "wins": int((g["outcome"] == "win").sum()),
            "losses": int((g["outcome"] == "loss").sum()),
            "hit_rate": float((g["outcome"] == "win").mean()) if len(g) else np.nan,
            "profit": float(g["profit"].sum()),
            "staked": float(g["stake"].sum()),
            "roi": float(g["profit"].sum() / g["stake"].sum()) if g["stake"].sum() else np.nan,
            "avg_edge": float(g["edge"].mean()),
            "avg_model_prob": float(g["model_prob"].mean()),
            "avg_lineup_minus_team_k_pct": float(g["lineup_minus_team_k_pct"].mean()) if "lineup_minus_team_k_pct" in g.columns else np.nan,
            "avg_abs_lineup_minus_team_k_pct": float(g["lineup_minus_team_k_pct"].abs().mean()) if "lineup_minus_team_k_pct" in g.columns else np.nan,
        })
    return pd.DataFrame(rows).sort_values("count", ascending=False)


def make_plots(primary: pd.DataFrame) -> list[str]:
    made = []
    try:
        import matplotlib.pyplot as plt
        PLOTS.mkdir(parents=True, exist_ok=True)
        if "p2q_pred_q50" in primary.columns and "p3q_pred_q50" in primary.columns:
            for q in ["q10", "q50"]:
                xcol = f"p2q_pred_{q}"
                ycol = f"p3q_pred_{q}"
                if xcol not in primary.columns or ycol not in primary.columns:
                    continue
                plot_df = primary.dropna(subset=[xcol, ycol])
                if plot_df.empty:
                    continue
                colors = np.where(plot_df["outcome"].eq("win"), "#2ca02c", "#d62728")
                fig, ax = plt.subplots(figsize=(7, 6))
                ax.scatter(plot_df[xcol], plot_df[ycol], c=colors, alpha=0.75, s=32)
                lo = min(plot_df[xcol].min(), plot_df[ycol].min()) - 0.25
                hi = max(plot_df[xcol].max(), plot_df[ycol].max()) + 0.25
                ax.plot([lo, hi], [lo, hi], linestyle="--", color="black", linewidth=1)
                ax.set_xlabel(f"Phase 2 {q.upper()}")
                ax.set_ylabel(f"Phase 3A tuned {q.upper()}")
                ax.set_title(f"Phase 2 BL bets: Phase 3A vs Phase 2 {q.upper()}")
                path = PLOTS / f"{q}_phase3a_vs_phase2_scatter.png"
                fig.tight_layout(); fig.savefig(path, dpi=150); plt.close(fig)
                made.append(str(path.relative_to(REPO)))

                shift_col = f"{q}_shift_toward_line"
                if shift_col in primary.columns:
                    fig, ax = plt.subplots(figsize=(7, 4))
                    primary[shift_col].dropna().hist(ax=ax, bins=20)
                    ax.axvline(0, color="black", linestyle="--", linewidth=1)
                    ax.set_xlabel("positive = Phase 3A moved closer to line")
                    ax.set_title(f"{q.upper()} shift toward market line")
                    path = PLOTS / f"{q}_shift_toward_line_hist.png"
                    fig.tight_layout(); fig.savefig(path, dpi=150); plt.close(fig)
                    made.append(str(path.relative_to(REPO)))
        if "edge_drop" in primary.columns:
            fig, ax = plt.subplots(figsize=(7, 4))
            primary["edge_drop"].dropna().hist(ax=ax, bins=20)
            ax.axvline(0, color="black", linestyle="--", linewidth=1)
            ax.set_xlabel("Phase 2 under edge - Phase 3A under edge")
            ax.set_title("Edge compression on Phase 2 BL bets")
            path = PLOTS / "edge_drop_hist.png"
            fig.tight_layout(); fig.savefig(path, dpi=150); plt.close(fig)
            made.append(str(path.relative_to(REPO)))
    except Exception as e:
        (OUT / "plot_error.txt").write_text(traceback.format_exc())
    return made


def fmt_pct(x: float | None) -> str:
    if x is None or pd.isna(x):
        return "n/a"
    return f"{x:+.2%}"


def _md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_(empty)_"
    view = df.copy()
    for c in view.columns:
        if pd.api.types.is_float_dtype(view[c]):
            view[c] = view[c].map(lambda x: "" if pd.isna(x) else f"{x:.4f}")
        else:
            view[c] = view[c].map(lambda x: "" if pd.isna(x) else str(x))
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, r in view.iterrows():
        lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    return "\n".join(lines)


def write_report(metrics_df: pd.DataFrame, bucket_df: pd.DataFrame, winner_bucket_df: pd.DataFrame, added_df: pd.DataFrame, added_summary: pd.DataFrame, qshift: pd.DataFrame, plot_paths: list[str], q_error: str | None, final_form: str) -> None:
    lines = []
    lines.append("# MLB Pitcher K Phase 3A Agreement Diagnostic")
    lines.append("")
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append("")
    lines.append("## Decision")
    lines.append("")
    lines.append("Do not promote Phase 3A tuned or untuned. Keep Phase 2 clean as the current production winner pending future non-overlap validation.")
    lines.append("")
    lines.append("## Compared artifacts")
    lines.append("")
    lines.append(f"- Phase 2: `{PHASE2_ARTIFACT.relative_to(REPO)}`")
    lines.append(f"- Phase 3A tuned: `{PHASE3A_TUNED_ARTIFACT.relative_to(REPO)}`")
    lines.append(f"- Primary BL config: tau={TARGET_BL['tau']}, z_max={TARGET_BL['z_max']}, max_weight={TARGET_BL['max_weight']}, edge={TARGET_BL['edge_threshold']}")
    lines.append("")
    lines.append("## Performance context")
    lines.append("")
    lines.append(_md_table(metrics_df))
    lines.append("")
    lines.append("## Phase 2 BL bet buckets vs Phase 3A tuned same BL config")
    lines.append("")
    lines.append(_md_table(bucket_df))
    lines.append("")
    lines.append("## Phase 2 BL winners only")
    lines.append("")
    lines.append(_md_table(winner_bucket_df))
    lines.append("")
    lines.append("## Phase 3A added bets vs same Phase 2 BL config")
    lines.append("")
    if added_summary.empty:
        lines.append("No added-bet summary was available.")
    else:
        lines.append(_md_table(added_summary))
    lines.append("")
    if not qshift.empty:
        lines.append("## Quantile shift toward sportsbook line")
        lines.append("")
        lines.append(_md_table(qshift))
        lines.append("")
    if q_error:
        lines.append("## Quantile/feature regeneration warning")
        lines.append("")
        lines.append("Saved sweep predictions did not contain quantile columns. Regeneration failed; see `quantile_regeneration_error.txt`.")
        lines.append("")
    if plot_paths:
        lines.append("## Plots")
        lines.append("")
        for p in plot_paths:
            lines.append(f"- `{p}`")
        lines.append("")
    lines.append("## Final causal form")
    lines.append("")
    lines.append(final_form)
    lines.append("")
    lines.append("## Saved CSV artifacts")
    lines.append("")
    for p in [
        "performance_context.csv",
        "phase2_bl_vs_phase3a_tuned_paired_bets.csv",
        "phase2_bl_bucket_summary.csv",
        "phase2_bl_winner_bucket_summary.csv",
        "phase3a_added_bets.csv",
        "phase3a_added_bet_summary.csv",
        "quantile_shift_summary.csv",
        "regenerated_quantiles.csv",
        "regenerated_lineup_features.csv",
    ]:
        if (OUT / p).exists():
            lines.append(f"- `{(OUT / p).relative_to(REPO)}`")
    (OUT / "README.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    # Load exact configs.
    p2_bl_dir, p2_bl_bets, p2_bl_preds, p2_bl_metrics = read_config(PHASE2_BL_SWEEP, TARGET_BL)
    p3_bl_dir, p3_bl_bets, p3_bl_preds, p3_bl_metrics = read_config(PHASE3A_TUNED_UNDER_SWEEP, TARGET_BL)
    p2_raw_dir, _p2_raw_bets, _p2_raw_preds, p2_raw_metrics = read_config(PHASE2_RAW_SWEEP, RAW_EDGE)
    p3_raw_dir, _p3_raw_bets, _p3_raw_preds, p3_raw_metrics = read_config(PHASE3A_RAW_SWEEP, RAW_EDGE)
    p3_tuned_raw_dir, p3_tuned_raw_bets, _p3_tuned_raw_preds, p3_tuned_raw_metrics = read_config(PHASE3A_TUNED_UNDER_SWEEP, {"tau": None, "z_max": 0.25, "max_weight": 0.50, "edge_threshold": 0.02})
    _, _, _, p3_tuned_both_metrics = read_config(PHASE3A_TUNED_BOTH_SWEEP, {"tau": 0.75, "z_max": 0.50, "max_weight": 0.50, "edge_threshold": 0.02})

    metadata = {
        "phase2_bl_config_dir": str(p2_bl_dir.relative_to(REPO)),
        "phase3a_tuned_same_bl_config_dir": str(p3_bl_dir.relative_to(REPO)),
        "phase2_raw_config_dir": str(p2_raw_dir.relative_to(REPO)),
        "phase3a_untuned_raw_config_dir": str(p3_raw_dir.relative_to(REPO)),
        "phase3a_tuned_raw_config_dir": str(p3_tuned_raw_dir.relative_to(REPO)),
    }
    (OUT / "config_dirs.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    metrics_df = pd.DataFrame([
        summarize_metrics("Phase 2 raw under edge=0.05", p2_raw_metrics),
        summarize_metrics("Phase 3A untuned raw under edge=0.05", p3_raw_metrics),
        summarize_metrics("Phase 3A tuned raw/no-BL under edge=0.02", p3_tuned_raw_metrics),
        summarize_metrics("Phase 2 BL under tau=.90 z=.25 mw=.80 edge=.02", p2_bl_metrics),
        summarize_metrics("Phase 3A tuned same BL under", p3_bl_metrics),
        summarize_metrics("Phase 3A tuned best meaningful both-direction", p3_tuned_both_metrics),
    ])
    metrics_df.to_csv(OUT / "performance_context.csv", index=False)

    primary = merge_primary(p2_bl_bets, p2_bl_preds, p3_bl_bets, p3_bl_preds)
    added = analyze_added_bets(p2_bl_bets, p3_bl_bets)

    # Regenerate quantiles/features for only relevant rows.
    wanted_keys = pd.concat([
        primary[KEY],
        added[KEY] if not added.empty else primary[KEY].iloc[0:0],
    ], ignore_index=True).drop_duplicates()
    qdf, fdf, q_error = try_regenerate_quantiles_and_features(
        wanted_keys,
        {"phase2": PHASE2_ARTIFACT, "phase3a_tuned": PHASE3A_TUNED_ARTIFACT},
        use_local=True,
    )
    if q_error:
        (OUT / "quantile_regeneration_error.txt").write_text(q_error, encoding="utf-8")
        # Retry remote/default DB if local failed.
        qdf, fdf, q_error2 = try_regenerate_quantiles_and_features(
            wanted_keys,
            {"phase2": PHASE2_ARTIFACT, "phase3a_tuned": PHASE3A_TUNED_ARTIFACT},
            use_local=False,
        )
        if q_error2:
            (OUT / "quantile_regeneration_error_remote.txt").write_text(q_error2, encoding="utf-8")
            q_error = q_error + "\n\nREMOTE RETRY:\n" + q_error2
        else:
            q_error = None

    if not qdf.empty:
        qdf.to_csv(OUT / "regenerated_quantiles.csv", index=False)
        primary = add_quantile_columns(primary, qdf)
    if not fdf.empty:
        fdf.to_csv(OUT / "regenerated_lineup_features.csv", index=False)
        primary = primary.merge(fdf, on=["game_date", "player_id", "game_id", "stat"], how="left")
        added = add_features_to_added(added, fdf)

    primary.to_csv(OUT / "phase2_bl_vs_phase3a_tuned_paired_bets.csv", index=False)
    bucket_df = summarize_buckets(primary)
    winner_bucket_df = summarize_buckets(primary[primary["outcome"] == "win"])
    bucket_df.to_csv(OUT / "phase2_bl_bucket_summary.csv", index=False)
    winner_bucket_df.to_csv(OUT / "phase2_bl_winner_bucket_summary.csv", index=False)

    added.to_csv(OUT / "phase3a_added_bets.csv", index=False)
    added_summary = summarize_added(added)
    if not added_summary.empty:
        added_summary.to_csv(OUT / "phase3a_added_bet_summary.csv", index=False)

    qshift = summarize_quantile_shift(primary)
    if not qshift.empty:
        qshift.to_csv(OUT / "quantile_shift_summary.csv", index=False)

    plot_paths = make_plots(primary)

    # Compute final causal form numbers.
    n = len(primary)
    compressed = int(primary["bucket"].isin(["same_side_lower_edge_still_cleared", "same_side_edge_dropped_below_threshold"]).sum())
    dropped = int((primary["bucket"] == "same_side_edge_dropped_below_threshold").sum())
    flipped = int((primary["bucket"] == "flipped_or_direction_invalidated").sum())
    p2_roi = p2_bl_metrics.get("roi", np.nan)
    p3_same_roi = p3_bl_metrics.get("roi", np.nan)
    edge_drop_mean = primary["edge_drop"].mean()
    winner_compressed = int(primary[(primary["outcome"] == "win") & primary["bucket"].isin(["same_side_lower_edge_still_cleared", "same_side_edge_dropped_below_threshold"])] .shape[0])
    winners = int((primary["outcome"] == "win").sum())
    added_roi = float(added["profit"].sum() / added["stake"].sum()) if not added.empty and added["stake"].sum() else np.nan
    q50_row = qshift[(qshift["subset"] == "phase2_bl_winners") & (qshift["quantile"] == "q50")]
    q10_row = qshift[(qshift["subset"] == "phase2_bl_winners") & (qshift["quantile"] == "q10")]
    q50_text = "Q50 shift unavailable"
    if not q50_row.empty:
        r = q50_row.iloc[0]
        q50_text = f"winner Q50 distance-to-line changed from {r['mean_p2_dist_to_line']:.3f} to {r['mean_p3_dist_to_line']:.3f}; {r['pct_moved_toward_line']:.1%} moved toward the line"
    q10_text = "Q10 shift unavailable"
    if not q10_row.empty:
        r = q10_row.iloc[0]
        q10_text = f"winner Q10 distance-to-line changed from {r['mean_p2_dist_to_line']:.3f} to {r['mean_p3_dist_to_line']:.3f}; {r['pct_moved_toward_line']:.1%} moved toward the line"

    # Feature mechanism phrase.
    mechanism_conclusion = "feature dilution/edge compression"
    if not added_summary.empty and "lineup_delta_bucket" in added_summary.columns:
        high = added_summary[added_summary["lineup_delta_bucket"] == "high_delta"]
        low = added_summary[added_summary["lineup_delta_bucket"] == "low_delta"]
        mid = added_summary[added_summary["lineup_delta_bucket"] == "mid_delta"]
        if not high.empty and not low.empty:
            high_roi = float(high.iloc[0]["roi"])
            low_roi = float(low.iloc[0]["roi"])
            mid_roi = float(mid.iloc[0]["roi"]) if not mid.empty else np.nan
            mech = (
                f"Phase 3A added-bet ROI was {fmt_pct(high_roi)} in high lineup/team-delta cases, "
                f"{fmt_pct(mid_roi)} in mid-delta cases, and {fmt_pct(low_roi)} in low-delta cases"
            )
            if high_roi > 0 and low_roi < 0:
                mechanism_conclusion = "blanket feature dilution/edge compression, not blanket lineup anti-signal; lineup information may be conditionally useful only when it materially differs from team average"
            elif high_roi < 0:
                mechanism_conclusion = "feature dilution plus likely market convergence/anti-signal in the exact high lineup-delta cases where Phase 3A should have helped"
        else:
            mech = "added-bet lineup-delta split was available but sparse"
    else:
        mech = "lineup-delta split was unavailable, so market-convergence vs generic dilution is inferred mainly from edge compression"

    final_form = (
        f"Phase 3A lost because {compressed}/{n} ({compressed/max(n,1):.1%}) of Phase 2 BL under bets were edge-compressed, "
        f"including {winner_compressed}/{winners} ({winner_compressed/max(winners,1):.1%}) of Phase 2 winners; "
        f"{dropped}/{n} dropped below threshold and {flipped}/{n} flipped or were directionally invalidated. "
        f"Mean under-edge drop was {edge_drop_mean:.4f}, while the same BL config ROI fell from {fmt_pct(float(p2_roi))} to {fmt_pct(float(p3_same_roi))}; "
        f"Phase 3A-only added bets returned {fmt_pct(added_roi)}. {q50_text}; {q10_text}. "
        f"{mech}. The mechanism is primarily {mechanism_conclusion}; no Phase 3A artifact should be promoted."
    )
    (OUT / "final_causal_form.txt").write_text(final_form, encoding="utf-8")

    write_report(metrics_df, bucket_df, winner_bucket_df, added, added_summary, qshift, plot_paths, q_error, final_form)

    print("Wrote diagnostic artifacts to", OUT)
    print("Final causal form:")
    print(final_form)


if __name__ == "__main__":
    main()
