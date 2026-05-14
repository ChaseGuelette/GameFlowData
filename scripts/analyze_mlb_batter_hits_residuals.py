#!/usr/bin/env python3
"""MLB batter_hits residual/uncertainty diagnostics for saved sweep outputs.

Phase 1 intentionally starts from saved `bets.csv` / `predictions.csv` files.
It does not join DB context. If context columns are absent, the markdown summary
recommends Phase 1B rather than silently broadening scope.

Uncertainty policy: ROI / hit-rate / calibration intervals use a block bootstrap
by game_date (or game_id fallback) so same-slate bets are resampled together.
This is intentionally wider than naive iid percentile bootstraps.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

try:  # scipy is available in the GameFlow venv, but keep fallbacks for import-time tests.
    from scipy.stats import chi2_contingency, fisher_exact, mannwhitneyu, spearmanr
except Exception:  # pragma: no cover - exercised only if scipy missing
    chi2_contingency = None
    fisher_exact = None
    mannwhitneyu = None
    spearmanr = None

METRIC_SCHEMA_BASE = [
    "config",
    "group",
    "n",
    "n_blocks",
    "sample_status",
    "bootstrap_method",
]

WIN_LOSS_SCHEMA = [
    "config",
    "dimension",
    "dimension_type",
    "test",
    "statistic",
    "p_value",
    "material",
    "n_wins",
    "n_losses",
    "win_mean",
    "loss_mean",
    "win_median",
    "loss_median",
    "notes",
]

FIXED_EDGE_BINS = [0.0, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.22, math.inf]
FIXED_EDGE_LABELS = ["0.00-0.05", "0.05-0.08", "0.08-0.10", "0.10-0.12", "0.12-0.15", "0.15-0.18", "0.18-0.22", "0.22+"]


@dataclass
class ConfigRun:
    label: str
    path: Path
    config: dict
    bets: pd.DataFrame
    predictions: pd.DataFrame


def sample_status(n: int, production_min_bets: int = 100) -> str:
    if n < 30:
        return "masked_small_n"
    if n < production_min_bets:
        return "exploratory"
    return "decision_eligible"


def american_to_decimal(odds: float) -> float:
    if pd.isna(odds):
        return float("nan")
    odds = float(odds)
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def american_to_implied_prob(odds: float) -> float:
    if pd.isna(odds):
        return float("nan")
    odds = float(odds)
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def normalize_bets(df: pd.DataFrame, flat_stake: float = 100.0) -> pd.DataFrame:
    out = df.copy()
    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"]).dt.date.astype(str)
    if "stake" not in out.columns:
        out["stake"] = flat_stake
    out["stake"] = pd.to_numeric(out["stake"], errors="coerce").fillna(flat_stake)
    if "profit" not in out.columns:
        out["profit"] = 0.0
    out["profit"] = pd.to_numeric(out["profit"], errors="coerce").fillna(0.0)
    if "outcome" not in out.columns and {"actual", "line", "side"}.issubset(out.columns):
        actual = pd.to_numeric(out["actual"], errors="coerce")
        line = pd.to_numeric(out["line"], errors="coerce")
        side = out["side"].astype(str).str.lower()
        out["outcome"] = np.select(
            [((side == "under") & (actual < line)) | ((side == "over") & (actual > line)), actual == line],
            ["win", "push"],
            default="loss",
        )
    out["outcome"] = out["outcome"].astype(str).str.lower()
    for col in ["line", "odds", "model_prob", "implied_prob", "edge", "actual", "posterior_prob"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def compute_bet_metrics(df: pd.DataFrame, flat_stake: float = 100.0) -> dict:
    if df is None or len(df) == 0:
        return {"n": 0, "wins": 0, "losses": 0, "pushes": 0, "hit_rate": np.nan, "profit": 0.0, "roi": np.nan, "flat_roi": np.nan}
    x = normalize_bets(df, flat_stake=flat_stake)
    wins = int((x["outcome"] == "win").sum())
    losses = int((x["outcome"] == "loss").sum())
    pushes = int((x["outcome"] == "push").sum())
    non_push = wins + losses
    hit_rate = wins / non_push if non_push else np.nan
    profit = float(x["profit"].sum())
    total_staked = float(x["stake"].sum())
    roi = profit / total_staked if total_staked else np.nan
    return {"n": int(len(x)), "wins": wins, "losses": losses, "pushes": pushes, "hit_rate": hit_rate, "profit": profit, "roi": roi, "flat_roi": roi}


def _block_col(df: pd.DataFrame, allow_iid: bool = False) -> tuple[str | None, str]:
    if "game_date" in df.columns:
        return "game_date", "block_by_game_date"
    if "game_id" in df.columns:
        return "game_id", "block_by_game_id"
    if allow_iid:
        return None, "iid_rows_explicit"
    raise ValueError("Block bootstrap requires game_date or game_id unless --allow-iid-bootstrap is set")


def block_bootstrap_ci(
    df: pd.DataFrame,
    metric_fn: Callable[[pd.DataFrame], float],
    n_resamples: int = 1000,
    ci_level: float = 0.95,
    seed: int = 17,
    allow_iid: bool = False,
) -> dict:
    if df is None or len(df) == 0:
        return {"estimate": np.nan, "ci_low": np.nan, "ci_high": np.nan, "n_blocks": 0, "method": "empty"}
    block_col, method = _block_col(df, allow_iid=allow_iid)
    estimate = float(metric_fn(df))
    rng = np.random.default_rng(seed)
    vals = []
    if block_col is None:
        n = len(df)
        for _ in range(n_resamples):
            idx = rng.integers(0, n, size=n)
            vals.append(float(metric_fn(df.iloc[idx])))
        n_blocks = n
    else:
        blocks = pd.Series(df[block_col].astype(str).unique())
        n_blocks = int(len(blocks))
        block_indices = {str(k): np.asarray(v, dtype=int) for k, v in df.groupby(df[block_col].astype(str), sort=False).indices.items()}
        for _ in range(n_resamples):
            sampled_blocks = rng.choice(blocks.to_numpy(), size=n_blocks, replace=True)
            idx = np.concatenate([block_indices[str(b)] for b in sampled_blocks])
            vals.append(float(metric_fn(df.iloc[idx])))
    vals = np.asarray([v for v in vals if np.isfinite(v)], dtype=float)
    alpha = (1.0 - ci_level) / 2.0
    if len(vals) == 0:
        low = high = np.nan
    else:
        low, high = np.quantile(vals, [alpha, 1.0 - alpha])
    return {"estimate": estimate, "ci_low": float(low), "ci_high": float(high), "n_blocks": n_blocks, "method": method}


def metric_row(config: str, group: str, df: pd.DataFrame, flat_stake: float, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> dict:
    metrics = compute_bet_metrics(df, flat_stake=flat_stake)
    row = {"config": config, "group": group, "n": metrics["n"], "sample_status": sample_status(metrics["n"], production_min_bets)}
    for metric in ["hit_rate", "roi", "flat_roi", "profit"]:
        ci = block_bootstrap_ci(df, lambda x, m=metric: compute_bet_metrics(x, flat_stake=flat_stake)[m], n_resamples, ci_level, allow_iid=allow_iid)
        row[metric] = ci["estimate"]
        row[f"{metric}_ci_low"] = ci["ci_low"]
        row[f"{metric}_ci_high"] = ci["ci_high"]
        row["n_blocks"] = ci["n_blocks"]
        row["bootstrap_method"] = ci["method"]
    row.update({"wins": metrics["wins"], "losses": metrics["losses"], "pushes": metrics["pushes"]})
    for col in ["edge", "model_prob", "implied_prob", "odds"]:
        if col in df.columns:
            row[f"avg_{col}"] = pd.to_numeric(df[col], errors="coerce").mean()
    if "odds" in df.columns:
        odds = pd.to_numeric(df["odds"], errors="coerce")
        row["avg_decimal_odds"] = odds.map(american_to_decimal).mean()
    return row


def assign_adaptive_edge_bins(df: pd.DataFrame, min_bin_size: int = 20) -> pd.Series:
    edges = pd.to_numeric(df["edge"], errors="coerce")
    base = pd.cut(edges, bins=FIXED_EDGE_BINS, labels=FIXED_EDGE_LABELS, right=False, include_lowest=True).astype("object")
    ordered = [label for label in FIXED_EDGE_LABELS if (base == label).any()]
    if not ordered:
        return pd.Series(["missing"] * len(df), index=df.index, dtype="object")
    merged: dict[str, str] = {}
    current: list[str] = []
    current_n = 0
    groups: list[list[str]] = []
    for label in ordered:
        current.append(label)
        current_n += int((base == label).sum())
        if current_n >= min_bin_size:
            groups.append(current)
            current = []
            current_n = 0
    if current:
        if groups:
            groups[-1].extend(current)
        else:
            groups.append(current)
    for group in groups:
        new_label = group[0] if len(group) == 1 else f"{group[0]}..{group[-1]}"
        for old in group:
            merged[old] = new_label
    return base.map(merged).fillna("missing")


def line_bucket(line: pd.Series) -> pd.Series:
    x = pd.to_numeric(line, errors="coerce")
    return np.select([x <= 0.5, x <= 1.5], ["0.5", "1.5"], default="2.5+")


def odds_bucket(odds: pd.Series) -> pd.Series:
    x = pd.to_numeric(odds, errors="coerce")
    return pd.cut(x, [-math.inf, -150, -110, 100, 150, math.inf], labels=["<=-150", "-150..-110", "-110..+100", "+100..+150", ">+150"]).astype("object")


def probability_bucket(series: pd.Series, name: str) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    return pd.cut(x, [0, 0.45, 0.55, 0.65, 0.75, 1.0], labels=[f"{name}<.45", f"{name}.45-.55", f"{name}.55-.65", f"{name}.65-.75", f"{name}>.75"], include_lowest=True).astype("object")


def _mw_test(wins: pd.Series, losses: pd.Series) -> tuple[str, float, float, bool, str]:
    wins = pd.to_numeric(wins, errors="coerce").dropna()
    losses = pd.to_numeric(losses, errors="coerce").dropna()
    if len(wins) < 3 or len(losses) < 3:
        return "mannwhitneyu", np.nan, np.nan, False, "insufficient_n"
    if mannwhitneyu is None:
        diff = abs(wins.mean() - losses.mean())
        return "mean_diff_no_scipy", diff, np.nan, False, "scipy_unavailable"
    res = mannwhitneyu(wins, losses, alternative="two-sided")
    p = float(res.pvalue)
    return "mannwhitneyu", float(res.statistic), p, bool(p < 0.01), ""


def _cat_test(win_vals: pd.Series, loss_vals: pd.Series) -> tuple[str, float, float, bool, str]:
    data = pd.DataFrame({"result": ["win"] * len(win_vals) + ["loss"] * len(loss_vals), "value": list(win_vals.astype(str)) + list(loss_vals.astype(str))})
    table = pd.crosstab(data["result"], data["value"])
    if table.shape[1] < 2 or table.to_numpy().sum() < 6:
        return "categorical", np.nan, np.nan, False, "insufficient_categories"
    if table.shape == (2, 2) and fisher_exact is not None and (table.to_numpy() < 5).any():
        oddsratio, p = fisher_exact(table.to_numpy())
        return "fisher_exact", float(oddsratio), float(p), bool(p < 0.01), ""
    if chi2_contingency is None:
        return "chi2", np.nan, np.nan, False, "scipy_unavailable"
    stat, p, _, expected = chi2_contingency(table)
    notes = "low_expected_counts" if (expected < 5).any() else ""
    return "chi2", float(stat), float(p), bool(p < 0.01), notes


def high_edge_win_loss_tests(df: pd.DataFrame, config: str = "") -> list[dict]:
    if len(df) == 0 or "outcome" not in df.columns:
        return []
    x = df[df["outcome"].isin(["win", "loss"])].copy()
    wins = x[x["outcome"] == "win"]
    losses = x[x["outcome"] == "loss"]
    rows: list[dict] = []
    continuous = [c for c in ["odds", "edge", "model_prob", "implied_prob"] if c in x.columns]
    categorical_sources = []
    if "bookmaker" in x.columns:
        categorical_sources.append("bookmaker")
    if "line_bucket" in x.columns:
        categorical_sources.append("line_bucket")
    if "time_bucket" in x.columns:
        categorical_sources.append("time_bucket")
    if "odds_bucket" in x.columns:
        categorical_sources.append("odds_bucket")
    if "model_prob_bucket" in x.columns:
        categorical_sources.append("model_prob_bucket")
    if "edge_bucket" in x.columns:
        categorical_sources.append("edge_bucket")
    for col in continuous:
        test, stat, p, material, notes = _mw_test(wins[col], losses[col])
        win_vals = pd.to_numeric(wins[col], errors="coerce")
        loss_vals = pd.to_numeric(losses[col], errors="coerce")
        rows.append({
            "config": config,
            "dimension": col,
            "dimension_type": "continuous",
            "test": test,
            "statistic": stat,
            "p_value": p,
            "material": bool(material),
            "n_wins": len(wins),
            "n_losses": len(losses),
            "win_mean": float(win_vals.mean()) if win_vals.notna().any() else np.nan,
            "loss_mean": float(loss_vals.mean()) if loss_vals.notna().any() else np.nan,
            "win_median": float(win_vals.median()) if win_vals.notna().any() else np.nan,
            "loss_median": float(loss_vals.median()) if loss_vals.notna().any() else np.nan,
            "notes": notes,
        })
    for col in categorical_sources:
        test, stat, p, material, notes = _cat_test(wins[col].dropna(), losses[col].dropna())
        rows.append({
            "config": config,
            "dimension": col,
            "dimension_type": "categorical",
            "test": test,
            "statistic": stat,
            "p_value": p,
            "material": bool(material),
            "n_wins": len(wins),
            "n_losses": len(losses),
            "win_mean": np.nan,
            "loss_mean": np.nan,
            "win_median": np.nan,
            "loss_median": np.nan,
            "notes": notes,
        })
    return rows


def compute_clv_table(df: pd.DataFrame) -> pd.DataFrame | None:
    if {"odds_at_bet", "odds_at_close"}.issubset(df.columns):
        out = df.copy()
        out["clv_type"] = "true_odds_clv_cents"
        out["clv_value"] = pd.to_numeric(out["odds_at_close"], errors="coerce") - pd.to_numeric(out["odds_at_bet"], errors="coerce")
        return out[["clv_type", "clv_value"] + [c for c in ["edge", "bookmaker", "game_date"] if c in out.columns]]
    if {"odds_at_bet", "consensus_close_implied_prob"}.issubset(df.columns):
        out = df.copy()
        bet_imp = pd.to_numeric(out["odds_at_bet"], errors="coerce").map(american_to_implied_prob)
        close_imp = pd.to_numeric(out["consensus_close_implied_prob"], errors="coerce")
        out["clv_type"] = "implied_clv_proxy"
        out["clv_value"] = close_imp - bet_imp
        return out[["clv_type", "clv_value"] + [c for c in ["edge", "bookmaker", "game_date"] if c in out.columns]]
    return None


def detect_drift(weekly: pd.DataFrame, third_rows: pd.DataFrame | None = None) -> dict:
    w = weekly.dropna(subset=["week_index", "roi"]).copy()
    r = p = np.nan
    spearman_flag = False
    if len(w) >= 3:
        if spearmanr is not None:
            res = spearmanr(w["week_index"], w["roi"])
            r, p = float(res.statistic), float(res.pvalue)
            spearman_flag = bool(r < 0 and p < 0.05)
        else:
            r = float(pd.Series(w["week_index"]).corr(pd.Series(w["roi"]), method="spearman"))
            p = np.nan
    third_flag = False
    if third_rows is not None and {"third", "roi", "roi_ci_low", "roi_ci_high"}.issubset(third_rows.columns):
        early = third_rows[third_rows["third"] == "early"]
        late = third_rows[third_rows["third"] == "late"]
        if not early.empty and not late.empty:
            third_flag = bool(float(late.iloc[0]["roi_ci_high"]) < float(early.iloc[0]["roi"]))
    return {
        "spearman_r": r,
        "spearman_p": p,
        "spearman_decay": spearman_flag,
        "third_ci_decay": third_flag,
        "decay_detected": bool(spearman_flag or third_flag),
        "decay_watchlist": bool((np.isfinite(r) and r <= -0.5) or spearman_flag or third_flag),
        "decay_watchlist_severity": (
            "retraining_review_trigger" if np.isfinite(r) and r <= -0.7 else
            "early_warning_underpowered" if np.isfinite(r) and r <= -0.5 else
            "confirmed_decay" if (spearman_flag or third_flag) else
            "none"
        ),
    }


def config_label(config: dict) -> str:
    tau = config.get("tau")
    edge = config.get("edge_threshold")
    z = config.get("z_max")
    mw = config.get("max_weight")
    if tau is None or (isinstance(tau, float) and math.isnan(tau)):
        return f"raw_no_BL_edge={edge:g}"
    return f"BL_tau={float(tau):g}_z={float(z):g}_mw={float(mw):g}_edge={float(edge):g}"


def load_config_run(path: Path, flat_stake: float = 100.0) -> ConfigRun | None:
    metrics_path = path / "metrics.json"
    bets_path = path / "bets.csv"
    pred_path = path / "predictions.csv"
    if not (metrics_path.exists() and bets_path.exists() and pred_path.exists()):
        return None
    with metrics_path.open("r", encoding="utf-8") as f:
        metrics = json.load(f)
    config = metrics.get("config", {})
    label = config_label(config)
    bets = normalize_bets(pd.read_csv(bets_path), flat_stake=flat_stake)
    preds = pd.read_csv(pred_path)
    if "game_date" in preds.columns:
        preds["game_date"] = pd.to_datetime(preds["game_date"]).dt.date.astype(str)
    for col in preds.columns:
        if col not in {"game_date", "stat", "bookmaker"}:
            converted = pd.to_numeric(preds[col], errors="coerce")
            if converted.notna().any():
                preds[col] = converted
    return ConfigRun(label=label, path=path, config=config, bets=bets, predictions=preds)


def discover_runs(sweep_dir: Path, flat_stake: float = 100.0) -> list[ConfigRun]:
    runs = []
    for p in sorted(sweep_dir.glob("config_*")):
        if p.is_dir():
            run = load_config_run(p, flat_stake=flat_stake)
            if run is not None:
                runs.append(run)
    return runs


def select_benchmark_runs(runs: list[ConfigRun]) -> list[ConfigRun]:
    targets = [
        (None, 0.15, None, None),
        (0.90, 0.10, 0.25, 0.65),
        (0.90, 0.08, 0.25, 0.65),
    ]
    selected = []
    for tau, edge, z, mw in targets:
        matches = []
        for run in runs:
            c = run.config
            rtau = c.get("tau")
            redge = c.get("edge_threshold")
            rz = c.get("z_max")
            rmw = c.get("max_weight")
            tau_ok = (tau is None and rtau is None) or (tau is not None and rtau is not None and abs(float(rtau) - tau) < 1e-9)
            z_ok = z is None or (rz is not None and abs(float(rz) - z) < 1e-9)
            mw_ok = mw is None or (rmw is not None and abs(float(rmw) - mw) < 1e-9)
            edge_ok = redge is not None and abs(float(redge) - edge) < 1e-9
            if tau_ok and z_ok and mw_ok and edge_ok:
                matches.append(run)
        if matches:
            selected.append(matches[0])
    return selected or runs[:3]


def calibration_table(config: str, predictions: pd.DataFrame, side: str, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> pd.DataFrame:
    prob_col = "under_prob" if side == "under" else "over_prob"
    if prob_col not in predictions.columns or "actual" not in predictions.columns or "line" not in predictions.columns:
        return pd.DataFrame()
    df = predictions.copy()
    df["prob"] = pd.to_numeric(df[prob_col], errors="coerce")
    actual = pd.to_numeric(df["actual"], errors="coerce")
    line = pd.to_numeric(df["line"], errors="coerce")
    df["win_numeric"] = np.where(side == "under", actual < line, actual > line).astype(float)
    df = df.dropna(subset=["prob", "win_numeric"])
    if df.empty:
        return pd.DataFrame()
    df["decile"] = pd.qcut(df["prob"].rank(method="first"), 10, labels=[f"D{i}" for i in range(1, 11)])
    rows = []
    for decile, g in df.groupby("decile", observed=True):
        row = {"config": config, "group": str(decile), "side": side, "n": int(len(g)), "sample_status": sample_status(len(g), production_min_bets)}
        for metric_name, fn in {
            "mean_predicted_prob": lambda x: float(x["prob"].mean()),
            "observed_win_rate": lambda x: float(x["win_numeric"].mean()),
            "calibration_gap": lambda x: float(x["win_numeric"].mean() - x["prob"].mean()),
        }.items():
            ci = block_bootstrap_ci(g, fn, n_resamples, ci_level, allow_iid=allow_iid)
            row[metric_name] = ci["estimate"]
            row[f"{metric_name}_ci_low"] = ci["ci_low"]
            row[f"{metric_name}_ci_high"] = ci["ci_high"]
            row["n_blocks"] = ci["n_blocks"]
            row["bootstrap_method"] = ci["method"]
        rows.append(row)
    return pd.DataFrame(rows)


def selected_bet_calibration(config: str, bets: pd.DataFrame, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> pd.DataFrame:
    df = bets.dropna(subset=["model_prob"]).copy()
    df = df[df["outcome"].isin(["win", "loss"])]
    if df.empty:
        return pd.DataFrame()
    df["win_numeric"] = (df["outcome"] == "win").astype(float)
    df["decile"] = pd.qcut(df["model_prob"].rank(method="first"), min(10, len(df)), labels=False, duplicates="drop")
    df["decile"] = df["decile"].map(lambda x: f"D{int(x)+1}")
    rows = []
    for decile, g in df.groupby("decile", observed=True):
        row = {"config": config, "group": str(decile), "n": len(g), "sample_status": sample_status(len(g), production_min_bets)}
        for metric_name, fn in {
            "mean_predicted_prob": lambda x: float(x["model_prob"].mean()),
            "observed_win_rate": lambda x: float(x["win_numeric"].mean()),
            "calibration_gap": lambda x: float(x["win_numeric"].mean() - x["model_prob"].mean()),
        }.items():
            ci = block_bootstrap_ci(g, fn, n_resamples, ci_level, allow_iid=allow_iid)
            row[metric_name] = ci["estimate"]
            row[f"{metric_name}_ci_low"] = ci["ci_low"]
            row[f"{metric_name}_ci_high"] = ci["ci_high"]
            row["n_blocks"] = ci["n_blocks"]
            row["bootstrap_method"] = ci["method"]
        rows.append(row)
    return pd.DataFrame(rows)


def time_bucket_tables(config: str, bets: pd.DataFrame, flat_stake: float, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df = bets.copy()
    df["date_dt"] = pd.to_datetime(df["game_date"])
    df["week_index"] = ((df["date_dt"] - df["date_dt"].min()).dt.days // 7 + 1).astype(int)
    weekly_rows = []
    for week, g in df.groupby("week_index"):
        row = metric_row(config, f"week_{week}", g, flat_stake, n_resamples, ci_level, production_min_bets, allow_iid)
        row["week_index"] = int(week)
        weekly_rows.append(row)
    weekly = pd.DataFrame(weekly_rows)
    ranked = df.sort_values("date_dt").copy()
    ranked["third"] = pd.qcut(ranked["date_dt"].rank(method="first"), 3, labels=["early", "middle", "late"])
    third_rows = []
    for third, g in ranked.groupby("third", observed=True):
        row = metric_row(config, str(third), g, flat_stake, n_resamples, ci_level, production_min_bets, allow_iid)
        row["third"] = str(third)
        third_rows.append(row)
    thirds = pd.DataFrame(third_rows)
    drift = detect_drift(weekly[["week_index", "roi"]], thirds)
    return weekly, thirds, drift


def bookmaker_table(config: str, bets: pd.DataFrame, flat_stake: float, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> pd.DataFrame:
    if "bookmaker" not in bets.columns:
        return pd.DataFrame()
    total_profit = bets["profit"].clip(lower=0).sum()
    total_losses = abs(bets["profit"].clip(upper=0).sum())
    rows = []
    for book, g in bets.groupby("bookmaker"):
        row = metric_row(config, str(book), g, flat_stake, n_resamples, ci_level, production_min_bets, allow_iid)
        book_profit = g["profit"].clip(lower=0).sum()
        book_losses = abs(g["profit"].clip(upper=0).sum())
        row["share_total_bets"] = len(g) / len(bets) if len(bets) else np.nan
        row["share_profit"] = book_profit / total_profit if total_profit else 0.0
        row["share_losses"] = book_losses / total_losses if total_losses else 0.0
        row["fragility_flag"] = bool(row["share_profit"] > 0.40 or row["share_losses"] > 0.50)
        rows.append(row)
    return pd.DataFrame(rows)


def edge_bin_table(config: str, bets: pd.DataFrame, flat_stake: float, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> pd.DataFrame:
    df = bets.copy()
    df["edge_bin"] = assign_adaptive_edge_bins(df, min_bin_size=20)
    rows = [metric_row(config, str(bin_label), g, flat_stake, n_resamples, ci_level, production_min_bets, allow_iid) for bin_label, g in df.groupby("edge_bin", observed=True)]
    return pd.DataFrame(rows)


def line_bucket_table(config: str, bets: pd.DataFrame, flat_stake: float, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> pd.DataFrame:
    df = bets.copy()
    df["line_bucket"] = line_bucket(df["line"])
    rows = [metric_row(config, str(bucket), g, flat_stake, n_resamples, ci_level, production_min_bets, allow_iid) for bucket, g in df.groupby("line_bucket", observed=True)]
    return pd.DataFrame(rows)


def direction_summary(label: str, runs: list[ConfigRun], direction: str, flat_stake: float, n_resamples: int, ci_level: float, production_min_bets: int, allow_iid: bool = False) -> pd.DataFrame:
    rows = []
    for run in runs:
        row = metric_row(run.label, direction, run.bets, flat_stake, n_resamples, ci_level, production_min_bets, allow_iid)
        row["run_dir"] = str(run.path)
        rows.append(row)
    return pd.DataFrame(rows)


def write_summary(out_path: Path, artifacts: dict, drifts: list[dict], clv_available: bool, material_tests: pd.DataFrame, bookmaker_flags: pd.DataFrame, direction_df: pd.DataFrame | None = None) -> None:
    decay = any(d.get("decay_detected") for d in drifts)
    watchlist = any(d.get("decay_watchlist") for d in drifts)
    material_any = (not material_tests.empty) and bool(material_tests["material"].fillna(False).any())
    fragility_any = (not bookmaker_flags.empty) and bool(bookmaker_flags.get("fragility_flag", pd.Series(dtype=bool)).fillna(False).any())
    if not clv_available:
        primary = "build Phase 1B line-history / CLV gate before feature work"
        prereq = "Add closing-line or consensus-close context, then verify predicted edge correlates with positive CLV/proxy before Phase 2."
    elif decay:
        primary = "prioritize calibration/selection/BL policy before features"
        prereq = "Retraining cadence / live recalibration check because time-bucket decay was detected."
    elif material_any:
        primary = "no feature work yet; confirm high-edge win/loss hypothesis on held-out/newer window"
        prereq = "Confirm material high-edge win/loss dimension before Phase 2."
    elif fragility_any:
        primary = "prioritize calibration/selection/BL policy before features"
        prereq = "Review bookmaker fragility before feature work."
    else:
        primary = "no feature work yet; collect more data or extend diagnostics"
        prereq = "+30 calendar days or +200 additional under-only bets, or Phase 1B safe-context join if Chase wants park/starter/team attribution now."

    benchmark_lines = []
    if direction_df is not None and not direction_df.empty:
        cols = ["config", "group", "n", "roi", "hit_rate", "avg_odds", "avg_decimal_odds", "avg_implied_prob", "avg_edge"]
        avail = [c for c in cols if c in direction_df.columns]
        for _, row in direction_df[direction_df.get("group", "") == "under"][avail].iterrows():
            benchmark_lines.append(
                f"- {row.get('config')} ({row.get('group')}): n={int(row.get('n'))}, ROI={row.get('roi'):.3f}, hit_rate={row.get('hit_rate'):.3f}, avg_odds={row.get('avg_odds'):.1f}, avg_decimal_odds={row.get('avg_decimal_odds'):.3f}, avg_implied_prob={row.get('avg_implied_prob'):.3f}, avg_edge={row.get('avg_edge'):.3f}"
            )

    material_lines = []
    if material_any:
        for _, row in material_tests[material_tests["material"].fillna(False)].iterrows():
            direction_note = ""
            if pd.notna(row.get("win_median")) and pd.notna(row.get("loss_median")):
                direction_note = f", win_median={row.get('win_median'):.3f}, loss_median={row.get('loss_median'):.3f}"
            material_lines.append(f"- {row.get('config')} / {row.get('dimension')}: p={row.get('p_value'):.4g}{direction_note}")

    drift_lines = []
    for d in drifts:
        severity = d.get("decay_watchlist_severity", "none")
        drift_lines.append(f"- {d.get('config')}: r={d.get('spearman_r')}, p={d.get('spearman_p')}, decay_detected={d.get('decay_detected')}, watchlist={d.get('decay_watchlist')} ({severity})")

    lines = [
        "# MLB Batter Hits Phase 1 Diagnostic Summary",
        "",
        "## Decision",
        f"- Primary decision: {primary}",
        f"- Prerequisite: {prereq}",
        "- Status: plausibly profitable but unconfirmed until CLV/line-history evidence exists.",
        "",
        "## Benchmark odds audit",
        *(benchmark_lines or ["- No benchmark direction rows available."]),
        "",
        "## Drift interpretation",
        "- Non-significant negative Spearman values are underpowered warnings, not proof of stability.",
        "- r <= -0.7 triggers retraining-review watchlist at any sample size; -0.7 < r <= -0.5 is early_warning_underpowered.",
        *(drift_lines or ["- No drift rows available."]),
        "",
        "## High-edge win/loss material screens",
        *(material_lines or ["- No material high-edge win/loss dimension under p < 0.01 screen."]),
        "",
        "## Market-asymmetry caution",
        "- Full-prediction under miscalibration plus near-zero over-side volume can mean the edge is market-asymmetry/selection driven: the model may be less wrong than the market rather than absolutely well-calibrated.",
        "- Do not globally recalibrate this away; use CLV evidence to determine whether the edge is real enough to gate Phase 2.",
        "",
        "## Uncertainty policy",
        "- ROI, hit-rate, profit, and calibration intervals use block bootstrap by game_date, not naive iid rows.",
        "- Same-day bets share slate/weather/park/lineup effects; date-block CIs are intentionally wider and more honest.",
        "",
        "## Multiple-comparison caveat",
        "- Deciles, line buckets, weekly buckets, bookmaker splits, and high-edge win/loss dimensions are hypothesis-generating, not confirmatory.",
        "- Any stratified finding that would drive feature work needs confirmation on a held-out/newer window or Phase 1B safe-context join.",
        "",
        "## CLV",
        "- CLV/proxy table created." if clv_available else "- CLV unavailable in saved sweeps; Phase 1B line-history/closing-price join is the next gate before Phase 2 feature work.",
        "",
        "## Output artifacts",
    ]
    for name, path in artifacts.items():
        lines.append(f"- {name}: `{path}`")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> Path:
    sweep_dir = Path(args.sweep_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) if args.output_dir else Path(f"backtest_results/mlb_batter_hits_residual_diagnostics_{timestamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    under_runs = discover_runs(sweep_dir, flat_stake=args.flat_stake)
    selected = select_benchmark_runs(under_runs)
    artifacts: dict[str, str] = {}

    full_cal, bet_cal, edge_rows, line_rows, weekly_rows, third_rows, book_rows, high_rows, high_tests = [], [], [], [], [], [], [], [], []
    drifts = []
    clv_frames = []

    for run_obj in selected:
        cfg = run_obj.label
        full_cal.append(calibration_table(cfg, run_obj.predictions, "under", args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap))
        bet_cal.append(selected_bet_calibration(cfg, run_obj.bets, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap))
        edge_rows.append(edge_bin_table(cfg, run_obj.bets, args.flat_stake, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap))
        line_rows.append(line_bucket_table(cfg, run_obj.bets, args.flat_stake, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap))
        weekly, thirds, drift = time_bucket_tables(cfg, run_obj.bets, args.flat_stake, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap)
        drifts.append({"config": cfg, **drift})
        weekly_rows.append(weekly)
        third_rows.append(thirds)
        book_rows.append(bookmaker_table(cfg, run_obj.bets, args.flat_stake, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap))

        he = run_obj.bets[run_obj.bets["edge"] >= args.high_edge_threshold].copy()
        if not he.empty:
            he["line_bucket"] = line_bucket(he["line"])
            he["time_bucket"] = "all"
            if "game_date" in he.columns:
                d = pd.to_datetime(he["game_date"])
                he["time_bucket"] = ((d - d.min()).dt.days // 7 + 1).map(lambda x: f"week_{x}")
            he["odds_bucket"] = odds_bucket(he["odds"])
            he["model_prob_bucket"] = probability_bucket(he["model_prob"], "model")
            he["edge_bucket"] = assign_adaptive_edge_bins(he, min_bin_size=5)
            he["config"] = cfg
            high_rows.append(he)
            high_tests.extend(high_edge_win_loss_tests(he, config=cfg))

        clv = compute_clv_table(run_obj.bets)
        if clv is not None:
            clv["config"] = cfg
            clv_frames.append(clv)

    outputs = {
        "full_prediction_calibration": pd.concat(full_cal, ignore_index=True) if full_cal else pd.DataFrame(),
        "bet_probability_calibration_selected": pd.concat(bet_cal, ignore_index=True) if bet_cal else pd.DataFrame(),
        "edge_bin_performance": pd.concat(edge_rows, ignore_index=True) if edge_rows else pd.DataFrame(),
        "line_bucket_residuals": pd.concat(line_rows, ignore_index=True) if line_rows else pd.DataFrame(),
        "time_bucket_performance": pd.concat(weekly_rows + third_rows, ignore_index=True) if weekly_rows or third_rows else pd.DataFrame(),
        "bookmaker_performance": pd.concat(book_rows, ignore_index=True) if book_rows else pd.DataFrame(),
        "high_edge_bets": pd.concat(high_rows, ignore_index=True) if high_rows else pd.DataFrame(),
        "high_edge_win_loss_comparison": pd.DataFrame(high_tests, columns=WIN_LOSS_SCHEMA),
        "drift_detection": pd.DataFrame(drifts),
    }

    if clv_frames:
        outputs["clv_proxy"] = pd.concat(clv_frames, ignore_index=True)

    # Direction summary is config-level and can include optional dirs.
    direction_frames = [direction_summary("under", select_benchmark_runs(under_runs), "under", args.flat_stake, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap)]
    for opt_dir, direction in [(args.both_dir, "both"), (args.over_dir, "over")]:
        if opt_dir:
            runs = discover_runs(Path(opt_dir), flat_stake=args.flat_stake)
            direction_frames.append(direction_summary(direction, select_benchmark_runs(runs), direction, args.flat_stake, args.bootstrap_samples, args.ci_level, args.production_min_bets, args.allow_iid_bootstrap))
    outputs["direction_summary"] = pd.concat(direction_frames, ignore_index=True)

    for name, df in outputs.items():
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        artifacts[name] = str(path)

    write_summary(
        output_dir / "diagnostic_summary.md",
        artifacts,
        drifts,
        clv_available="clv_proxy" in outputs,
        material_tests=outputs["high_edge_win_loss_comparison"],
        bookmaker_flags=outputs["bookmaker_performance"],
        direction_df=outputs["direction_summary"],
    )
    artifacts["diagnostic_summary"] = str(output_dir / "diagnostic_summary.md")
    print(f"Wrote diagnostics to {output_dir}")
    return output_dir


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze MLB batter_hits residuals from saved sweep outputs")
    parser.add_argument("--sweep-dir", required=True)
    parser.add_argument("--both-dir")
    parser.add_argument("--over-dir")
    parser.add_argument("--min-bets", type=int, default=30)
    parser.add_argument("--production-min-bets", type=int, default=100)
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--ci-level", type=float, default=0.95)
    parser.add_argument("--flat-stake", type=float, default=100.0)
    parser.add_argument("--high-edge-threshold", type=float, default=0.15)
    parser.add_argument("--output-dir")
    parser.add_argument("--allow-iid-bootstrap", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    run(parse_args())
