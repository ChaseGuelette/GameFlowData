#!/usr/bin/env python
"""Diagnostics for MLB pitcher-K workload/leash ablation.

Read-only analysis:
- Loads train/cal data through MLBFeatureStore using local DB.
- Appends predicted-IP features from an existing ip_feature_model artifact.
- Computes Pearson/Spearman/MI vs actual_ip and actual_so.
- Computes residual correlations after controlling for rolling IP features.
- Checks standalone predicted-IP quantile coverage and pinball loss.

Usage:
    venv/Scripts/python.exe scripts/mlb_workload_leash_diagnostics.py --local --output backtest_results/mlb_workload_leash_diagnostics_20260513.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.feature_selection import mutual_info_regression
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.db.client import get_engine  # noqa: E402
from src.models.mlb.mlb_feature_store import MLBFeatureStore  # noqa: E402
from src.models.mlb.mlb_quantile_trainer import MLBPitcherKPipeline  # noqa: E402

HOOK_FEATURES = [
    "team_starter_avg_ip_l30",
    "team_starter_short_hook_rate_l30",
    "team_starter_deep_start_rate_l30",
]
EXISTING_WORKLOAD_FEATURES = [
    "team_starter_avg_ip_l10",
    "team_starter_short_start_rate_l10",
    "team_starter_avg_pitches_l10",
    "pitcher_avg_ip_l5",
    "pitcher_avg_ip_szn",
    "pitcher_avg_pitches_per_start_l5",
    "pitcher_short_start_rate_l5",
]
PREDICTED_IP_FEATURES = [
    "predicted_ip_q25",
    "predicted_ip_q50",
    "predicted_ip_spread",
    "predicted_ip_q25_delta",
]
CANDIDATE_FEATURES = HOOK_FEATURES + EXISTING_WORKLOAD_FEATURES + PREDICTED_IP_FEATURES
CONTROL_IP_FEATURES = ["pitcher_avg_ip_l5", "pitcher_avg_ip_szn"]
CONTROL_K_FEATURES = [
    "pitcher_avg_so_l5",
    "pitcher_avg_so_szn",
    "pitcher_avg_ip_l5",
    "pitcher_avg_ip_szn",
    "prop_line_pitcher_strikeouts",
]


def _safe_float(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _load_dataset(store: MLBFeatureStore, seasons: list[int]) -> pd.DataFrame:
    df = store.get_training_dataset(seasons=seasons)
    df = store.enrich_with_matchup_features(df)
    df = store._add_interaction_features(df)
    return df


def _append_ip_predictions(df: pd.DataFrame, ip_model_dir: Path) -> pd.DataFrame:
    pipeline = MLBPitcherKPipeline.load(str(ip_model_dir), model_name="pitcher_k")
    feature_names = pipeline.model.all_feature_names
    X = df.reindex(columns=feature_names, fill_value=0).fillna(0)
    preds = pipeline.predict(X)
    out = df.copy()
    out["predicted_ip_q25"] = preds["q25"].values
    out["predicted_ip_q50"] = preds["q50"].values
    out["predicted_ip_spread"] = preds["q75"].values - preds["q25"].values
    baseline = out.get("pitcher_avg_ip_l5", pd.Series(0.0, index=out.index)).fillna(0)
    out["predicted_ip_q25_delta"] = out["predicted_ip_q25"] - baseline
    return out


def _valid_xy(df: pd.DataFrame, feat: str, target: str) -> tuple[pd.Series, pd.Series]:
    x = pd.to_numeric(df[feat], errors="coerce")
    y = pd.to_numeric(df[target], errors="coerce")
    mask = x.notna() & y.notna()
    return x[mask], y[mask]


def _corrs(df: pd.DataFrame, feat: str, target: str) -> dict:
    if feat not in df.columns or target not in df.columns:
        return {"n": 0, "pearson": None, "spearman": None, "mi": None}
    x, y = _valid_xy(df, feat, target)
    if len(x) < 10 or x.nunique() < 2 or y.nunique() < 2:
        return {"n": int(len(x)), "pearson": None, "spearman": None, "mi": None}
    pear = pearsonr(x, y).statistic
    spear = spearmanr(x, y, nan_policy="omit").statistic
    try:
        mi = mutual_info_regression(x.to_numpy().reshape(-1, 1), y.to_numpy(), random_state=42)[0]
    except Exception:
        mi = None
    return {"n": int(len(x)), "pearson": _safe_float(pear), "spearman": _safe_float(spear), "mi": _safe_float(mi)}


def _residual_corr(df: pd.DataFrame, feat: str, target: str, controls: list[str]) -> dict:
    # Exclude the candidate feature from controls and de-duplicate columns; otherwise
    # pandas returns duplicate-name DataFrames for sub[feat]/sub[control].
    controls = [c for c in dict.fromkeys(controls) if c != feat]
    needed = list(dict.fromkeys([feat, target] + controls))
    if any(c not in df.columns for c in needed):
        return {"n": 0, "pearson": None, "spearman": None}
    sub = df[needed].apply(pd.to_numeric, errors="coerce").dropna()
    controls_present = [c for c in controls if c in sub.columns and sub[c].nunique() > 1]
    if len(sub) < 25 or not controls_present or sub[feat].nunique() < 2 or sub[target].nunique() < 2:
        return {"n": int(len(sub)), "pearson": None, "spearman": None}
    X = sub[controls_present]
    y_target = sub[target]
    y_feat = sub[feat]
    target_resid = y_target - LinearRegression().fit(X, y_target).predict(X)
    # residualize feature too, so this is partial-ish correlation beyond controls
    feat_resid = y_feat - LinearRegression().fit(X, y_feat).predict(X)
    if np.std(target_resid) == 0 or np.std(feat_resid) == 0:
        return {"n": int(len(sub)), "pearson": None, "spearman": None}
    pear = pearsonr(feat_resid, target_resid).statistic
    spear = spearmanr(feat_resid, target_resid, nan_policy="omit").statistic
    return {"n": int(len(sub)), "pearson": _safe_float(pear), "spearman": _safe_float(spear)}


def _pinball(y: pd.Series, qhat: pd.Series, q: float) -> float | None:
    sub = pd.concat([pd.to_numeric(y, errors="coerce"), pd.to_numeric(qhat, errors="coerce")], axis=1).dropna()
    if sub.empty:
        return None
    yy = sub.iloc[:, 0].to_numpy()
    pp = sub.iloc[:, 1].to_numpy()
    err = yy - pp
    return float(np.mean(np.maximum(q * err, (q - 1) * err)))


def _coverage(df: pd.DataFrame, pred_col: str, q: float) -> dict:
    sub = df[["actual_ip", pred_col]].apply(pd.to_numeric, errors="coerce").dropna()
    if sub.empty:
        return {"n": 0, "coverage": None, "target": q, "error": None, "pinball": None}
    cov = float((sub["actual_ip"] <= sub[pred_col]).mean())
    return {
        "n": int(len(sub)),
        "coverage": cov,
        "target": q,
        "error": cov - q,
        "pinball": _pinball(sub["actual_ip"], sub[pred_col], q),
    }


def _naive_pinball(df: pd.DataFrame, features: Iterable[str], q: float) -> dict:
    out = {}
    for feat in features:
        if feat in df.columns:
            out[feat] = _pinball(df["actual_ip"], df[feat], q)
    return out


def summarize_frame(df: pd.DataFrame) -> dict:
    out = {
        "rows": int(len(df)),
        "date_min": str(pd.to_datetime(df["game_date"]).min().date()) if "game_date" in df and len(df) else None,
        "date_max": str(pd.to_datetime(df["game_date"]).max().date()) if "game_date" in df and len(df) else None,
        "features": {},
        "ip_quantile_calibration": {},
    }
    for feat in CANDIDATE_FEATURES:
        if feat not in df.columns:
            continue
        out["features"][feat] = {
            "non_null": int(pd.to_numeric(df[feat], errors="coerce").notna().sum()),
            "mean": _safe_float(pd.to_numeric(df[feat], errors="coerce").mean()),
            "std": _safe_float(pd.to_numeric(df[feat], errors="coerce").std()),
            "actual_ip": _corrs(df, feat, "actual_ip"),
            "actual_so": _corrs(df, feat, "actual_so"),
            "resid_actual_ip_ctrl_rolling_ip": _residual_corr(df, feat, "actual_ip", CONTROL_IP_FEATURES),
            "resid_actual_so_ctrl_baseline_k": _residual_corr(df, feat, "actual_so", CONTROL_K_FEATURES),
        }
    if "predicted_ip_q25" in df.columns:
        out["ip_quantile_calibration"]["q25"] = _coverage(df, "predicted_ip_q25", 0.25)
    if "predicted_ip_q50" in df.columns:
        out["ip_quantile_calibration"]["q50"] = _coverage(df, "predicted_ip_q50", 0.50)
    out["ip_quantile_calibration"]["naive_pinball_q25"] = _naive_pinball(df, ["pitcher_avg_ip_l5", "pitcher_avg_ip_szn", "team_starter_avg_ip_l10", "team_starter_avg_ip_l30"], 0.25)
    out["ip_quantile_calibration"]["naive_pinball_q50"] = _naive_pinball(df, ["pitcher_avg_ip_l5", "pitcher_avg_ip_szn", "team_starter_avg_ip_l10", "team_starter_avg_ip_l30"], 0.50)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--train-seasons", nargs="+", type=int, default=[2024, 2025])
    parser.add_argument("--cal-season", type=int, default=2026)
    parser.add_argument("--cal-end-date", default="2026-04-12")
    parser.add_argument("--ip-model-dir", default="src/models/mlb/artifacts/ip_ablation_ip_only/mlb_run_20260513_121143/ip_feature_model")
    parser.add_argument("--output", default="backtest_results/mlb_workload_leash_diagnostics_20260513.json")
    args = parser.parse_args()

    engine = get_engine(local=args.local)
    store = MLBFeatureStore(engine)
    print(f"Loading train seasons {args.train_seasons}...")
    train_df = _load_dataset(store, args.train_seasons)
    print(f"Train rows: {len(train_df)}")
    print(f"Loading cal season {args.cal_season}...")
    cal_df = _load_dataset(store, [args.cal_season])
    pre = len(cal_df)
    cal_df = cal_df[pd.to_datetime(cal_df["game_date"]) <= pd.Timestamp(args.cal_end_date)].reset_index(drop=True)
    print(f"Cal rows: {len(cal_df)} filtered from {pre}; cutoff={args.cal_end_date}")

    ip_model_dir = Path(args.ip_model_dir)
    print(f"Appending IP predictions from {ip_model_dir}...")
    train_df = _append_ip_predictions(train_df, ip_model_dir)
    cal_df = _append_ip_predictions(cal_df, ip_model_dir)

    report = {
        "config": vars(args),
        "train": summarize_frame(train_df),
        "cal": summarize_frame(cal_df),
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, sort_keys=True))
    print(f"Wrote {out}")

    # Compact terminal summary for quick triage.
    for split in ["train", "cal"]:
        print(f"\n== {split.upper()} rows={report[split]['rows']} dates={report[split]['date_min']}..{report[split]['date_max']} ==")
        ranked = []
        for feat, metrics in report[split]["features"].items():
            ranked.append((
                feat,
                metrics["actual_ip"].get("pearson"),
                metrics["actual_ip"].get("spearman"),
                metrics["actual_ip"].get("mi"),
                metrics["resid_actual_ip_ctrl_rolling_ip"].get("pearson"),
                metrics["actual_so"].get("pearson"),
                metrics["actual_so"].get("mi"),
            ))
        ranked = sorted(ranked, key=lambda x: abs(x[1] or 0), reverse=True)
        print("feat\tactual_ip_r\tactual_ip_spear\tactual_ip_mi\tresid_ip_r\tactual_so_r\tactual_so_mi")
        for row in ranked:
            print("\t".join([str(row[0])] + ["" if v is None else f"{v:.4f}" for v in row[1:]]))
        print("IP calibration:", json.dumps(report[split]["ip_quantile_calibration"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
