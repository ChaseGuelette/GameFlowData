"""Diagnostics for MLB quote-clean red flags.

Read-only local diagnostic. Intended to be run from native Windows PowerShell with
LOCAL_DATABASE_URL set and the Windows venv active/interpreter.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.backtesting.mlb.run_mlb_sweep import (  # noqa: E402
    SweepConfig,
    _fetch_lines_for_date,
    precompute_mlb_base_probs,
    run_shared_phases,
    run_single_config_fast_mlb,
)
from src.db.client import get_engine  # noqa: E402
from src.models.mlb.mlb_feature_store import MLBFeatureStore  # noqa: E402
from src.models.mlb.mlb_model_suite import MLBModelSuite  # noqa: E402

START = date(2026, 4, 13)
END = date(2026, 5, 10)
STATS = ["pitcher_strikeouts"]
QUOTE_CUTOFF = "13:30"
CONFIG = SweepConfig(tau=0.75, edge_threshold=0.02, kelly_fraction=0.125, z_max=0.25, max_weight=0.65)

MODELS = {
    "static": {
        "artifact": "src/models/mlb/artifacts/mlb_run_20260513_111207",
        "flat_bets": "backtest_results/quote_clean_2026_static_fixed_flat100_20260513/config_01_tau0.75_edge0.02_kelly0.125/bets.csv",
        "flat_predictions": "backtest_results/quote_clean_2026_static_fixed_flat100_20260513/config_01_tau0.75_edge0.02_kelly0.125/predictions.csv",
    },
    "hook": {
        "artifact": "src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657",
        "flat_bets": "backtest_results/quote_clean_2026_hook_deep_start_l30_fixed_flat100_20260513/config_01_tau0.75_edge0.02_kelly0.125/bets.csv",
        "flat_predictions": "backtest_results/quote_clean_2026_hook_deep_start_l30_fixed_flat100_20260513/config_01_tau0.75_edge0.02_kelly0.125/predictions.csv",
    },
}

OUT_JSON = Path("reports/mlb_quote_clean_red_flag_diagnostics_20260513.json")
OUT_DROPPED = Path("reports/mlb_quote_clean_dropped_predictions_20260513.csv")
OUT_SNAPSHOT = Path("reports/mlb_quote_clean_snapshot_delta_bets_20260513.csv")


def key_for_prediction(pred) -> str:
    return "|".join([str(pred.game_date), str(pred.player_id), str(pred.game_id), str(pred.stat)])


def key_for_row(row: pd.Series) -> str:
    return "|".join([str(row["game_date"]), str(int(row["player_id"])), str(int(row["game_id"])), str(row["stat"])])


def metrics_from_result(result):
    m = result.metrics
    return {
        "total_bets": int(m.total_bets),
        "wins": int(m.wins),
        "losses": int(m.losses),
        "hit_rate": float(m.hit_rate),
        "roi": float(m.roi),
        "total_profit": float(m.total_profit),
        "total_staked": float(m.total_staked),
        "sharpe_ratio": float(m.sharpe_ratio),
        "max_drawdown": float(m.max_drawdown),
    }


def get_game_ids_by_date(engine):
    q = text("""
        SELECT game_date, game_id
        FROM mlb_game_schedule
        WHERE game_date BETWEEN :start AND :end
          AND status != 'Cancelled'
        ORDER BY game_date, game_id
    """)
    out: dict[date, list[int]] = {}
    with engine.connect() as conn:
        for row in conn.execute(q, {"start": START, "end": END}):
            out.setdefault(row[0], []).append(int(row[1]))
    return out


def get_game_start_map(engine):
    cols = []
    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'mlb_game_schedule'
        """))]
    candidates = [
        "game_time_utc", "game_datetime", "game_time", "start_time", "commence_time", "scheduled_time",
        "game_date_time", "game_timestamp", "first_pitch_time",
    ]
    chosen = next((c for c in candidates if c in cols), None)
    if not chosen:
        return None, {}
    q = text(f"""
        SELECT game_id, {chosen} AS game_start
        FROM mlb_game_schedule
        WHERE game_date BETWEEN :start AND :end
    """)
    out = {}
    with engine.connect() as conn:
        for row in conn.execute(q, {"start": START, "end": END}):
            out[int(row[0])] = row[1]
    return chosen, out


def diagnose_model(name: str, info: dict, engine, game_ids_by_date):
    pitcher_fs = MLBFeatureStore(engine)
    suite = MLBModelSuite.from_directory(info["artifact"], n_samples=5000)
    date_list, date_predictions, quote_lines, date_actuals = run_shared_phases(
        engine=engine,
        pitcher_feature_store=pitcher_fs,
        batter_feature_store=None,
        suite=suite,
        start_date=START,
        end_date=END,
        stats=STATS,
        quote_clean_cutoff_time_et=QUOTE_CUTOFF,
    )
    all_keys = {key_for_prediction(pred) for preds in date_predictions.values() for pred in preds}
    qc_df = precompute_mlb_base_probs(date_list, date_predictions, quote_lines, date_actuals)
    qc_keys = set()
    if not qc_df.empty:
        qc_df = qc_df.copy()
        qc_df["pred_key"] = qc_df.apply(key_for_row, axis=1)
        qc_keys = set(qc_df["pred_key"])
    dropped_keys = all_keys - qc_keys

    legacy_lines = {}
    for gd, gids in game_ids_by_date.items():
        legacy_lines[gd] = _fetch_lines_for_date(engine, gids, STATS, quote_clean_cutoff_ts=None)
    legacy_df = precompute_mlb_base_probs(date_list, date_predictions, legacy_lines, date_actuals)
    legacy_drop_df = legacy_df.iloc[0:0].copy()
    if not legacy_df.empty:
        legacy_df = legacy_df.copy()
        legacy_df["pred_key"] = legacy_df.apply(key_for_row, axis=1)
        legacy_drop_df = legacy_df[legacy_df["pred_key"].isin(dropped_keys)].copy()

    # Evaluate dropped subset with legacy fallback pricing (flat and Kelly diagnostic only).
    dropped_flat = run_single_config_fast_mlb(
        config=CONFIG,
        precomputed_df=legacy_drop_df,
        game_dates=date_list,
        starting_bankroll=10000.0,
        flat_bet_size=100.0,
    )
    dropped_kelly = run_single_config_fast_mlb(
        config=CONFIG,
        precomputed_df=legacy_drop_df,
        game_dates=date_list,
        starting_bankroll=10000.0,
        flat_bet_size=None,
    )

    dropped_rows = []
    for key in sorted(dropped_keys):
        gd, player_id, game_id, stat = key.split("|")
        dropped_rows.append({
            "model": name,
            "game_date": gd,
            "player_id": player_id,
            "game_id": game_id,
            "stat": stat,
            "has_legacy_fallback_line": key in set(legacy_drop_df.get("pred_key", [])) if not legacy_drop_df.empty else False,
        })

    return {
        "date_count": len(date_list),
        "all_prediction_count": len(all_keys),
        "quote_clean_row_count": len(qc_keys),
        "dropped_prediction_count": len(dropped_keys),
        "dropped_prediction_pct": len(dropped_keys) / len(all_keys) if all_keys else None,
        "legacy_rows_for_dropped_predictions": int(len(legacy_drop_df)),
        "dropped_flat100_legacy_fallback_metrics": metrics_from_result(dropped_flat),
        "dropped_kelly_legacy_fallback_metrics": metrics_from_result(dropped_kelly),
        "all_keys": all_keys,
        "qc_keys": qc_keys,
        "dropped_keys": dropped_keys,
        "dropped_rows": dropped_rows,
    }


def load_flat_with_snapshot(name: str, info: dict, start_col: str | None, start_map: dict):
    bets = pd.read_csv(info["flat_bets"])
    preds = pd.read_csv(info["flat_predictions"])
    join_cols = ["game_date", "player_id", "game_id", "stat", "line", "bookmaker"]
    for df in (bets, preds):
        df["game_date"] = pd.to_datetime(df["game_date"]).dt.date.astype(str)
        df["player_id"] = df["player_id"].astype(int)
        df["game_id"] = df["game_id"].astype(int)
        df["line"] = df["line"].astype(float)
    merged = bets.merge(
        preds[join_cols + ["selected_snapshot_time", "over_snapshot_time", "under_snapshot_time"]],
        on=join_cols,
        how="left",
        suffixes=("", "_pred"),
    )
    merged["model"] = name
    if start_col:
        merged["game_start_time"] = merged["game_id"].map(start_map)
        snap = pd.to_datetime(merged["selected_snapshot_time"], utc=True, errors="coerce")
        start = pd.to_datetime(merged["game_start_time"], utc=True, errors="coerce")
        merged["snapshot_to_start_hours"] = (start - snap).dt.total_seconds() / 3600.0
    return merged


def bucket_metrics(df: pd.DataFrame, bucket_col: str):
    out = {}
    for bucket, g in df.groupby(bucket_col, dropna=False):
        staked = float(g["stake"].sum())
        profit = float(g["profit"].sum())
        wins = int((g["outcome"] == "win").sum())
        losses = int((g["outcome"] == "loss").sum())
        out[str(bucket)] = {
            "bets": int(len(g)),
            "wins": wins,
            "losses": losses,
            "hit_rate": wins / (wins + losses) if wins + losses else None,
            "profit": profit,
            "staked": staked,
            "roi": profit / staked if staked else None,
        }
    return out


def main():
    engine = get_engine(local=True)
    game_ids_by_date = get_game_ids_by_date(engine)
    start_col, start_map = get_game_start_map(engine)

    model_diags = {}
    all_dropped_rows = []
    for name, info in MODELS.items():
        diag = diagnose_model(name, info, engine, game_ids_by_date)
        all_dropped_rows.extend(diag.pop("dropped_rows"))
        # JSON cannot serialize sets.
        diag["all_keys"] = sorted(diag["all_keys"])
        diag["qc_keys"] = sorted(diag["qc_keys"])
        diag["dropped_keys"] = sorted(diag["dropped_keys"])
        model_diags[name] = diag

    static_dropped = set(model_diags["static"]["dropped_keys"])
    hook_dropped = set(model_diags["hook"]["dropped_keys"])
    dropout_overlap = {
        "same_dropped_count": len(static_dropped & hook_dropped),
        "static_only_dropped_count": len(static_dropped - hook_dropped),
        "hook_only_dropped_count": len(hook_dropped - static_dropped),
        "jaccard": len(static_dropped & hook_dropped) / len(static_dropped | hook_dropped) if (static_dropped | hook_dropped) else None,
    }

    snapshot_frames = [load_flat_with_snapshot(name, info, start_col, start_map) for name, info in MODELS.items()]
    snapshot_df = pd.concat(snapshot_frames, ignore_index=True)
    if "snapshot_to_start_hours" in snapshot_df.columns:
        snapshot_df["snapshot_age_bucket"] = pd.cut(
            snapshot_df["snapshot_to_start_hours"],
            bins=[-999, 0, 1, 3, 6, 12, 24, 9999],
            labels=["after_start", "0-1h", "1-3h", "3-6h", "6-12h", "12-24h", "24h+"],
        ).astype(str)
    else:
        snapshot_df["snapshot_age_bucket"] = "unknown_no_start_column"

    snapshot_summary = {
        name: {
            "start_column": start_col,
            "overall": bucket_metrics(g, "model").get(name),
            "by_snapshot_age_bucket": bucket_metrics(g, "snapshot_age_bucket"),
            "by_side": bucket_metrics(g, "side"),
        }
        for name, g in snapshot_df.groupby("model")
    }

    if all_dropped_rows:
        pd.DataFrame(all_dropped_rows).to_csv(OUT_DROPPED, index=False)
    snapshot_df.to_csv(OUT_SNAPSHOT, index=False)

    report = {
        "window": {"start": str(START), "end": str(END), "quote_cutoff_et": QUOTE_CUTOFF},
        "config": asdict(CONFIG),
        "models": model_diags,
        "dropout_overlap": dropout_overlap,
        "snapshot_summary": snapshot_summary,
        "outputs": {
            "dropped_predictions_csv": str(OUT_DROPPED),
            "snapshot_delta_bets_csv": str(OUT_SNAPSHOT),
        },
    }
    OUT_JSON.write_text(json.dumps(report, indent=2, sort_keys=True, default=str))
    print(json.dumps({
        "dropout_overlap": dropout_overlap,
        "model_counts": {k: {kk: v[kk] for kk in ["all_prediction_count", "quote_clean_row_count", "dropped_prediction_count", "dropped_prediction_pct", "legacy_rows_for_dropped_predictions"]} for k, v in model_diags.items()},
        "dropped_fallback_metrics": {k: {"flat100": v["dropped_flat100_legacy_fallback_metrics"], "kelly": v["dropped_kelly_legacy_fallback_metrics"]} for k, v in model_diags.items()},
        "snapshot_summary": snapshot_summary,
        "report_json": str(OUT_JSON),
    }, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
