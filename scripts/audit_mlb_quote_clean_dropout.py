#!/usr/bin/env python3
"""Audit MLB quote-clean prediction dropout after a sweep/replay.

This script is intentionally report-only: it does not retrain models, mutate the
DB, or change backtest math. It rebuilds the all-prediction denominator from the
same shared replay path used by `run_mlb_sweep.py`, then classifies why each
prediction did or did not have a quote-clean line / bettable edge.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from datetime import date, datetime
from datetime import time as datetime_time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.backtesting.mlb.line_selection import DEFAULT_EXCLUDED_BOOKMAKERS, STAT_TO_MARKET_KEY

DROPOUT_BUCKETS = [
    "clean_quote_available",
    "clean_quote_exists_below_edge",
    "no_raw_prop_rows",
    "only_excluded_books",
    "only_after_cutoff",
    "only_post_commence",
    "no_paired_over_under",
    "unknown_unclassified",
]

REQUIRED_SAVED_PREDICTION_COLUMNS = {
    "player_id",
    "game_id",
    "stat",
    "line",
    "bookmaker",
    "over_odds",
    "under_odds",
    "selected_snapshot_time",
    "over_snapshot_time",
    "under_snapshot_time",
}

REQUIRED_SAVED_BET_COLUMNS = {
    "player_id",
    "game_id",
    "stat",
    "side",
    "line",
    "odds",
    "bookmaker",
}

KEY_COLUMNS = ["player_id", "game_id", "stat"]


def market_key_for_stat(stat: str) -> str:
    """Translate an internal stat key to the odds table market key."""
    return STAT_TO_MARKET_KEY.get(stat, stat)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_quote_clean_cutoff_ts(game_date: str | date, cutoff_time_et: str) -> pd.Timestamp:
    """Build timezone-aware UTC timestamp for a YYYY-MM-DD and ET HH:MM cutoff."""
    if isinstance(game_date, str):
        game_date = parse_date(game_date)
    try:
        hour_s, minute_s = cutoff_time_et.split(":", 1)
        cutoff_t = datetime_time(hour=int(hour_s), minute=int(minute_s))
    except Exception as exc:
        raise ValueError(f"Invalid --quote-cutoff-time-et={cutoff_time_et!r}; expected HH:MM") from exc
    return pd.Timestamp(datetime.combine(game_date, cutoff_t, tzinfo=ZoneInfo("America/New_York"))).tz_convert("UTC")


def _to_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def normalize_raw_props(raw_props: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw prop rows for deterministic dropout classification."""
    out = raw_props.copy()
    if out.empty:
        return out
    for col in ["snapshot_time", "inserted_at", "market_last_update", "commence_time"]:
        if col in out.columns:
            out[col] = _to_utc(out[col])
    if "snapshot_time" not in out.columns:
        out["snapshot_time"] = pd.NaT
    if "inserted_at" not in out.columns:
        out["inserted_at"] = pd.NaT
    out["effective_snapshot_time"] = out["snapshot_time"].fillna(out["inserted_at"])
    if "market_last_update" not in out.columns:
        out["market_last_update"] = pd.NaT
    if "commence_time" not in out.columns:
        out["commence_time"] = pd.NaT
    out["bookmaker"] = out["bookmaker"].astype(str).str.lower()
    out["outcome_label"] = out["outcome_label"].astype(str).str.lower()
    out["market_key"] = out["market_key"].astype(str)
    for col in ["player_id", "game_id"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    return out


def _allowed_book_rows(raw: pd.DataFrame, excluded_books: Iterable[str]) -> pd.DataFrame:
    excluded = {str(x).lower() for x in excluded_books}
    return raw[~raw["bookmaker"].isin(excluded)].copy()


def _pre_cutoff_rows(raw: pd.DataFrame, cutoff_ts: pd.Timestamp | None) -> pd.DataFrame:
    if cutoff_ts is None:
        return raw.copy()
    cutoff_ts = pd.Timestamp(cutoff_ts).tz_convert("UTC") if pd.Timestamp(cutoff_ts).tzinfo else pd.Timestamp(cutoff_ts, tz="UTC")
    market_ok = raw["market_last_update"].isna() | (raw["market_last_update"] <= cutoff_ts)
    snapshot_ok = raw["effective_snapshot_time"].notna() & (raw["effective_snapshot_time"] <= cutoff_ts)
    return raw[market_ok & snapshot_ok].copy()


def _pre_commence_rows(raw: pd.DataFrame) -> pd.DataFrame:
    commence_missing = raw["commence_time"].isna()
    market_ok = commence_missing | raw["market_last_update"].isna() | (raw["market_last_update"] < raw["commence_time"])
    snapshot_ok = commence_missing | (raw["effective_snapshot_time"] < raw["commence_time"])
    return raw[market_ok & snapshot_ok].copy()


def find_atomic_clean_quotes(
    raw_props: pd.DataFrame,
    cutoff_ts: pd.Timestamp | None,
    excluded_books: Iterable[str] = DEFAULT_EXCLUDED_BOOKMAKERS,
) -> pd.DataFrame:
    """Return same-book/same-line/same-snapshot Over+Under pairs.

    Atomic pairing is intentionally strict: Over and Under must share the same
    player/game/market/book/line/effective_snapshot_time.
    """
    raw = normalize_raw_props(raw_props)
    if raw.empty:
        return pd.DataFrame()
    raw = _allowed_book_rows(raw, excluded_books)
    raw = _pre_cutoff_rows(raw, cutoff_ts)
    raw = _pre_commence_rows(raw)
    if raw.empty:
        return pd.DataFrame()

    grouped = raw.groupby(
        ["player_id", "game_id", "market_key", "bookmaker", "line", "effective_snapshot_time"],
        dropna=False,
    )
    rows: list[dict] = []
    for key, group in grouped:
        labels = set(group["outcome_label"].str.lower())
        if not {"over", "under"}.issubset(labels):
            continue
        over = group[group["outcome_label"] == "over"].sort_values("market_last_update").iloc[-1]
        under = group[group["outcome_label"] == "under"].sort_values("market_last_update").iloc[-1]
        player_id, game_id, market_key, bookmaker, line, eff_snapshot = key
        rows.append(
            {
                "player_id": int(player_id),
                "game_id": int(game_id),
                "market_key": market_key,
                "bookmaker": bookmaker,
                "line": float(line),
                "over_odds": over.get("odds_american"),
                "under_odds": under.get("odds_american"),
                "selected_snapshot_time": eff_snapshot,
                "over_snapshot_time": over.get("effective_snapshot_time"),
                "under_snapshot_time": under.get("effective_snapshot_time"),
                "commence_time": over.get("commence_time") if pd.notna(over.get("commence_time")) else under.get("commence_time"),
            }
        )
    return pd.DataFrame(rows)


def _prediction_filter(df: pd.DataFrame, pred: pd.Series, market_key: str | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    mk = market_key if market_key is not None else market_key_for_stat(str(pred["stat"]))
    mask = (
        (pd.to_numeric(df["player_id"], errors="coerce").astype("Int64") == int(pred["player_id"]))
        & (pd.to_numeric(df["game_id"], errors="coerce").astype("Int64") == int(pred["game_id"]))
        & (df["market_key"].astype(str) == mk)
    )
    return df[mask]


def classify_prediction_dropout(
    prediction: pd.Series,
    raw_props_for_prediction: pd.DataFrame,
    clean_quotes_for_prediction: pd.DataFrame,
    placed_bet_keys: set[tuple[int, int, str]] | None,
    *,
    cutoff_ts: pd.Timestamp | None = None,
    excluded_books: Iterable[str] = DEFAULT_EXCLUDED_BOOKMAKERS,
) -> str:
    """Classify one prediction using the deterministic bucket precedence."""
    key = (int(prediction["player_id"]), int(prediction["game_id"]), str(prediction["stat"]))
    placed_bet_keys = placed_bet_keys or set()
    if clean_quotes_for_prediction is not None and not clean_quotes_for_prediction.empty:
        return "clean_quote_available" if key in placed_bet_keys else "clean_quote_exists_below_edge"

    raw = normalize_raw_props(raw_props_for_prediction)
    if raw.empty:
        return "no_raw_prop_rows"

    allowed = _allowed_book_rows(raw, excluded_books)
    if allowed.empty:
        return "only_excluded_books"

    cutoff_rows = _pre_cutoff_rows(allowed, cutoff_ts)
    if cutoff_rows.empty:
        return "only_after_cutoff"

    pre_commence = _pre_commence_rows(cutoff_rows)
    if pre_commence.empty:
        return "only_post_commence"

    return "no_paired_over_under"


def validate_saved_predictions_columns(path: Path) -> None:
    df = pd.read_csv(path, nrows=1)
    missing = REQUIRED_SAVED_PREDICTION_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing required quote audit columns: {sorted(missing)}")


def validate_saved_bets_columns(path: Path) -> None:
    df = pd.read_csv(path, nrows=1)
    missing = REQUIRED_SAVED_BET_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing required bet columns: {sorted(missing)}")


def flatten_date_predictions(date_predictions: dict[date, list[object]]) -> pd.DataFrame:
    rows: list[dict] = []
    for game_date, preds in date_predictions.items():
        for pred in preds:
            rows.append(
                {
                    "game_date": game_date,
                    "player_id": int(pred.player_id),
                    "game_id": int(pred.game_id),
                    "stat": str(pred.stat),
                    "market_key": market_key_for_stat(str(pred.stat)),
                }
            )
    return pd.DataFrame(rows)


def flatten_date_lines(date_lines: dict[date, pd.DataFrame]) -> pd.DataFrame:
    frames = []
    for game_date, df in date_lines.items():
        if df is not None and not df.empty:
            part = df.copy()
            part["game_date"] = game_date
            frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def collect_saved_bet_keys(sweep_output_dir: Path | None) -> set[tuple[int, int, str]]:
    keys: set[tuple[int, int, str]] = set()
    if not sweep_output_dir:
        return keys
    bet_files = []
    if (sweep_output_dir / "bets.csv").exists():
        bet_files.append(sweep_output_dir / "bets.csv")
    bet_files.extend(sorted(sweep_output_dir.glob("config_*/bets.csv")))
    for path in bet_files:
        validate_saved_bets_columns(path)
        bets = pd.read_csv(path, usecols=["player_id", "game_id", "stat"])
        for row in bets.itertuples(index=False):
            keys.add((int(row.player_id), int(row.game_id), str(row.stat)))
    return keys


def validate_sweep_outputs(sweep_output_dir: Path | None) -> None:
    if not sweep_output_dir:
        return
    pred_files = []
    if (sweep_output_dir / "predictions.csv").exists():
        pred_files.append(sweep_output_dir / "predictions.csv")
    pred_files.extend(sorted(sweep_output_dir.glob("config_*/predictions.csv")))
    for path in pred_files:
        validate_saved_predictions_columns(path)
    for path in sorted(sweep_output_dir.glob("**/bets.csv")):
        validate_saved_bets_columns(path)


def fetch_raw_props_for_predictions(engine, predictions: pd.DataFrame, batch_size: int = 50) -> pd.DataFrame:
    """Fetch raw prop rows by indexed game_id chunks for prediction keys."""
    if predictions.empty:
        return pd.DataFrame()
    game_ids = [int(x) for x in sorted(predictions["game_id"].dropna().unique())]
    player_ids = [int(x) for x in sorted(predictions["player_id"].dropna().unique())]
    market_keys = sorted(predictions["market_key"].dropna().unique())
    rows = []
    for start in range(0, len(game_ids), batch_size):
        gid_batch = game_ids[start : start + batch_size]
        params: dict[str, object] = {}
        gid_ph = []
        for i, gid in enumerate(gid_batch):
            params[f"gid_{i}"] = gid
            gid_ph.append(f":gid_{i}")
        pid_ph = []
        for i, pid in enumerate(player_ids):
            params[f"pid_{i}"] = pid
            pid_ph.append(f":pid_{i}")
        mk_ph = []
        for i, mk in enumerate(market_keys):
            params[f"mk_{i}"] = mk
            mk_ph.append(f":mk_{i}")
        sql = text(
            f"""
            SELECT
                player_id, game_id, bookmaker, market_key, line, outcome_label,
                odds_american, snapshot_time, inserted_at, market_last_update,
                commence_time
            FROM mlb_raw_player_props
            WHERE game_id IN ({', '.join(gid_ph)})
              AND player_id IN ({', '.join(pid_ph)})
              AND market_key IN ({', '.join(mk_ph)})
              AND player_id IS NOT NULL
            """
        )
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '300000'"))
            rows.append(pd.read_sql(sql, conn, params=params))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def fetch_props_for_predictions(
    engine,
    predictions: pd.DataFrame,
    *,
    batch_size: int = 50,
    source_table: str = "mlb_raw_player_props",
) -> pd.DataFrame:
    if source_table == "mlb_raw_player_props":
        return fetch_raw_props_for_predictions(engine, predictions, batch_size=batch_size)
    if source_table != "mlb_player_props_clv_snapshots":
        raise ValueError(f"Unsupported audit prop source table: {source_table}")
    if predictions.empty:
        return pd.DataFrame()
    game_ids = [int(x) for x in sorted(predictions["game_id"].dropna().unique())]
    player_ids = [int(x) for x in sorted(predictions["player_id"].dropna().unique())]
    market_keys = sorted(predictions["market_key"].dropna().unique())
    rows = []
    for start in range(0, len(game_ids), batch_size):
        gid_batch = game_ids[start : start + batch_size]
        params: dict[str, object] = {}
        gid_ph = []
        for i, gid in enumerate(gid_batch):
            params[f"gid_{i}"] = gid
            gid_ph.append(f":gid_{i}")
        pid_ph = []
        for i, pid in enumerate(player_ids):
            params[f"pid_{i}"] = pid
            pid_ph.append(f":pid_{i}")
        mk_ph = []
        for i, mk in enumerate(market_keys):
            params[f"mk_{i}"] = mk
            mk_ph.append(f":mk_{i}")
        sql = text(
            f"""
            SELECT
                player_id, game_id, bookmaker, market_key, line, outcome_label,
                odds_american, snapshot_time, NULL::timestamptz AS inserted_at,
                market_last_update, commence_time
            FROM mlb_player_props_clv_snapshots
            WHERE game_id IN ({', '.join(gid_ph)})
              AND player_id IN ({', '.join(pid_ph)})
              AND market_key IN ({', '.join(mk_ph)})
              AND game_id IS NOT NULL
              AND player_id IS NOT NULL
            """
        )
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '300000'"))
            rows.append(pd.read_sql(sql, conn, params=params))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def build_dropout_rows(
    predictions: pd.DataFrame,
    raw_props: pd.DataFrame,
    clean_quotes: pd.DataFrame,
    placed_bet_keys: set[tuple[int, int, str]],
    cutoff_time_et: str,
) -> pd.DataFrame:
    raw = normalize_raw_props(raw_props)
    clean = clean_quotes.copy() if clean_quotes is not None else pd.DataFrame()
    rows = []
    for _, pred in predictions.iterrows():
        market_key = market_key_for_stat(str(pred["stat"]))
        cutoff_ts = build_quote_clean_cutoff_ts(pred["game_date"], cutoff_time_et)
        pred_raw = _prediction_filter(raw, pred, market_key)
        pred_clean = _prediction_filter(clean, pred, market_key) if not clean.empty else pd.DataFrame()
        bucket = classify_prediction_dropout(
            pred,
            pred_raw,
            pred_clean,
            placed_bet_keys,
            cutoff_ts=cutoff_ts,
        )
        row = pred.to_dict()
        row.update({"market_key": market_key, "dropout_bucket": bucket})
        rows.append(row)
    return pd.DataFrame(rows)


def summarize_and_write_outputs(dropout_rows: pd.DataFrame, clean_quotes: pd.DataFrame, output_dir: Path, cutoff_time_et: str) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = (
        dropout_rows["dropout_bucket"].value_counts().reindex(DROPOUT_BUCKETS, fill_value=0).rename_axis("dropout_bucket").reset_index(name="count")
    )
    total = int(summary["count"].sum())
    summary["pct"] = summary["count"] / total if total else 0.0
    summary.to_csv(output_dir / "dropout_summary_by_bucket.csv", index=False)
    dropout_rows.to_csv(output_dir / "dropout_rows.csv", index=False)
    if not clean_quotes.empty:
        clean_quotes.to_csv(output_dir / "selected_clean_quotes.csv", index=False)
    else:
        pd.DataFrame().to_csv(output_dir / "selected_clean_quotes.csv", index=False)

    for col, filename in [
        ("game_date", "dropout_by_date.csv"),
        ("game_id", "dropout_by_game.csv"),
    ]:
        if col in dropout_rows.columns:
            pd.crosstab(dropout_rows[col], dropout_rows["dropout_bucket"]).to_csv(output_dir / filename)
        else:
            pd.DataFrame().to_csv(output_dir / filename)

    if not clean_quotes.empty and "bookmaker" in clean_quotes.columns:
        clean_quotes.groupby("bookmaker").size().reset_index(name="clean_quotes").to_csv(output_dir / "dropout_by_bookmaker.csv", index=False)
    else:
        pd.DataFrame(columns=["bookmaker", "clean_quotes"]).to_csv(output_dir / "dropout_by_bookmaker.csv", index=False)

    cutoff_violations = 0
    commence_violations = 0
    if not clean_quotes.empty:
        selected_ts = _to_utc(clean_quotes["selected_snapshot_time"])
        if "game_date" in clean_quotes.columns:
            cutoffs = clean_quotes["game_date"].map(lambda x: build_quote_clean_cutoff_ts(x, cutoff_time_et))
            cutoff_violations = int((selected_ts > pd.to_datetime(cutoffs, utc=True)).sum())
        if "commence_time" in clean_quotes.columns:
            commence = _to_utc(clean_quotes["commence_time"])
            commence_violations = int((selected_ts >= commence).fillna(False).sum())

    if cutoff_violations or commence_violations:
        decision = "FAIL"
        reason = "Selected clean quotes include cutoff or commence-time violations."
    elif total and float(summary.loc[summary["dropout_bucket"] == "unknown_unclassified", "pct"].iloc[0]) > 0.01:
        decision = "WARN"
        reason = "Some predictions remain unknown/unclassified; inspect row-level output."
    else:
        decision = "PASS"
        reason = "Selected clean quotes have no detected cutoff/commence violations; dropout buckets are classified."

    audit_summary = {
        "decision": decision,
        "reason": reason,
        "total_predictions": total,
        "cutoff_violations": cutoff_violations,
        "commence_violations": commence_violations,
        "bucket_counts": dict(zip(summary["dropout_bucket"], summary["count"])),
    }
    (output_dir / "audit_summary.json").write_text(json.dumps(audit_summary, indent=2, default=str), encoding="utf-8")
    write_markdown_summary(output_dir / "audit_summary.md", audit_summary, summary)
    return audit_summary


def write_markdown_summary(path: Path, audit_summary: dict, summary: pd.DataFrame) -> None:
    lines = [
        "# MLB Quote-Clean Dropout Audit Summary",
        "",
        f"Decision: **{audit_summary['decision']}**",
        "",
        audit_summary["reason"],
        "",
        f"Total predictions: {audit_summary['total_predictions']}",
        f"Cutoff violations: {audit_summary['cutoff_violations']}",
        f"Commence violations: {audit_summary['commence_violations']}",
        "",
        "## Dropout buckets",
        "",
        "| Bucket | Count | Pct |",
        "|---|---:|---:|",
    ]
    for row in summary.itertuples(index=False):
        lines.append(f"| {row.dropout_bucket} | {int(row.count)} | {float(row.pct):.1%} |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- PASS means selected clean quotes had no detected cutoff/commence violations.",
            "- WARN means dropout is classified but should be manually reviewed for concentration or unknown buckets.",
            "- FAIL means timing/pairing violations block promotion evidence.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_audit(args: argparse.Namespace) -> dict:
    from src.backtesting.mlb.run_mlb_sweep import run_shared_phases
    from src.db.client import get_engine
    from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
    from src.models.mlb.mlb_feature_store import MLBFeatureStore
    from src.models.mlb.mlb_model_suite import MLBModelSuite

    engine = get_engine(local=args.local)
    suite = MLBModelSuite.from_directory(Path(args.model_dir))
    stats = args.stats
    has_batter_stats = any(str(s).startswith("batter_") and suite.has_stat(s) for s in stats)
    batter_feature_store = MLBBatterFeatureStore(engine) if has_batter_stats else None
    pitcher_feature_store = MLBFeatureStore(engine)

    game_dates, date_predictions, date_lines, _ = run_shared_phases(
        engine=engine,
        pitcher_feature_store=pitcher_feature_store,
        batter_feature_store=batter_feature_store,
        suite=suite,
        start_date=parse_date(args.start),
        end_date=parse_date(args.end),
        stats=stats,
        quote_clean_cutoff_time_et=args.quote_cutoff_time_et,
        quote_decision_policy=args.quote_decision_policy,
        quote_relative_minutes=args.quote_relative_minutes,
        line_source=args.line_source,
    )
    predictions = flatten_date_predictions(date_predictions)
    clean_quotes = flatten_date_lines(date_lines)
    raw_props = fetch_props_for_predictions(engine, predictions, batch_size=args.batch_size, source_table=args.line_source)
    sweep_dir = Path(args.sweep_output_dir) if args.sweep_output_dir else None
    validate_sweep_outputs(sweep_dir)
    placed_keys = collect_saved_bet_keys(sweep_dir)
    dropout_rows = build_dropout_rows(predictions, raw_props, clean_quotes, placed_keys, args.quote_cutoff_time_et)
    return summarize_and_write_outputs(dropout_rows, clean_quotes, Path(args.output_dir), args.quote_cutoff_time_et)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit MLB quote-clean prediction dropout")
    parser.add_argument("--model-dir", required=True, help="MLB model artifact directory")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--stats", nargs="+", required=True, help="MLB stat keys, e.g. batter_hits pitcher_strikeouts")
    parser.add_argument("--quote-cutoff-time-et", required=True, help="Decision cutoff HH:MM ET")
    parser.add_argument("--output-dir", required=True, help="Directory for audit outputs")
    parser.add_argument("--local", action="store_true", help="Use local Postgres")
    parser.add_argument("--direction", choices=["over", "under", "both"], default="both", help="Documentary direction filter for the audited sweep")
    parser.add_argument("--sweep-output-dir", default=None, help="Optional existing quote-clean sweep/config output directory")
    parser.add_argument("--edge", type=float, default=None, help="Optional edge threshold metadata")
    parser.add_argument("--tau", type=str, default=None, help="Optional BL tau metadata")
    parser.add_argument("--z-max", type=float, default=None, help="Optional BL z_max metadata")
    parser.add_argument("--max-weight", type=float, default=None, help="Optional BL max_weight metadata")
    parser.add_argument("--batch-size", type=int, default=50, help="game_id chunk size for raw prop queries")
    parser.add_argument(
        "--line-source",
        choices=["mlb_raw_player_props", "mlb_player_props_clv_snapshots"],
        default="mlb_raw_player_props",
        help="Odds table for quote-clean audit rebuild. Dense table requires linked game_id/player_id.",
    )
    parser.add_argument(
        "--quote-decision-policy",
        choices=["fixed_et", "skip_early_fixed_et", "relative_to_commence", "slate_or_tminus"],
        default="fixed_et",
        help="Decision-time policy used by the audited sweep.",
    )
    parser.add_argument("--quote-relative-minutes", type=int, default=60)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    summary = run_audit(args)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
