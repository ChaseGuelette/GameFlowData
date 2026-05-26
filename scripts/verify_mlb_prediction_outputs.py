#!/usr/bin/env python3
"""Verify MLB prediction outputs and Kalshi edge linkage.

Read-only production/local audit for the trading-readiness prediction linkage lane.
It fails loudly when scheduled MLB games have no predictions/samples, or when
open Kalshi MLB markets exist but edge refresh did not populate model fields.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify MLB prediction and Kalshi linkage outputs")
    parser.add_argument("--date", required=True, help="Target date YYYY-MM-DD")
    parser.add_argument("--sport", default="mlb", choices=["mlb"], help="Only mlb is supported")
    parser.add_argument("--remote", action="store_true", help="Use DATABASE_URL via src.db.client.get_engine()")
    parser.add_argument("--local", action="store_true", help="Use LOCAL_DATABASE_URL via src.db.client.get_engine(local=True)")
    parser.add_argument("--database-url", help="Explicit database URL override")
    parser.add_argument("--min-line-coverage", type=float, default=0.50, help="Minimum prediction line coverage when predictions exist")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary only")
    return parser.parse_args()


def _get_engine(args: argparse.Namespace):
    if args.database_url:
        return create_engine(args.database_url, pool_pre_ping=True, connect_args={"options": "-c statement_timeout=15000"})

    from src.db.client import get_engine

    if args.local and args.remote:
        raise SystemExit("Choose only one of --local or --remote")
    return get_engine(local=args.local)


def _scalar(conn, sql: str, params: dict) -> int:
    return int(conn.execute(text(sql), params).scalar() or 0)


def _rows(conn, sql: str, params: dict) -> list[dict]:
    return [dict(row._mapping) for row in conn.execute(text(sql), params).fetchall()]


def _table_exists(conn, table: str) -> bool:
    return bool(conn.execute(text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = :table
    """), {"table": table}).fetchone())


def _columns(conn, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table
    """), {"table": table}).fetchall()
    return {row[0] for row in rows}


def _et_window_utc(target_date: date) -> tuple[datetime, datetime]:
    et = ZoneInfo("America/New_York")
    start_et = datetime.combine(target_date, time.min, tzinfo=et)
    end_et = start_et + timedelta(days=1)
    return start_et.astimezone(UTC), end_et.astimezone(UTC)


def _critical_null_condition(cols: set[str]) -> str:
    critical = ["player_id", "game_id", "stat", "pred_mean", "pred_q50"]
    present = [col for col in critical if col in cols]
    if not present:
        return "FALSE"
    return " OR ".join(f"{col} IS NULL" for col in present)


def run(args: argparse.Namespace) -> tuple[dict, list[str]]:
    target_date = date.fromisoformat(args.date)
    start_utc, end_utc = _et_window_utc(target_date)
    engine = _get_engine(args)
    failures: list[str] = []
    warnings: list[str] = []
    summary: dict = {
        "date": target_date.isoformat(),
        "sport": args.sport,
        "prediction_tables": {},
        "kalshi": {},
        "queue_tables": {},
        "warnings": warnings,
    }

    with engine.connect() as conn:
        prediction_cols = _columns(conn, "mlb_daily_predictions")
        scheduled_games = _scalar(conn, """
            SELECT COUNT(*)
            FROM mlb_game_schedule
            WHERE game_date = :target_date
              AND status != 'Cancelled'
        """, {"target_date": target_date})
        summary["scheduled_games"] = scheduled_games

        prediction_by_stat = _rows(conn, """
            SELECT stat, COUNT(*) AS rows
            FROM mlb_daily_predictions
            WHERE prediction_date = :target_date
            GROUP BY stat
            ORDER BY stat
        """, {"target_date": target_date})
        sample_by_stat = _rows(conn, """
            SELECT stat, COUNT(*) AS rows, COALESCE(SUM(n_samples), 0) AS total_samples
            FROM mlb_daily_prediction_samples
            WHERE prediction_date = :target_date
            GROUP BY stat
            ORDER BY stat
        """, {"target_date": target_date})
        prediction_count = sum(int(row["rows"]) for row in prediction_by_stat)
        sample_count = sum(int(row["rows"]) for row in sample_by_stat)
        summary["prediction_tables"].update({
            "predictions_by_stat": prediction_by_stat,
            "samples_by_stat": sample_by_stat,
            "prediction_count": prediction_count,
            "sample_count": sample_count,
        })

        if scheduled_games > 0 and prediction_count == 0:
            failures.append(f"{scheduled_games} scheduled games but zero mlb_daily_predictions rows")
        if prediction_count > 0 and sample_count == 0:
            failures.append("Predictions exist but zero mlb_daily_prediction_samples rows")

        line_cols = ["line", "over_edge", "under_edge", "bl_over_prob", "bl_under_prob", "bl_over_edge", "bl_under_edge", "is_recommended"]
        present_line_cols = [col for col in line_cols if col in prediction_cols]
        if prediction_count > 0 and present_line_cols:
            expressions = ",\n                ".join(
                f"COUNT(*) FILTER (WHERE {col} IS NOT NULL) AS {col}_count" for col in present_line_cols
            )
            coverage = conn.execute(text(f"""
                SELECT {expressions}
                FROM mlb_daily_predictions
                WHERE prediction_date = :target_date
            """), {"target_date": target_date}).fetchone()
            coverage_dict = dict(coverage._mapping) if coverage else {}
            summary["prediction_tables"]["coverage_counts"] = coverage_dict
            line_count = int(coverage_dict.get("line_count") or 0)
            if line_count / prediction_count < args.min_line_coverage:
                failures.append(
                    f"Low MLB line coverage: {line_count}/{prediction_count} "
                    f"(< {args.min_line_coverage:.0%})"
                )

        null_condition = _critical_null_condition(prediction_cols)
        critical_null_rows = _scalar(conn, f"""
            SELECT COUNT(*)
            FROM mlb_daily_predictions
            WHERE prediction_date = :target_date
              AND ({null_condition})
        """, {"target_date": target_date})
        summary["prediction_tables"]["critical_null_rows"] = critical_null_rows
        if critical_null_rows > 0:
            failures.append(f"{critical_null_rows} prediction rows have null critical fields")

        market_cols = _columns(conn, "kalshi_markets")
        market_populated_cols = [
            col for col in ("model_prob", "raw_edge", "bl_model_prob", "bl_edge")
            if col in market_cols
        ]
        if "snapshot_time" in market_cols:
            open_markets = _scalar(conn, """
                SELECT COUNT(*)
                FROM kalshi_markets
                WHERE sport = :sport
                  AND snapshot_time >= :start_utc
                  AND snapshot_time < :end_utc
                  AND market_status = 'open'
                  AND line IS NOT NULL
            """, {"sport": args.sport, "start_utc": start_utc, "end_utc": end_utc})
            summary["kalshi"]["open_markets"] = open_markets
            if market_populated_cols:
                populated_expr = " AND ".join(f"{col} IS NOT NULL" for col in market_populated_cols)
                populated_markets = _scalar(conn, f"""
                    SELECT COUNT(*)
                    FROM kalshi_markets
                    WHERE sport = :sport
                      AND snapshot_time >= :start_utc
                      AND snapshot_time < :end_utc
                      AND market_status = 'open'
                      AND line IS NOT NULL
                      AND {populated_expr}
                """, {"sport": args.sport, "start_utc": start_utc, "end_utc": end_utc})
                summary["kalshi"]["edge_populated_markets"] = populated_markets
                summary["kalshi"]["edge_columns_checked"] = market_populated_cols
                if open_markets > 0 and sample_count > 0 and populated_markets == 0:
                    failures.append("Open Kalshi MLB markets and samples exist, but zero markets have populated model/edge columns")
                if open_markets > 0 and sample_count == 0:
                    failures.append("Open Kalshi MLB markets exist but prediction samples are missing; edge refresh cannot update model_prob")
        else:
            warnings.append("kalshi_markets.snapshot_time missing; skipped sargable Kalshi market window check")

        for table in ("kalshi_paper_bets", "kalshi_trade_queue", "kalshi_live_orders", "paper_bets"):
            if not _table_exists(conn, table):
                warnings.append(f"{table} missing; skipped")
                continue
            cols = _columns(conn, table)
            if "game_date" not in cols:
                warnings.append(f"{table}.game_date missing; skipped target-date count")
                continue
            where = "game_date = :target_date"
            params = {"target_date": target_date}
            if "sport" in cols:
                where += " AND sport = :sport"
                params["sport"] = args.sport
            total = _scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params)
            model_cols = [col for col in ("model_prob", "raw_edge", "edge", "bl_model_prob", "bl_edge") if col in cols]
            populated = None
            if model_cols:
                populated_condition = " AND ".join(f"{col} IS NOT NULL" for col in model_cols)
                populated = _scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where} AND {populated_condition}", params)
            summary["queue_tables"][table] = {
                "rows": total,
                "model_edge_populated_rows": populated,
                "columns_checked": model_cols,
            }

    summary["status"] = "fail" if failures else "ok"
    summary["failures"] = failures
    return summary, failures


def main() -> None:
    args = _parse_args()
    summary, failures = run(args)
    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(f"MLB prediction/Kalshi output verification for {summary['date']}")
        print(f"Status: {summary['status'].upper()}")
        print(f"Scheduled games: {summary.get('scheduled_games')}")
        pt = summary["prediction_tables"]
        print(f"Predictions: {pt.get('prediction_count', 0)} rows; samples: {pt.get('sample_count', 0)} rows")
        print(f"Predictions by stat: {pt.get('predictions_by_stat', [])}")
        print(f"Samples by stat: {pt.get('samples_by_stat', [])}")
        print(f"Kalshi: {summary.get('kalshi', {})}")
        print(f"Queue/live tables: {summary.get('queue_tables', {})}")
        for warning in summary.get("warnings", []):
            print(f"WARNING: {warning}")
        for failure in failures:
            print(f"FAIL: {failure}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
