r"""Local MLB dense CLV audit preflight probes.

Run from GameFlowData repo root with the Windows venv Python, for example:
    .\venv\Scripts\python.exe .\.hermes\tmp\local_dense_clv_audit_probe.py --start-date 2026-05-18 --end-date 2026-06-21 --market batter_hits
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def _jsonish(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def _print_row(label: str, row: dict[str, Any]) -> None:
    print(label)
    for key, value in row.items():
        print(f"  {key}: {_jsonish(value)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe local dense CLV/schedule/stat coverage before MLB audit runs.")
    parser.add_argument("--start-date", required=True, help="Inclusive start date, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Inclusive end date, YYYY-MM-DD")
    parser.add_argument("--market", default="batter_hits", help="Dense CLV market_key/stat, default: batter_hits")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    start_ts = datetime.combine(start_date, time.min, tzinfo=UTC)
    # Avoid date arithmetic dependency/ambiguity in SQL. Python handles the inclusive end-date -> exclusive timestamp.
    end_exclusive_ts = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=UTC)

    load_dotenv()
    database_url = os.environ.get("LOCAL_DATABASE_URL")
    if not database_url:
        raise SystemExit("LOCAL_DATABASE_URL is not set. Load .env or set it before running this probe.")

    engine = create_engine(database_url)
    with engine.connect() as conn:
        dense = conn.execute(
            text(
                """
                select
                    count(*) as rows,
                    count(*) filter (where game_id is null) as game_unlinked,
                    count(*) filter (where player_id is null) as player_unlinked,
                    min(id) as min_id,
                    max(id) as max_id,
                    min(snapshot_time) as min_snapshot,
                    max(snapshot_time) as max_snapshot
                from mlb_player_props_clv_snapshots
                where market_key = :market
                  and snapshot_time >= :start_ts
                  and snapshot_time < :end_exclusive_ts
                """
            ),
            {"market": args.market, "start_ts": start_ts, "end_exclusive_ts": end_exclusive_ts},
        ).mappings().one()
        _print_row("dense_clv", dict(dense))

        schedule = conn.execute(
            text(
                """
                select count(*) as rows, min(game_date) as min_date, max(game_date) as max_date
                from mlb_game_schedule
                where game_date between :start_date and :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings().one()
        _print_row("schedule", dict(schedule))

        batting = conn.execute(
            text(
                """
                select count(*) as rows, min(game_date) as min_date, max(game_date) as max_date
                from mlb_player_game_stats_batting
                where game_date between :start_date and :end_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings().one()
        _print_row("batting_stats", dict(batting))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
