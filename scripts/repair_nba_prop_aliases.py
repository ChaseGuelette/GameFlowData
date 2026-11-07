#!/usr/bin/env python3
"""Repair unresolved NBA T-60 player aliases using cached responses only.

Default mode reads remote reference/task data and previews cached imports. It
never calls the odds provider. --execute reuses the existing atomic importer.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_nba_prop_lines_t60 import (  # noqa: E402
    ALLOWED_BOOKMAKERS,
    FEATURE_MARKETS,
    POLICY_VERSION,
    OddsAPI,
    StateStore,
    fill_team_ids,
    process_event,
    task_id,
    validate_historical_event_payload,
)
from src.processing.nba_prop_snapshots import (  # noqa: E402
    SnapshotContractError,
    exact_link_quote,
    iso_z,
    load_exact_linker_context,
    parse_odds_event,
    select_t60_features,
)

ET = ZoneInfo("America/New_York")


class CacheOnlyAPI(OddsAPI):
    def __init__(self) -> None:
        # Do not initialize the provider client or create an HTTP session.
        self.requests_used = 0
        self.credits_used = 0

    def get(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("Alias repair is cache-only; provider requests are prohibited")


class CacheOnlyState(StateStore):
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.discovery_dir = self.root / "discovery"
        self.event_dir = self.root / "events"
        self.journal_path = self.root / "journal.jsonl"
        if not self.event_dir.is_dir():
            raise ValueError(f"Cached event directory does not exist: {self.event_dir}")

    def load_event(self, event_id: str, target: datetime) -> dict[str, Any]:
        payload = super().load_event(event_id, target)
        if payload is None:
            raise SnapshotContractError(f"Missing cache for {event_id} at {iso_z(target)}; no API request made")
        return payload


class BoundedConnection:
    """Set limits in each real transaction; poolers can ignore connect options."""

    def __init__(self, connection: Any, *, readonly: bool) -> None:
        self.connection = connection
        self.readonly = readonly

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        cur = self.connection.cursor(*args, **kwargs)
        cur.execute("SET LOCAL statement_timeout = '15s'; SET LOCAL lock_timeout = '2s'; "
                    "SELECT current_setting('transaction_read_only') AS readonly, "
                    "current_setting('statement_timeout') AS statement_timeout, "
                    "current_setting('lock_timeout') AS lock_timeout")
        row = cur.fetchone()
        settings = tuple(row.values()) if isinstance(row, Mapping) else row
        if settings is None or settings[1:] != ("15s", "2s") or settings[0] != ("on" if self.readonly else "off"):
            cur.close()
            raise RuntimeError("Actual transaction safety guards did not take effect")
        return cur

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()


def task_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(start, dtime.min, tzinfo=ET).astimezone(UTC) - timedelta(hours=1),
        datetime.combine(end + timedelta(days=1), dtime.min, tzinfo=ET).astimezone(UTC) - timedelta(hours=1),
    )


def load_alias_tasks(conn: Any, start: date, end: date, limit: int) -> tuple[list[dict[str, Any]], bool]:
    from psycopg2.extras import RealDictCursor

    lower, upper = task_bounds(start, end)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT task_id::text, api_game_id, target_snapshot_time, markets, bookmakers
            FROM public.nba_prop_capture_tasks
            WHERE status = 'unresolved'
              AND source_kind = 'historical' AND selection_policy_version = %s
              AND error_message LIKE '%%unresolved exact player identity:%%'
              AND target_snapshot_time >= %s AND target_snapshot_time < %s
            ORDER BY target_snapshot_time, api_game_id
            LIMIT %s
        """, (POLICY_VERSION, lower, upper, limit + 1))
        rows = [dict(row) for row in cur.fetchall()]
    return rows[:limit], len(rows) > limit


def cached_event(state: CacheOnlyState, task: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    event_id, target = task["api_game_id"], task["target_snapshot_time"]
    if task_id(event_id, target) != task["task_id"]:
        raise SnapshotContractError("Capture task identity differs from the fixed T-60 policy")
    if set(task["markets"]) != set(FEATURE_MARKETS) or set(task["bookmakers"]) != set(ALLOWED_BOOKMAKERS):
        raise SnapshotContractError("Capture task markets/books differ from the fixed T-60 policy")
    payload = state.load_event(event_id, target)
    data = payload.get("data")
    if not isinstance(data, dict):
        raise SnapshotContractError("Cached response has no event object")
    event = {
        "id": event_id,
        "commence_time": iso_z(target + timedelta(hours=1)),
        "home_team": data.get("home_team"),
        "away_team": data.get("away_team"),
    }
    return event, payload


def preview_event(conn: Any, state: CacheOnlyState, task: dict[str, Any], context: dict[str, Any]) -> tuple[int, int]:
    event, payload = cached_event(state, task)
    target = task["target_snapshot_time"]
    data, source_time = validate_historical_event_payload(
        payload, event_id=task["api_game_id"], commence=target + timedelta(hours=1), event=event,
    )
    quotes = parse_odds_event(
        data, requested_snapshot_time=target, source_snapshot_time=source_time,
        source_kind="historical", observed_at=source_time, expected_event_id=task["api_game_id"],
        expected_home_team=event["home_team"], expected_away_team=event["away_team"],
    )
    linked, errors = [], set()
    for quote in quotes:
        try:
            linked.append(exact_link_quote(quote, context))
        except SnapshotContractError as exc:
            errors.add(str(exc))
    if errors:
        raise SnapshotContractError("; ".join(sorted(errors)))
    linked = fill_team_ids(conn, linked, context)
    features, conflicts = select_t60_features(linked)
    if conflicts or not features:
        raise SnapshotContractError("; ".join(conflicts[:3]) or "No eligible T-60 feature rows")
    return len(quotes), len(features)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".gameflow/nba-props-t60")
    parser.add_argument("--max-events", type=int, default=1000)
    parser.add_argument("--report", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="Read-only database/cache preview (the default)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 0 <= (args.end_date - args.start_date).days <= 1460:
        raise SystemExit("Require an ordered date range of at most 1461 inclusive dates")
    if not 1 <= args.max_events <= 2000:
        raise SystemExit("--max-events must be between 1 and 2000")
    state = CacheOnlyState(args.state_dir)

    import psycopg2
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from sqlalchemy import event as sqlalchemy_event
    from sqlalchemy.pool import NullPool

    load_dotenv(ROOT / ".env")
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is required; no ODDS_API_KEY is used")
    options = "-c statement_timeout=15000 -c lock_timeout=2000"
    engine = create_engine(url, poolclass=NullPool, connect_args={
        "connect_timeout": 10,
        "options": options + " -c default_transaction_read_only=on",
        "application_name": "gameflow:nba_prop_alias_reference",
    })
    @sqlalchemy_event.listens_for(engine, "begin")
    def guard_reference_transaction(connection: Any) -> None:
        connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        connection.exec_driver_sql("SET LOCAL statement_timeout = '15s'")
        connection.exec_driver_sql("SET LOCAL lock_timeout = '2s'")
        settings = connection.exec_driver_sql(
            "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout'), "
            "current_setting('lock_timeout')"
        ).one()
        if tuple(settings) != ("on", "15s", "2s"):
            raise RuntimeError("Reference transaction safety guards did not take effect")

    raw_connection = psycopg2.connect(url, connect_timeout=10, options=options,
                                     application_name="gameflow:nba_prop_alias_repair")
    raw_connection.set_session(readonly=not args.execute)
    conn = BoundedConnection(raw_connection, readonly=not args.execute)
    outcomes: Counter[str] = Counter()
    details: list[dict[str, Any]] = []
    api = CacheOnlyAPI()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')")
            guards = cur.fetchone()
            if guards is None:
                raise RuntimeError("Database session safety guard readback returned no row")
            readonly, timeout = guards
            if (not args.execute and readonly != "on") or timeout != "15s":
                raise RuntimeError("Database session safety guards did not take effect")
        tasks, capped = load_alias_tasks(conn, args.start_date, args.end_date, args.max_events)
        conn.rollback()
        print(f"mode={'execute' if args.execute else 'read-only-preview'} tasks={len(tasks)} "
              f"capped={capped} provider_requests=prohibited", flush=True)
        context = load_exact_linker_context(engine, args.start_date, args.end_date + timedelta(days=1)) if tasks else {}
        for index, task in enumerate(tasks, 1):
            detail: dict[str, Any] = {"task_id": task["task_id"], "api_game_id": task["api_game_id"]}
            try:
                if args.execute:
                    event, _ = cached_event(state, task)
                    status, raw_count, feature_count = process_event(api, state, conn, context, event)
                else:
                    raw_count, feature_count = preview_event(conn, state, task, context)
                    status = "ready"
                    conn.rollback()
                detail.update(status=status, raw_quotes=raw_count, features=feature_count)
                outcomes[status] += 1
            except Exception as exc:
                conn.rollback()
                detail.update(status="blocked", error=f"{type(exc).__name__}: {exc}")
                outcomes["blocked"] += 1
            details.append(detail)
            if detail["status"] not in {"ready", "success"} or index == 1 or index % 50 == 0 or index == len(tasks):
                print(f"[{index}/{len(tasks)}] {task['api_game_id']} {detail['status']} "
                      f"features={detail.get('features', 0)} {detail.get('error', '')}", flush=True)
    finally:
        conn.close()
        engine.dispose()
    summary = {
        "mode": "execute" if args.execute else "read-only-preview",
        "start_date": str(args.start_date), "end_date": str(args.end_date),
        "selected_tasks": len(tasks), "capped": capped, "outcomes": dict(outcomes),
        "feature_rows": sum(item.get("features", 0) for item in details if item["status"] in {"ready", "success"}),
        "api_requests": api.requests_used, "api_credits": api.credits_used,
    }
    print(json.dumps(summary, indent=2))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({"summary": summary, "events": details}, indent=2), encoding="utf-8")
    return 2 if capped or any(outcomes[name] for name in ("blocked", "unresolved", "error", "empty")) else 0


if __name__ == "__main__":
    raise SystemExit(main())
