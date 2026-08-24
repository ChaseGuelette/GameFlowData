from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.events import summary_hash, transition_event
from gameflow_engineering_os.models import DailyBrief, HealthCheckResult, HealthEvent, HealthStatus


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                begin;
                create table if not exists collection_runs (
                  id integer primary key autoincrement,
                  observed_at text not null,
                  status text not null,
                  payload_json text not null
                );
                create table if not exists latest_checks (
                  check_id text primary key,
                  status text not null,
                  summary text not null,
                  observed_at text not null,
                  payload_json text not null
                );
                create table if not exists check_state (
                  check_id text primary key,
                  last_status text not null,
                  last_summary_hash text not null,
                  first_failed_at text,
                  last_observed_at text not null,
                  last_event_at text,
                  recovered_at text
                );
                create table if not exists health_events (
                  id integer primary key autoincrement,
                  check_id text not null,
                  status text not null,
                  transition_type text not null,
                  summary text not null,
                  observed_at text not null,
                  created_at text not null
                );
                create table if not exists daily_briefs (
                  brief_date text primary key,
                  generated_at text not null,
                  payload_json text not null,
                  text text not null
                );
                commit;
                """
            )

    @staticmethod
    def _dt(value: str | None) -> datetime | None:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC) if value else None

    def persist_results(self, results: Iterable[HealthCheckResult], config: EngineeringOSConfig) -> list[HealthEvent]:
        payloads = list(results)
        events: list[HealthEvent] = []
        overall = worst_status([r.status for r in payloads]).value
        with self._connect() as conn:
            conn.execute("begin immediate")
            try:
                conn.execute(
                    "insert into collection_runs(observed_at,status,payload_json) values(?,?,?)",
                    (datetime.now(UTC).isoformat(), overall, json.dumps([r.model_dump(mode="json") for r in payloads], sort_keys=True)),
                )
                for item in payloads:
                    prev = conn.execute("select * from check_state where check_id=?", (item.check_id,)).fetchone()
                    event = transition_event(
                        HealthStatus(prev["last_status"]) if prev else None,
                        prev["last_summary_hash"] if prev else None,
                        self._dt(prev["last_event_at"]) if prev else None,
                        item,
                        config,
                    )
                    conn.execute(
                        "insert or replace into latest_checks(check_id,status,summary,observed_at,payload_json) values(?,?,?,?,?)",
                        (item.check_id, item.status.value, item.summary, item.observed_at.isoformat(), item.model_dump_json()),
                    )
                    first_failed_at = prev["first_failed_at"] if prev else None
                    recovered_at = prev["recovered_at"] if prev else None
                    if item.status in {HealthStatus.WARNING, HealthStatus.FAILED} and not first_failed_at:
                        first_failed_at = item.observed_at.isoformat()
                    if item.status == HealthStatus.HEALTHY and prev and HealthStatus(prev["last_status"]) in {HealthStatus.WARNING, HealthStatus.FAILED}:
                        recovered_at = item.observed_at.isoformat()
                        first_failed_at = None
                    last_event_at = prev["last_event_at"] if prev else None
                    if event:
                        conn.execute(
                            "insert into health_events(check_id,status,transition_type,summary,observed_at,created_at) values(?,?,?,?,?,?)",
                            (event.check_id, event.status.value, event.transition_type, event.summary, event.observed_at.isoformat(), event.created_at.isoformat()),
                        )
                        event.id = conn.execute("select last_insert_rowid()").fetchone()[0]
                        events.append(event)
                        last_event_at = event.created_at.isoformat()
                    conn.execute(
                        "insert or replace into check_state(check_id,last_status,last_summary_hash,first_failed_at,last_observed_at,last_event_at,recovered_at) values(?,?,?,?,?,?,?)",
                        (item.check_id, item.status.value, summary_hash(item.summary), first_failed_at, item.observed_at.isoformat(), last_event_at, recovered_at),
                    )
                conn.execute("commit")
            except Exception:
                conn.execute("rollback")
                raise
        return events

    def latest_results(self) -> list[HealthCheckResult]:
        with self._connect() as conn:
            rows = conn.execute("select payload_json from latest_checks order by check_id").fetchall()
        return [HealthCheckResult.model_validate_json(row["payload_json"]) for row in rows]

    def save_brief(self, brief: DailyBrief, retain_days: int | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "insert or replace into daily_briefs(brief_date,generated_at,payload_json,text) values(?,?,?,?)",
                (brief.brief_date, brief.generated_at.isoformat(), brief.model_dump_json(), brief.text),
            )
            if retain_days is not None:
                cutoff = date.fromisoformat(brief.brief_date) - timedelta(days=retain_days)
                conn.execute("delete from daily_briefs where brief_date < ?", (cutoff.isoformat(),))

    def latest_brief(self) -> DailyBrief | None:
        with self._connect() as conn:
            row = conn.execute("select payload_json from daily_briefs order by brief_date desc limit 1").fetchone()
        return DailyBrief.model_validate_json(row["payload_json"]) if row else None

    def brief_history(self, limit: int = 30) -> list[DailyBrief]:
        with self._connect() as conn:
            rows = conn.execute("select payload_json from daily_briefs order by brief_date desc limit ?", (limit,)).fetchall()
        return [DailyBrief.model_validate_json(row["payload_json"]) for row in rows]

    def events(self, check_id: str | None = None, limit: int = 50) -> list[HealthEvent]:
        query = "select * from health_events"
        args: list[object] = []
        if check_id:
            query += " where check_id=?"
            args.append(check_id)
        query += " order by created_at desc limit ?"
        args.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, args).fetchall()
        return [
            HealthEvent(
                id=row["id"],
                check_id=row["check_id"],
                status=HealthStatus(row["status"]),
                transition_type=row["transition_type"],
                summary=row["summary"],
                observed_at=self._dt(row["observed_at"]) or datetime.now(UTC),
                created_at=self._dt(row["created_at"]) or datetime.now(UTC),
            )
            for row in rows
        ]


def store_for_config(config: EngineeringOSConfig) -> StateStore:
    return StateStore(config.paths.state_dir / "engineering_os.sqlite3")


def worst_status(statuses: Iterable[HealthStatus]) -> HealthStatus:
    order = [HealthStatus.FAILED, HealthStatus.WARNING, HealthStatus.UNKNOWN, HealthStatus.HEALTHY, HealthStatus.NOT_CONFIGURED]
    values = set(statuses)
    for status in order:
        if status in values:
            return status
    return HealthStatus.UNKNOWN
