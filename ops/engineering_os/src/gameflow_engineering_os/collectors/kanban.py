from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from gameflow_engineering_os.collectors.common import result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus


def _parse_time(value: str | int | float | None) -> datetime | None:
    if not value:
        return None
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value, UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def collect(config: EngineeringOSConfig):
    source = f"sqlite readonly {config.paths.kanban_db}"
    if not config.paths.kanban_db.exists():
        return result("kanban.board", HealthStatus.UNKNOWN, "kanban database missing", source, recommended_action="verify configured kanban_db path")
    wal_path = config.paths.kanban_db.with_name(config.paths.kanban_db.name + "-wal")
    if wal_path.exists() and wal_path.stat().st_size > 0:
        return result(
            "kanban.board",
            HealthStatus.UNKNOWN,
            "kanban has an uncheckpointed WAL; immutable snapshot may be stale",
            source,
            evidence=[str(wal_path)],
            recommended_action="wait for the Kanban WAL checkpoint and collect again",
        )
    try:
        uri = f"file:{config.paths.kanban_db.resolve().as_posix()}?mode=ro&immutable=1"
        with sqlite3.connect(uri, uri=True, timeout=config.collector.timeout_seconds) as conn:
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            rows = conn.execute("select status, count(*) from tasks group by status").fetchall()
            cols = [r[1] for r in conn.execute("pragma table_info(tasks)").fetchall()]
            selected = [column for column in ("id", "title", "status") if column in cols]
            active_tasks: list[dict[str, object]] = []
            if "status" in selected:
                query = f"select {', '.join(selected)} from tasks where status in ('running','blocked') order by status, rowid limit 20"
                active_tasks = [dict(zip(selected, row, strict=True)) for row in conn.execute(query)]
            stale = 0
            if {"status", "last_heartbeat_at", "claim_expires"}.issubset(set(cols)):
                candidates = conn.execute("select last_heartbeat_at, claim_expires from tasks where status='running'").fetchall()
                cutoff = datetime.now(UTC) - timedelta(minutes=config.thresholds.stuck_task_minutes)
                for heartbeat, claim_expires in candidates:
                    hb = _parse_time(heartbeat) or _parse_time(claim_expires)
                    if hb and hb < cutoff:
                        stale += 1
    except (sqlite3.Error, OSError, ValueError) as exc:
        return result("kanban.board", HealthStatus.UNKNOWN, "kanban database unreadable or malformed", source, evidence=[str(exc)])
    counts = {status: count for status, count in rows}
    if integrity != "ok":
        return result("kanban.board", HealthStatus.FAILED, "kanban integrity check failed", source, {"integrity": integrity, **counts}, [integrity])
    status = HealthStatus.WARNING if stale else HealthStatus.HEALTHY
    summary = f"kanban ok; {counts.get('running', 0)} running, {counts.get('blocked', 0)} blocked, {stale} stale"
    return result(
        "kanban.board",
        status,
        summary,
        source,
        {"integrity": integrity, "stale_claims": stale, "active_tasks": active_tasks, **counts},
    )
