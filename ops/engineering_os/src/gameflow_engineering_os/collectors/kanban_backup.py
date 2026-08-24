from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from gameflow_engineering_os.collectors.common import result
from gameflow_engineering_os.config import EngineeringOSConfig
from gameflow_engineering_os.models import HealthStatus


def collect(config: EngineeringOSConfig):
    source = f"newest backup under {config.paths.kanban_backups}"
    if not config.paths.kanban_backups.exists():
        return result("kanban.backup", HealthStatus.UNKNOWN, "kanban backup directory missing", source)
    files = [
        p
        for p in config.paths.kanban_backups.iterdir()
        if p.is_file() and p.suffix.lower() in {".db", ".sqlite", ".sqlite3"}
    ]
    if not files:
        return result("kanban.backup", HealthStatus.FAILED, "no SQLite kanban backups found", source)
    newest = max(files, key=lambda p: p.stat().st_mtime)
    age_hours = (datetime.now(UTC).timestamp() - newest.stat().st_mtime) / 3600
    evidence: list[str] = [str(newest)]
    integrity = "not_checked"
    if newest.suffix in {".db", ".sqlite", ".sqlite3"}:
        try:
            with sqlite3.connect(
                f"file:{newest.resolve().as_posix()}?mode=ro&immutable=1",
                uri=True,
                timeout=config.collector.timeout_seconds,
            ) as conn:
                integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        except sqlite3.Error as exc:
            integrity = f"error: {exc}"
    status = HealthStatus.WARNING if age_hours > config.thresholds.backup_max_age_hours else HealthStatus.HEALTHY
    if integrity not in {"ok", "not_checked"}:
        status = HealthStatus.FAILED
    return result("kanban.backup", status, f"newest backup age {age_hours:.1f}h", source, {"age_hours": round(age_hours, 2), "integrity": integrity}, evidence)
