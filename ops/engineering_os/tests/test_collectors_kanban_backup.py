from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime, timedelta

from conftest import create_kanban_db
from gameflow_engineering_os.collectors import kanban, kanban_backup
from gameflow_engineering_os.models import HealthStatus


def test_kanban_healthy(cfg):
    create_kanban_db(cfg.paths.kanban_db)
    source_sidecars = [cfg.paths.kanban_db.with_name(cfg.paths.kanban_db.name + suffix) for suffix in ("-wal", "-shm")]
    for sidecar in source_sidecars:
        sidecar.unlink(missing_ok=True)
    result = kanban.collect(cfg)
    assert result.status == HealthStatus.HEALTHY
    assert result.metrics["blocked"] == 1
    assert result.metrics["active_tasks"] == [{"id": "blocked-1", "title": "Blocked fixture", "status": "blocked"}]
    assert not any(sidecar.exists() for sidecar in source_sidecars)


def test_kanban_nonempty_wal_is_unknown_instead_of_reading_stale_snapshot(cfg):
    create_kanban_db(cfg.paths.kanban_db)
    wal = cfg.paths.kanban_db.with_name(cfg.paths.kanban_db.name + "-wal")
    wal.write_bytes(b"uncheckpointed")
    assert kanban.collect(cfg).status == HealthStatus.UNKNOWN


def test_kanban_stale_running_warning(cfg):
    stale = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    create_kanban_db(cfg.paths.kanban_db, stale)
    assert kanban.collect(cfg).status == HealthStatus.WARNING


def test_kanban_accepts_unix_integer_heartbeat_timestamps(cfg):
    cfg.paths.kanban_db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(cfg.paths.kanban_db) as conn:
        conn.execute(
            "create table tasks (status text, last_heartbeat_at integer, claim_expires integer)"
        )
        conn.execute(
            "insert into tasks values ('running', ?, ?)",
            (int((datetime.now(UTC) - timedelta(hours=1)).timestamp()), None),
        )

    outcome = kanban.collect(cfg)

    assert outcome.status == HealthStatus.WARNING
    assert outcome.metrics["stale_claims"] == 1



def test_kanban_missing_unknown(cfg):
    assert kanban.collect(cfg).status == HealthStatus.UNKNOWN


def test_backup_newest_healthy(cfg):
    cfg.paths.kanban_backups.mkdir()
    backup = cfg.paths.kanban_backups / "kanban.db"
    create_kanban_db(backup)
    result = kanban_backup.collect(cfg)
    assert result.status == HealthStatus.HEALTHY
    assert result.metrics["integrity"] == "ok"


def test_backup_missing_and_corrupt_states(cfg):
    assert kanban_backup.collect(cfg).status == HealthStatus.UNKNOWN
    cfg.paths.kanban_backups.mkdir()
    corrupt = cfg.paths.kanban_backups / "corrupt.db"
    corrupt.write_bytes(b"not sqlite")
    assert kanban_backup.collect(cfg).status == HealthStatus.FAILED


def test_backup_stale_warning(cfg):
    cfg.paths.kanban_backups.mkdir()
    backup = cfg.paths.kanban_backups / "backup.db"
    with sqlite3.connect(backup) as conn:
        conn.execute("create table ok(id integer)")
    old = (datetime.now(UTC) - timedelta(hours=40)).timestamp()
    os.utime(backup, (old, old))
    sidecar = cfg.paths.kanban_backups / "backup.db-shm"
    sidecar.write_bytes(b"newer sidecar")

    outcome = kanban_backup.collect(cfg)

    assert outcome.status == HealthStatus.WARNING
    assert outcome.metrics["age_hours"] >= 39
    assert outcome.evidence == [str(backup)]
    assert not (cfg.paths.kanban_backups / "backup.db-wal").exists()
