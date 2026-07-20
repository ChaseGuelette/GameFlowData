from __future__ import annotations

from argparse import Namespace
from datetime import datetime
from pathlib import Path

import pytest

import scripts.sync_local_db as sync_local_db
from scripts.sync_local_db import (
    MLB_TABLES,
    build_table_plan,
    build_where_clause,
    load_sync_state,
    main,
    parse_sync_date,
    print_sync_status,
    redact_database_url,
    render_sync_state_table,
    write_sync_state,
)


def test_dense_clv_snapshots_registered_incremental_on_snapshot_time() -> None:
    assert MLB_TABLES["mlb_player_props_clv_snapshots"] == ("snapshot_time", "incremental")


def test_build_where_clause_uses_incremental_max_and_inclusive_end_date() -> None:
    where, params = build_where_clause(
        "snapshot_time",
        start_date="2026-04-01",
        end_date="2026-04-03",
        incremental_max=datetime(2026, 4, 2, 12, 0, 0),
    )

    assert '"snapshot_time" > %(incremental_max)s' in where
    assert '"snapshot_time" < %(end_exclusive)s' in where
    assert params["incremental_max"].startswith("2026-04-02T12:00:00")
    assert params["end_exclusive"].startswith("2026-04-04T00:00:00")


def test_build_table_plan_rejects_unknown_tables_by_default() -> None:
    args = Namespace(tables=["missing_table"], sport="all", allow_unknown_full_refresh=False)
    with pytest.raises(ValueError, match="Unknown table"):
        build_table_plan(args)


def test_parse_sync_date_accepts_iso_timestamp() -> None:
    assert parse_sync_date("2026-04-02T12:34:56+00:00") == datetime(2026, 4, 2, 12, 34, 56)


def test_redact_database_url_removes_password_from_logs() -> None:
    assert (
        redact_database_url("postgresql://postgres:***@localhost:5432/gameflow_local")
        == "postgresql://postgres:***@localhost:5432/gameflow_local"
    )


def test_render_sync_state_table_is_human_readable_for_status_output() -> None:
    state = {
        "schema_version": 1,
        "status": "failed",
        "mode": "incremental",
        "sport": "mlb",
        "started_at": "2026-07-18T17:46:00Z",
        "finished_at": "2026-07-18T17:47:00Z",
        "failed_tables": ["raw_player_props_combined"],
        "tables": [
            {
                "table": "mlb_player_props_clv_snapshots",
                "status": "success",
                "mode": "incremental",
                "strategy": "incremental",
                "rows_selected": 12,
                "rows_copied": 12,
                "duration_seconds": 2.5,
            }
        ],
    }

    rendered = render_sync_state_table(state)
    assert "Schema v1" in rendered
    assert "Mode: incremental" in rendered
    assert "raw_player_props_combined" in rendered
    assert "mlb_player_props_clv_snapshots" in rendered


def test_print_sync_status_on_missing_state_file(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing_sync_state.json"
    assert print_sync_status(missing_path) == 1


def test_print_sync_status_reads_temporary_journal(tmp_path: Path) -> None:
    state = {
        "schema_version": 1,
        "status": "success",
        "mode": "full",
        "sport": "all",
        "started_at": "2026-07-18T17:46:00Z",
        "finished_at": "2026-07-18T17:47:00Z",
        "failed_tables": [],
        "tables": [],
    }
    state_path = tmp_path / "sync_state.json"
    write_sync_state(state, state_path)

    assert print_sync_status(state_path) == 0


def test_write_sync_state_preserves_previous_journal_if_rewrite_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "sync_state.json"
    write_sync_state({"status": "success"}, state_path)

    def fail_dump(*_args, **_kwargs) -> None:
        raise OSError("simulated interrupted write")

    monkeypatch.setattr(sync_local_db.json, "dump", fail_dump)
    with pytest.raises(OSError, match="simulated interrupted write"):
        write_sync_state({"status": "running"}, state_path)

    assert load_sync_state(state_path) == {"status": "success"}
    assert not state_path.with_name("sync_state.json.tmp").exists()


def test_main_preflight_failure_replaces_stale_success_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "sync_state.json"
    write_sync_state({"status": "success"}, state_path)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(
        sync_local_db.sys,
        "argv",
        ["sync_local_db.py", "--state-file", str(state_path)],
    )

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 1
    state = load_sync_state(state_path)
    assert state["status"] == "failed"
    assert "DATABASE_URL not set" in state["error"]


def test_main_status_does_not_open_database_connections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "sync_state.json"
    write_sync_state({"schema_version": 1, "status": "success", "tables": []}, state_path)
    monkeypatch.setattr(
        sync_local_db.sys,
        "argv",
        ["sync_local_db.py", "--status", "--state-file", str(state_path)],
    )
    monkeypatch.setattr(
        sync_local_db,
        "create_engine",
        lambda *_args, **_kwargs: pytest.fail("status opened a SQLAlchemy engine"),
    )
    monkeypatch.setattr(
        sync_local_db.psycopg2,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("status opened a psycopg2 connection"),
    )

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 0
