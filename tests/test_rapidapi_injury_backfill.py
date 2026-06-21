"""Tests for RapidAPI injury backfill scheduler-safety behavior."""

import sys


def test_runtime_bootstrap_ddl_does_not_drop_or_alter_indexes():
    from src.scrapers import rapidapi_injury_backfill as backfill

    ddl = backfill.DDL_CREATE_TABLE.upper()
    assert "DROP INDEX" not in ddl
    assert "ALTER TABLE" not in ddl
    assert not hasattr(backfill, "DDL_MIGRATE_TABLE")


def test_dry_run_does_not_require_api_key_or_db(monkeypatch):
    from src.scrapers import rapidapi_injury_backfill as backfill

    monkeypatch.delenv("RAPIDAPI_KEY", raising=False)
    monkeypatch.setattr(sys, "argv", [
        "rapidapi_injury_backfill.py",
        "--dry-run",
        "--start",
        "2026-06-21",
        "--end",
        "2026-06-21",
    ])

    def fail_get_engine():  # pragma: no cover - should never be called
        raise AssertionError("dry-run should not open a DB connection")

    monkeypatch.setattr(backfill, "get_engine", fail_get_engine)

    backfill.main()


def test_schema_bootstrap_is_explicitly_opt_in(monkeypatch):
    from src.scrapers import rapidapi_injury_backfill as backfill

    calls = {"ensure_table": 0}

    monkeypatch.setenv("RAPIDAPI_KEY", "test-key")
    monkeypatch.setattr(sys, "argv", [
        "rapidapi_injury_backfill.py",
        "--start",
        "2026-06-21",
        "--end",
        "2026-06-21",
    ])
    monkeypatch.setattr(backfill, "get_engine", lambda: object())
    monkeypatch.setattr(backfill, "ensure_table", lambda engine: calls.__setitem__("ensure_table", calls["ensure_table"] + 1))
    monkeypatch.setattr(
        backfill,
        "run_backfill",
        lambda **kwargs: {"total": 1, "fetched": 0, "records": 0, "errors": 0, "skipped": 1},
    )

    backfill.main()

    assert calls["ensure_table"] == 0

    monkeypatch.setattr(sys, "argv", [
        "rapidapi_injury_backfill.py",
        "--ensure-schema",
        "--start",
        "2026-06-21",
        "--end",
        "2026-06-21",
    ])

    backfill.main()

    assert calls["ensure_table"] == 1
