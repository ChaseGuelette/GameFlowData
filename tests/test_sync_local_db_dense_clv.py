from __future__ import annotations

from argparse import Namespace
from datetime import datetime

import pytest

from scripts.sync_local_db import (
    MLB_TABLES,
    build_table_plan,
    build_where_clause,
    parse_sync_date,
    redact_database_url,
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
        redact_database_url("postgresql://postgres:secret-password@localhost:5432/gameflow_local")
        == "postgresql://postgres:***@localhost:5432/gameflow_local"
    )
