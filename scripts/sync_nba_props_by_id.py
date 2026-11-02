#!/usr/bin/env python3
"""Safely mirror raw_player_props_combined to an existing local table by ID.

The default mode is a read-only preflight. Only --execute permits local writes.
The remote connection is always read-only, and the destination is never truncated.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import ipaddress
import json
import os
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

TABLE_SCHEMA = "public"
TABLE_NAME = "raw_player_props_combined"
KEY_COLUMN = "staging_id"
EXPECTED_COLUMNS = (
    "staging_id",
    "api_game_id",
    "api_player_name",
    "bookmaker",
    "market_key",
    "outcome_label",
    "line",
    "odds_american",
    "commence_time",
    "home_team",
    "away_team",
    "inserted_at",
    "market_last_update",
    "bookmaker_last_update",
    "bookmaker_name",
    "snapshot_time",
    "game_id",
    "player_id",
    "team_id",
)
JOURNAL_VERSION = 1
ADVISORY_LOCK_KEY = 7_144_608_753_706_443_921
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_FILE = REPO_ROOT / "logs" / "sync" / "nba_props_by_id.json"
TEMP_TABLE_NAME = "_nba_props_by_id_stage"
LOCAL_HOST_NAMES = {"localhost", "localhost.localdomain"}
ALLOWED_DSN_QUERY_KEYS = {
    "application_name",
    "channel_binding",
    "connect_timeout",
    "gssencmode",
    "keepalives",
    "keepalives_count",
    "keepalives_idle",
    "keepalives_interval",
    "sslcert",
    "sslkey",
    "sslmode",
    "sslpassword",
    "sslrootcert",
}
VALID_JOURNAL_STATUSES = {"running", "paused", "complete", "failed", "interrupted"}


class SafeSyncError(RuntimeError):
    """An expected failure whose message is safe to print."""


@dataclass(frozen=True)
class DsnIdentity:
    host: str
    port: int
    dbname: str
    user: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
        }


@dataclass(frozen=True)
class SchemaInfo:
    columns: tuple[dict[str, Any], ...]
    primary_key: tuple[str, ...]
    signature: str


@dataclass(frozen=True)
class SourceBounds:
    first_id: int | None
    last_id: int | None
    estimated_rows: int


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or positive")
    return parsed


def bounded_positive_int(maximum: int):
    def parser(value: str) -> int:
        parsed = positive_int(value)
        if parsed > maximum:
            raise argparse.ArgumentTypeError(f"must be <= {maximum}")
        return parsed

    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Safely mirror public.raw_player_props_combined to the existing local table "
            "using resumable staging_id keyset pages. Default: read-only preflight."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight", action="store_true", help="Run read-only metadata and ID-bound checks (default)")
    mode.add_argument("--execute", action="store_true", help="Authorize staged, verified local upserts")
    mode.add_argument("--status", action="store_true", help="Read the checkpoint offline and exit")
    parser.add_argument("--batch-size", type=bounded_positive_int(100_000), default=10_000)
    parser.add_argument(
        "--max-batches",
        type=nonnegative_int,
        default=1,
        help="Maximum pages this invocation; 0 runs until the frozen upper ID is exhausted",
    )
    parser.add_argument("--start-after-id", type=int, default=None)
    parser.add_argument("--end-id", type=int, default=None)
    parser.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help=f"Checkpoint path (default: {DEFAULT_STATE_FILE})",
    )
    parser.add_argument("--statement-timeout-seconds", type=bounded_positive_int(120), default=15)
    parser.add_argument("--lock-timeout-seconds", type=bounded_positive_int(120), default=1)
    parser.add_argument("--connect-timeout-seconds", type=bounded_positive_int(120), default=8)
    return parser


def validate_arguments(args: argparse.Namespace) -> None:
    if args.start_after_id is not None and args.end_id is not None and args.start_after_id >= args.end_id:
        raise SafeSyncError("--start-after-id must be less than --end-id")


def resolve_state_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def load_journal(path: Path, *, required: bool) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        if required:
            raise SafeSyncError(f"No sync journal found at {path}") from None
        return None
    except OSError:
        raise SafeSyncError(f"Unable to read sync journal at {path}") from None

    try:
        state = json.loads(raw)
    except json.JSONDecodeError:
        raise SafeSyncError(f"Sync journal at {path} contains invalid JSON") from None
    validate_journal(state)
    return state


def require_int(state: dict[str, Any], name: str, *, nullable: bool = False) -> int | None:
    value = state.get(name)
    if nullable and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise SafeSyncError(f"Sync journal field {name!r} is invalid")
    return value


def validate_identity_dict(value: Any, name: str) -> None:
    if not isinstance(value, dict):
        raise SafeSyncError(f"Sync journal field {name!r} is invalid")
    expected = {"host", "port", "dbname", "user", "server_addr", "server_port"}
    if set(value) != expected:
        raise SafeSyncError(f"Sync journal field {name!r} has an incompatible shape")
    for key in ("host", "dbname", "user", "server_addr"):
        if not isinstance(value[key], str) or not value[key]:
            raise SafeSyncError(f"Sync journal field {name}.{key} is invalid")
    for key in ("port", "server_port"):
        if isinstance(value[key], bool) or not isinstance(value[key], int):
            raise SafeSyncError(f"Sync journal field {name}.{key} is invalid")


def validate_journal(state: Any) -> None:
    if not isinstance(state, dict):
        raise SafeSyncError("Sync journal root must be an object")
    required = {
        "journal_version",
        "table",
        "status",
        "source_identity",
        "target_identity",
        "schema_signature",
        "initial_lower_id",
        "initial_upper_id",
        "last_committed_id",
        "committed_source_rows",
        "committed_batches",
        "last_page",
        "started_at",
        "updated_at",
        "finished_at",
        "message",
    }
    if set(state) != required:
        raise SafeSyncError("Sync journal has an incompatible field set")
    if state["journal_version"] != JOURNAL_VERSION or state["table"] != f"{TABLE_SCHEMA}.{TABLE_NAME}":
        raise SafeSyncError("Sync journal belongs to an incompatible tool or table")
    if state["status"] not in VALID_JOURNAL_STATUSES:
        raise SafeSyncError("Sync journal status is invalid")
    validate_identity_dict(state["source_identity"], "source_identity")
    validate_identity_dict(state["target_identity"], "target_identity")
    if not isinstance(state["schema_signature"], str) or len(state["schema_signature"]) != 64:
        raise SafeSyncError("Sync journal schema signature is invalid")
    lower = require_int(state, "initial_lower_id", nullable=True)
    upper = require_int(state, "initial_upper_id", nullable=True)
    cursor = require_int(state, "last_committed_id", nullable=True)
    committed_rows = require_int(state, "committed_source_rows")
    committed_batches = require_int(state, "committed_batches")
    if committed_rows is None or committed_rows < 0 or committed_batches is None or committed_batches < 0:
        raise SafeSyncError("Sync journal counters are invalid")
    if lower is None:
        if upper is not None or cursor is not None:
            raise SafeSyncError("Sync journal empty-range bounds are inconsistent")
    elif upper is None or cursor is None or not (lower <= cursor <= upper):
        raise SafeSyncError("Sync journal bounds or cursor are inconsistent")
    last_page = state["last_page"]
    if last_page is not None:
        if not isinstance(last_page, dict) or set(last_page) != {"first_id", "last_id", "source_rows", "changed_rows"}:
            raise SafeSyncError("Sync journal last_page is invalid")
        first = require_int(last_page, "first_id")
        last = require_int(last_page, "last_id")
        rows = require_int(last_page, "source_rows")
        changed = require_int(last_page, "changed_rows")
        if first is None or last is None or rows is None or changed is None or first > last or rows <= 0 or changed < 0:
            raise SafeSyncError("Sync journal last_page values are invalid")
    for key in ("started_at", "updated_at"):
        if not isinstance(state[key], str) or not state[key]:
            raise SafeSyncError(f"Sync journal field {key!r} is invalid")
    if state["finished_at"] is not None and not isinstance(state["finished_at"], str):
        raise SafeSyncError("Sync journal field 'finished_at' is invalid")
    if not isinstance(state["message"], str):
        raise SafeSyncError("Sync journal field 'message' is invalid")


def atomic_write_journal(path: Path, state: dict[str, Any]) -> None:
    validate_journal(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
    try:
        with temp_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        if os.name != "nt":
            try:
                directory_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                # Replacement already succeeded. Some filesystems do not
                # support directory fsync, so do not report a stale cursor.
                pass
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def status_command(path: Path) -> int:
    state = load_journal(path, required=True)
    assert state is not None
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


def parse_dsn_identity(dsn: str, *, local: bool) -> DsnIdentity:
    try:
        parts = urlsplit(dsn)
    except ValueError:
        raise SafeSyncError("Database URL is not a valid PostgreSQL URI") from None
    if parts.scheme not in {"postgresql", "postgres"}:
        raise SafeSyncError("Database URL must use a postgresql:// URI")
    if parts.fragment:
        raise SafeSyncError("Database URL fragments are not supported")
    try:
        query_items = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        raise SafeSyncError("Database URL query parameters are invalid") from None
    query_keys = [key for key, _ in query_items]
    if len(query_keys) != len(set(query_keys)):
        raise SafeSyncError("Duplicate database URL query parameters are not supported")
    unsupported = set(query_keys) - ALLOWED_DSN_QUERY_KEYS
    if unsupported:
        raise SafeSyncError("Database URL contains unsupported query options")
    try:
        host = parts.hostname
        port = parts.port or 5432
    except ValueError:
        raise SafeSyncError("Database URL port is invalid") from None
    if not host or not parts.username:
        raise SafeSyncError("Database URL must explicitly include one host and user")
    host = unquote(host).strip().lower().rstrip(".")
    if not host or "," in host or host.startswith("/") or any(ch.isspace() for ch in host):
        raise SafeSyncError("Database URL must use one TCP host, not a socket or multi-host target")
    dbname = unquote(parts.path.lstrip("/"))
    if not dbname or "/" in dbname:
        raise SafeSyncError("Database URL must explicitly include one database name")
    user = unquote(parts.username)
    if not user:
        raise SafeSyncError("Database URL user is empty")
    host_is_local = is_loopback_host(host)
    if local and not host_is_local:
        raise SafeSyncError("LOCAL_DATABASE_URL host must be localhost or a loopback address")
    if not local and host_is_local:
        raise SafeSyncError("DATABASE_URL host must not be localhost or a loopback address")
    return DsnIdentity(host=host, port=port, dbname=dbname, user=user)


def is_loopback_host(host: str) -> bool:
    if host in LOCAL_HOST_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def is_loopback_address(address: str) -> bool:
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def connection_options(args: argparse.Namespace) -> str:
    statement_ms = args.statement_timeout_seconds * 1000
    lock_ms = args.lock_timeout_seconds * 1000
    idle_ms = max(statement_ms * 2, 30_000)
    return (
        f"-c statement_timeout={statement_ms} "
        f"-c lock_timeout={lock_ms} "
        f"-c idle_in_transaction_session_timeout={idle_ms} "
        "-c timezone=UTC -c client_encoding=UTF8 -c standard_conforming_strings=on"
    )


def connect_database(dsn: str, *, role: str, args: argparse.Namespace):
    application_name = f"gameflow:nba-props-by-id:{role}"
    try:
        connection = psycopg2.connect(
            dsn,
            application_name=application_name,
            connect_timeout=args.connect_timeout_seconds,
            options=connection_options(args),
        )
    except Exception as exc:
        raise database_error(f"connect-{role}", exc) from None
    return connection


def database_error(phase: str, exc: BaseException) -> SafeSyncError:
    sqlstate = getattr(exc, "pgcode", None)
    suffix = f" sqlstate={sqlstate}" if isinstance(sqlstate, str) and sqlstate else ""
    return SafeSyncError(f"Database operation failed during {phase}: {type(exc).__name__}{suffix}")


def set_read_only(connection, *, role: str) -> None:
    try:
        connection.rollback()
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            value = cursor.fetchone()[0]
        if value != "on":
            raise SafeSyncError(f"{role} transaction_read_only verification failed")
    except SafeSyncError:
        raise
    except Exception as exc:
        raise database_error(f"enforce-read-only-{role}", exc) from None


def set_local_writable(connection) -> None:
    try:
        connection.rollback()
        connection.set_session(readonly=False, autocommit=False)
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            value = cursor.fetchone()[0]
        connection.rollback()
        if value != "off":
            raise SafeSyncError("Local writable-session verification failed")
    except SafeSyncError:
        raise
    except Exception as exc:
        raise database_error("enable-local-writes", exc) from None


def actual_identity(connection, configured: DsnIdentity, *, role: str) -> dict[str, Any]:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT host(inet_server_addr()), inet_server_port(), current_database(), current_user"
            )
            server_addr, server_port, dbname, user = cursor.fetchone()
    except Exception as exc:
        raise database_error(f"identify-{role}", exc) from None
    if not server_addr:
        raise SafeSyncError(f"{role} connection is not a TCP connection")
    expected_user = configured.user
    if configured.host.endswith(".pooler.supabase.com") and "." in expected_user:
        # Shared-pooler login names append the project reference to the DB role.
        expected_user = expected_user.rsplit(".", 1)[0]
    if dbname != configured.dbname or user != expected_user:
        raise SafeSyncError(f"{role} connection identity differs from its database URL")
    if role == "local" and not is_loopback_address(server_addr):
        raise SafeSyncError("Actual local database server address is not loopback")
    if role == "remote" and is_loopback_address(server_addr):
        raise SafeSyncError("Actual remote database server address is loopback")
    result = configured.as_dict()
    result.update(server_addr=server_addr, server_port=int(server_port))
    return result


def verify_distinct_endpoints(source: dict[str, Any], target: dict[str, Any]) -> None:
    source_endpoint = (source["server_addr"], source["server_port"], source["dbname"])
    target_endpoint = (target["server_addr"], target["server_port"], target["dbname"])
    if source_endpoint == target_endpoint:
        raise SafeSyncError("Source and target resolve to the same database endpoint")


def read_schema(connection, *, role: str) -> SchemaInfo:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    a.attnum,
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    a.attnotnull,
                    a.attidentity,
                    a.attgenerated
                FROM pg_catalog.pg_attribute AS a
                JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relname = %s
                  AND c.relkind IN ('r', 'p')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (TABLE_SCHEMA, TABLE_NAME),
            )
            rows = cursor.fetchall()
            if not rows:
                raise SafeSyncError(f"{role} table {TABLE_SCHEMA}.{TABLE_NAME} does not exist")
            cursor.execute(
                """
                SELECT a.attname
                FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS c ON c.oid = i.indrelid
                JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS keys(attnum, ord) ON true
                JOIN pg_catalog.pg_attribute AS a
                  ON a.attrelid = i.indrelid AND a.attnum = keys.attnum
                WHERE n.nspname = %s AND c.relname = %s AND i.indisprimary
                ORDER BY keys.ord
                """,
                (TABLE_SCHEMA, TABLE_NAME),
            )
            primary_key = tuple(row[0] for row in cursor.fetchall())
    except SafeSyncError:
        raise
    except Exception as exc:
        raise database_error(f"read-schema-{role}", exc) from None

    columns = tuple(
        {
            "position": int(position),
            "name": name,
            "type": type_name,
            "nullable": not bool(not_null),
            "identity": identity or "",
            "generated": generated or "",
        }
        for position, name, type_name, not_null, identity, generated in rows
    )
    signature_payload = json.dumps(
        {"columns": columns, "primary_key": primary_key}, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return SchemaInfo(
        columns=columns,
        primary_key=primary_key,
        signature=hashlib.sha256(signature_payload).hexdigest(),
    )


def validate_schemas(remote: SchemaInfo, local: SchemaInfo) -> None:
    remote_names = tuple(column["name"] for column in remote.columns)
    local_names = tuple(column["name"] for column in local.columns)
    if remote_names != EXPECTED_COLUMNS:
        raise SafeSyncError("Remote table ordered columns differ from the approved 19-column contract")
    if local_names != EXPECTED_COLUMNS:
        raise SafeSyncError("Local table ordered columns differ from the approved 19-column contract")
    if remote.primary_key != (KEY_COLUMN,) or local.primary_key != (KEY_COLUMN,):
        raise SafeSyncError("Both tables must have primary key exactly (staging_id)")
    if remote.columns[0]["type"] != "bigint" or remote.columns[0]["nullable"]:
        raise SafeSyncError("Remote staging_id must be non-null bigint")
    if local.columns[0]["type"] != "bigint" or local.columns[0]["nullable"]:
        raise SafeSyncError("Local staging_id must be non-null bigint")
    if remote.columns != local.columns or remote.signature != local.signature:
        raise SafeSyncError("Remote and local column types, nullability, identity, or generated attributes do not match")


def source_bounds(connection) -> SourceBounds:
    table = sql.Identifier(TABLE_SCHEMA, TABLE_NAME)
    key = sql.Identifier(KEY_COLUMN)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT {key} FROM {table} ORDER BY {key} ASC LIMIT 1").format(key=key, table=table))
            first_row = cursor.fetchone()
            cursor.execute(sql.SQL("SELECT {key} FROM {table} ORDER BY {key} DESC LIMIT 1").format(key=key, table=table))
            last_row = cursor.fetchone()
            cursor.execute(
                """
                SELECT COALESCE(c.reltuples, 0)::bigint
                FROM pg_catalog.pg_class AS c
                JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (TABLE_SCHEMA, TABLE_NAME),
            )
            estimate_row = cursor.fetchone()
    except Exception as exc:
        raise database_error("read-source-bounds", exc) from None
    return SourceBounds(
        first_id=int(first_row[0]) if first_row else None,
        last_id=int(last_row[0]) if last_row else None,
        estimated_rows=int(estimate_row[0]) if estimate_row else 0,
    )


def preflight(args: argparse.Namespace, remote_dsn: str, local_dsn: str) -> int:
    remote_config = parse_dsn_identity(remote_dsn, local=False)
    local_config = parse_dsn_identity(local_dsn, local=True)
    remote = local = None
    try:
        remote = connect_database(remote_dsn, role="remote", args=args)
        local = connect_database(local_dsn, role="local", args=args)
        set_read_only(remote, role="remote")
        set_read_only(local, role="local")
        source_identity = actual_identity(remote, remote_config, role="remote")
        target_identity = actual_identity(local, local_config, role="local")
        verify_distinct_endpoints(source_identity, target_identity)
        remote_schema = read_schema(remote, role="remote")
        local_schema = read_schema(local, role="local")
        validate_schemas(remote_schema, local_schema)
        bounds = source_bounds(remote)
        saved_state = load_journal(resolve_state_path(args.state_file), required=False)
        report = {
            "mode": "preflight",
            "read_only": {"remote": True, "local": True},
            "table": f"{TABLE_SCHEMA}.{TABLE_NAME}",
            "schema_compatible": True,
            "schema_signature": remote_schema.signature,
            "source_identity": source_identity,
            "target_identity": target_identity,
            "source_bounds": {
                "first_id": bounds.first_id,
                "last_id": bounds.last_id,
                "estimated_rows_catalog": bounds.estimated_rows,
            },
            "saved_state": saved_state,
            "notes": [
                "Catalog row estimate is not an exact count.",
                "No checkpoint or database data was written.",
            ],
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    finally:
        close_connection(remote)
        close_connection(local)


def initial_range(args: argparse.Namespace, bounds: SourceBounds) -> tuple[int | None, int | None]:
    if bounds.first_id is None or bounds.last_id is None:
        if args.start_after_id is not None or args.end_id is not None:
            raise SafeSyncError("Explicit ID bounds cannot be used while the source table is empty")
        return None, None
    lower = args.start_after_id if args.start_after_id is not None else bounds.first_id - 1
    upper = args.end_id if args.end_id is not None else bounds.last_id
    if lower < bounds.first_id - 1:
        raise SafeSyncError("--start-after-id is below the current indexed source range")
    if upper > bounds.last_id:
        raise SafeSyncError("--end-id is above the current indexed source range")
    if lower >= upper:
        raise SafeSyncError("Initial lower ID must be less than the frozen upper ID")
    return lower, upper


def new_journal(
    *,
    source_identity: dict[str, Any],
    target_identity: dict[str, Any],
    schema_signature: str,
    lower: int | None,
    upper: int | None,
) -> dict[str, Any]:
    now = utc_now()
    empty = lower is None
    return {
        "journal_version": JOURNAL_VERSION,
        "table": f"{TABLE_SCHEMA}.{TABLE_NAME}",
        "status": "complete" if empty else "running",
        "source_identity": source_identity,
        "target_identity": target_identity,
        "schema_signature": schema_signature,
        "initial_lower_id": lower,
        "initial_upper_id": upper,
        "last_committed_id": lower,
        "committed_source_rows": 0,
        "committed_batches": 0,
        "last_page": None,
        "started_at": now,
        "updated_at": now,
        "finished_at": now if empty else None,
        "message": "Source range was empty" if empty else "Ready to copy frozen ID range",
    }


def verify_resume_state(
    state: dict[str, Any],
    *,
    args: argparse.Namespace,
    source_identity: dict[str, Any],
    target_identity: dict[str, Any],
    schema_signature: str,
) -> None:
    if state["source_identity"] != source_identity:
        raise SafeSyncError("Checkpoint source identity does not match this connection")
    if state["target_identity"] != target_identity:
        raise SafeSyncError("Checkpoint target identity does not match this connection")
    if state["schema_signature"] != schema_signature:
        raise SafeSyncError("Checkpoint schema signature does not match current tables")
    if args.start_after_id is not None and args.start_after_id != state["initial_lower_id"]:
        raise SafeSyncError("--start-after-id does not match the checkpoint's initial lower ID")
    if args.end_id is not None and args.end_id != state["initial_upper_id"]:
        raise SafeSyncError("--end-id does not match the checkpoint's frozen upper ID")


def acquire_advisory_lock(connection) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
            acquired = bool(cursor.fetchone()[0])
        connection.commit()
    except Exception as exc:
        connection.rollback()
        raise database_error("acquire-advisory-lock", exc) from None
    if not acquired:
        raise SafeSyncError("Another NBA props ID sync holds the local advisory lock")


def release_advisory_lock(connection) -> None:
    try:
        connection.rollback()
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
    except Exception:
        pass


def reject_overlapping_local_work(connection) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_stat_activity
                    WHERE pid <> pg_backend_pid()
                      AND datname = current_database()
                      AND state <> 'idle'
                      AND (
                          application_name = 'gameflow:sync_local_db:local_copy'
                          OR (
                              query ILIKE %s
                              AND query ~* %s
                          )
                      )
                )
                """,
                (f"%{TABLE_NAME}%", "(copy|insert|update|delete|truncate|create[[:space:]]+table)"),
            )
            overlap = bool(cursor.fetchone()[0])
        connection.rollback()
    except Exception as exc:
        connection.rollback()
        raise database_error("check-overlapping-local-work", exc) from None
    if overlap:
        raise SafeSyncError("Refusing to overlap active local work targeting raw_player_props_combined")


def export_page(connection, *, last_id: int, upper_id: int, batch_size: int) -> tuple[str, list[int]]:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in EXPECTED_COLUMNS)
    query = sql.SQL(
        "SELECT {columns} FROM {table} "
        "WHERE {key} > %s AND {key} <= %s ORDER BY {key} LIMIT %s"
    ).format(
        columns=columns,
        table=sql.Identifier(TABLE_SCHEMA, TABLE_NAME),
        key=sql.Identifier(KEY_COLUMN),
    )
    try:
        with connection.cursor() as cursor:
            rendered_query = cursor.mogrify(query.as_string(connection), (last_id, upper_id, batch_size)).decode("utf-8")
            copy_sql = f"COPY ({rendered_query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE, NULL '\\N')"
            payload = io.StringIO(newline="")
            cursor.copy_expert(copy_sql, payload)
    except Exception as exc:
        raise database_error("export-page", exc) from None
    csv_payload = payload.getvalue()
    ids = validate_page_payload(csv_payload, last_id=last_id, upper_id=upper_id, batch_size=batch_size)
    return csv_payload, ids


def validate_page_payload(payload: str, *, last_id: int, upper_id: int, batch_size: int) -> list[int]:
    try:
        reader = csv.reader(io.StringIO(payload, newline=""))
        header = next(reader)
        if tuple(header) != EXPECTED_COLUMNS:
            raise SafeSyncError("Remote COPY header differs from the approved column contract")
        ids: list[int] = []
        for row in reader:
            if len(row) != len(EXPECTED_COLUMNS):
                raise SafeSyncError("Remote COPY returned a row with an unexpected column count")
            try:
                current_id = int(row[0])
            except ValueError:
                raise SafeSyncError("Remote COPY returned a non-integer staging_id") from None
            if current_id <= last_id or current_id > upper_id:
                raise SafeSyncError("Remote COPY returned a staging_id outside the requested bounds")
            if ids and current_id <= ids[-1]:
                raise SafeSyncError("Remote COPY staging_ids are not strictly increasing")
            ids.append(current_id)
    except csv.Error:
        raise SafeSyncError("Remote COPY returned invalid CSV") from None
    if len(ids) > batch_size:
        raise SafeSyncError("Remote COPY returned more rows than the requested batch size")
    return ids


def import_and_verify_page(
    connection,
    payload: str,
    expected_ids: list[int],
    *,
    override_system_value: bool = False,
) -> int:
    if not expected_ids:
        return 0
    target = sql.Identifier(TABLE_SCHEMA, TABLE_NAME)
    stage = sql.Identifier(TEMP_TABLE_NAME)
    columns = [sql.Identifier(column) for column in EXPECTED_COLUMNS]
    column_list = sql.SQL(", ").join(columns)
    non_key_columns = tuple(column for column in EXPECTED_COLUMNS if column != KEY_COLUMN)
    assignments = sql.SQL(", ").join(
        sql.SQL("{column} = EXCLUDED.{column}").format(column=sql.Identifier(column))
        for column in non_key_columns
    )
    target_row = sql.SQL(", ").join(
        sql.SQL("target.{column}").format(column=sql.Identifier(column)) for column in non_key_columns
    )
    excluded_row = sql.SQL(", ").join(
        sql.SQL("EXCLUDED.{column}").format(column=sql.Identifier(column)) for column in non_key_columns
    )
    equality_checks = sql.SQL(" AND ").join(
        sql.SQL("target.{column} IS NOT DISTINCT FROM stage.{column}").format(column=sql.Identifier(column))
        for column in EXPECTED_COLUMNS
    )
    override_clause = sql.SQL(" OVERRIDING SYSTEM VALUE") if override_system_value else sql.SQL("")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "CREATE TEMP TABLE IF NOT EXISTS {stage} ON COMMIT PRESERVE ROWS "
                    "AS SELECT {columns} FROM {target} WITH NO DATA"
                ).format(stage=stage, columns=column_list, target=target)
            )
            cursor.execute(sql.SQL("TRUNCATE TABLE {stage}").format(stage=stage))
            copy_sql = sql.SQL(
                "COPY {stage} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '\\N')"
            ).format(stage=stage, columns=column_list)
            cursor.copy_expert(copy_sql.as_string(connection), io.StringIO(payload, newline=""))
            cursor.execute(sql.SQL("SELECT count(*) FROM {stage}").format(stage=stage))
            stage_count = int(cursor.fetchone()[0])
            if stage_count != len(expected_ids):
                raise SafeSyncError("Local staging row count does not match the source page")
            cursor.execute(
                sql.SQL(
                    "INSERT INTO {target} AS target ({columns}){override_clause} SELECT {columns} FROM {stage} "
                    "ON CONFLICT ({key}) DO UPDATE SET {assignments} "
                    "WHERE ({target_row}) IS DISTINCT FROM ({excluded_row})"
                ).format(
                    target=target,
                    columns=column_list,
                    override_clause=override_clause,
                    stage=stage,
                    key=sql.Identifier(KEY_COLUMN),
                    assignments=assignments,
                    target_row=target_row,
                    excluded_row=excluded_row,
                )
            )
            changed_rows = max(cursor.rowcount, 0)
            cursor.execute(
                sql.SQL(
                    "SELECT count(*) FROM {stage} AS stage "
                    "JOIN {target} AS target ON target.{key} = stage.{key} "
                    "WHERE {equality_checks}"
                ).format(
                    stage=stage,
                    target=target,
                    key=sql.Identifier(KEY_COLUMN),
                    equality_checks=equality_checks,
                )
            )
            equal_count = int(cursor.fetchone()[0])
            if equal_count != stage_count:
                raise SafeSyncError("Full-row stage-to-local verification failed")
        connection.commit()
        return changed_rows
    except SafeSyncError:
        connection.rollback()
        raise
    except Exception as exc:
        connection.rollback()
        raise database_error("import-verify-commit-page", exc) from None


def checkpoint_after_page(
    path: Path,
    state: dict[str, Any],
    *,
    ids: list[int],
    changed_rows: int,
    complete: bool,
) -> dict[str, Any]:
    candidate = deepcopy(state)
    candidate["last_committed_id"] = ids[-1]
    candidate["committed_source_rows"] += len(ids)
    candidate["committed_batches"] += 1
    candidate["last_page"] = {
        "first_id": ids[0],
        "last_id": ids[-1],
        "source_rows": len(ids),
        "changed_rows": changed_rows,
    }
    candidate["status"] = "complete" if complete else "running"
    candidate["updated_at"] = utc_now()
    candidate["finished_at"] = candidate["updated_at"] if complete else None
    candidate["message"] = "Frozen upper ID exhausted" if complete else "Page committed and checkpointed"
    atomic_write_journal(path, candidate)
    return candidate


def save_terminal_state(path: Path, state: dict[str, Any], *, status: str, message: str) -> dict[str, Any]:
    candidate = deepcopy(state)
    candidate["status"] = status
    candidate["updated_at"] = utc_now()
    candidate["finished_at"] = candidate["updated_at"] if status in {"complete", "failed", "interrupted"} else None
    candidate["message"] = message
    atomic_write_journal(path, candidate)
    return candidate


def save_failure_if_cursor_current(
    path: Path,
    state: dict[str, Any],
    *,
    status: str,
    message: str,
) -> None:
    """Save a failure marker only if disk still has this committed cursor."""
    current = load_journal(path, required=True)
    assert current is not None
    guard_fields = (
        "source_identity",
        "target_identity",
        "schema_signature",
        "initial_lower_id",
        "initial_upper_id",
        "last_committed_id",
        "committed_source_rows",
        "committed_batches",
    )
    if any(current[field] != state[field] for field in guard_fields):
        return
    save_terminal_state(path, state, status=status, message=message)


def execute_sync(args: argparse.Namespace, remote_dsn: str, local_dsn: str) -> int:
    remote_config = parse_dsn_identity(remote_dsn, local=False)
    local_config = parse_dsn_identity(local_dsn, local=True)
    state_path = resolve_state_path(args.state_file)
    remote = local = None
    state: dict[str, Any] | None = None
    state_verified = False
    lock_acquired = False
    phase = "connect"
    try:
        remote = connect_database(remote_dsn, role="remote", args=args)
        local = connect_database(local_dsn, role="local", args=args)
        set_read_only(remote, role="remote")
        set_read_only(local, role="local")
        source_identity = actual_identity(remote, remote_config, role="remote")
        target_identity = actual_identity(local, local_config, role="local")
        verify_distinct_endpoints(source_identity, target_identity)
        remote_schema = read_schema(remote, role="remote")
        local_schema = read_schema(local, role="local")
        validate_schemas(remote_schema, local_schema)
        bounds = source_bounds(remote)

        phase = "local-lock"
        set_local_writable(local)
        acquire_advisory_lock(local)
        lock_acquired = True
        reject_overlapping_local_work(local)

        phase = "journal"
        state = load_journal(state_path, required=False)
        if state is None:
            lower, upper = initial_range(args, bounds)
            state = new_journal(
                source_identity=source_identity,
                target_identity=target_identity,
                schema_signature=remote_schema.signature,
                lower=lower,
                upper=upper,
            )
            atomic_write_journal(state_path, state)
            state_verified = True
        else:
            verify_resume_state(
                state,
                args=args,
                source_identity=source_identity,
                target_identity=target_identity,
                schema_signature=remote_schema.signature,
            )
            state_verified = True

        if state["status"] == "complete":
            print("Checkpoint is complete for its frozen upper ID; nothing to do.")
            return 0
        lower = state["last_committed_id"]
        upper = state["initial_upper_id"]
        if lower is None or upper is None:
            state = save_terminal_state(state_path, state, status="complete", message="Source range was empty")
            print("Source range is empty; checkpoint marked complete.")
            return 0

        processed_this_run = 0
        while args.max_batches == 0 or processed_this_run < args.max_batches:
            phase = "export-page"
            payload, ids = export_page(remote, last_id=lower, upper_id=upper, batch_size=args.batch_size)
            if not ids:
                state = save_terminal_state(
                    state_path,
                    state,
                    status="complete",
                    message="No source rows remain at or below the frozen upper ID",
                )
                print("Sync complete for the frozen upper ID.")
                return 0

            phase = "import-page"
            changed_rows = import_and_verify_page(
                local,
                payload,
                ids,
                override_system_value=local_schema.columns[0]["identity"] == "a",
            )
            complete = ids[-1] >= upper or len(ids) < args.batch_size
            phase = "checkpoint-page"
            state = checkpoint_after_page(
                state_path,
                state,
                ids=ids,
                changed_rows=changed_rows,
                complete=complete,
            )
            lower = ids[-1]
            processed_this_run += 1
            print(
                f"Committed source page {ids[0]}..{ids[-1]} "
                f"({len(ids)} source rows; {changed_rows} inserted/changed rows)."
            )
            if complete:
                print("Sync complete for the frozen upper ID.")
                return 0

        state = save_terminal_state(
            state_path,
            state,
            status="paused",
            message=f"Paused after this invocation reached --max-batches={args.max_batches}",
        )
        print("Sync paused/incomplete at the requested batch cap; rerun the same command to resume.")
        return 0
    except KeyboardInterrupt:
        if local is not None:
            try:
                local.rollback()
            except Exception:
                pass
        if state is not None and state_verified:
            try:
                save_failure_if_cursor_current(
                    state_path,
                    state,
                    status="interrupted",
                    message=f"Interrupted during {phase}; last committed cursor is unchanged",
                )
            except Exception:
                pass
        print(f"Interrupted during {phase}; current page was not checkpointed.", file=sys.stderr)
        return 130
    except SafeSyncError as exc:
        if local is not None:
            try:
                local.rollback()
            except Exception:
                pass
        if state is not None and state_verified:
            try:
                save_failure_if_cursor_current(
                    state_path,
                    state,
                    status="failed",
                    message=f"Failed during {phase}; last committed cursor is unchanged",
                )
            except Exception:
                pass
        raise exc
    except Exception as exc:
        if local is not None:
            try:
                local.rollback()
            except Exception:
                pass
        if state is not None and state_verified:
            try:
                save_failure_if_cursor_current(
                    state_path,
                    state,
                    status="failed",
                    message=f"Failed during {phase}; last committed cursor is unchanged",
                )
            except Exception:
                pass
        raise database_error(phase, exc) from None
    finally:
        if local is not None and lock_acquired:
            release_advisory_lock(local)
        close_connection(remote)
        close_connection(local)


def close_connection(connection) -> None:
    if connection is None:
        return
    try:
        if not connection.closed:
            connection.rollback()
            connection.close()
    except Exception:
        pass


def load_database_urls() -> tuple[str, str]:
    load_dotenv(REPO_ROOT / ".env", override=False)
    remote_dsn = os.getenv("DATABASE_URL")
    local_dsn = os.getenv("LOCAL_DATABASE_URL")
    if not remote_dsn or not local_dsn:
        raise SafeSyncError("DATABASE_URL and LOCAL_DATABASE_URL must both be set")
    return remote_dsn, local_dsn


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        validate_arguments(args)
        args.state_file = resolve_state_path(args.state_file)
        if args.status:
            return status_command(args.state_file)
        remote_dsn, local_dsn = load_database_urls()
        if args.execute:
            return execute_sync(args, remote_dsn, local_dsn)
        return preflight(args, remote_dsn, local_dsn)
    except SafeSyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        sqlstate = getattr(exc, "pgcode", None)
        suffix = f" sqlstate={sqlstate}" if isinstance(sqlstate, str) and sqlstate else ""
        print(f"ERROR: unexpected {type(exc).__name__}{suffix}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
