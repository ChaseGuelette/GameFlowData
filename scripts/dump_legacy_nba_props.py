#!/usr/bin/env python3
"""Prepare and optionally create safe local dumps of legacy NBA prop tables."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

TABLES = ("public.raw_player_props_combined", "public.raw_player_props_archive")
DEFAULT_OUTPUT_DIR = Path.home() / "GameFlowArchives" / "nba-props-2026-11-12"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight", action="store_true", help="connect for remote size/free-space checks, but do not dump")
    mode.add_argument("--execute", action="store_true", help="run preflight and create both dumps")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--pg-dump", type=Path, help="explicit pg_dump executable")
    parser.add_argument("--force", action="store_true", help="replace existing dump/list/manifest files")
    return parser


def executable_candidates(explicit: Path | None) -> list[Path]:
    candidates: list[Path] = []
    if explicit:
        candidates.append(explicit.expanduser())
    found = shutil.which("pg_dump") or shutil.which("pg_dump.exe")
    if found:
        candidates.append(Path(found))
    program_files = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "PostgreSQL"
    if program_files.exists():
        candidates.extend(sorted(program_files.glob("*/bin/pg_dump.exe"), reverse=True))
        candidates.extend(sorted(program_files.glob("*/bin/pg_dump"), reverse=True))
    return candidates


def discover_tools(explicit: Path | None) -> tuple[Path, Path]:
    for candidate in executable_candidates(explicit):
        candidate = candidate.resolve()
        restore = candidate.with_name("pg_restore.exe" if candidate.suffix.lower() == ".exe" else "pg_restore")
        if candidate.is_file() and restore.is_file():
            return candidate, restore
    raise RuntimeError("pg_dump/pg_restore were not found; pass --pg-dump explicitly")


def tool_version(executable: Path) -> str:
    result = subprocess.run([str(executable), "--version"], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def safe_plan(args: argparse.Namespace, pg_dump: Path, pg_restore: Path) -> dict[str, Any]:
    return {
        "mode": "execute" if args.execute else "preflight" if args.preflight else "dry-run",
        "tables": list(TABLES),
        "format": "PostgreSQL custom",
        "output_directory": str(args.output_dir.expanduser().resolve()),
        "pg_dump": str(pg_dump),
        "pg_restore": str(pg_restore),
        "force": args.force,
        "remote_mutation": False,
    }


def load_database_url() -> str:
    from dotenv import load_dotenv

    load_dotenv()
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is not configured")
    return value


def connection_parts(database_url: str) -> dict[str, str]:
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not parsed.path:
        raise RuntimeError("DATABASE_URL is not a PostgreSQL connection URL")
    parts = {
        "host": parsed.hostname,
        "port": str(parsed.port or 5432),
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "dbname": unquote(parsed.path.lstrip("/")),
    }
    query = dict(item.split("=", 1) for item in parsed.query.split("&") if "=" in item)
    if query.get("sslmode"):
        parts["sslmode"] = query["sslmode"]
    return parts


def child_environment(parts: dict[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    env.pop("DATABASE_URL", None)
    env.pop("LOCAL_DATABASE_URL", None)
    env.update(
        {
            "PGHOST": parts["host"],
            "PGPORT": parts["port"],
            "PGUSER": parts["user"],
            "PGPASSWORD": parts["password"],
            "PGDATABASE": parts["dbname"],
            "PGAPPNAME": "gameflow:legacy-nba-props-dump",
        }
    )
    if parts.get("sslmode"):
        env["PGSSLMODE"] = parts["sslmode"]
    return env


def dump_path(output_dir: Path, table: str) -> Path:
    return output_dir / f"{table.split('.', 1)[1]}.dump"


def list_path(output_dir: Path, table: str) -> Path:
    return output_dir / f"{table.split('.', 1)[1]}.restore-list.txt"


def manifest_path(output_dir: Path) -> Path:
    return output_dir / "manifest.json"


def ensure_outputs_available(args: argparse.Namespace) -> None:
    paths = [manifest_path(args.output_dir)]
    for table in TABLES:
        paths.extend((dump_path(args.output_dir, table), list_path(args.output_dir, table)))
    existing = [path for path in paths if path.exists()]
    if existing and not args.force:
        joined = "\n  ".join(str(path) for path in existing)
        raise RuntimeError(f"output files already exist; use --force to replace them:\n  {joined}")


def existing_disk_probe(path: Path) -> Path:
    probe = path
    while not probe.exists():
        parent = probe.parent
        if parent == probe:
            raise RuntimeError(f"no existing parent found for output directory {path}")
        probe = parent
    return probe


def remote_table_metadata(database_url: str) -> dict[str, dict[str, Any]]:
    import psycopg2

    result: dict[str, dict[str, Any]] = {}
    with psycopg2.connect(database_url, application_name="gameflow:legacy-nba-props-dump-preflight") as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            cur.execute("SET LOCAL lock_timeout = '5s'")
            for table in TABLES:
                cur.execute(
                    """
                    SELECT pg_relation_size(%s::regclass),
                           pg_total_relation_size(%s::regclass),
                           pg_get_serial_sequence(%s, 'staging_id')
                    """,
                    (table, table, table),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"size query returned no row for {table}")
                relation_bytes, total_bytes, owned_sequence = row
                result[table] = {
                    "relation_bytes": int(relation_bytes),
                    "total_bytes": int(total_bytes),
                    "owned_staging_id_sequence": owned_sequence,
                }
    return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_dump(
    pg_dump: Path,
    pg_restore: Path,
    env: dict[str, str],
    output_dir: Path,
    table: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    destination = dump_path(output_dir, table)
    listing = list_path(output_dir, table)
    started = datetime.now(UTC)
    command = [str(pg_dump), "--format=custom", "--no-owner", "--no-privileges", f"--table={table}"]
    owned_sequence = metadata.get("owned_staging_id_sequence")
    if owned_sequence:
        command.append(f"--table={owned_sequence}")
    command.append(f"--file={destination}")
    subprocess.run(
        command,
        env=env,
        check=True,
    )
    restore_result = subprocess.run([str(pg_restore), "--list", str(destination)], check=True, capture_output=True, text=True)
    relation_name = table.split('.', 1)[1]
    if f"TABLE DATA public {relation_name}" not in restore_result.stdout:
        raise RuntimeError(f"restore listing does not contain TABLE DATA for {table}")
    if owned_sequence and "SEQUENCE" not in restore_result.stdout:
        raise RuntimeError(f"restore listing does not contain the owned sequence for {table}")
    listing.write_text(restore_result.stdout, encoding="utf-8")
    stat = destination.stat()
    return {
        "table": table,
        "dump_file": destination.name,
        "restore_list_file": listing.name,
        "size_bytes": stat.st_size,
        "sha256": sha256_file(destination),
        "owned_staging_id_sequence": owned_sequence,
        "started_at": started.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "file_mtime_utc": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.output_dir = args.output_dir.expanduser().resolve()
    pg_dump, pg_restore = discover_tools(args.pg_dump)
    plan = safe_plan(args, pg_dump, pg_restore)
    plan["pg_dump_version"] = tool_version(pg_dump)
    plan["pg_restore_version"] = tool_version(pg_restore)
    print(json.dumps(plan, indent=2, sort_keys=True))
    if not args.preflight and not args.execute:
        print("Dry-run only: no environment file was loaded and no database connection was attempted.")
        return 0

    database_url = load_database_url()
    parts = connection_parts(database_url)
    if parts["port"] == "6543":
        raise RuntimeError("pg_dump requires a direct/session connection; DATABASE_URL uses transaction-pooler port 6543")
    ensure_outputs_available(args)
    metadata = remote_table_metadata(database_url)
    disk_probe = existing_disk_probe(args.output_dir)
    free_bytes = shutil.disk_usage(disk_probe).free
    total_remote_bytes = sum(item["total_bytes"] for item in metadata.values())
    preflight = {"remote_table_metadata": metadata, "local_free_bytes": free_bytes, "remote_total_bytes": total_remote_bytes}
    print(json.dumps(preflight, indent=2, sort_keys=True))
    if free_bytes < total_remote_bytes:
        raise RuntimeError("local free space is smaller than the remote total table size")
    if not args.execute:
        print("Preflight complete: no dump was created and no remote data was mutated.")
        return 0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    env = child_environment(parts)
    manifest = {
        "created_at": datetime.now(UTC).isoformat(),
        "format": "PostgreSQL custom",
        "tables": [],
        "remote_table_metadata": metadata,
        "local_free_bytes_before_dump": free_bytes,
        "tools": {"pg_dump": plan["pg_dump_version"], "pg_restore": plan["pg_restore_version"]},
        "connection_metadata": {"database": parts["dbname"], "port": parts["port"], "sslmode": parts.get("sslmode")},
        "remote_mutation": False,
    }
    for table in TABLES:
        manifest["tables"].append(run_dump(pg_dump, pg_restore, env, args.output_dir, table, metadata[table]))
        manifest["updated_at"] = datetime.now(UTC).isoformat()
        manifest_path(args.output_dir).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
