from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

TABLE = "public.mlb_player_props_clv_snapshots"
MIN_ID = 9_223_438
MAX_ID = 10_951_689
CHUNK_SIZE = 250_000
MARKET = "pitcher_strikeouts"
START_UTC = datetime(2026, 7, 1, 4, 0, tzinfo=timezone.utc)  # 2026-07-01 00:00 ET
END_UTC = datetime(2026, 7, 19, 4, 0, tzinfo=timezone.utc)   # 2026-07-19 00:00 ET
ET = ZoneInfo("America/New_York")
TAG = "targeted_pitcher_strikeouts_july_2026"

# Every data query is SELECT-only and starts with the indexed primary-key ID bound.
TARGET_SQL = f"""
SELECT
    id,
    requested_snapshot_time,
    snapshot_time,
    commence_time,
    market_last_update,
    bookmaker_last_update,
    inserted_at,
    game_id,
    player_id,
    api_player_name,
    scrape_reason,
    target_offset_minutes,
    player_link_method
FROM {TABLE}
WHERE id >= %(chunk_start_id)s
  AND id <= %(chunk_end_id)s
  AND market_key = %(market_key)s
  AND requested_snapshot_time >= %(start_utc)s
  AND requested_snapshot_time < %(end_utc)s
ORDER BY id
""".strip()


def iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def ts_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: iso(row[key])
        for key in (
            "id",
            "requested_snapshot_time",
            "snapshot_time",
            "commence_time",
            "market_last_update",
            "bookmaker_last_update",
            "inserted_at",
        )
    }


def extrema(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    present = [(r[field], r["id"]) for r in rows if r[field] is not None]
    if not present:
        return {"min": None, "min_id": None, "max": None, "max_id": None}
    low = min(present)
    high = max(present)
    return {"min": iso(low[0]), "min_id": low[1], "max": iso(high[0]), "max_id": high[1]}


def fetch(label: str, env_name: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    url = os.environ.get(env_name)
    if not url:
        raise RuntimeError(f"Missing {env_name}")
    conn = psycopg2.connect(
        url,
        connect_timeout=10,
        application_name="hermes_final_pitcher_k_july_select_verifier",
        options="-c statement_timeout=60000 -c lock_timeout=5000",
    )
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)
    rows: list[dict[str, Any]] = []
    chunks: list[dict[str, Any]] = []
    try:
        chunk_start = MIN_ID
        with conn.cursor() as cur:
            while chunk_start <= MAX_ID:
                chunk_end = min(chunk_start + CHUNK_SIZE - 1, MAX_ID)
                params = {
                    "chunk_start_id": chunk_start,
                    "chunk_end_id": chunk_end,
                    "market_key": MARKET,
                    "start_utc": START_UTC,
                    "end_utc": END_UTC,
                }
                started = time.monotonic()
                cur.execute(TARGET_SQL, params)
                names = [d.name for d in cur.description]
                batch = [dict(zip(names, item)) for item in cur.fetchall()]
                elapsed_ms = round((time.monotonic() - started) * 1000, 1)
                rows.extend(batch)
                chunks.append(
                    {
                        "chunk_start_id": chunk_start,
                        "chunk_end_id": chunk_end,
                        "rows": len(batch),
                        "elapsed_ms": elapsed_ms,
                    }
                )
                chunk_start = chunk_end + 1
        conn.rollback()  # close the read-only snapshot without committing anything
    finally:
        conn.close()
    return rows, chunks


def summarize(rows: list[dict[str, Any]], chunks: list[dict[str, Any]]) -> dict[str, Any]:
    rows.sort(key=lambda r: r["id"])
    by_date = Counter(r["requested_snapshot_time"].astimezone(ET).date().isoformat() for r in rows)
    by_reason = Counter(r["scrape_reason"] for r in rows)
    by_offset = Counter("NULL" if r["target_offset_minutes"] is None else str(r["target_offset_minutes"]) for r in rows)
    by_date_reason_offset = Counter(
        (
            r["requested_snapshot_time"].astimezone(ET).date().isoformat(),
            r["scrape_reason"],
            "NULL" if r["target_offset_minutes"] is None else str(r["target_offset_minutes"]),
        )
        for r in rows
    )
    unlinked = Counter(r["api_player_name"] for r in rows if r["player_id"] is None)
    tag_methods = Counter(
        r["player_link_method"]
        for r in rows
        if r["player_link_method"] is not None and TAG in r["player_link_method"]
    )

    def at_or_after(field: str) -> list[int]:
        return [
            r["id"]
            for r in rows
            if r[field] is not None and r["commence_time"] is not None and r[field] >= r["commence_time"]
        ]

    violation_sets = {
        field: at_or_after(field)
        for field in (
            "requested_snapshot_time",
            "snapshot_time",
            "market_last_update",
            "bookmaker_last_update",
        )
    }
    any_violation = sorted({item for ids in violation_sets.values() for item in ids})
    return {
        "total_rows": len(rows),
        "first_id_row_timestamps": ts_row(rows[0]) if rows else None,
        "last_id_row_timestamps": ts_row(rows[-1]) if rows else None,
        "timestamp_extrema": {
            field: extrema(rows, field)
            for field in (
                "requested_snapshot_time",
                "snapshot_time",
                "commence_time",
                "market_last_update",
                "bookmaker_last_update",
                "inserted_at",
            )
        },
        "game_id_null": sum(r["game_id"] is None for r in rows),
        "player_id_null": sum(r["player_id"] is None for r in rows),
        "distinct_unlinked_names_count": len(unlinked),
        "distinct_unlinked_names": [
            {"api_player_name": name, "rows": count}
            for name, count in sorted(unlinked.items(), key=lambda item: (str(item[0]), item[1]))
        ],
        "counts_by_et_date": dict(sorted(by_date.items())),
        "counts_by_reason": dict(sorted(by_reason.items(), key=lambda item: str(item[0]))),
        "counts_by_offset_minutes": dict(sorted(by_offset.items())),
        "counts_by_et_date_reason_offset": [
            {"et_date": key[0], "scrape_reason": key[1], "target_offset_minutes": key[2], "rows": count}
            for key, count in sorted(by_date_reason_offset.items(), key=lambda item: item[0])
        ],
        "post_commence_violations": {
            field: {"count": len(ids), "ids": ids}
            for field, ids in violation_sets.items()
        } | {"any_of_the_above": {"count": len(any_violation), "ids": any_violation}},
        "player_link_method_tag": {
            "match_expression": f"player_link_method LIKE '%{TAG}%'",
            "matching_rows": sum(tag_methods.values()),
            "breakdown": [
                {"player_link_method": method, "rows": count}
                for method, count in sorted(tag_methods.items())
            ],
        },
        "chunk_execution": chunks,
    }


remote_rows, remote_chunks = fetch("remote", "DATABASE_URL")
local_rows, local_chunks = fetch("local", "LOCAL_DATABASE_URL")
remote_ids = {r["id"] for r in remote_rows}
local_ids = {r["id"] for r in local_rows}
result = {
    "scope": {
        "table": TABLE,
        "id_min_inclusive": MIN_ID,
        "id_max_inclusive": MAX_ID,
        "market_key": MARKET,
        "et_date_start_inclusive": "2026-07-01",
        "et_date_end_inclusive": "2026-07-18",
        "utc_start_inclusive": iso(START_UTC),
        "utc_end_exclusive": iso(END_UTC),
        "chunk_size_ids": CHUNK_SIZE,
        "statement_timeout_ms": 60_000,
        "lock_timeout_ms": 5_000,
        "transaction": "READ ONLY, REPEATABLE READ; rolled back",
    },
    "remote": summarize(remote_rows, remote_chunks),
    "local": summarize(local_rows, local_chunks),
    "id_set_comparison": {
        "remote_target_ids_absent_locally": {
            "count": len(remote_ids - local_ids),
            "ids": sorted(remote_ids - local_ids),
        },
        "local_target_ids_absent_remotely": {
            "count": len(local_ids - remote_ids),
            "ids": sorted(local_ids - remote_ids),
        },
    },
    "exact_sql": TARGET_SQL,
    "parameterization": {
        "market_key": MARKET,
        "start_utc": iso(START_UTC),
        "end_utc": iso(END_UTC),
        "chunk_windows": [
            [start, min(start + CHUNK_SIZE - 1, MAX_ID)]
            for start in range(MIN_ID, MAX_ID + 1, CHUNK_SIZE)
        ],
    },
}
out_path = ROOT / ".hermes" / "tmp" / "verify_pitcher_k_july_results.json"
out_path.write_text(json.dumps(result, indent=2, default=iso), encoding="utf-8")
print(json.dumps({
    "result_path": str(out_path),
    "remote_total": result["remote"]["total_rows"],
    "local_total": result["local"]["total_rows"],
    "remote_missing_local": result["id_set_comparison"]["remote_target_ids_absent_locally"]["count"],
    "local_missing_remote": result["id_set_comparison"]["local_target_ids_absent_remotely"]["count"],
}, indent=2))
