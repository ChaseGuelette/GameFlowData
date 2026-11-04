"""DB-first injury capture using the existing injury and job-execution tables.

Raw injury observations stay on rapidapi_injuries; job_executions.metrics is
only the existing operational completion/checkpoint record. No filesystem
state is required by the normal API ingestion path.
"""

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sqlalchemy import bindparam, text

VERSION = "nba-injury-db-v1"
JOB_NAME = "nba_injury_ingest"
VALID_STATUSES = {"Out", "Doubtful", "Questionable", "Probable", "Available"}

CAPTURE_COLUMNS_SQL = """
ALTER TABLE public.rapidapi_injuries
    ADD COLUMN IF NOT EXISTS game_id text,
    ADD COLUMN IF NOT EXISTS source_observations jsonb;
"""

UPSERT_SQL = """
INSERT INTO public.rapidapi_injuries
    (report_date, team, player, status, reason, report_time,
     injury_category, injury_detail, source_observations)
SELECT report_date, team, player, status, reason, report_time,
       injury_category, injury_detail, source_observations
FROM jsonb_to_recordset(CAST(:records AS jsonb)) AS incoming(
    report_date date, team text, player text, status text, reason text,
    report_time text, injury_category text, injury_detail text,
    source_observations jsonb)
ON CONFLICT (report_date, COALESCE(team, ''), player, status, COALESCE(reason, ''))
DO UPDATE SET
    report_time = EXCLUDED.report_time,
    source_observations = EXCLUDED.source_observations
                          || COALESCE(rapidapi_injuries.source_observations, '{}'::jsonb)
RETURNING id
"""


def ensure_capture_columns(engine):
    """Explicit operator bootstrap only; never called by ordinary ingestion."""
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '3s'"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        conn.execute(text(CAPTURE_COLUMNS_SQL))
    require_capture_schema(engine)


def require_capture_schema(engine):
    with engine.connect() as conn:
        conn.execute(text("SELECT game_id, source_observations FROM public.rapidapi_injuries WHERE false"))
        conn.execute(text("SELECT metrics FROM public.job_executions WHERE false"))


def prepare_records(requested_date, records, reason_parser, capture_metadata=None):
    """Build table rows; the legacy report_date column means applicable game date.

    Keep the API request date and every original record in source_observations.
    Grouping follows the existing unique key, without losing separate captures.
    Invalid status strings remain raw evidence and are excluded by the linker.
    """
    if not isinstance(records, list):
        raise ValueError("An API error object is not an injury record list")
    metadata = dict(capture_metadata or {})
    observed = metadata.get("retrieved_at_utc") or datetime.now(UTC).isoformat()
    instant = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    if instant.utcoffset() is None:
        raise ValueError("Capture time must include a timezone")
    digest = metadata.get("raw_body_sha256") or hashlib.sha256(
        json.dumps(records, sort_keys=True, allow_nan=False).encode("utf-8")
    ).hexdigest()
    # Identical content for one request date is one retained capture, even when
    # the recurring Railway job sees it again. Keep the first stored receipt.
    capture_id = hashlib.sha256(f"{requested_date}:{digest}".encode()).hexdigest()
    grouped = {}
    invalid_statuses = 0
    for ordinal, record in enumerate(records, 1):
        if not isinstance(record, dict):
            raise ValueError(f"Record {ordinal} is not an object")
        day = record.get("date")
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError(f"Record {ordinal} has no valid payload game date")
        for field in ("player", "status"):
            if not isinstance(record.get(field), str) or not record[field].strip():
                raise ValueError(f"Record {ordinal} has no {field}; refusing a partial DB capture")
        for field in ("team", "reason", "reportTime"):
            if record.get(field) is not None and not isinstance(record[field], str):
                raise ValueError(f"Record {ordinal} has invalid {field}")
        team, reason = record.get("team") or None, record.get("reason") or None
        key = (day, team or "", record["player"], record["status"], reason or "")
        invalid_statuses += record["status"] not in VALID_STATUSES
        if key not in grouped:
            category, detail = reason_parser(reason)
            grouped[key] = {
                "report_date": day, "team": team, "player": record["player"],
                "status": record["status"], "reason": reason,
                "report_time": record.get("reportTime"), "injury_category": category,
                "injury_detail": detail, "source_observations": {capture_id: {
                    "requested_date": requested_date.isoformat(), "retrieved_at_utc": observed,
                    "capture_sha256": digest,
                    "hash_kind": "http_body" if metadata.get("raw_body_sha256") else "canonical_records",
                    "source": "rapidapi", "records": [],
                }},
            }
        grouped[key]["source_observations"][capture_id]["records"].append({
            "ordinal": ordinal, "raw": record,
            "valid_status": record["status"] in VALID_STATUSES,
        })
    metrics = {
        "version": VERSION, "requested_date": requested_date.isoformat(),
        "capture_id": capture_id, "capture_sha256": digest,
        "source_record_count": len(records), "distinct_row_count": len(grouped),
        "invalid_status_count": invalid_statuses, "retrieved_at_utc": observed,
        "empty_response": not records and not metadata.get("missing_report", False),
        "missing_report": bool(metadata.get("missing_report", False)),
        "http_status": metadata.get("http_status", 200), "source_acceptance": "unverified",
    }
    return list(grouped.values()), metrics


def persist_capture(engine, requested_date, records, reason_parser, capture_metadata=None):
    rows, metrics = prepare_records(requested_date, records, reason_parser, capture_metadata)
    started = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '3s'"))
        conn.execute(text("SET LOCAL statement_timeout = '30s'"))
        row_ids = []
        if rows:
            result = conn.execute(text(UPSERT_SQL), {"records": json.dumps(rows, allow_nan=False)})
            row_ids = [int(row[0]) for row in result.fetchall()]
            if len(row_ids) != len(rows):
                raise RuntimeError("Injury upsert did not return every expected row; rolling back")
        metrics["row_ids"] = row_ids
        ended = datetime.now(UTC)
        # Completion and injury rows commit together, including genuine empty lists.
        conn.execute(text("""
            INSERT INTO public.job_executions
                (job_name, started_at, ended_at, status, duration_seconds, metrics)
            VALUES (:name, :started, :ended, 'success', :duration, CAST(:metrics AS jsonb))
        """), {"name": JOB_NAME, "started": started, "ended": ended,
                 "duration": (ended - started).total_seconds(), "metrics": json.dumps(metrics)})
    return len(rows)


def completed_dates(engine, start_date, end_date):
    with engine.begin() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '5s'"))
        rows = conn.execute(text("""
            SELECT DISTINCT metrics->>'requested_date'
            FROM public.job_executions
            WHERE job_name = :name AND status = 'success'
              AND metrics->>'version' = :version
              AND metrics->>'requested_date' BETWEEN :start AND :end
        """), {"name": JOB_NAME, "version": VERSION,
                 "start": start_date.isoformat(), "end": end_date.isoformat()}).fetchall()
    return {date.fromisoformat(row[0]) for row in rows}


def historical_request_dates(engine, seasons, start_date, end_date):
    """Read historical game dates, not props or training-player eligibility."""
    if not seasons or any(value not in {"22023", "22024", "22025", "22026"} for value in seasons):
        raise ValueError("Unsupported NBA season selection")
    query = text("""
        SELECT game_date FROM public.player_game_stats
        WHERE season_id IN :seasons AND game_date IS NOT NULL
          AND game_date BETWEEN :start AND :end
        LIMIT 200001
    """).bindparams(bindparam("seasons", expanding=True))
    dates, rows_read = set(), 0
    with engine.begin() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '5s'"))
        result = conn.execution_options(stream_results=True).execute(
            query, {"seasons": seasons, "start": start_date, "end": end_date}
        )
        for batch in iter(lambda: result.fetchmany(1000), []):
            rows_read += len(batch)
            if rows_read >= 200001:
                raise ValueError("Historical game-date query exceeded its row budget")
            dates.update(row[0] for row in batch)
    if not dates:
        raise ValueError("No historical game dates found")
    return sorted(dates | {day - timedelta(days=1) for day in dates})


def saved_captures(path):
    """Stream completed local recovery captures for a one-time DB import.

    This is optional recovery input, not a dependency of the Railway API job.
    """
    from src.scrapers.nba_injury_pilot import decode_body, is_missing_report, read_json, verified_capture

    path = Path(path).resolve()
    if path.is_dir():
        path = path / "partitions.json"
    manifest = read_json(path)
    if manifest.get("complete") is False:
        raise ValueError("Capture/import manifest is incomplete")
    manifests = [path.parent / name for name in manifest["manifests"]] if "manifests" in manifest else [path]
    seen = set()
    for capture_manifest in manifests:
        data = read_json(capture_manifest)
        if data.get("complete") is False:
            raise ValueError("Capture partition is incomplete")
        entries = data.get("captures", data.get("receipts"))
        if not isinstance(entries, list):
            raise ValueError("Capture manifest has no receipts")
        for entry in entries:
            receipt, body = verified_capture(entry, capture_manifest.parent)
            key = (receipt["requested_date"], receipt["raw_body_sha256"], receipt.get("retrieved_at_utc"))
            if key in seen:
                continue
            seen.add(key)
            if is_missing_report(receipt, body):
                receipt["missing_report"] = True
                records = []
            else:
                records = decode_body(receipt, body)
            yield date.fromisoformat(receipt["requested_date"]), records, receipt
