#!/usr/bin/env python
"""Bounded recovery of official NBA PDF publication observations onto existing rows."""

from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date
from pathlib import Path

from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.client import get_engine  # noqa: E402
from src.models.features.nba.season import is_competitive_nba_game_id, resolve_nba_season  # noqa: E402
from src.scrapers.nba_official_injury_reports import (  # noqa: E402
    OfficialInjuryReport,
    OfficialInjuryRow,
    fetch_official_injury_report,
)

MAX_REPORT_URLS = 4
MAX_GAME_IDS = 10
MAX_DB_ROWS = 2000

_SELECT_ROWS = text("""
    SELECT id, report_date, game_id, team, player, status, reason, source_observations
    FROM public.rapidapi_injuries
    WHERE report_date = :game_date
      AND game_id IN :game_ids
    ORDER BY id
    LIMIT 2001
""").bindparams(bindparam("game_ids", expanding=True))

_UPDATE_ROW = text("""
    UPDATE public.rapidapi_injuries
    SET source_observations = jsonb_set(
        COALESCE(source_observations, '{}'::jsonb),
        ARRAY[CAST(:capture_id AS text)],
        CAST(:observation AS jsonb),
        true
    )
    WHERE id = :row_id
      AND COALESCE(source_observations, '{}'::jsonb) -> CAST(:capture_id AS text)
          IS DISTINCT FROM CAST(:observation AS jsonb)
""")

_READBACK = text("""
    SELECT id, source_observations
    FROM public.rapidapi_injuries
    WHERE id IN :row_ids
    ORDER BY id
""").bindparams(bindparam("row_ids", expanding=True))


def _parse_iso_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must be canonical YYYY-MM-DD")
    return parsed


def _normalize(value: object) -> str:
    """Strip accents, punctuation, and spacing only; do not guess identities."""
    decomposed = unicodedata.normalize("NFKD", "" if value is None else str(value))
    return "".join(character.casefold() for character in decomposed if character.isalnum())


def _signature(*, game_date: object, team: object, player: object, status: object, reason: object) -> tuple[str, ...]:
    return (
        str(game_date),
        _normalize(team),
        _normalize(player),
        _normalize(status),
        _normalize(reason),
    )


def _load_rows(engine, game_date: date, game_ids: list[str]) -> list[dict]:
    with engine.begin() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        _verify_read_guards(conn)
        rows = [dict(row._mapping) for row in conn.execute(
            _SELECT_ROWS, {"game_date": game_date, "game_ids": game_ids}
        ).fetchall()]
    if len(rows) > MAX_DB_ROWS:
        raise RuntimeError(f"Bounded injury query exceeded {MAX_DB_ROWS} rows")
    return rows


def _verify_read_guards(conn) -> None:
    if conn.execute(text("SHOW transaction_read_only")).scalar_one() != "on":
        raise RuntimeError("Read-only transaction guard was not applied")
    if conn.execute(text("SHOW statement_timeout")).scalar_one() != "15s":
        raise RuntimeError("Statement timeout guard was not applied")


def _official_header(report: OfficialInjuryReport) -> dict[str, str]:
    return {
        "printed": report.printed_header,
        "publication_date": report.publication_date,
        "publication_time": report.publication_time,
        "timezone": "America/New_York",
    }


def _plan_updates(
    reports: list[OfficialInjuryReport], db_rows: list[dict], game_date: date
) -> tuple[list[dict], dict]:
    db_by_signature: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    for row in db_rows:
        db_by_signature[_signature(
            game_date=row["report_date"],
            team=row["team"],
            player=row["player"],
            status=row["status"],
            reason=row["reason"],
        )].append(row)

    metrics: Counter = Counter(db_rows_read=len(db_rows), pdfs=len(reports))
    planned: list[dict] = []
    examples: list[dict] = []
    for report in reports:
        report_signatures: dict[tuple[str, ...], list[tuple[int, OfficialInjuryRow]]] = defaultdict(list)
        for ordinal, official in enumerate(report.rows, 1):
            if official.game_date != game_date.isoformat():
                metrics["official_rows_other_game_date"] += 1
                continue
            signature = _signature(
                game_date=official.game_date,
                team=official.team,
                player=official.player,
                status=official.status,
                reason=official.reason,
            )
            report_signatures[signature].append((ordinal, official))

        metrics["official_rows_target_date"] += sum(len(values) for values in report_signatures.values())
        for signature, official_matches in report_signatures.items():
            if len(official_matches) != 1:
                metrics["nonunique_official_signatures"] += len(official_matches)
                continue
            db_matches = db_by_signature.get(signature, [])
            if len(db_matches) != 1:
                metrics["unmatched_or_nonunique_db_rows"] += 1
                continue
            ordinal, official = official_matches[0]
            db_row = db_matches[0]
            raw = {
                "date": official.game_date,
                "reportTime": report.publication_time,
                "team": db_row["team"],
                "player": db_row["player"],
                "status": db_row["status"],
                "reason": db_row["reason"],
            }
            observation = {
                "source": "official_nba_pdf",
                "lineage": "official_reconstruction",
                "requested_date": report.publication_date,
                "source_url": report.source_url,
                "capture_sha256": report.capture_sha256,
                "hash_kind": "http_body",
                "official_report_header": _official_header(report),
                "published_at_utc": report.published_at_utc,
                "raw_official_row": asdict(official),
                "records": [{"ordinal": ordinal, "raw": raw, "official_raw": asdict(official)}],
            }
            capture_id = report.capture_sha256
            existing = db_row.get("source_observations")
            if isinstance(existing, str):
                try:
                    existing = json.loads(existing)
                except ValueError:
                    existing = None
            existing_capture = existing.get(capture_id) if isinstance(existing, dict) else None
            if existing_capture == observation:
                metrics["already_present"] += 1
                continue
            planned.append({
                "row_id": int(db_row["id"]),
                "game_id": db_row["game_id"],
                "capture_id": capture_id,
                "observation": observation,
            })
            metrics["matched_observations"] += 1
            if len(examples) < 5:
                examples.append({
                    "game_id": db_row["game_id"],
                    "row_id": int(db_row["id"]),
                    "player": db_row["player"],
                    "team": db_row["team"],
                    "status": db_row["status"],
                    "reason": db_row["reason"],
                    "published_at_utc": report.published_at_utc,
                })
    metrics["distinct_db_rows_matched"] = len({item["row_id"] for item in planned})
    return planned, {**dict(metrics), "examples": examples}


def _execute_updates(engine, planned: list[dict]) -> int:
    changed = 0
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '3s'"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        if conn.execute(text("SHOW transaction_read_only")).scalar_one() != "off":
            raise RuntimeError("Recovery requires a writable transaction")
        for item in planned:
            result = conn.execute(_UPDATE_ROW, {
                "row_id": item["row_id"],
                "capture_id": item["capture_id"],
                "observation": json.dumps(item["observation"], ensure_ascii=False, sort_keys=True),
            })
            changed += result.rowcount
    return changed


def _verify_readback(engine, planned: list[dict]) -> int:
    if not planned:
        return 0
    expected = {(item["row_id"], item["capture_id"]): item["observation"] for item in planned}
    row_ids = sorted({item["row_id"] for item in planned})
    with engine.begin() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        _verify_read_guards(conn)
        rows = conn.execute(_READBACK, {"row_ids": row_ids}).fetchall()
    verified = 0
    for row in rows:
        observations = row.source_observations
        if isinstance(observations, str):
            observations = json.loads(observations)
        for capture_id, expected_observation in (
            (capture_id, observation)
            for (row_id, capture_id), observation in expected.items()
            if row_id == row.id
        ):
            if not isinstance(observations, dict) or observations.get(capture_id) != expected_observation:
                raise RuntimeError(f"Official observation readback mismatch for injury row {row.id}")
            verified += 1
    if verified != len(expected):
        raise RuntimeError(f"Official observation readback verified {verified}/{len(expected)} updates")
    return verified


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach reconstructed official NBA PDF publication instants to exact existing injury rows."
    )
    parser.add_argument("--game-date", required=True, type=_parse_iso_date)
    parser.add_argument("--game-id", action="append", required=True, help="Exact existing NBA game_id; repeat up to 10")
    parser.add_argument(
        "--report-url",
        action="append",
        required=True,
        help="Explicit official HTTPS ak-static.cms.nba.com injury-report PDF; repeat up to 4",
    )
    parser.add_argument("--local", action="store_true", help="Use get_engine(local=True)")
    parser.add_argument("--execute", action="store_true", help="Apply additive JSONB updates; default is read-only dry run")
    parser.add_argument("--output", type=Path, help="Optional JSON receipt path")
    args = parser.parse_args()
    args.game_id = list(dict.fromkeys(args.game_id))
    args.report_url = list(dict.fromkeys(args.report_url))
    if not 1 <= len(args.game_id) <= MAX_GAME_IDS:
        parser.error(f"--game-id requires 1..{MAX_GAME_IDS} distinct values")
    if any(not is_competitive_nba_game_id(value) for value in args.game_id):
        parser.error("--game-id values must be exact competitive NBA identifiers")
    try:
        for game_id in args.game_id:
            resolve_nba_season(game_id, args.game_date)
    except ValueError as exc:
        parser.error(str(exc))
    if not 1 <= len(args.report_url) <= MAX_REPORT_URLS:
        parser.error(f"--report-url requires 1..{MAX_REPORT_URLS} distinct values")
    return args


def main() -> int:
    args = parse_args()
    reports = [fetch_official_injury_report(url) for url in args.report_url]
    engine = get_engine(local=args.local)
    db_rows = _load_rows(engine, args.game_date, args.game_id)
    planned, metrics = _plan_updates(reports, db_rows, args.game_date)
    changed = 0
    verified = 0
    if args.execute:
        changed = _execute_updates(engine, planned)
        verified = _verify_readback(engine, planned)
        if changed != len(planned):
            raise RuntimeError(f"Expected {len(planned)} distinct updates, database changed {changed}")
    receipt = {
        "mode": "execute" if args.execute else "dry_run_read_only",
        "game_date": args.game_date.isoformat(),
        "game_ids": args.game_id,
        "report_urls": args.report_url,
        "pdf_receipts": [
            {
                "source_url": report.source_url,
                "capture_sha256": report.capture_sha256,
                "printed_header": report.printed_header,
                "published_at_utc": report.published_at_utc,
                "page_count": report.page_count,
                "parsed_rows": len(report.rows),
            }
            for report in reports
        ],
        "metrics": metrics,
        "planned_updates": len(planned),
        "planned_row_ids": sorted({item["row_id"] for item in planned}),
        "rows_changed": changed,
        "readback_verified": verified,
        "completeness_claimed": False,
        "lineage": "official_reconstruction",
    }
    rendered = json.dumps(receipt, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
