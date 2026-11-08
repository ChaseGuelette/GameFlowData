#!/usr/bin/env python
"""Resume-safe bounded recovery of historical NBA injury PDF observations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from dataclasses import fields
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.recover_nba_injury_report_times import _normalize, _plan_updates  # noqa: E402
from src.db.client import get_engine  # noqa: E402
from src.models.features.nba.season import is_competitive_nba_game_id  # noqa: E402
from src.processing.nba_injury_game_timing import (  # noqa: E402
    GameTimingRequest,
    persist_game_timings,
    resolve_game_timings,
)
from src.scrapers.nba_official_injury_reports import (  # noqa: E402
    OfficialInjuryReport,
    OfficialInjuryRow,
    UnsupportedOfficialInjuryReportError,
    _validated_url,
    fetch_official_injury_report,
)

MAX_DAYS = 31
MAX_GAMES = 100
MAX_REQUESTS = 20
MAX_URLS = 4
MAX_INJURY_ROWS = 2000
DEFAULT_SLOTS = ["05PM"]

_SELECT_GAMES = text(
    """
    SELECT tgs.game_id, tgs.game_date::date AS game_date,
           array_agg(DISTINCT tgs.team_id ORDER BY tgs.team_id) AS team_ids
    FROM public.team_game_stats tgs
    WHERE tgs.game_date::date BETWEEN :start_date AND :end_date
      AND (:all_games OR tgs.game_id IN :game_ids)
      AND (:include_verified OR tgs.game_id NOT IN :verified_ids)
      AND tgs.game_id ~ '^(002|004|005)[0-9]{7}$'
    GROUP BY tgs.game_id, tgs.game_date::date
    ORDER BY COALESCE((CAST(:attempts AS jsonb) ->> tgs.game_id)::integer, 0),
             tgs.game_date::date, tgs.game_id
    LIMIT :limit
    """
).bindparams(bindparam("game_ids", expanding=True), bindparam("verified_ids", expanding=True))

_SELECT_INJURIES = text(
    """
    WITH targets AS (
      SELECT game_date, nba_team_id
      FROM jsonb_to_recordset(CAST(:targets AS jsonb))
           AS requested(game_date date, nba_team_id bigint)
    )
    SELECT ri.id, ri.report_date, ri.game_id, ri.nba_team_id, ri.player_id,
           ri.team, ri.player, ri.status, ri.reason, ri.source_observations
    FROM public.rapidapi_injuries ri
    JOIN targets t ON t.game_date = ri.report_date
                  AND t.nba_team_id = ri.nba_team_id
    ORDER BY ri.id
    LIMIT 2001
    """
)

_SELECT_ROWS_BY_ID = text(
    """
    SELECT id, report_date, game_id, nba_team_id, player_id,
           team, player, status, reason, source_observations
    FROM public.rapidapi_injuries
    WHERE id IN :row_ids
    ORDER BY id
    """
).bindparams(bindparam("row_ids", expanding=True))

_UPDATE_ROW = text(
    """
    UPDATE public.rapidapi_injuries
    SET game_id = COALESCE(game_id, :game_id),
        source_observations = jsonb_set(
          COALESCE(source_observations, '{}'::jsonb),
          ARRAY[CAST(:capture_id AS text)], CAST(:observation AS jsonb), true)
    WHERE id = :row_id
      AND report_date = :report_date
      AND nba_team_id = :nba_team_id
      AND player_id = :player_id
      AND team IS NOT DISTINCT FROM :team
      AND player IS NOT DISTINCT FROM :player
      AND status IS NOT DISTINCT FROM :status
      AND reason IS NOT DISTINCT FROM :reason
      AND (game_id IS NULL OR game_id = :game_id)
      AND COALESCE(source_observations, '{}'::jsonb) -> CAST(:capture_id AS text) IS NULL
    """
)

_LINK_ROW = text(
    """
    UPDATE public.rapidapi_injuries
    SET game_id = :game_id
    WHERE id = :row_id
      AND report_date = :report_date
      AND nba_team_id = :nba_team_id
      AND player_id = :player_id
      AND team IS NOT DISTINCT FROM :team
      AND player IS NOT DISTINCT FROM :player
      AND status IS NOT DISTINCT FROM :status
      AND reason IS NOT DISTINCT FROM :reason
      AND game_id IS NULL
      AND COALESCE(source_observations, '{}'::jsonb) -> CAST(:capture_id AS text)
          = CAST(:observation AS jsonb)
    """
)


def _iso_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be canonical YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must be canonical YYYY-MM-DD")
    return parsed


def _read_guards(conn) -> None:
    conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SET LOCAL lock_timeout = '3s'"))
    conn.execute(text("SET LOCAL statement_timeout = '15s'"))
    if conn.execute(text("SHOW transaction_read_only")).scalar_one() != "on":
        raise RuntimeError("read-only transaction guard was not applied")
    if conn.execute(text("SHOW lock_timeout")).scalar_one() != "3s":
        raise RuntimeError("read lock timeout guard was not applied")
    if conn.execute(text("SHOW statement_timeout")).scalar_one() != "15s":
        raise RuntimeError("read statement timeout guard was not applied")


def _write_guards(conn) -> None:
    conn.execute(text("SET LOCAL lock_timeout = '3s'"))
    conn.execute(text("SET LOCAL statement_timeout = '15s'"))
    if conn.execute(text("SHOW transaction_read_only")).scalar_one() != "off":
        raise RuntimeError("recovery requires a writable transaction")
    if conn.execute(text("SHOW lock_timeout")).scalar_one() != "3s":
        raise RuntimeError("write lock timeout guard was not applied")
    if conn.execute(text("SHOW statement_timeout")).scalar_one() != "15s":
        raise RuntimeError("write statement timeout guard was not applied")


def _jsonable(value: Any) -> Any:
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=_jsonable)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


@contextmanager
def _state_lock(state_file: Path):
    state_file.parent.mkdir(parents=True, exist_ok=True)
    lock = state_file.with_name(state_file.name + ".lock")
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(
            f"state lock exists: {lock}; verify no recovery process is running, then remove only that lock"
        ) from exc
    try:
        os.write(descriptor, f"pid={os.getpid()}\n".encode())
        os.close(descriptor)
        yield
    finally:
        try:
            lock.unlink()
        except FileNotFoundError:
            pass


def _scope(args: argparse.Namespace) -> dict:
    return {
        "start_date": args.start_date.isoformat(),
        "end_date": args.end_date.isoformat(),
        "game_ids": args.game_id,
        "report_urls": args.report_url,
        "report_slots": args.report_slot,
        "mode": "execute" if args.execute else "dry_run",
        "mirror_local": args.mirror_local,
    }


def _fingerprint(scope: dict) -> str:
    return hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _new_state(args: argparse.Namespace) -> dict:
    scope = _scope(args)
    return {
        "version": 1,
        "scope": scope,
        "fingerprint": _fingerprint(scope),
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
        "request_attempts": [],
        "report_cache": {},
        "games": {},
    }


def _load_state(args: argparse.Namespace) -> dict:
    if args.state_file.exists():
        if not args.resume:
            raise ValueError("state file already exists; pass --resume or choose a new --state-file")
        try:
            state = json.loads(args.state_file.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError("state file is not readable valid JSON") from exc
        if state.get("fingerprint") != _fingerprint(_scope(args)):
            raise ValueError("resume inputs do not match the state-file fingerprint")
        return state
    if args.resume:
        raise ValueError("--resume requires an existing --state-file")
    return _new_state(args)


def _save_state(path: Path, state: dict) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat()
    _atomic_json(path, state)


def _select_games(engine, args: argparse.Namespace, state: dict, *, recheck: bool = False) -> tuple[list[dict], bool]:
    requested = args.game_id or ["__none__"]
    verified_ids = [
        game_id for game_id, outcome in state["games"].items()
        if outcome.get("status") == "verified"
    ] or ["__none__"]
    with engine.begin() as conn:
        _read_guards(conn)
        rows = [dict(row) for row in conn.execute(
            _SELECT_GAMES,
            {
                "start_date": args.start_date,
                "end_date": args.end_date,
                "all_games": not bool(args.game_id),
                "game_ids": requested,
                "include_verified": bool(args.game_id) or recheck,
                "verified_ids": verified_ids,
                "attempts": json.dumps({key: value.get("attempt", 0) for key, value in state["games"].items()}),
                "limit": MAX_GAMES + 1,
            },
        ).mappings()]
    if args.game_id:
        found = {row["game_id"] for row in rows}
        missing = sorted(set(args.game_id) - found)
        if missing:
            raise ValueError(f"supplied game IDs are absent from the exact date cohort: {missing[:5]}")
    for row in rows:
        if not is_competitive_nba_game_id(row["game_id"]):
            raise RuntimeError(f"selected non-competitive NBA game ID: {row['game_id']}")
        if len(row["team_ids"] or []) != 2:
            raise RuntimeError(f"game {row['game_id']} does not have exactly two unique team identities")
    identities = [
        (row["game_date"], team_id)
        for row in rows
        for team_id in row["team_ids"]
    ]
    if len(identities) != len(set(identities)):
        raise RuntimeError("selected games contain a non-unique game-date/team identity")
    # Check uniqueness against all fixtures, not only this capped/filtered batch.
    selected = rows[:args.max_games]
    if selected:
        with engine.begin() as conn:
            _read_guards(conn)
            fixtures = conn.execute(text("""
                SELECT t.game_date::date AS game_date, t.team_id,
                       array_agg(DISTINCT t.game_id) AS game_ids
                FROM public.team_game_stats t
                JOIN jsonb_to_recordset(CAST(:targets AS jsonb))
                     AS x(game_date date, team_id bigint)
                  ON t.game_date = x.game_date AND t.team_id = x.team_id
                GROUP BY t.game_date::date, t.team_id
            """), {"targets": json.dumps([
                {"game_date": game["game_date"].isoformat(), "team_id": team}
                for game in selected for team in game["team_ids"]
            ])}).mappings().all()
        expected = {(game["game_date"], team): game["game_id"] for game in selected for team in game["team_ids"]}
        if len(fixtures) != len(expected) or any(
            row["game_ids"] != [expected[(row["game_date"], row["team_id"])]] for row in fixtures
        ):
            raise RuntimeError("date/team fixture is not unique in canonical stats")
    return rows[:MAX_GAMES], len(rows) > MAX_GAMES


def _load_injuries(engine, games: list[dict]) -> list[dict]:
    targets = [
        {"game_date": game["game_date"].isoformat(), "nba_team_id": team_id}
        for game in games
        for team_id in game["team_ids"]
    ]
    with engine.begin() as conn:
        _read_guards(conn)
        rows = [dict(row) for row in conn.execute(
            _SELECT_INJURIES, {"targets": json.dumps(targets)}
        ).mappings()]
    if len(rows) > MAX_INJURY_ROWS:
        raise RuntimeError(f"bounded injury query exceeded {MAX_INJURY_ROWS} rows")
    return rows


def _report_from_dict(payload: dict) -> OfficialInjuryReport:
    allowed_report = {field.name for field in fields(OfficialInjuryReport)}
    allowed_row = {field.name for field in fields(OfficialInjuryRow)}
    values = {key: payload[key] for key in allowed_report if key != "rows"}
    values["rows"] = tuple(
        OfficialInjuryRow(**{key: row[key] for key in allowed_row}) for row in payload["rows"]
    )
    return OfficialInjuryReport(**values)


def _candidate_urls(args: argparse.Namespace, game_date: date) -> list[str]:
    if args.report_url:
        return [url for url in args.report_url if _validated_url(url)[0] == game_date]
    return [
        f"https://ak-static.cms.nba.com/referee/injury/Injury-Report_{game_date.isoformat()}_{slot}.pdf"
        for slot in args.report_slot
    ]


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, UnsupportedOfficialInjuryReportError):
        message = str(exc)
    elif exc.__class__.__module__.startswith("requests"):
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        message = f"HTTP {status}" if status is not None else "HTTP request failed"
    elif isinstance(exc, ValueError | RuntimeError):
        message = str(exc)
    else:
        message = "operation failed"
    return {"type": type(exc).__name__, "message": message}


def _fetch_reports(
    args: argparse.Namespace,
    state: dict,
    urls: list[str],
    *,
    request_start_count: int,
    attempted_urls: set[str],
) -> tuple[list[OfficialInjuryReport], bool]:
    reports: list[OfficialInjuryReport] = []
    exhausted = False
    for url in urls:
        cached = state["report_cache"].get(url)
        if cached and cached.get("status") == "parsed":
            reports.append(_report_from_dict(cached["report"]))
            continue
        if cached and cached.get("status") == "error":
            # A recorded failure is an accounted source gap, not fresh budget
            # work. Retrying it on every resume can keep a window stuck forever.
            continue
        if getattr(args, "cache_only", False):
            raise RuntimeError("cache-only replay requires a recorded report; no request made")
        if url in attempted_urls:
            continue
        if len(state["request_attempts"]) - request_start_count >= args.max_requests:
            exhausted = True
            continue
        attempt = {
            "url": url,
            "started_at": datetime.now(UTC).isoformat(),
            "number": len(state["request_attempts"]) + 1,
            "outcome": "started",
        }
        state["request_attempts"].append(attempt)
        _save_state(args.state_file, state)
        attempted_urls.add(url)
        try:
            report = fetch_official_injury_report(url)
            attempt.update({
                "outcome": "parsed",
                "finished_at": datetime.now(UTC).isoformat(),
                "capture_sha256": report.capture_sha256,
                "parsed_rows": len(report.rows),
            })
            state["report_cache"][url] = {
                "status": "parsed",
                "capture_sha256": report.capture_sha256,
                "report": report.to_dict(),
            }
            reports.append(report)
        except Exception as exc:
            error = _safe_error(exc)
            attempt.update({"outcome": "error", "finished_at": datetime.now(UTC).isoformat(), "error": error})
            state["report_cache"][url] = {"status": "error", "error": error}
        _save_state(args.state_file, state)
    return reports, exhausted


def _parse_observations(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _plan_game(game: dict, timing, rows: list[dict], reports: list[OfficialInjuryReport]) -> dict:
    result: dict[str, Any] = {
        "game_id": game["game_id"],
        "game_date": game["game_date"].isoformat(),
        "timing": timing.as_json(),
        "reports_seen": len(reports),
        "publication_matches": 0,
        "pre_cutoff_matches": 0,
        "planned": [],
        "already_present": [],
        "skipped": [],
        "blocked": [],
    }
    game_rows = []
    team_ids = set(game["team_ids"])
    for row in rows:
        if row["report_date"] != game["game_date"] or row["nba_team_id"] not in team_ids:
            continue
        if row["player_id"] is None:
            result["blocked"].append({"row_id": row["id"], "reason": "missing_player_id"})
            continue
        if row["game_id"] not in (None, game["game_id"]):
            result["blocked"].append({"row_id": row["id"], "reason": "existing_wrong_game_id"})
            continue
        game_rows.append(row)
    if timing.status != "resolved" or timing.game_start_utc is None:
        result["blocked"].append({"reason": "game_start_unknown", "detail": timing.reason})
        return result

    cutoff = timing.game_start_utc.astimezone(UTC) - timedelta(minutes=60)
    usable_reports = []
    target_names = {_normalize(row["team"]) for row in game_rows}
    for report in reports:
        published = datetime.fromisoformat(report.published_at_utc.replace("Z", "+00:00")).astimezone(UTC)
        report_rows = [row for row in report.rows if row.game_date == game["game_date"].isoformat()
                       and _normalize(row.team) in target_names]
        if report_rows:
            result["publication_matches"] += len(report_rows)
        if published <= cutoff:
            usable_reports.append(report)
            result["pre_cutoff_matches"] += len(report_rows)
        elif report_rows:
            result["skipped"].append({
                "reason": "after_cutoff_report",
                "source_url": report.source_url,
                "published_at_utc": report.published_at_utc,
            })

    matching_rows = [{**row, "source_observations": {}} for row in game_rows]
    planned, metrics = _plan_updates(usable_reports, matching_rows, game["game_date"])
    by_id = {int(row["id"]): row for row in game_rows}
    for item in planned:
        row = by_id[item["row_id"]]
        existing = _parse_observations(row.get("source_observations")).get(item["capture_id"])
        item["game_id"] = game["game_id"]
        item["precondition"] = {
            key: _jsonable(row[key])
            for key in ("report_date", "nba_team_id", "player_id", "team", "player", "status", "reason")
        }
        if existing == item["observation"]:
            item["link_only"] = row["game_id"] is None
            result["already_present"].append(item)
        elif existing is not None:
            result["blocked"].append({"row_id": item["row_id"], "reason": "capture_key_content_conflict"})
        else:
            result["planned"].append(item)
    result["matching_metrics"] = metrics
    matched_ids = {item["row_id"] for item in planned}
    unmatched = [int(row["id"]) for row in game_rows if int(row["id"]) not in matched_ids]
    if unmatched:
        result["blocked"].append({"reason": "existing_rows_without_exact_eligible_match", "row_ids": unmatched})
    if not planned:
        result["blocked"].append({"reason": "no_exact_eligible_existing_rows"})
    if not usable_reports:
        result["blocked"].append({"reason": "no_pre_cutoff_supported_report"})
    return result


def _execute_rows(engine, items: list[dict]) -> int:
    changed = 0
    with engine.begin() as conn:
        _write_guards(conn)
        for item in items:
            params = {
                "row_id": item["row_id"],
                "game_id": item["game_id"],
                "capture_id": item["capture_id"],
                "observation": json.dumps(item["observation"], ensure_ascii=False, sort_keys=True),
                **item["precondition"],
            }
            result = conn.execute(_LINK_ROW if item.get("link_only") else _UPDATE_ROW, params)
            if result.rowcount == 0:
                current = conn.execute(_SELECT_ROWS_BY_ID, {"row_ids": [item["row_id"]]}).mappings().one_or_none()
                if current is None or not _item_matches(current, item):
                    raise RuntimeError(f"stale or conflicting injury row {item['row_id']}")
            else:
                changed += result.rowcount
    return changed


def _read_rows(engine, row_ids: list[int]) -> list[dict]:
    if not row_ids:
        return []
    with engine.begin() as conn:
        _read_guards(conn)
        return [dict(row) for row in conn.execute(_SELECT_ROWS_BY_ID, {"row_ids": row_ids}).mappings()]


def _item_matches(row, item: dict) -> bool:
    return (
        row["game_id"] == item["game_id"]
        and str(row["report_date"]) == item["precondition"]["report_date"]
        and all(row[key] == item["precondition"][key]
                for key in ("nba_team_id", "player_id", "team", "player", "status", "reason"))
        and _parse_observations(row["source_observations"]).get(item["capture_id"]) == item["observation"]
    )


def _verify_rows(engine, items: list[dict]) -> int:
    rows = {row["id"]: row for row in _read_rows(engine, sorted({item["row_id"] for item in items}))}
    for item in items:
        row = rows.get(item["row_id"])
        if row is None or not _item_matches(row, item):
            raise RuntimeError(f"identity/observation readback mismatch for injury row {item['row_id']}")
    return len(items)


def _verify_timing(engine, timing) -> None:
    with engine.begin() as conn:
        _read_guards(conn)
        row = conn.execute(text("""
            SELECT game_date, game_start_utc, basis, status FROM public.nba_injury_game_timing
            WHERE game_id = :game_id
        """), {"game_id": timing.game_id}).mappings().one_or_none()
    expected = (timing.game_date, timing.game_start_utc, timing.basis, timing.status)
    if row is None or tuple(row[key] for key in ("game_date", "game_start_utc", "basis", "status")) != expected:
        raise RuntimeError(f"persisted timing readback mismatch for {timing.game_id}")


def _mirror(engine, timing, items: list[dict]) -> dict:
    if not items:
        return {"status": "not_needed", "changed": 0, "verified": 0}
    local_rows = {row["id"]: row for row in _read_rows(engine, [item["row_id"] for item in items])}
    for item in items:
        row = local_rows.get(item["row_id"])
        if row is None:
            return {"status": "mirror_pending", "reason": f"local row {item['row_id']} is missing"}
        pre = item["precondition"]
        if str(row["report_date"]) != pre["report_date"] or any(
            row[key] != pre[key]
            for key in ("nba_team_id", "player_id", "team", "player", "status", "reason")
        ):
            return {"status": "mirror_pending", "reason": f"local row {item['row_id']} identity mismatch"}
        if row["game_id"] not in (None, item["game_id"]):
            return {"status": "mirror_pending", "reason": f"local row {item['row_id']} game mismatch"}
        existing = _parse_observations(row["source_observations"]).get(item["capture_id"])
        if existing not in (None, item["observation"]):
            return {"status": "mirror_pending", "reason": f"local row {item['row_id']} capture conflict"}
    pending = [
        dict(item) for item in items
        if local_rows[item["row_id"]]["game_id"] is None
        or _parse_observations(local_rows[item["row_id"]]["source_observations"]).get(item["capture_id"]) is None
    ]
    for item in pending:
        item["link_only"] = (
            _parse_observations(local_rows[item["row_id"]]["source_observations"]).get(item["capture_id"])
            == item["observation"]
        )
    persist_game_timings(engine, [timing])
    _verify_timing(engine, timing)
    changed = _execute_rows(engine, pending)
    verified = _verify_rows(engine, items)
    return {"status": "verified", "changed": changed, "verified": verified}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover bounded historical NBA injury observations from official PDFs.")
    parser.add_argument("--start-date", required=True, type=_iso_date)
    parser.add_argument("--end-date", required=True, type=_iso_date)
    parser.add_argument("--game-id", action="append", default=[])
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--report-url", action="append", default=[])
    source.add_argument("--report-slot", action="append", default=[])
    parser.add_argument("--state-file", required=True, type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--cache-only", action="store_true", help="Never download reports; require existing cache entries.")
    parser.add_argument("--max-games", type=int, default=10)
    parser.add_argument("--max-requests", type=int, default=4)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--mirror-local", action="store_true")
    args = parser.parse_args(argv)
    args.game_id = list(dict.fromkeys(args.game_id))
    args.report_url = list(dict.fromkeys(args.report_url))
    args.report_slot = list(dict.fromkeys(args.report_slot or DEFAULT_SLOTS))
    if args.start_date > args.end_date or (args.end_date - args.start_date).days >= MAX_DAYS:
        parser.error("date range must be inclusive, ordered, and at most 31 days")
    if not 1 <= args.max_games <= MAX_GAMES:
        parser.error(f"--max-games must be between 1 and {MAX_GAMES}")
    if not 1 <= args.max_requests <= MAX_REQUESTS:
        parser.error(f"--max-requests must be between 1 and {MAX_REQUESTS}")
    if len(args.report_url) > MAX_URLS or len(args.report_slot) > MAX_URLS:
        parser.error(f"at most {MAX_URLS} distinct report URLs or slots are allowed")
    if args.mirror_local and not args.execute:
        parser.error("--mirror-local requires --execute")
    if len(args.game_id) > MAX_GAMES or any(not is_competitive_nba_game_id(value) for value in args.game_id):
        parser.error("--game-id values must be at most 100 exact competitive NBA identifiers")
    try:
        for url in args.report_url:
            report_date, _ = _validated_url(url)
            if not args.start_date <= report_date <= args.end_date:
                parser.error("every --report-url filename date must be inside the requested date range")
        for slot in args.report_slot:
            _validated_url(
                f"https://ak-static.cms.nba.com/referee/injury/Injury-Report_{args.start_date.isoformat()}_{slot}.pdf"
            )
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        with _state_lock(args.state_file):
            state = _load_state(args)
            _save_state(args.state_file, state)
            remote = get_engine(local=False)
            games, range_truncated = _select_games(remote, args, state)
            if not games and state["games"]:
                games, range_truncated = _select_games(remote, args, state, recheck=True)
            if not games:
                raise RuntimeError("no competitive games in the requested stats/date scope")
            pending = games
            selected = pending[: args.max_games]
            budget_remaining = range_truncated
            injuries = _load_injuries(remote, selected) if selected else []
            timings = {
                item.game_id: item
                for item in resolve_game_timings(
                    remote, [GameTimingRequest(game["game_id"], game["game_date"]) for game in selected]
                )
            } if selected else {}
            local = get_engine(local=True) if args.mirror_local else None
            if local is not None and (remote.url.host, remote.url.port, remote.url.database) == (
                local.url.host, local.url.port, local.url.database
            ):
                raise RuntimeError("remote and local routes must target distinct databases")
            blocked = False
            request_start_count = len(state["request_attempts"])
            attempted_urls: set[str] = set()
            for game in selected:
                game_id = game["game_id"]
                try:
                    reports, exhausted = _fetch_reports(
                        args,
                        state,
                        _candidate_urls(args, game["game_date"]),
                        request_start_count=request_start_count,
                        attempted_urls=attempted_urls,
                    )
                    budget_remaining = budget_remaining or exhausted
                    timing = timings[game_id]
                    outcome = _plan_game(game, timing, injuries, reports)
                    if exhausted or len(reports) != len(_candidate_urls(args, game["game_date"])):
                        outcome["blocked"].append({"reason": "report_budget_or_fetch_incomplete"})
                    verified_items = [*outcome["already_present"]]
                    if args.execute:
                        persist_game_timings(remote, [timing])
                        _verify_timing(remote, timing)
                        write_items = [
                            *outcome["planned"],
                            *(item for item in outcome["already_present"] if item.get("link_only")),
                        ]
                        changed = _execute_rows(remote, write_items)
                        verified_items.extend(outcome["planned"])
                        verified = _verify_rows(remote, verified_items)
                        outcome.update({"changed": changed, "verified": verified})
                        if local is not None:
                            outcome["mirror"] = _mirror(local, timing, verified_items)
                            if outcome["mirror"]["status"] != "verified" and verified_items:
                                outcome["blocked"].append({"reason": "mirror_pending", "detail": outcome["mirror"]})
                    else:
                        outcome.update({"changed": 0, "verified": 0, "dry_run_plan": True})
                    outcome["status"] = "blocked" if outcome["blocked"] else "verified"
                    blocked = blocked or bool(outcome["blocked"])
                    state["games"][game_id] = outcome
                except Exception as exc:
                    blocked = True
                    state["games"][game_id] = {
                        "game_id": game_id,
                        "game_date": game["game_date"].isoformat(),
                        "status": "error",
                        "error": _safe_error(exc),
                    }
                state["sequence"] = state.get("sequence", 0) + 1
                state["games"][game_id]["attempt"] = state["sequence"]
                _save_state(args.state_file, state)

            selected_ids = {game["game_id"] for game in games}
            processed_ids = {game["game_id"] for game in selected}
            accounted = selected_ids <= set(state["games"])
            # Recorded source/identity gaps are accounted outcomes, not budget
            # work. Unvisited games and never-attempted reports still need a batch.
            budget_remaining = budget_remaining or any(
                game["game_id"] not in state["games"]
                or any(url not in state["report_cache"] for url in _candidate_urls(args, game["game_date"]))
                for game in games if game["game_id"] not in processed_ids
            )
            blocked = blocked or any(
                state["games"].get(game_id, {}).get("status") in {"blocked", "error"}
                for game_id in selected_ids
            )
            receipt = {
                "mode": "execute" if args.execute else "dry_run_read_only",
                "state_file": str(args.state_file),
                "selected_games": len(games),
                "processed_this_run": len(selected),
                "request_attempts_this_state": len(state["request_attempts"]),
                "scope_accounted": accounted and not range_truncated,
                "remaining_due_to_budget": budget_remaining or not accounted,
                "blocked_or_errors": blocked,
                "completeness_claimed": False,
            }
            print(json.dumps(receipt, indent=2, sort_keys=True))
            if blocked:
                return 4
            if budget_remaining or not accounted:
                return 3
            return 0
    except (ValueError, RuntimeError) as exc:
        print(json.dumps({"error": _safe_error(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    except Exception as exc:
        print(json.dumps({"error": _safe_error(exc)}, sort_keys=True), file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
