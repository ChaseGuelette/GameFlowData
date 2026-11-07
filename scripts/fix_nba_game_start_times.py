#!/usr/bin/env python
"""Backfill the official NBA schedule authority; independent of injury recovery.

Workstation-only acquisition from stats.nba.com. Default: bounded network/cache
and SELECT preview. --execute writes remote first; --mirror-local copies the
verified canonical rows. Never derives starts from odds or injury PDFs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.client import get_engine  # noqa: E402
from src.models.features.nba.season import is_competitive_nba_game_id  # noqa: E402
from src.scrapers.nba_schedule import NBA_TIMEZONE, parse_schedule  # noqa: E402

URL = "https://stats.nba.com/stats/scheduleleaguev2"
SOURCE = "nba_stats_schedule"
COLUMNS = (
    "game_id", "season_id", "season", "game_type", "game_date", "commence_time_utc",
    "home_team_id", "away_team_id", "game_status", "status_text", "postponed_status",
    "venue_name", "source", "source_updated_at", "observed_at",
)
VALUES = COLUMNS[1:]
CHUNK = 100
MAX_GAMES = 6000
READ = text(
    f"SELECT {', '.join(COLUMNS)} FROM public.nba_game_schedule WHERE game_id IN :ids"
).bindparams(bindparam("ids", expanding=True))
UPSERT = text(f"""
    INSERT INTO public.nba_game_schedule ({', '.join(COLUMNS)})
    VALUES ({', '.join(':' + key for key in COLUMNS)})
    ON CONFLICT (game_id) DO UPDATE SET
        {', '.join(key + ' = EXCLUDED.' + key for key in VALUES)}, updated_at = now()
    WHERE (public.nba_game_schedule.commence_time_utc IS NULL OR :mirror)
      AND public.nba_game_schedule.game_date = EXCLUDED.game_date
      AND (public.nba_game_schedule.home_team_id IS NULL
           OR public.nba_game_schedule.home_team_id = EXCLUDED.home_team_id)
      AND (public.nba_game_schedule.away_team_id IS NULL
           OR public.nba_game_schedule.away_team_id = EXCLUDED.away_team_id)
      AND ({', '.join('public.nba_game_schedule.' + key for key in VALUES)})
          IS DISTINCT FROM ({', '.join('EXCLUDED.' + key for key in VALUES)})
""")


def stamp() -> str:
    return datetime.now(UTC).isoformat()


def save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("w", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, sort_keys=True, default=str)
        stream.write("\n")
    os.replace(temp, path)


def digest(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def season_label(value: str) -> str:
    if not re.fullmatch(r"20\d{2}-\d{2}", value) or int(value[-2:]) != (int(value[:4]) + 1) % 100:
        raise argparse.ArgumentTypeError("season must be an exact YYYY-YY NBA season")
    return value


def iso_date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD")
    return parsed


def load_official_schedule(season: str, cache: Path, *, refresh: bool = False) -> dict:
    """One official season request, cached for dry-run -> execute -> no-op reuse."""
    path = cache / f"official_schedule_{season}.json"
    if path.exists() and not refresh:
        captured = json.loads(path.read_text(encoding="utf-8"))
        if (captured.get("url") != URL or captured.get("season") != season
                or captured.get("sha256") != digest(captured["payload"])):
            raise ValueError(f"Invalid official schedule cache: {path}")
        return captured
    # Same direct browser-compatible local transport as the existing NBA stats
    # scraper. Do not wire this workstation-only endpoint into Railway jobs.
    from curl_cffi import CurlOpt
    from curl_cffi.requests import Session
    from nba_api.stats.library import http as nba_http

    with Session(impersonate="chrome", trust_env=False, curl_options={CurlOpt.PROXY: ""}) as session:
        response = session.get(URL, params={"LeagueID": "00", "Season": season},
                               headers=getattr(nba_http, "STATS_HEADERS"), timeout=30)
        response.raise_for_status()
        if len(response.content) > 15_000_000:
            raise ValueError("Official season payload exceeds 15 MB")
        payload = response.json()
    if payload.get("leagueSchedule", {}).get("seasonYear") != season:
        raise ValueError("NBA returned a different season")
    captured = {"url": URL, "season": season, "observed_at": stamp(),
                "sha256": digest(payload), "payload": payload}
    save(path, captured)
    return captured


def official_rows(captured: dict) -> tuple[list[dict], list[dict]]:
    """Reuse gameDateTimeUTC parsing; historical payloads need no live-CDN age gate."""
    payload = captured["payload"]
    if payload.get("leagueSchedule", {}).get("seasonYear") != captured["season"]:
        raise ValueError("Cached season does not match the official payload")
    observed = datetime.fromisoformat(captured["observed_at"])
    if observed.tzinfo is None:
        raise ValueError("Schedule observation time must have a timezone")
    # The CDN meta-clock convention does not establish the stats API's meta
    # convention. Retain raw meta in the cache, without inventing update time.
    parsed = parse_schedule({**payload, "meta": {}}, observed_at=observed)
    valid, skipped = [], []
    for row in parsed:
        start = row["commence_time_utc"]
        reason = None
        if row["season"] != captured["season"]:
            raise ValueError(f"Official game belongs to another season: {row['game_id']}")
        # Historical feeds can retain an archive/postponement flag (2023-24
        # returns 'A' even on completed games). A game explicitly marked Final
        # is no longer pending postponement; retain the raw flag in the cache.
        if row["game_status"] == 3 and str(row["status_text"] or "").lower().startswith("final"):
            row["postponed_status"] = "N"
        if start is None or start.astimezone(NBA_TIMEZONE).date() != row["game_date"]:
            reason = "missing_or_date_incoherent_scheduled_start"
        elif not row["home_team_id"] or not row["away_team_id"] or row["home_team_id"] == row["away_team_id"]:
            reason = "missing_or_conflicting_team_identity"
        elif str(row["postponed_status"] or "").strip().lower() not in {"", "n", "0", "false"}:
            reason = "postponed_game"
        elif any(term in str(row["status_text"] or "").lower() for term in ("postpon", "cancel", "tbd")):
            reason = "unresolved_game_status"
        if reason:
            skipped.append({"game_id": row["game_id"], "reason": reason})
            continue
        row.update(source=SOURCE, source_updated_at=None)
        valid.append(row)
    return valid, skipped


def guards(conn, *, write: bool = False) -> None:
    if not write:
        conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SET LOCAL statement_timeout = '15s'"))
    conn.execute(text("SET LOCAL lock_timeout = '3s'"))
    actual = conn.execute(text("SELECT current_setting('transaction_read_only'), "
                               "current_setting('statement_timeout'), current_setting('lock_timeout')")).one()
    if tuple(actual) != ("off" if write else "on", "15s", "3s"):
        raise RuntimeError("Database transaction guards did not apply")


def read_rows(engine, ids: list[str]) -> dict[str, dict]:
    found = {}
    for offset in range(0, len(ids), CHUNK):
        with engine.begin() as conn:
            guards(conn)
            found.update({r["game_id"]: dict(r) for r in conn.execute(READ, {"ids": ids[offset:offset + CHUNK]}).mappings()})
    return found


def same_identity(left: dict, right: dict) -> bool:
    return left["game_date"] == right["game_date"] and all(
        left[key] is None or left[key] == right[key] for key in ("home_team_id", "away_team_id")
    )


def prepare_remote(rows: list[dict], existing: dict[str, dict]) -> tuple[list[dict], list[dict], list[dict]]:
    changes, canonical, differences = [], [], []
    for row in rows:
        previous = existing.get(row["game_id"])
        if previous:
            if not same_identity(previous, row):
                raise ValueError(f"Existing schedule identity disagrees: {row['game_id']}")
            # The table is the owner's authority. This filler does not silently
            # override a populated official schedule with another reconstruction.
            if previous["commence_time_utc"] is not None:
                if previous["source"] not in {"nba_cdn", SOURCE}:
                    raise ValueError(f"Unrecognized schedule provenance: {row['game_id']}")
                if previous["commence_time_utc"] != row["commence_time_utc"]:
                    differences.append({"game_id": row["game_id"], "table_start": previous["commence_time_utc"],
                                        "fetched_start": row["commence_time_utc"], "action": "preserved_table_authority"})
                canonical.append(previous)
                continue
        changes.append(row)
        canonical.append(row)
    return changes, canonical, differences


def write_and_verify(engine, rows: list[dict], *, mirror: bool) -> int:
    changed = 0
    for offset in range(0, len(rows), CHUNK):
        batch = rows[offset:offset + CHUNK]
        with engine.begin() as conn:
            guards(conn, write=True)
            result = conn.execute(UPSERT, [{**row, "mirror": mirror} for row in batch])
            changed += max(result.rowcount or 0, 0)
        actual = read_rows(engine, [row["game_id"] for row in batch])
        for row in batch:
            if actual.get(row["game_id"]) != row:
                raise RuntimeError(f"Schedule fresh readback mismatch: {row['game_id']}")
    return changed


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", action="append", required=True, type=season_label)
    parser.add_argument("--game-id", action="append", default=[])
    parser.add_argument("--start-date", type=iso_date)
    parser.add_argument("--end-date", type=iso_date)
    parser.add_argument("--max-games", type=int, default=2000)
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "logs/receipts/nba_official_schedule_fixer/cache")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--mirror-local", action="store_true", help="Preview/copy only the selected canonical schedule rows")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    args.season = list(dict.fromkeys(args.season))
    args.game_id = list(dict.fromkeys(args.game_id))
    if len(args.season) > 4 or not 1 <= args.max_games <= MAX_GAMES:
        parser.error("Use at most four seasons and --max-games between 1 and 6000")
    if bool(args.start_date) != bool(args.end_date) or (args.start_date and args.start_date > args.end_date):
        parser.error("Supply an ordered --start-date/--end-date pair")
    if any(not is_competitive_nba_game_id(gid) for gid in args.game_id):
        parser.error("--game-id must be an exact competitive NBA identifier")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    receipt = {"mode": "execute" if args.execute else "dry_run", "started_at": stamp(),
               "sources": [], "remote_changed": 0, "local_changed": 0, "status": "started",
               "authority": "public.nba_game_schedule.commence_time_utc", "odds_or_pdf_requests": 0}
    try:
        rows, skipped = [], []
        for season in args.season:
            captured = load_official_schedule(season, args.cache_dir, refresh=args.refresh)
            parsed, rejected = official_rows(captured)
            rows.extend(parsed)
            skipped.extend(rejected)
            receipt["sources"].append({key: captured[key] for key in ("season", "url", "observed_at", "sha256")})
        if args.start_date:
            rows = [r for r in rows if args.start_date <= r["game_date"] <= args.end_date]
        if args.game_id:
            rows = [r for r in rows if r["game_id"] in args.game_id]
            missing = sorted(set(args.game_id) - {r["game_id"] for r in rows})
            if missing:
                raise ValueError(f"Requested games lack valid official scheduled starts: {missing}")
        rows.sort(key=lambda r: (r["game_date"], r["game_id"]))
        if not rows or len(rows) > args.max_games or len({r['game_id'] for r in rows}) != len(rows):
            raise ValueError("Selected schedule is empty, duplicated or exceeds --max-games; narrow scope or raise its explicit cap")
        remote = get_engine(local=False)
        ids = [r["game_id"] for r in rows]
        before = read_rows(remote, ids)
        planned, canonical, differences = prepare_remote(rows, before)
        local = get_engine(local=True) if args.mirror_local else None
        local_before = {}
        if local is not None:
            if (remote.url.host, remote.url.port, remote.url.database) == (local.url.host, local.url.port, local.url.database):
                raise ValueError("Remote and local database routes must differ")
            local_before = read_rows(local, ids)
            for row in canonical:
                if row["game_id"] in local_before and not same_identity(local_before[row["game_id"]], row):
                    raise ValueError(f"Local schedule identity disagrees: {row['game_id']}")
        receipt.update(selected=len(rows), remote_planned=len(planned), existing_authoritative=len(rows) - len(planned),
                       local_planned=sum(local_before.get(r['game_id']) != r for r in canonical) if local else 0,
                       preserved_table_differences=differences, source_skipped=skipped,
                       games=[{key: r[key] for key in ("game_id", "game_date", "commence_time_utc", "home_team_id", "away_team_id", "source")} for r in canonical])
        save(args.output, receipt)
        if args.execute:
            receipt["remote_changed"] = write_and_verify(remote, planned, mirror=False)
            canonical = list(read_rows(remote, ids).values())
            if len(canonical) != len(rows):
                raise RuntimeError("Canonical schedule readback count mismatch")
            receipt.update(status="remote_verified", remote_verified=len(canonical))
            save(args.output, receipt)
            if local is not None:
                receipt["local_changed"] = write_and_verify(local, canonical, mirror=True)
                receipt["local_verified"] = len(canonical)
        receipt.update(status="complete" if args.execute else "preview", finished_at=stamp())
        save(args.output, receipt)
        print(json.dumps({key: value for key, value in receipt.items() if key not in {"games", "source_skipped"}}, indent=2, default=str))
        return 0
    except Exception as exc:
        # SQL/transport exceptions can include credential-bearing connection URLs.
        receipt.update(status="failed", error_type=type(exc).__name__,
                       error=str(exc) if isinstance(exc, ValueError | RuntimeError) else "Operation failed; retained cache/receipt supports a bounded retry",
                       finished_at=stamp())
        save(args.output, receipt)
        print(json.dumps(receipt, default=str), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
