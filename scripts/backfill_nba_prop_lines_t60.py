#!/usr/bin/env python3
"""Bounded one-time historical Odds API rescrape for NBA compact T-60 lines.

This script never reads or writes legacy raw/archive prop tables. Default mode is
an offline plan. Real API and DB work requires --execute plus finite request,
credit, and event limits.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.processing.nba_prop_snapshots import (  # noqa: E402
    ALLOWED_BOOKMAKERS,
    FEATURE_MARKETS,
    POLICY_VERSION,
    SnapshotContractError,
    exact_link_quote,
    iso_z,
    load_exact_linker_context,
    normalize_team,
    parse_odds_event,
    parse_utc,
    select_t60_features,
    upsert_feature_lines,
)

ET = ZoneInfo("America/New_York")
SPORT = "basketball_nba"
API_ROOT = "https://api.the-odds-api.com/v4"
DISCOVERY_HOURS_ET = (5, 9, 13, 17, 21, 23)
TASK_NAMESPACE = uuid.UUID("59736df4-8f2c-48a6-a99f-cb85a4c2389a")


class BudgetExceededError(RuntimeError):
    pass


class OddsAPIResponseError(RuntimeError):
    """Typed HTTP/provider response failure; never interpreted as empty data."""


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def iter_dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fresh historical NBA Odds API rescrape into compact t60_v1 features. Default is DB/API-free dry-run."
    )
    parser.add_argument("--start-date", type=parse_date, required=True)
    parser.add_argument("--end-date", type=parse_date, required=True)
    parser.add_argument("--max-events", type=int, default=25)
    parser.add_argument("--max-requests", type=int, default=200)
    parser.add_argument("--max-credits", type=int, default=1000)
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".gameflow" / "nba-props-t60")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="Explicit alias for the safe default")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.end_date < args.start_date:
        raise SystemExit("--end-date must be on or after --start-date")
    if (args.end_date - args.start_date).days > 1460:
        raise SystemExit("one invocation may cover at most 1461 inclusive dates")
    if args.max_events <= 0 or args.max_events > 10000:
        raise SystemExit("--max-events must be between 1 and 10000")
    if args.max_requests <= 0 or args.max_requests > 50000:
        raise SystemExit("--max-requests must be between 1 and 50000")
    if args.max_credits <= 0 or args.max_credits > 500000:
        raise SystemExit("--max-credits must be between 1 and 500000")


def task_id(event_id: str, target: datetime) -> str:
    identity = f"{event_id}|{iso_z(target)}|{','.join(FEATURE_MARKETS)}|{','.join(ALLOWED_BOOKMAKERS)}|{POLICY_VERSION}"
    return str(uuid.uuid5(TASK_NAMESPACE, identity))


class StateStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.discovery_dir = self.root / "discovery"
        self.event_dir = self.root / "events"
        self.journal_path = self.root / "journal.jsonl"
        self.discovery_dir.mkdir(parents=True, exist_ok=True)
        self.event_dir.mkdir(parents=True, exist_ok=True)

    def _read_gzip(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else None

    def _write_gzip(self, path: Path, payload: dict[str, Any]) -> None:
        temp = path.with_suffix(path.suffix + ".tmp")
        with gzip.open(temp, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
        temp.replace(path)

    def discovery_path(self, snapshot: datetime) -> Path:
        return self.discovery_dir / (snapshot.strftime("%Y%m%dT%H%M%SZ") + ".json.gz")

    def event_path(self, event_id: str, target: datetime) -> Path:
        return self.event_dir / f"{event_id}_{target.strftime('%Y%m%dT%H%M%SZ')}.json.gz"

    def load_discovery(self, snapshot: datetime) -> dict[str, Any] | None:
        return self._read_gzip(self.discovery_path(snapshot))

    def save_discovery(self, snapshot: datetime, payload: dict[str, Any]) -> None:
        self._write_gzip(self.discovery_path(snapshot), payload)

    def load_event(self, event_id: str, target: datetime) -> dict[str, Any] | None:
        return self._read_gzip(self.event_path(event_id, target))

    def save_event(self, event_id: str, target: datetime, payload: dict[str, Any]) -> None:
        self._write_gzip(self.event_path(event_id, target), payload)

    def record(self, **payload: Any) -> None:
        row = {"recorded_at": iso_z(datetime.now(UTC)), **payload}
        with self.journal_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


class OddsAPI:
    def __init__(self, api_key: str, *, max_requests: int, max_credits: int) -> None:
        import requests

        self.api_key = api_key
        self.max_requests = max_requests
        self.max_credits = max_credits
        self.requests_used = 0
        self.credits_used = 0
        self.session = requests.Session()

    def get(self, path: str, params: dict[str, Any], *, reserve_credits: int) -> dict[str, Any]:
        safe_params = {**params, "apiKey": self.api_key}
        for attempt in range(4):
            if self.requests_used >= self.max_requests:
                raise BudgetExceededError("request budget exhausted")
            if self.credits_used + reserve_credits > self.max_credits:
                raise BudgetExceededError("insufficient credit budget reserved for next request")
            self.requests_used += 1
            try:
                response = self.session.get(API_ROOT + path, params=safe_params, timeout=30)
            except Exception as exc:
                self.credits_used += reserve_credits
                raise RuntimeError(f"Odds API transport error: {type(exc).__name__}") from exc
            raw_last = response.headers.get("x-requests-last")
            if raw_last is None:
                last = reserve_credits
            else:
                try:
                    last = int(raw_last)
                    if last < 0:
                        raise ValueError
                except (TypeError, ValueError) as exc:
                    self.credits_used += reserve_credits
                    raise OddsAPIResponseError("malformed x-requests-last header") from exc
            self.credits_used += last
            if self.credits_used > self.max_credits:
                raise BudgetExceededError("credit budget exhausted")
            if response.status_code == 429 and attempt < 3:
                time.sleep(min(2**attempt, 8))
                if self.requests_used >= self.max_requests:
                    raise BudgetExceededError("request budget exhausted during retry")
                continue
            if not 200 <= response.status_code < 300:
                raise OddsAPIResponseError(f"Odds API HTTP {response.status_code}")
            try:
                payload = response.json()
            except Exception as exc:
                raise OddsAPIResponseError("Odds API returned invalid JSON") from exc
            if not isinstance(payload, dict):
                raise OddsAPIResponseError("Odds API returned a non-object response")
            return payload
        raise RuntimeError("Odds API request failed after bounded retries")


def discovery_snapshots(start: date, end: date) -> list[datetime]:
    snapshots = []
    for day in iter_dates(start, end):
        for hour in DISCOVERY_HOURS_ET:
            snapshots.append(datetime.combine(day, dtime(hour, tzinfo=ET)).astimezone(UTC))
    # One early following-day query catches games whose final pregame listing
    # crossed the local midnight boundary; ET commence-date filtering remains exact.
    snapshots.append(datetime.combine(end + timedelta(days=1), dtime(1, tzinfo=ET)).astimezone(UTC))
    return snapshots


def validate_discovery_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    parse_utc(payload.get("timestamp"), field="discovery wrapper timestamp")
    data = payload.get("data")
    if not isinstance(data, list):
        raise SnapshotContractError("discovery response data must be a list")
    validated: list[dict[str, Any]] = []
    for event in data:
        if not isinstance(event, dict) or not str(event.get("id") or "").strip():
            raise SnapshotContractError("discovery event has malformed identity")
        parse_utc(event.get("commence_time"), field="discovery commence_time")
        home = str(event.get("home_team") or "").strip()
        away = str(event.get("away_team") or "").strip()
        if not home or not away or home.casefold() == away.casefold():
            raise SnapshotContractError("discovery event has missing/contradictory teams")
        validated.append(event)
    return validated


def validate_historical_event_payload(
    payload: dict[str, Any], *, event_id: str, commence: datetime, event: dict[str, Any]
) -> tuple[dict[str, Any], datetime]:
    source_time = parse_utc(payload.get("timestamp"), field="historical wrapper timestamp")
    assert source_time is not None
    if source_time > commence - timedelta(minutes=60):
        raise SnapshotContractError("historical wrapper snapshot is later than the requested T-60 cutoff")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise SnapshotContractError("historical response data must be an event object")
    if not isinstance(data.get("bookmakers"), list):
        raise SnapshotContractError("historical event bookmakers must be a list")
    if str(data.get("id") or "") != event_id:
        raise SnapshotContractError("historical response event ID differs from discovery event")
    response_commence = parse_utc(data.get("commence_time"), field="response commence_time")
    assert response_commence is not None
    if response_commence != commence:
        raise SnapshotContractError(
            f"historical response commence_time changed: discovery={iso_z(commence)} "
            f"odds={iso_z(response_commence)}; inspect the cached response before retrying"
        )
    home = str(data.get("home_team") or "").strip()
    away = str(data.get("away_team") or "").strip()
    if home.casefold() != str(event.get("home_team") or "").strip().casefold():
        raise SnapshotContractError("historical response home-team identity mismatch")
    if away.casefold() != str(event.get("away_team") or "").strip().casefold():
        raise SnapshotContractError("historical response away-team identity mismatch")
    return data, source_time


def discover_events(
    api: OddsAPI, state: StateStore, *, start: date, end: date
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    events: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    snapshots = discovery_snapshots(start, end)
    for snapshot_index, snapshot in enumerate(snapshots, 1):
        payload = state.load_discovery(snapshot)
        if payload is None:
            try:
                payload = api.get(
                    f"/historical/sports/{SPORT}/events", {"date": iso_z(snapshot)}, reserve_credits=1
                )
                validated_events = validate_discovery_payload(payload)
            except BudgetExceededError:
                raise
            except Exception as exc:
                message = f"discovery {iso_z(snapshot)}: {type(exc).__name__}: {exc}"
                errors.append(message)
                state.record(kind="discovery", snapshot=iso_z(snapshot), status="error", error=message)
                continue
            # A strictly validated 2xx empty list is a confirmed provider snapshot.
            state.save_discovery(snapshot, payload)
            state.record(
                kind="discovery", snapshot=iso_z(snapshot),
                status="success" if payload["data"] else "empty",
            )
        else:
            validated_events = validate_discovery_payload(payload)
        discovery_time = parse_utc(payload.get("timestamp"), field="discovery wrapper timestamp")
        assert discovery_time is not None
        if discovery_time > snapshot:
            raise SnapshotContractError("discovery wrapper is later than its requested snapshot")
        for event in validated_events:
            commence = parse_utc(event.get("commence_time"), field="commence_time")
            assert commence is not None
            # Historical listings can retain started games and revise their start
            # to an actual tipoff. Never overwrite a pregame plan with that update.
            if discovery_time >= commence:
                continue
            if start <= commence.astimezone(ET).date() <= end:
                events[str(event["id"])] = dict(event)
        if snapshot_index == 1 or snapshot_index % 50 == 0 or snapshot_index == len(snapshots):
            print(f"discovery [{snapshot_index}/{len(snapshots)}] events={len(events)} "
                  f"requests={api.requests_used} credits={api.credits_used}", flush=True)
    return events, errors


def fill_team_ids(conn, quotes, context):
    pairs = sorted({(int(q.player_id), str(q.game_id)) for q in quotes if q.player_id is not None and q.game_id})
    if not pairs:
        return list(quotes)
    values = ",".join(["(%s,%s)"] * len(pairs))
    flat = [value for pair in pairs for value in pair]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT player_id, game_id, team_id FROM player_game_stats WHERE (player_id, game_id) IN ({values})",
            flat,
        )
        relations: dict[tuple[int, str], set[int]] = {}
        for player_id, game_id, team_id in cur.fetchall():
            relations.setdefault((int(player_id), str(game_id)), set()).add(int(team_id))
    output = []
    for quote in quotes:
        teams = relations.get((int(quote.player_id), str(quote.game_id)), set()) if quote.player_id else set()
        allowed = set(context["team_ids"].get(normalize_team(quote.home_team), set()))
        allowed.update(context["team_ids"].get(normalize_team(quote.away_team), set()))
        if teams and (len(teams) != 1 or not teams.issubset(allowed)):
            raise SnapshotContractError("observed player team conflicts with event home/away identity")
        output.append(replace(quote, team_id=next(iter(teams)) if len(teams) == 1 else None))
    return output


def update_task(
    conn,
    *,
    identifier: str,
    event_id: str,
    target: datetime,
    status: str,
    request_count: int,
    credit_count: int,
    raw_count: int,
    feature_count: int,
    error: str | None,
) -> None:
    completed = status in {"success", "empty"}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.nba_prop_capture_tasks (
                task_id, api_game_id, target_snapshot_time, markets, bookmakers,
                selection_policy_version, source_kind, status, request_count,
                credit_count, raw_quote_count, feature_row_count, error_message, completed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,'historical',%s,%s,%s,%s,%s,%s,CASE WHEN %s THEN now() ELSE NULL END)
            ON CONFLICT (task_id) DO UPDATE SET
                status=EXCLUDED.status,
                request_count=CASE WHEN EXCLUDED.request_count = 0 THEN nba_prop_capture_tasks.request_count ELSE EXCLUDED.request_count END,
                credit_count=CASE WHEN EXCLUDED.request_count = 0 THEN nba_prop_capture_tasks.credit_count ELSE EXCLUDED.credit_count END,
                raw_quote_count=EXCLUDED.raw_quote_count,
                feature_row_count=EXCLUDED.feature_row_count, error_message=EXCLUDED.error_message,
                completed_at=EXCLUDED.completed_at, updated_at=now()
            """,
            (
                identifier, event_id, target, list(FEATURE_MARKETS), list(ALLOWED_BOOKMAKERS),
                POLICY_VERSION, status, request_count, credit_count, raw_count, feature_count,
                error[:1000] if error else None, completed,
            ),
        )


def load_terminal_db_statuses(conn, start: date, end: date) -> dict[str, str]:
    """Read atomic committed captures, even if newer daily lines superseded them."""
    start_utc = datetime.combine(start, dtime(0, tzinfo=ET)).astimezone(UTC) - timedelta(hours=24)
    end_utc = datetime.combine(end + timedelta(days=1), dtime(0, tzinfo=ET)).astimezone(UTC)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.task_id::text, t.status
            FROM public.nba_prop_capture_tasks t
            WHERE target_snapshot_time >= %s AND target_snapshot_time < %s
              AND selection_policy_version = %s
              AND (
                t.status = 'empty'
                OR (
                  t.status = 'success' AND t.feature_row_count > 0
                )
              )
            """,
            (start_utc, end_utc, POLICY_VERSION),
        )
        return {str(identifier): str(status) for identifier, status in cur.fetchall()}


def process_event(api: OddsAPI, state: StateStore, conn, context, event: dict[str, Any]) -> tuple[str, int, int]:
    event_id = str(event["id"])
    commence = parse_utc(event.get("commence_time"), field="commence_time")
    assert commence is not None
    target = commence - timedelta(minutes=60)
    identifier = task_id(event_id, target)
    before_requests, before_credits = api.requests_used, api.credits_used
    payload = state.load_event(event_id, target)
    try:
        if payload is None:
            payload = api.get(
                f"/historical/sports/{SPORT}/events/{event_id}/odds",
                {
                    "date": iso_z(target),
                    "markets": ",".join(FEATURE_MARKETS),
                    "bookmakers": ",".join(ALLOWED_BOOKMAKERS),
                    "oddsFormat": "american",
                    "dateFormat": "iso",
                }, reserve_credits=40,
            )
            data, source_time = validate_historical_event_payload(
                payload, event_id=event_id, commence=commence, event=event
            )
            state.save_event(event_id, target, payload)
        else:
            data, source_time = validate_historical_event_payload(
                payload, event_id=event_id, commence=commence, event=event
            )
        raw_quotes = parse_odds_event(
            data,
            requested_snapshot_time=target,
            source_snapshot_time=source_time,
            source_kind="historical",
            observed_at=source_time,
            expected_event_id=event_id,
            expected_home_team=event.get("home_team"),
            expected_away_team=event.get("away_team"),
        )
        linked, unresolved = [], []
        for quote in raw_quotes:
            try:
                linked.append(exact_link_quote(quote, context))
            except SnapshotContractError as exc:
                unresolved.append(str(exc))
        linked = fill_team_ids(conn, linked, context)
        features, conflicts = select_t60_features(linked)
        if not raw_quotes:
            update_task(
                conn, identifier=identifier, event_id=event_id, target=target, status="empty",
                request_count=api.requests_used - before_requests,
                credit_count=api.credits_used - before_credits, raw_count=0, feature_count=0, error=None,
            )
            conn.commit()
            state.record(task_id=identifier, event_id=event_id, status="empty")
            return "empty", 0, 0
        if unresolved or conflicts or (raw_quotes and not features):
            detail = "; ".join((unresolved + conflicts)[:10]) or "no eligible linked t60 feature rows"
            update_task(
                conn, identifier=identifier, event_id=event_id, target=target, status="unresolved",
                request_count=api.requests_used - before_requests,
                credit_count=api.credits_used - before_credits, raw_count=len(raw_quotes),
                feature_count=len(features), error=detail,
            )
            conn.commit()
            state.record(task_id=identifier, event_id=event_id, status="unresolved", error=detail)
            return "unresolved", len(raw_quotes), len(features)
        # Establish the FK target in the same transaction before inserting
        # feature rows; only the final success update and commit make it terminal.
        update_task(
            conn, identifier=identifier, event_id=event_id, target=target, status="pending",
            request_count=api.requests_used - before_requests,
            credit_count=api.credits_used - before_credits, raw_count=len(raw_quotes),
            feature_count=0, error=None,
        )
        affected = upsert_feature_lines(conn, features, task_id=identifier)
        update_task(
            conn, identifier=identifier, event_id=event_id, target=target, status="success",
            request_count=api.requests_used - before_requests,
            credit_count=api.credits_used - before_credits, raw_count=len(raw_quotes),
            feature_count=len(features), error=None,
        )
        conn.commit()
        state.record(task_id=identifier, event_id=event_id, status="success", features=len(features))
        return "success", len(raw_quotes), affected
    except Exception as exc:
        conn.rollback()
        if isinstance(exc, SnapshotContractError) and payload is not None:
            # Keep paid evidence for diagnosis/revalidation. Reads still run the
            # complete validator; caching a rejection never makes it eligible.
            state.save_event(event_id, target, payload)
        try:
            update_task(
                conn, identifier=identifier, event_id=event_id, target=target, status="error",
                request_count=api.requests_used - before_requests,
                credit_count=api.credits_used - before_credits, raw_count=0, feature_count=0,
                error=f"{type(exc).__name__}: {exc}",
            )
            conn.commit()
        except Exception:
            conn.rollback()
        state.record(task_id=identifier, event_id=event_id, status="error", error=f"{type(exc).__name__}: {exc}")
        raise


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    validate_args(args)
    discovery_count = len(discovery_snapshots(args.start_date, args.end_date))
    print(
        f"policy={POLICY_VERSION} dates={args.start_date}..{args.end_date} markets={','.join(FEATURE_MARKETS)} "
        f"books={','.join(ALLOWED_BOOKMAKERS)} max_events={args.max_events} "
        f"max_requests={args.max_requests} max_credits={args.max_credits} "
        f"mode={'execute' if args.execute else 'dry-run'}"
    )
    if not args.execute:
        print(
            f"dry-run: discovery_requests_at_most={discovery_count}; event_requests_at_most={args.max_events}; "
            "no environment file, API request, state write, or database connection was attempted"
        )
        return 0

    import psycopg2
    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    load_dotenv(ROOT / ".env")
    api_key = os.getenv("ODDS_API_KEY")
    database_url = os.getenv("DATABASE_URL")
    if not api_key or not database_url:
        raise SystemExit("ODDS_API_KEY and DATABASE_URL are required with --execute")

    state = StateStore(args.state_dir)
    api = OddsAPI(api_key, max_requests=args.max_requests, max_credits=args.max_credits)
    context_engine = create_engine(database_url)
    conn = psycopg2.connect(
        database_url, connect_timeout=10,
        application_name="gameflow:nba_props_t60_backfill",
        options="-c statement_timeout=20000 -c lock_timeout=2000",
    )
    counts = {"success": 0, "empty": 0, "unresolved": 0, "error": 0, "skipped": 0}
    partial_reason = None
    try:
        try:
            events, discovery_errors = discover_events(api, state, start=args.start_date, end=args.end_date)
        except BudgetExceededError as exc:
            print(
                f"partial: discovery stopped: {exc}; requests={api.requests_used} "
                f"credits={api.credits_used}"
            )
            return 2
        if discovery_errors:
            partial_reason = f"{len(discovery_errors)} discovery errors"
        ordered = sorted(events.values(), key=lambda row: (row.get("commence_time") or "", row.get("id") or ""))
        statuses = load_terminal_db_statuses(conn, args.start_date, args.end_date)
        conn.rollback()  # Do not hold a read transaction while resolving/fetching events.
        pending = []
        for event in ordered:
            commence = parse_utc(event.get("commence_time"), field="commence_time")
            assert commence is not None
            identifier = task_id(str(event["id"]), commence - timedelta(minutes=60))
            if statuses.get(identifier) in {"success", "empty"}:
                counts["skipped"] += 1
                continue
            pending.append(event)
        selected = pending[: args.max_events]
        if len(pending) > len(selected):
            partial_reason = f"max-events left {len(pending) - len(selected)} pending events"
        context = load_exact_linker_context(context_engine, args.start_date, args.end_date + timedelta(days=1))
        for index, event in enumerate(selected, 1):
            try:
                status, raw_count, feature_count = process_event(api, state, conn, context, event)
                counts[status] += 1
                print(
                    f"[{index}/{len(selected)}] event={event['id']} status={status} "
                    f"raw={raw_count} features={feature_count} requests={api.requests_used} credits={api.credits_used}"
                )
            except BudgetExceededError as exc:
                counts["error"] += 1
                partial_reason = str(exc)
                break
            except Exception as exc:
                counts["error"] += 1
                partial_reason = f"event errors present; last={type(exc).__name__}: {exc}"
                print(f"[{index}/{len(selected)}] event={event.get('id')} status=error error={type(exc).__name__}: {exc}")
    finally:
        conn.close()
        context_engine.dispose()

    print(
        "complete: " + " ".join(f"{key}={value}" for key, value in counts.items())
        + f" requests={api.requests_used} credits={api.credits_used}"
    )
    if partial_reason or counts["unresolved"] or counts["error"]:
        print(f"partial: {partial_reason or 'unresolved tasks remain'}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
