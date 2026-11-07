"""Bounded NBA game-start reconciliation for historical injury cutoffs."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from src.models.features.nba.season import is_competitive_nba_game_id, resolve_nba_season

NBA_TIMEZONE = ZoneInfo("America/New_York")
MAX_GAME_IDS = 100
MAX_QUOTES_PER_GAME = 2000
CHUNK_SIZE = 100
_INVALID_STATUS_TERMS = ("postpon", "cancel", "tbd")

_SCHEDULE_SQL = text(
    """
    SELECT game_id, game_date, commence_time_utc, game_status, status_text, postponed_status
    FROM public.nba_game_schedule
    WHERE game_id IN :game_ids
    """
).bindparams(bindparam("game_ids", expanding=True))

_ODDS_SQL = text(
    """
    WITH targets AS (
        SELECT game_id, game_date
        FROM jsonb_to_recordset(CAST(:targets AS jsonb)) AS requested(game_id text, game_date date)
    )
    SELECT targets.game_id, quotes.commence_time
    FROM targets
    CROSS JOIN LATERAL (
        SELECT commence_time
        FROM public.raw_game_lines_staging
        WHERE nba_game_id = targets.game_id
        LIMIT :quote_limit
    ) quotes
    ORDER BY targets.game_id
    """
)

_UPSERT_SQL = text(
    """
    INSERT INTO public.nba_injury_game_timing
        (game_id, game_date, game_start_utc, basis, status)
    VALUES
        (:game_id, :game_date, :game_start_utc, :basis, :status)
    ON CONFLICT (game_id) DO UPDATE SET
        game_date = EXCLUDED.game_date,
        game_start_utc = EXCLUDED.game_start_utc,
        basis = EXCLUDED.basis,
        status = EXCLUDED.status,
        updated_at = now()
    WHERE (public.nba_injury_game_timing.game_date,
           public.nba_injury_game_timing.game_start_utc,
           public.nba_injury_game_timing.basis,
           public.nba_injury_game_timing.status)
          IS DISTINCT FROM
          (EXCLUDED.game_date, EXCLUDED.game_start_utc, EXCLUDED.basis, EXCLUDED.status)
    """
)


@dataclass(frozen=True)
class GameTimingRequest:
    game_id: str
    game_date: date


@dataclass(frozen=True)
class GameTimingReceipt:
    game_id: str
    game_date: date
    game_start_utc: datetime | None
    basis: str
    status: str
    reason: str | None
    inspected_quotes: int = 0

    def as_json(self) -> dict[str, object]:
        payload = asdict(self)
        payload["game_date"] = self.game_date.isoformat()
        payload["game_start_utc"] = self.game_start_utc.isoformat() if self.game_start_utc else None
        return payload


def normalize_requests(items: Iterable[GameTimingRequest | tuple[str, date]]) -> list[GameTimingRequest]:
    """Validate bounded, exact game/date requests and reject conflicting duplicates."""
    by_id: dict[str, GameTimingRequest] = {}
    for item in items:
        request = item if isinstance(item, GameTimingRequest) else GameTimingRequest(*item)
        if not is_competitive_nba_game_id(request.game_id):
            raise ValueError(f"Invalid competitive NBA game ID: {request.game_id!r}")
        if not isinstance(request.game_date, date) or isinstance(request.game_date, datetime):
            raise ValueError(f"Invalid game date for {request.game_id}: {request.game_date!r}")
        resolve_nba_season(request.game_id, request.game_date)
        existing = by_id.get(request.game_id)
        if existing and existing.game_date != request.game_date:
            raise ValueError(
                f"Conflicting dates for {request.game_id}: {existing.game_date} and {request.game_date}"
            )
        by_id[request.game_id] = request
    if not by_id:
        raise ValueError("At least one game ID/date pair is required")
    if len(by_id) > MAX_GAME_IDS:
        raise ValueError(f"At most {MAX_GAME_IDS} distinct game IDs are allowed per reconciliation call")
    return list(by_id.values())


def _guard_read_transaction(conn) -> None:
    conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SET LOCAL lock_timeout = '3s'"))
    conn.execute(text("SET LOCAL statement_timeout = '10s'"))
    if conn.execute(text("SHOW transaction_read_only")).scalar_one() != "on":
        raise RuntimeError("Read-only transaction guard was not applied")
    if conn.execute(text("SHOW lock_timeout")).scalar_one() != "3s":
        raise RuntimeError("Read lock timeout guard was not applied")
    if conn.execute(text("SHOW statement_timeout")).scalar_one() != "10s":
        raise RuntimeError("Read statement timeout guard was not applied")


def _guard_write_transaction(conn) -> None:
    conn.execute(text("SET LOCAL lock_timeout = '3s'"))
    conn.execute(text("SET LOCAL statement_timeout = '15s'"))
    if conn.execute(text("SHOW transaction_read_only")).scalar_one() != "off":
        raise RuntimeError("Write transaction unexpectedly became read-only")
    if conn.execute(text("SHOW lock_timeout")).scalar_one() != "3s":
        raise RuntimeError("Write lock timeout guard was not applied")
    if conn.execute(text("SHOW statement_timeout")).scalar_one() != "15s":
        raise RuntimeError("Write statement timeout guard was not applied")


def require_timing_schema(conn) -> None:
    """Fail clearly when the explicit timing migration has not been applied."""
    try:
        conn.execute(
            text(
                "SELECT game_id, game_date, game_start_utc, basis, status "
                "FROM public.nba_injury_game_timing WHERE false"
            )
        )
    except SQLAlchemyError as exc:
        raise RuntimeError(
            "Missing or incompatible public.nba_injury_game_timing; apply the reviewed migration explicitly"
        ) from exc


def _schedule_invalid_reason(row: dict[str, object], requested_date: date) -> str | None:
    if row["game_date"] != requested_date:
        return f"schedule_date_mismatch:{row['game_date']}"
    if row["commence_time_utc"] is None:
        return "schedule_start_missing"
    postponed = str(row["postponed_status"] or "").strip().lower()
    if postponed not in {"", "n", "0", "false"}:
        return f"schedule_postponed_status:{postponed}"
    status_text = str(row["status_text"] or "").strip().lower()
    if any(term in status_text for term in _INVALID_STATUS_TERMS):
        return f"schedule_status:{status_text}"
    start = row["commence_time_utc"]
    if not isinstance(start, datetime) or start.tzinfo is None:
        return "schedule_start_not_timezone_aware"
    if start.astimezone(NBA_TIMEZONE).date() != requested_date:
        return f"schedule_start_date_mismatch:{start.astimezone(NBA_TIMEZONE).date()}"
    return None


def _schedule_receipt(request: GameTimingRequest, row: dict[str, object]) -> GameTimingReceipt:
    reason = _schedule_invalid_reason(row, request.game_date)
    if reason:
        return GameTimingReceipt(
            request.game_id, request.game_date, None, "schedule_invalid", "unknown", reason
        )
    start = row["commence_time_utc"]
    assert isinstance(start, datetime)
    return GameTimingReceipt(
        request.game_id, request.game_date, start.astimezone(UTC), "schedule", "resolved", None
    )


def _odds_receipt(
    request: GameTimingRequest,
    values: Sequence[datetime | None],
    *,
    max_quotes: int,
) -> GameTimingReceipt:
    inspected = len(values)
    if inspected > max_quotes:
        return GameTimingReceipt(
            request.game_id,
            request.game_date,
            None,
            "odds_unknown",
            "unknown",
            "quote_limit_exceeded",
            inspected,
        )
    starts = []
    for value in values:
        if value is None:
            continue
        if not isinstance(value, datetime) or value.tzinfo is None:
            return GameTimingReceipt(
                request.game_id,
                request.game_date,
                None,
                "odds_unknown",
                "unknown",
                "odds_start_not_timezone_aware",
                inspected,
            )
        starts.append(value.astimezone(UTC))
    if not starts:
        reason = "odds_rows_null_only" if inspected else "odds_rows_missing"
        return GameTimingReceipt(
            request.game_id, request.game_date, None, "odds_unknown", "unknown", reason, inspected
        )
    distinct = set(starts)
    if len(distinct) != 1:
        return GameTimingReceipt(
            request.game_id,
            request.game_date,
            None,
            "odds_unknown",
            "unknown",
            "odds_start_conflict",
            inspected,
        )
    start = starts[0]
    local_date = start.astimezone(NBA_TIMEZONE).date()
    if local_date != request.game_date:
        return GameTimingReceipt(
            request.game_id,
            request.game_date,
            None,
            "odds_unknown",
            "unknown",
            f"odds_start_date_mismatch:{local_date}",
            inspected,
        )
    return GameTimingReceipt(
        request.game_id, request.game_date, start, "odds_fallback", "resolved", None, inspected
    )


def resolve_game_timings(
    engine,
    items: Iterable[GameTimingRequest | tuple[str, date]],
    *,
    allow_odds_fallback: bool = True,
    max_quotes: int = MAX_QUOTES_PER_GAME,
) -> list[GameTimingReceipt]:
    """Resolve exact games schedule-first; only absent schedules may use bounded odds rows."""
    requests = normalize_requests(items)
    if not 1 <= max_quotes <= MAX_QUOTES_PER_GAME:
        raise ValueError(f"max_quotes must be between 1 and {MAX_QUOTES_PER_GAME}")
    request_by_id = {request.game_id: request for request in requests}

    with engine.begin() as conn:
        _guard_read_transaction(conn)
        require_timing_schema(conn)
        schedule_rows = {
            row["game_id"]: dict(row)
            for row in conn.execute(
                _SCHEDULE_SQL, {"game_ids": list(request_by_id)}
            ).mappings()
        }
        missing = [request for request in requests if request.game_id not in schedule_rows]
        odds_by_id: dict[str, list[datetime | None]] = {request.game_id: [] for request in missing}
        if missing and allow_odds_fallback:
            targets = json.dumps(
                [{"game_id": request.game_id, "game_date": request.game_date.isoformat()} for request in missing]
            )
            for row in conn.execute(
                _ODDS_SQL, {"targets": targets, "quote_limit": max_quotes + 1}
            ).mappings():
                odds_by_id[row["game_id"]].append(row["commence_time"])

    receipts = []
    for request in requests:
        schedule_row = schedule_rows.get(request.game_id)
        if schedule_row is not None:
            receipts.append(_schedule_receipt(request, schedule_row))
        elif allow_odds_fallback:
            receipts.append(_odds_receipt(request, odds_by_id[request.game_id], max_quotes=max_quotes))
        else:
            receipts.append(
                GameTimingReceipt(
                    request.game_id,
                    request.game_date,
                    None,
                    "schedule_absent",
                    "unknown",
                    "schedule_row_missing",
                )
            )
    return receipts


def persist_game_timings(engine, receipts: Sequence[GameTimingReceipt]) -> int:
    """Persist resolved or explicitly unknown state; unchanged rows remain untouched."""
    if not receipts:
        return 0
    with engine.begin() as conn:
        _guard_write_transaction(conn)
        require_timing_schema(conn)
        result = conn.execute(
            _UPSERT_SQL,
            [
                {
                    "game_id": receipt.game_id,
                    "game_date": receipt.game_date,
                    "game_start_utc": receipt.game_start_utc,
                    "basis": receipt.basis,
                    "status": receipt.status,
                }
                for receipt in receipts
            ],
        )
    return max(result.rowcount or 0, 0)


def select_injury_linked_games(
    engine,
    start_date: date,
    end_date: date,
    *,
    max_games: int,
) -> list[GameTimingRequest]:
    """Select a bounded range of injury-linked games, refusing ambiguous or overflowing input."""
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    if not 1 <= max_games <= MAX_GAME_IDS:
        raise ValueError(f"max_games must be between 1 and {MAX_GAME_IDS}")
    query = text(
        """
        SELECT DISTINCT game_id, report_date AS game_date
        FROM public.rapidapi_injuries
        WHERE game_id IS NOT NULL
          AND report_date BETWEEN :start_date AND :end_date
        ORDER BY game_id, game_date
        LIMIT :limit
        """
    )
    with engine.begin() as conn:
        _guard_read_transaction(conn)
        rows = conn.execute(
            query, {"start_date": start_date, "end_date": end_date, "limit": max_games + 1}
        ).mappings().all()
    if len(rows) > max_games:
        raise ValueError(f"Injury-linked selection exceeds --max-games={max_games}; narrow the date range")
    return normalize_requests((row["game_id"], row["game_date"]) for row in rows)


def reconcile_persisted_schedule_timings(engine, game_ids: Iterable[str]) -> int:
    """Reconcile only submitted IDs from persisted schedule state, never historical odds."""
    unique_ids = list(dict.fromkeys(game_ids))
    if not unique_ids:
        return 0
    for game_id in unique_ids:
        if not is_competitive_nba_game_id(game_id):
            raise ValueError(f"Invalid competitive NBA game ID: {game_id!r}")

    changed = 0
    for offset in range(0, len(unique_ids), CHUNK_SIZE):
        chunk = unique_ids[offset : offset + CHUNK_SIZE]
        with engine.begin() as conn:
            _guard_read_transaction(conn)
            require_timing_schema(conn)
            rows = conn.execute(_SCHEDULE_SQL, {"game_ids": chunk}).mappings().all()
        persisted = {row["game_id"]: row for row in rows}
        if missing := [game_id for game_id in chunk if game_id not in persisted]:
            raise RuntimeError(f"Submitted schedule IDs missing after upsert: {missing[:5]}")
        requests = [GameTimingRequest(row["game_id"], row["game_date"]) for row in rows]
        receipts = resolve_game_timings(engine, requests, allow_odds_fallback=False)
        changed += persist_game_timings(engine, receipts)
    return changed
