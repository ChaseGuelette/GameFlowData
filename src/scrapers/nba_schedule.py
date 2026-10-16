#!/usr/bin/env python3
"""Read and explicitly ingest the NBA CDN league schedule."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.models.features.nba.season import is_competitive_nba_game_id, resolve_nba_season  # noqa: E402
from src.scrapers.nba_cdn_scraper import fetch_schedule  # noqa: E402

SOURCE_NAME = "nba_cdn"
EXCLUDED_GAME_PREFIXES = ("001", "003", "006")
MAX_SOURCE_AGE = timedelta(hours=48)
MAX_CLOCK_SKEW = timedelta(minutes=15)
NBA_TIMEZONE = ZoneInfo("America/New_York")

UPSERT_SQL = text(
    """
    INSERT INTO public.nba_game_schedule (
        game_id,
        season_id,
        season,
        game_type,
        game_date,
        commence_time_utc,
        home_team_id,
        away_team_id,
        game_status,
        status_text,
        postponed_status,
        venue_name,
        source,
        source_updated_at,
        observed_at
    ) VALUES (
        :game_id,
        :season_id,
        :season,
        :game_type,
        :game_date,
        :commence_time_utc,
        :home_team_id,
        :away_team_id,
        :game_status,
        :status_text,
        :postponed_status,
        :venue_name,
        :source,
        :source_updated_at,
        :observed_at
    )
    ON CONFLICT (game_id) DO UPDATE SET
        season_id = EXCLUDED.season_id,
        season = EXCLUDED.season,
        game_type = EXCLUDED.game_type,
        game_date = EXCLUDED.game_date,
        commence_time_utc = EXCLUDED.commence_time_utc,
        home_team_id = EXCLUDED.home_team_id,
        away_team_id = EXCLUDED.away_team_id,
        game_status = EXCLUDED.game_status,
        status_text = EXCLUDED.status_text,
        postponed_status = EXCLUDED.postponed_status,
        venue_name = EXCLUDED.venue_name,
        source = EXCLUDED.source,
        source_updated_at = EXCLUDED.source_updated_at,
        observed_at = EXCLUDED.observed_at,
        updated_at = NOW()
    WHERE public.nba_game_schedule.source_updated_at IS NULL
       OR EXCLUDED.source_updated_at >= public.nba_game_schedule.source_updated_at
    """
)


def _parse_utc_timestamp(value: Any, field_name: str, *, nullable: bool) -> datetime | None:
    if value in (None, ""):
        if nullable:
            return None
        raise ValueError(f"{field_name} is required")
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed.astimezone(UTC)


def _parse_source_meta_time(value: Any) -> datetime | None:
    """CDN meta.time is Eastern wall time mislabeled with Z, unlike gameDateTimeUTC."""
    parsed = _parse_utc_timestamp(value, "meta.time", nullable=True)
    if parsed is None:
        return None
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("meta.time must be an Eastern wall-clock string with Z suffix")
    return parsed.replace(tzinfo=NBA_TIMEZONE).astimezone(UTC)


def _parse_game_date(value: Any) -> date:
    if not isinstance(value, str):
        raise ValueError("gameDate must be a string")
    try:
        return datetime.strptime(value, "%m/%d/%Y %H:%M:%S").date()
    except ValueError as exc:
        raise ValueError(f"Invalid gameDate: {value!r}") from exc


def _team_id(game: dict[str, Any], field_name: str) -> int | None:
    team = game.get(field_name)
    if team is None:
        return None
    if not isinstance(team, dict):
        raise ValueError(f"{field_name} must be an object")
    value = team.get("teamId")
    if value in (None, 0, "0", ""):
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name}.teamId must be an integer or null")
    return value


def _validate_game_id(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 10 or not value.isascii() or not value.isdigit():
        raise ValueError(f"Invalid NBA game ID: {value!r}")
    if not is_competitive_nba_game_id(value) and not value.startswith(EXCLUDED_GAME_PREFIXES):
        raise ValueError(f"Unknown NBA game type for game ID: {value!r}")
    return value


def _schedule_dates(schedule_data: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(schedule_data, dict):
        raise ValueError("Schedule payload must be an object")
    league_schedule = schedule_data.get("leagueSchedule")
    if not isinstance(league_schedule, dict):
        raise ValueError("Schedule payload is missing leagueSchedule")
    game_dates = league_schedule.get("gameDates")
    if not isinstance(game_dates, list) or not game_dates:
        raise ValueError("Schedule payload is missing leagueSchedule.gameDates")
    return game_dates


def validate_source_freshness(rows: list[dict[str, Any]], observed_at: datetime) -> None:
    """Fail closed on an empty or stale CDN schedule before use or persistence."""
    if not rows:
        raise ValueError("No competitive games in the CDN schedule")
    source_time = rows[0]["source_updated_at"]
    if source_time is None:
        raise ValueError("Schedule source meta.time is missing")
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    age = observed_at.astimezone(UTC) - source_time
    if age > MAX_SOURCE_AGE:
        raise ValueError(f"CDN schedule source is stale ({age})")
    if age < -MAX_CLOCK_SKEW:
        raise ValueError(f"CDN schedule source time is in the future ({age})")


def parse_schedule(
    schedule_data: dict[str, Any],
    *,
    observed_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Parse competitive games from a full NBA CDN schedule payload."""
    game_dates = _schedule_dates(schedule_data)
    source_updated_at = _parse_source_meta_time(
        schedule_data.get("meta", {}).get("time") if isinstance(schedule_data.get("meta", {}), dict) else None,
    )
    observed = observed_at or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    observed = observed.astimezone(UTC)

    rows: list[dict[str, Any]] = []
    seen_game_ids: set[str] = set()
    for date_entry in game_dates:
        if not isinstance(date_entry, dict):
            raise ValueError("Each gameDates entry must be an object")
        game_date = _parse_game_date(date_entry.get("gameDate"))
        games = date_entry.get("games")
        if not isinstance(games, list):
            raise ValueError("Each gameDates entry must contain a games list")

        for game in games:
            if not isinstance(game, dict):
                raise ValueError("Each schedule game must be an object")
            game_id = _validate_game_id(game.get("gameId"))
            if game_id in seen_game_ids:
                raise ValueError(f"Duplicate game ID in schedule payload: {game_id}")
            seen_game_ids.add(game_id)
            if not is_competitive_nba_game_id(game_id):
                continue

            status = game.get("gameStatus")
            if status is not None and (isinstance(status, bool) or not isinstance(status, int)):
                raise ValueError(f"gameStatus must be an integer or null for {game_id}")
            status_text = game.get("gameStatusText")
            if status_text is not None and not isinstance(status_text, str):
                raise ValueError(f"gameStatusText must be a string or null for {game_id}")
            postponed_status = game.get("postponedStatus")
            if postponed_status is not None and not isinstance(postponed_status, str):
                raise ValueError(f"postponedStatus must be a string or null for {game_id}")
            venue_name = game.get("arenaName")
            if venue_name is not None and not isinstance(venue_name, str):
                raise ValueError(f"arenaName must be a string or null for {game_id}")

            season = resolve_nba_season(game_id, game_date)
            rows.append(
                {
                    "game_id": game_id,
                    "season_id": season.season_id,
                    "season": season.label,
                    "game_type": game_id[:3],
                    "game_date": game_date,
                    "commence_time_utc": _parse_utc_timestamp(
                        game.get("gameDateTimeUTC"),
                        f"gameDateTimeUTC for {game_id}",
                        nullable=True,
                    ),
                    "home_team_id": _team_id(game, "homeTeam"),
                    "away_team_id": _team_id(game, "awayTeam"),
                    "game_status": status,
                    "status_text": status_text,
                    "postponed_status": postponed_status,
                    "venue_name": venue_name,
                    "source": SOURCE_NAME,
                    "source_updated_at": source_updated_at,
                    "observed_at": observed,
                }
            )

    return rows


def upsert_schedule(engine: Any, rows: list[dict[str, Any]]) -> int:
    """Upsert a validated schedule batch in one transaction."""
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)
    return len(rows)


def ingest_schedule(
    *,
    engine: Any | None = None,
    schedule_data: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> int:
    """Fetch, validate, and explicitly upsert one schedule snapshot."""
    payload = schedule_data if schedule_data is not None else fetch_schedule()
    rows = parse_schedule(payload, observed_at=observed_at)
    validate_source_freshness(rows, rows[0]["observed_at"] if rows else observed_at or datetime.now(UTC))
    if engine is None:
        from src.db.client import get_engine

        engine = get_engine()
    return upsert_schedule(engine, rows)


def canary_summary(
    schedule_data: dict[str, Any] | None = None,
    *,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Fetch and validate one schedule snapshot without creating a DB engine."""
    payload = schedule_data if schedule_data is not None else fetch_schedule()
    rows = parse_schedule(payload, observed_at=observed_at)
    validate_source_freshness(rows, rows[0]["observed_at"] if rows else observed_at or datetime.now(UTC))
    raw_games = [
        game
        for date_entry in _schedule_dates(payload)
        for game in date_entry.get("games", [])
        if isinstance(game, dict)
    ]
    dates = [row["game_date"] for row in rows]
    return {
        "included_games": len(rows),
        "excluded_006": sum(str(game.get("gameId", "")).startswith("006") for game in raw_games),
        "tbd_games": sum(
            row["home_team_id"] is None or row["away_team_id"] is None or row["status_text"] == "TBD" for row in rows
        ),
        "missing_utc": sum(row["commence_time_utc"] is None for row in rows),
        "postponed_games": sum(row["postponed_status"] not in (None, "", "N") for row in rows),
        "date_start": min(dates) if dates else None,
        "date_end": max(dates) if dates else None,
        "observed_at": rows[0]["observed_at"],
        "source_meta_time_raw": payload.get("meta", {}).get("time"),
        "source_updated_at": rows[0]["source_updated_at"] if rows else _parse_utc_timestamp(
            payload.get("meta", {}).get("time") if isinstance(payload.get("meta", {}), dict) else None,
            "meta.time",
            nullable=True,
        ),
    }


def _print_canary(summary: dict[str, Any]) -> None:
    print(f"Included competitive games: {summary['included_games']}")
    print(f"Excluded 006 games: {summary['excluded_006']}")
    print(f"TBD/unassigned games: {summary['tbd_games']}")
    print(f"Games missing UTC start: {summary['missing_utc']}")
    print(f"Postponed games: {summary['postponed_games']}")
    print(f"Schedule date span: {summary['date_start']} to {summary['date_end']}")
    print(f"Source meta time: {summary['source_updated_at']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate or ingest the NBA CDN schedule")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--canary", action="store_true", help="Fetch and validate only; never connect to the database")
    action.add_argument("--ingest", action="store_true", help="Explicitly upsert the validated schedule")
    parser.add_argument("--output", type=Path, help="Write the read-only canary summary as JSON")
    args = parser.parse_args()
    if args.output and not args.canary:
        parser.error("--output is only supported with --canary")

    if args.canary:
        summary = canary_summary()
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(summary, indent=2, default=lambda value: value.isoformat()) + "\n", encoding="utf-8")
            print(f"Canary report: {args.output.resolve()}")
        _print_canary(summary)
    else:
        print(f"Upserted {ingest_schedule()} NBA schedule rows")


if __name__ == "__main__":
    main()
