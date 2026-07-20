#!/usr/bin/env python3
"""Targeted July 2026 pitcher-strikeout CLV player linker.

Dry-run/read-only by default. Writes require ``--execute``. Both snapshot
queries and updates are constrained to the same explicit half-open ID range,
half-open commence/requested timestamp window, market, and NULL player_id.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import os
import re
from typing import Iterable, NamedTuple
import unicodedata


MARKET_KEY = "pitcher_strikeouts"
DEFAULT_START_DATE = "2026-07-01"
DEFAULT_END_DATE = "2026-07-19"
STATEMENT_TIMEOUT_MS = 120_000
LOCK_TIMEOUT_MS = 5_000
IDLE_TRANSACTION_TIMEOUT_MS = 120_000
ALIASES = {"Samuel Aldegheri": "Sam Aldegheri"}
NORMALIZED_LINK_METHOD = (
    "targeted_pitcher_strikeouts_july_2026:python_nfkd_normalized_unique"
)
ALIAS_LINK_METHOD = (
    "targeted_pitcher_strikeouts_july_2026:explicit_alias_unique:samuel_aldegheri_to_sam_aldegheri"
)


class Resolution(NamedTuple):
    player_id: int
    linked_name: str
    decision: str


def normalize_name(value: str | None) -> str:
    """Return an accent-free, lowercase alphanumeric exact-match key."""
    if not value:
        return ""
    decomposed = unicodedata.normalize("NFKD", value)
    without_accents = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return re.sub(r"[^a-z0-9]+", "", without_accents.casefold())


def resolve_player_name(
    api_name: str, players: Iterable[tuple[int, str]]
) -> Resolution | None:
    """Resolve an exact normalized name or the one approved explicit alias.

    A mapping is returned only when the selected normalized key has exactly one
    mlb_players row. There is deliberately no fuzzy or similarity fallback.
    """
    candidates_by_key: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for player_id, player_name in players:
        key = normalize_name(player_name)
        if key:
            candidates_by_key[key].append((int(player_id), player_name))

    alias_target = ALIASES.get(api_name)
    if alias_target is not None:
        candidates = candidates_by_key.get(normalize_name(alias_target), [])
        decision = "explicit_alias_unique"
    else:
        candidates = candidates_by_key.get(normalize_name(api_name), [])
        decision = "normalized_unique"

    if len(candidates) != 1:
        return None
    player_id, linked_name = candidates[0]
    return Resolution(player_id, linked_name, decision)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Link bounded July pitcher_strikeouts snapshots by unique normalized MLB player name"
    )
    parser.add_argument("--start-id", type=int, required=True, help="Inclusive snapshot ID")
    parser.add_argument("--end-id", type=int, required=True, help="Exclusive snapshot ID")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Inclusive UTC date")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Exclusive UTC date")
    parser.add_argument("--local", action="store_true", help="Use LOCAL_DATABASE_URL_AGENT, then LOCAL_DATABASE_URL")
    parser.add_argument("--execute", action="store_true", help="Apply updates; omitted means read-only dry-run")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.start_id < 0:
        raise SystemExit("--start-id must be non-negative")
    if args.end_id <= args.start_id:
        raise SystemExit("--end-id must be greater than --start-id (half-open range)")
    try:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    except ValueError as exc:
        raise SystemExit(f"Dates must use YYYY-MM-DD: {exc}") from exc
    if end_date <= start_date:
        raise SystemExit("--end-date must be after --start-date (half-open range)")


def choose_database_url(local: bool) -> tuple[str, str]:
    if local:
        for key in ("LOCAL_DATABASE_URL_AGENT", "LOCAL_DATABASE_URL"):
            value = os.getenv(key)
            if value:
                return value, key
        raise SystemExit("Missing LOCAL_DATABASE_URL_AGENT and LOCAL_DATABASE_URL")
    value = os.getenv("DATABASE_URL")
    if not value:
        raise SystemExit("Missing DATABASE_URL")
    return value, "DATABASE_URL"


def bounded_params(args: argparse.Namespace) -> dict[str, object]:
    return {
        "market_key": MARKET_KEY,
        "start_id": args.start_id,
        "end_id": args.end_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
    }


TARGET_NAMES_SQL = """
    SELECT api_player_name, COUNT(*) AS row_count
    FROM mlb_player_props_clv_snapshots
    WHERE market_key = %(market_key)s
      AND id >= %(start_id)s
      AND id < %(end_id)s
      AND commence_time >= %(start_date)s::date
      AND commence_time < %(end_date)s::date
      AND requested_snapshot_time >= %(start_date)s::date
      AND requested_snapshot_time < %(end_date)s::date
      AND player_id IS NULL
      AND api_player_name IS NOT NULL
    GROUP BY api_player_name
    ORDER BY api_player_name
"""

PLAYERS_SQL = """
    SELECT player_id, player_name
    FROM mlb_players
    WHERE player_id IS NOT NULL
      AND player_name IS NOT NULL
"""

UPDATE_SQL = """
    UPDATE mlb_player_props_clv_snapshots
       SET player_id = %(player_id)s,
           linked_player_name = %(linked_name)s,
           player_link_method = %(link_method)s,
           linked_at = now()
     WHERE market_key = %(market_key)s
       AND id >= %(start_id)s
       AND id < %(end_id)s
       AND commence_time >= %(start_date)s::date
       AND commence_time < %(end_date)s::date
       AND requested_snapshot_time >= %(start_date)s::date
       AND requested_snapshot_time < %(end_date)s::date
       AND player_id IS NULL
       AND api_player_name = %(api_name)s
"""


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    validate_args(args)

    # Imports stay out of the pure normalization/resolution unit-test path.
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv()
    database_url, env_name = choose_database_url(args.local)
    mode = "execute" if args.execute else "dry-run/read-only"
    print(
        f"target={env_name} mode={mode} market_key={MARKET_KEY} "
        f"ids=[{args.start_id},{args.end_id}) "
        f"commence/requested=[{args.start_date},{args.end_date})"
    )

    connection = psycopg2.connect(
        database_url,
        connect_timeout=15,
        application_name="gameflow:targeted_pitcher_k_july_player_linker",
        options=(
            f"-c statement_timeout={STATEMENT_TIMEOUT_MS} "
            f"-c lock_timeout={LOCK_TIMEOUT_MS} "
            f"-c idle_in_transaction_session_timeout={IDLE_TRANSACTION_TIMEOUT_MS}"
        ),
    )
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ WRITE" if args.execute else "SET TRANSACTION READ ONLY")
            cursor.execute(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}")
            cursor.execute(f"SET LOCAL lock_timeout = {LOCK_TIMEOUT_MS}")
            cursor.execute(
                f"SET LOCAL idle_in_transaction_session_timeout = {IDLE_TRANSACTION_TIMEOUT_MS}"
            )

            params = bounded_params(args)
            cursor.execute(TARGET_NAMES_SQL, params)
            target_names = [(name, int(count)) for name, count in cursor.fetchall()]

            cursor.execute(PLAYERS_SQL)
            players = [(int(player_id), player_name) for player_id, player_name in cursor.fetchall()]

            mappings: list[tuple[str, int, Resolution]] = []
            unresolved: list[tuple[str, int]] = []
            for api_name, row_count in target_names:
                resolution = resolve_player_name(api_name, players)
                if resolution is None:
                    unresolved.append((api_name, row_count))
                    print(f"name={api_name!r} rows={row_count} decision=UNRESOLVED")
                    continue
                mappings.append((api_name, row_count, resolution))
                print(
                    f"name={api_name!r} rows={row_count} decision={resolution.decision} "
                    f"player_id={resolution.player_id} linked_name={resolution.linked_name!r}"
                )

            would_update_total = sum(row_count for _, row_count, _ in mappings)
            updated_total = 0
            if args.execute:
                for api_name, expected_rows, resolution in mappings:
                    update_params = {
                        **params,
                        "api_name": api_name,
                        "player_id": resolution.player_id,
                        "linked_name": resolution.linked_name,
                        "link_method": (
                            ALIAS_LINK_METHOD
                            if resolution.decision == "explicit_alias_unique"
                            else NORMALIZED_LINK_METHOD
                        ),
                    }
                    cursor.execute(UPDATE_SQL, update_params)
                    updated = cursor.rowcount
                    updated_total += updated
                    print(
                        f"apply name={api_name!r} expected={expected_rows} updated={updated}"
                    )

            print("unresolved_names:")
            if unresolved:
                for api_name, row_count in unresolved:
                    print(f"  {api_name!r}: rows={row_count}")
            else:
                print("  (none)")
            print(
                f"summary names={len(target_names)} mapped_names={len(mappings)} "
                f"unresolved_names={len(unresolved)} would_update={would_update_total} "
                f"updated={updated_total}"
            )

        if args.execute:
            connection.commit()
        else:
            connection.rollback()
        return 0
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
