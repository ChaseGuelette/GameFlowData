"""Guard the reviewed NBA schedule migration and schema snapshot against drift."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "database" / "schema.sql"
MIGRATION = ROOT / "database" / "migrations" / "20260924_nba_game_schedule.sql"


def _schedule_definition(sql: str) -> str:
    match = re.search(
        r"create table(?: if not exists)? public\.nba_game_schedule\s*\((.*?)\n\)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match is not None
    return re.sub(r"\s+", " ", match.group(1).lower())


def test_schedule_migration_matches_canonical_schema():
    schema = SCHEMA.read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")
    snapshot = _schedule_definition(schema)
    ddl = _schedule_definition(migration)
    assert snapshot.strip() == ddl.strip()
    for column in (
        "game_id text not null",
        "season_id text not null",
        "season text not null",
        "game_type text not null",
        "game_date date not null",
        "commence_time_utc timestamp with time zone null",
        "home_team_id bigint null",
        "away_team_id bigint null",
        "game_status smallint null",
        "status_text text null",
        "postponed_status text null",
        "venue_name text null",
        "source text not null",
        "source_updated_at timestamp with time zone null",
        "observed_at timestamp with time zone not null",
        "updated_at timestamp with time zone not null default now()",
    ):
        assert column in snapshot
        assert column in ddl
    for constraint in (
        "primary key (game_id)",
        "game_type in ('002', '004', '005')",
        "foreign key (home_team_id)",
        "foreign key (away_team_id)",
        "references public.teams (team_id)",
    ):
        assert constraint in snapshot
        assert constraint in ddl
    for sql in (schema.lower(), migration.lower()):
        assert "enable row level security" in sql
        assert "grant select on public.nba_game_schedule to authenticated" in sql
        assert "for select to authenticated using (true)" in sql
        assert "idx_nba_game_schedule_date" in sql
    assert "begin;" in migration.lower()
    assert "commit;" in migration.lower()
