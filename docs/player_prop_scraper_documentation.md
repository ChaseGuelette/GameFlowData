# Historical NBA player-prop scraper

Status: active manual backfill reference. Reconciled 2026-08-24.

`src/scrapers/player_prop_scraper.py` downloads historical NBA player-prop snapshots from The Odds API and inserts every valid quote into `raw_player_props_combined`.

This is a credit-consuming manual backfill tool. It is not the Railway live-lines job.

## Requirements

The module validates these variables during import, including for `--dry-run`:

- `DATABASE_URL`
- `ODDS_API_KEY`

The progress file is `scrape_progress.json` in the current working directory. Keep separate working directories or explicitly manage that file when running different backfill lanes.

## Markets

Default core markets:

- points, rebounds, assists, threes
- blocks, steals, turnovers

Optional combo markets:

- PRA, points+rebounds, points+assists, rebounds+assists
- double-double, triple-double

`--markets` accepts an explicit list and overrides the presets.

## Commands

Run from the repository root:

```powershell
.\venv\Scripts\python.exe src\scrapers\player_prop_scraper.py --help
.\venv\Scripts\python.exe src\scrapers\player_prop_scraper.py --start-date 2025-01-01 --end-date 2025-06-30 --dry-run
.\venv\Scripts\python.exe src\scrapers\player_prop_scraper.py --combos --start-date 2025-01-01
.\venv\Scripts\python.exe src\scrapers\player_prop_scraper.py --combos-only --start-date 2024-10-22 --end-date 2025-04-15
.\venv\Scripts\python.exe src\scrapers\player_prop_scraper.py --markets player_double_double player_triple_double
```

Use `--no-resume` only when intentionally discarding the progress file. It deletes `scrape_progress.json` before the run.

## Resume behavior

Progress is keyed by the sorted market set and stores `(snapshot_timestamp, event_id)` pairs. A different market set does not reuse prior progress. Events are marked processed even when The Odds API returns no prop data, preventing endless retries of empty/422 responses.

Progress is written after each snapshot and on interrupt/error.

## Time range limitation

`generate_snapshot_timestamps()` contains an explicit season calendar ending with the 2025-26 playoffs in June 2026. `--start-date` and `--end-date` filter that built-in calendar; they do not create dates outside it. Extend and review the calendar before using this tool for a later season.

## Output contract

Inserted fields include game/player/book identifiers, market/outcome, line and American odds, commence time, teams, request snapshot time, market update time, bookmaker update time, and bookmaker display name.

The script inserts raw snapshots directly into `raw_player_props_combined`; documentation referring to `raw_player_props_staging_v2` is obsolete.

## Safety

- Start with `--dry-run` and review estimated API credits.
- Chase launches broad/credit-heavy backfills.
- Do not print the API key or database URL.
- Do not run blocking index creation on `raw_player_props_combined`.
- Preserve pregame/as-of timestamps; downstream quote-clean work depends on them.

## Related documentation

- [Documentation index](README.md)
- [Daily pipeline automation](daily_pipeline_automation.md)
- [NBA feature store](feature_store_documentation.md)
