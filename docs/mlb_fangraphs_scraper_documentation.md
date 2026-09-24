# MLB FanGraphs advanced-stat scraper

Status: active local-only ingestion reference. Reconciled 2026-08-24.

`src/scrapers/mlb/mlb_fangraphs_scraper.py` fetches qualified season-level batting and pitching statistics through `pybaseball`.

Never schedule this scraper on Railway. FanGraphs/pybaseball collection is workstation-only, consistent with the broader rule that blocked advanced-stat sources do not run from datacenter infrastructure.

## Output tables

Each successful row is written to both:

- `mlb_player_season_advanced`: latest-value compatibility table, upserted by `(player_id, season, player_type)`.
- `mlb_player_season_advanced_history`: dated snapshot table, inserted by `(player_id, season, player_type, as_of_date)`.

Model features must use the history table with strict `as_of_date < game_date`. The latest-value table is not temporally safe for historical training by itself.

When `--as-of-date` is omitted, the scraper uses yesterday so the snapshot represents statistics through the preceding day.

## Player resolution

Resolution uses:

1. normalized direct name matching against `mlb_players`;
2. suffix variations;
3. FanGraphs-to-MLBAM reverse lookup through `pybaseball`;
4. a stub player insert when an MLBAM identity resolves but is absent locally.

Populate MLB players from game/boxscore ingestion before a broad FanGraphs run to improve match coverage.

## Commands

Run locally from the repository root:

```powershell
.\venv\Scripts\python.exe -m src.scrapers.mlb.mlb_fangraphs_scraper --help
.\venv\Scripts\python.exe -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025 --as-of-date 2025-09-01
.\venv\Scripts\python.exe -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025 --batting-only
.\venv\Scripts\python.exe -m src.scrapers.mlb.mlb_fangraphs_scraper --season 2025 --pitching-only
.\venv\Scripts\python.exe -m src.scrapers.mlb.mlb_fangraphs_scraper --all-seasons
```

`--all-seasons` uses the implementation's `DEFAULT_SEASONS`; inspect that constant before assuming a newly completed season is included.

## Collected families

Batting includes WAR, BABIP, wRC+, wOBA, ISO, walk/strikeout/hard-contact rates, slash-line statistics, plate appearances, and FanGraphs identity.

Pitching includes WAR, BABIP, FIP/xFIP/xERA/SIERA/ERA, strand/ground-ball/strikeout/walk rates, K/9, BB/9, HR/9, innings pitched, and FanGraphs identity.

The exact column mapping in the scraper is authoritative.

## Safety and validation

- This command performs network and database writes; it is not a dry run.
- Broad history collection is a Chase-launched job.
- Review unmatched-player logs before treating a row count as complete.
- Validate the history-table `as_of_date` coverage before training.
- Do not substitute the latest-value table for point-in-time history.
- Do not expose connection strings in logs.

## Related documentation

- [MLB processing pipeline](mlb_processing_pipeline_documentation.md)
- [MLB feature catalog](mlb_feature_catalog.md)
- [Railway deployment](railway_deployment.md)
