# NBA rolling-average population

Status: active source-backed reference. Reconciled 2026-08-24.

The NBA rolling-average pipeline owns point-in-time player and team summary tables used by the feature store.

## Entrypoints

- Full rebuild: `src/processing/populate_average_stats.py`
- Daily incremental update: `src/processing/populate_average_stats_incremental.py`

Run the full rebuild as a module from the repository root. Direct script execution cannot resolve the project package imports in a clean checkout.

```powershell
.\venv\Scripts\python.exe -m src.processing.populate_average_stats --help
.\venv\Scripts\python.exe -m src.processing.populate_average_stats --table all
.\venv\Scripts\python.exe -m src.processing.populate_average_stats --season 2024-25 --table player
.\venv\Scripts\python.exe -m src.processing.populate_average_stats --from-year 2021 --table player_advanced

.\venv\Scripts\python.exe -m src.processing.populate_average_stats_incremental --help
.\venv\Scripts\python.exe -m src.processing.populate_average_stats_incremental
.\venv\Scripts\python.exe -m src.processing.populate_average_stats_incremental --date 2026-02-09
```

The full path supports `player`, `player_advanced`, `team`, or `all`. The incremental path updates the players active on the selected date.

## Tables

The full pipeline populates:

- `player_average_game_stats`
- `player_average_advanced_stats`
- `team_average_game_stats`

Inputs are `player_game_stats`, `player_game_advanced_stats`, and `team_game_stats`.

## Temporal contract

Rolling feature inputs are shifted by one game before aggregation. Therefore the average row labeled with game date X contains only information from games before X. Feature-store joins may safely select that precomputed row with `<= game_date`.

`MIN_MINUTES_FOR_STATS = 8` masks short player appearances from rolling stat values. Schedule counters still count all appearances:

- `rest_days`
- games in the preceding seven calendar days
- window/season game counts

## Derived families

The pipeline calculates:

- L5, L15, and season averages
- L3 recent-form averages
- L5 standard deviations
- rest and schedule density
- minimum-minute floor
- recent starter count and starter probability inputs

The exact columns and stable model memberships are owned by the implementation and `src/models/features/nba/contracts.py`; see the [NBA feature catalog](nba_feature_catalog.md).

## Full versus incremental

Use the incremental path for ordinary daily processing. It limits work to active players, fetches their required history, and upserts into `player_average_game_stats`.

The full path can delete/repopulate broad table slices and is appropriate only for intentional rebuilds. Chase launches broad backfills after a scoped preflight; do not treat a documentation example as authorization to run one.

## Validation

After changing rolling logic:

1. run focused processing tests;
2. verify first-game null/default behavior;
3. verify shift(1) with a small known player sequence;
4. exercise an NBA feature-store date load and single-player inference path;
5. verify incremental upserts do not change unrelated dates.

## Related documentation

- [Documentation index](README.md)
- [NBA feature store](feature_store_documentation.md)
- [NBA feature catalog](nba_feature_catalog.md)
- [Daily pipeline automation](daily_pipeline_automation.md)
