# MLB processing and model-data pipeline

Status: active source-backed reference. Reconciled 2026-08-24.

This page maps retained MLB ingestion output into point-in-time features and the three production artifact consumers. Exact schema and feature membership live in source; this page describes ownership and safe operating order.

## Active production boundary

The production artifact audit requires and loads:

- `pitcher_strikeouts`
- `batter_hits`
- `batter_rbis`

Training lifecycle profiles exist for the same three stats in `src/models/mlb/training/profiles.py`.

Other batter contracts and pitcher-outs helpers remain available for research/compatibility, but they are not evidence of production promotion. Total bases, runs, home runs, HRR, and other removed lanes must not be described as active without a new artifact audit and promotion decision.

## Processing owners

### Entity linking

- `src/processing/mlb/mlb_linker.py`: direct database incremental/backfill linker.
- `src/processing/mlb/mlb_linker_local.py`: CSV/checkpoint workflow for large local reconciliation.
- `src/processing/mlb/mlb_config.py`: rolling windows, stat columns, batch sizes, fuzzy threshold, and team aliases.

The linkers populate game/player/team identities for raw MLB lines and props. Canonical team aliases include `AZ` and `ATH`; changing aliases requires coverage validation.

Commands:

```powershell
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_linker --help
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_linker incremental
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_linker backfill

.\venv\Scripts\python.exe -m src.processing.mlb.mlb_linker_local --help
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_linker_local status
```

The local linker has download/process/upload stages and checkpoint state. Upload/backfill modes perform broad DB writes; Chase launches them only after a read-only coverage preflight. Do not infer current coverage from old session totals in documentation.

### Rolling averages

- `src/processing/mlb/mlb_populate_averages.py`: broad batting/pitching rebuild.
- `src/processing/mlb/mlb_populate_averages_incremental.py`: target-date update.
- `src/processing/mlb/mlb_populate_statcast_averages.py`: Statcast rolling summaries.

```powershell
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_populate_averages --help
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_populate_averages --table all --season 2025
.\venv\Scripts\python.exe -m src.processing.mlb.mlb_populate_averages_incremental --date 2026-08-24
```

Rolling values use shift(1). Rate statistics are calculated from rolling numerator/denominator sums rather than the mean of per-game ratios.

### Matchup/context transforms

- `src/processing/mlb/mlb_matchup_features.py`: team-level batting context for pitcher models.
- `src/processing/mlb/mlb_batter_matchup_features.py`: opposing-pitcher, platoon, bullpen, lineup, and related batter context.

Feature stores own final mode alignment and defaults; processing modules should not silently redefine model contracts.

## Feature-store boundary

- `src/models/mlb/mlb_feature_store.py`: thin pitcher compatibility facade.
- `src/models/mlb/mlb_batter_feature_store.py`: thin batter compatibility facade.
- `src/models/mlb/features/contracts.py`: canonical pitcher/batter feature contracts.
- `src/models/mlb/features/`: loaders, transforms, and legacy implementation owners.

At reconciliation time, the pitcher contract contains 89 computable features and 76 training-eligible features. The 13 Phase 3A lineup/umpire features remain computable for compatibility but are explicitly locked out of new training. See [MLB feature catalog](mlb_feature_catalog.md).

## Point-in-time advanced statistics

FanGraphs/other season-level features must come from `mlb_player_season_advanced_history` with strict `as_of_date < game_date`. The latest-value compatibility table cannot safely train historical models by itself. See [MLB FanGraphs scraper](mlb_fangraphs_scraper_documentation.md).

## Pipeline order

1. Collect schedules, players, box scores, props/lines, weather, lineups, and approved local-only advanced sources.
2. Link raw quotes to game/player/team identities.
3. Populate basic and Statcast rolling tables with shift(1).
4. Build matchup, bullpen, lineup, park, weather, and point-in-time advanced context.
5. Load mode-aligned pitcher/batter feature frames.
6. Train/evaluate through the YAML lifecycle or exact trainer entrypoint.
7. Run quote-clean sweep, audit, ranker/dropout/timing evidence, then paper-only promotion gates.
8. Package only explicitly approved artifacts into `src/models/mlb/artifacts/production`.

## Safety

- Railway may run retained daily MLB jobs, but advanced scraping blocked by datacenter/network behavior remains workstation-only.
- Broad backfills, linkers, and training are Chase-launched.
- Feature selection is not an ablation; validate correlated families explicitly.
- Use empirical sample probabilities, never Gaussian approximations.
- Do not apply global conformal offsets.
- Never infer model promotion from a feature contract or artifact filename alone.

## Related documentation

- [MLB feature catalog](mlb_feature_catalog.md)
- [MLB lifecycle guide](development_docs/mlb_model_lifecycle_usage_guide.md)
- [MLB FanGraphs scraper](mlb_fangraphs_scraper_documentation.md)
- [Daily pipeline automation](daily_pipeline_automation.md)
