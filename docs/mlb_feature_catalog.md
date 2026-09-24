# MLB feature-contract catalog

Status: active contract summary. Reconciled 2026-08-24.

Exact feature names and membership are owned by:

- `src/models/mlb/features/contracts.py`
- `src/models/mlb/training/profiles.py`

This page intentionally summarizes the contracts instead of duplicating all runtime lists. Tests and manifests must compare against source, not this prose.

## Production profiles

The supported lifecycle profiles and required production artifacts are:

- `pitcher_strikeouts`
- `batter_hits`
- `batter_rbis`

Other feature contracts retained in `contracts.py` are research/compatibility surfaces, not production promotion.

## Contract sizes

| Contract | Computable features | Training posture |
|---|---:|---|
| Pitcher strikeouts | 89 | 76 eligible; 13 Phase 3A lineup/umpire features locked out |
| Batter base | 77 | Shared base for batter models |
| Batter hits | 84 | Active lifecycle/production profile |
| Batter RBIs | 79 | Active lifecycle/production profile |

The counts are a reconciliation snapshot. `contracts.py`, artifact feature manifests, and training metadata are authoritative for a specific run.

## Pitcher-strikeouts families

The runtime contract covers:

- rolling strikeout, innings, batters-faced, pitch-count, and workload/leash history;
- recent/season Statcast stuff and velocity context;
- team hook and bullpen pressure;
- manager/pitcher downside indicators;
- point-in-time FanGraphs FIP and K%;
- opponent strikeout/contact/whiff/chase context;
- park, home/away, total, weather, and prop-line context;
- inning-level fatigue and early/late-game behavior;
- repertoire diversity and interaction features;
- projected-lineup and umpire compatibility features.

`PITCHER_K_PHASE3A_REJECTED_FEATURES` contains 13 lineup/umpire features that compressed the Phase 2 edge. They remain computable for old artifacts but are excluded from `PITCHER_K_TRAINING_FEATURES`.

The YAML lifecycle family registry lives in `src/models/mlb/training/profiles.py`. Human-readable family membership is maintained in `configs/mlb/examples/FEATURE_FAMILIES.md` and is test-covered.

## Batter families

`BATTER_BASE_FEATURES` combines:

- L5/L10/L20/season batting volume, rates, variability, rest, and game number;
- point-in-time Statcast and advanced-history signals;
- lineup position and projected opportunity;
- opposing starter, platoon, bullpen, inning-level, and umpire context;
- park, weather, home/away, and game total;
- derived recent-form interactions.

Stat extensions add the relevant park factor, prop line, and specialized opportunity/contact features.

Lifecycle-controllable batter families are:

- market
- recent form
- contact quality
- matchup pitcher
- bullpen
- platoon
- environment
- opportunity

The shared market family currently names the batter-hits prop line. For RBI experiments, inspect the resolved config and exact feature manifest; prefer no-prop-line attribution unless the RBI market feature is explicitly verified.

## Temporal source rules

- Rolling tables use shift(1) snapshots.
- Advanced season stats use `mlb_player_season_advanced_history` with `as_of_date < game_date`.
- Raw prop/game lines require quote-clean as-of and pre-commence selection.
- Lineup, weather, umpire, and bullpen sources must return neutral defaults when an optional context table is unavailable; they must not leak future state.
- Exact training and inference feature sets must match the artifact manifest.

## Required artifact evidence

Before interpreting a run, inspect:

- `run_config.json`
- `training_metadata.json`
- `feature_manifest.json` or `model_manifest.json`
- resolved lifecycle configuration
- exact sweep/audit identity

A selector output alone is not an ablation result. Family claims require controlled include/exclude evidence on the exact artifact.

## Related documentation

- [MLB processing pipeline](mlb_processing_pipeline_documentation.md)
- [MLB lifecycle guide](development_docs/mlb_model_lifecycle_usage_guide.md)
- [Lifecycle feature families](../configs/mlb/examples/FEATURE_FAMILIES.md)
- [Model pipeline runbook](model_pipeline_runbook.md)
