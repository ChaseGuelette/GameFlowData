# NBA pretraining hardening

## Status and launch restriction

Chronological training gates, date-group inner splits, historical training CLI integration, and cumulative initial-population acquisition limits are implemented. Offline checks establish mechanics only, not source authenticity or model readiness. No source-ready production policy or baseline YAML was created.

The coordinator connected the real sweep parser, `src/backtesting/nba/run_nba_sweep.py`, and verified actual replay/sweep/audit/lifecycle execution with controlled external boundaries. Real source/cohort acceptance and remaining owner policy choices are still required before launch; a green mechanics check is not training readiness.

Delivery and evidence: [implementation receipt](../operations/nba-pretraining-hardening-implementation-2026-11-03.md).

## Training contract

- Intended fitting seasons remain `22023`/`22024`, calibration `22025`, and current `22026` stays a separate evaluation route. No numeric model defaults changed.
- CLI preflight and direct full/partial entrypoints reject malformed/duplicate season tokens and train/calibration intersection. Literal tokens are never normalized.
- After loading, nonempty frames must contain valid `game_date`, `game_id`, `player_id`, and `season_id`; seasons must belong to the request. Duplicate player-game keys, inconsistent game identities, cross-frame games, and non-strict chronology fail. Frames sort by date/game/player with pandas attrs retained.
- Early stopping, shared/per-quantile tuning, and feature ranking/count CV split whole dates. Too few dates fail rather than fall back to row splitting. Shared callers without optional date metadata retain their previous behavior.
- `--hyperparams-path` selects fixed mode; it cannot accompany `--tune`. Both paths operate on fitting data only.
- Selected population counts, ranges, seasons, and a canonical sorted date/game/player/season key digest are recorded in `run_config.json.execution` and `artifact_manifest.json.data_provenance.training.execution`. The finalizer rechecks population identities and attached input bytes.
- Historical partial retraining requires a manifested parent with the exact requested fitting/calibration seasons, matching acquisition limits, compatible historical inputs and chronology inside the declared windows. Legacy, foreign-population or unprovable parent history is rejected before model loading. Every requested fitting season must appear in the actual loaded population. This intentionally does not certify old parents retroactively.

## Historical CLI inputs

`src.models.train_pipeline`, `src.backtesting.run_backtest`, and `src.backtesting.nba.run_nba_sweep` accept:

- `--local --historical-policy PATH`
- Optional `--historical-source-metadata PATH`
- All four operational limits: `--max-dates`, `--max-games`, `--max-population-rows`, `--population-timeout-ms`

Supply explicit positive integers; partial configurations, bools, zero, negative values, and limits without a policy are rejected. No default budget or policy is inferred.

For training, `--cal-end-date` must equal the policy's exclusive calibration cutoff; requested seasons must belong to its cohort. The policy does not declare separate role-to-season lists: explicit CLI seasons are intersected with policy role windows in population SQL, and the resulting identities/chronology are validated before fitting. Fitting uses inclusive `train_start`/`train_end`; calibration uses inclusive start and exclusive end. There is no posthoc historical cutoff filter to hide out-of-window rows.

Training `--dry-run`/`--preflight` validates files and prints a plan without engine creation, model creation/loading, or output directory creation. It does not query data or certify readiness. Historical `--calibrate-only` is explicitly unsupported and rejected before side effects; historical full/partial training does use the correctly scoped holdout for existing diagnostics.

Lifecycle `historical` config contains `policy`, optional `source_metadata`, and `acquisition_limits` with the four snake_case limit keys. Train mode accepts these inputs; commands capture exact attachment paths and budgets. Config, attachment bytes, budgets, and relevant source identities participate in stage reuse. A trained artifact must match requested historical inputs, budgets, seasons, cutoff, and its observed chronology before evaluation. Lifecycle dry-run writes planning documents only and launches no child.

## Budget semantics

One FeatureStore owns one cumulative budget. Full/partial training shares it across all fitting seasons and calibration. Replay shares it across the requested population; a sweep acquires shared inputs once, not once per cell.

- Initial SQL is restricted to selected seasons and role/date windows, deterministically ordered by date/game/player, and fetches at most remaining row allowance plus one.
- The extra row detects overflow; it is never silently truncated. Row usage counts all queried rows, including repeated acquisitions. Games/dates count distinct unions across the store lifetime.
- Row/game/date overflows fail before native source acquisition and feature assembly. Date ranges also reject calendar spans exceeding `max_dates` before population querying.
- Initial population queries use `set_config('statement_timeout', ..., true)` on their actual connection inside an explicitly rolled-back transaction. No pooled session setting or committed DB mutation is intended. Timeout/errors propagate, never become partial-success rows.
- Controlled historical fixture mode can still omit budgets; new native execution cannot. Old saved v2 evidence remains readable without retroactively claiming bounded acquisition.

These limits bound initial population acquisition, not every downstream source query, quote expansion, process memory allocation, or total wall time. Connection/transaction behavior was tested with controlled boundaries, not a live database.

## Preserved boundaries

No changes to cold-start decisions, structural-zero rules, target/rolling thresholds, historical positions/trades, source eligibility/authenticity semantics, missingness, feature formulas, model objectives/search space, empirical CDF, quantile monotonicity, or numerical calibration ownership. Global conformal offsets remain diagnostic-only. No training, DB/network work, deployments, or promotion occurred during implementation.
