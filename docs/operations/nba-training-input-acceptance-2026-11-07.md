# NBA training-input acceptance — 2026-11-07

## Current result — direct recheck after cached replay

The coordinator reran the inspected checker directly in an isolated local read-only process, without a subagent, and independently compared repaired observation keys against the actual public injury consumer. **The bounded fit/calibration input check passes; holdout injury acceptance does not.** This is not a full feature-matrix build, training result, missingness-balance certification or promotion verdict.

Eligibility is unchanged: 46,249 fitting rows / 2,452 games; 23,898 calibration rows / 1,278 games; 1,962 initial-holdout rows / 104 games through November 4. All 3,834 eligible games have valid schedule-derived starts, with no missing starts or timing fallback.

| Season | Actual accepted injury items | Games with accepted evidence | Coverage |
|---|---:|---:|---:|
| `22023` fitting | 7,774 | 799 / 1,227 | 65.12% |
| `22024` fitting | 9,789 | 923 / 1,225 | 75.35% |
| `22025` calibration | **3,325** | **360 / 1,278** | **28.17%** |
| `22026` holdout through November 4 | 0 | 0 / 104 | 0% |

Exact `(injury_row_id, capture_id, game_id)` comparison matched all **3,325** repaired observations to the public consumer: zero missing keys, zero unexpected keys and zero after-cutoff items. The remaining calibration missingness is substantial and explicit; nonzero coverage alone does not establish representative coverage. Missing reports remain unknown, not healthy, and listed-player evidence is not complete-team-edition proof.

### Holdout needs a separate, narrow source repair

For **October 20–November 4, 2026**, canonical remote and local daily aggregate checks agree on **626 existing injury rows**, all lacking game IDs and retained source observations; three also lack player IDs. Existing rows begin October 27, so October 20–26 has no rows in this inspected table. This is not merely an unmirrored local update. Current starts already cover all 104 eligible holdout games.

The retained recovery cache has **15 unique attempted report URLs, all HTTP 403 failures**, and no parsed report available for this holdout replay. Consequently the calibration cache-only command cannot repair this window. Next: inspect/recover authentic historical report/capture evidence for only this holdout window, populate genuinely missing daily statuses where supported, link exact games/players, then mirror and repeat the public-consumer check. Do not repeat the three-season schedule repair, full historical crawler, or assume a current status was known before an earlier game's cutoff. Source repair is distinct from tuning on holdout outcomes.

All check transactions were verified read-only with 15-second statement and 3-second lock timeouts. No DB writes, external source requests, model loads, fitting or backtesting were performed. The original pre-replay evidence below is retained for comparison; its zero-calibration finding and 409-game pending-replay recommendation are superseded.

Current evidence:
- `logs/receipts/nba_training_input_acceptance_20261107/post_cached_replay/acceptance_results.json`
- `logs/receipts/nba_training_input_acceptance_20261107/post_cached_replay/executed_data_queries.sql`
- `logs/receipts/nba_training_input_acceptance_20261107/post_cached_replay/exact_repaired_key_check.json`
- `logs/receipts/nba_training_input_acceptance_20261107/post_cached_replay/holdout_remote_source.json` and `.sql`

## Before cached replay — superseded verdict

**Bounded cached injury replay is needed before the exploratory fixed-parameter baseline.**

The exact ordinary training-loader eligibility predicate has substantial nonempty fit and calibration populations, and the official schedule sync gives the actual training consumer a valid schedule-derived start for every eligible game. However, the actual public injury selector accepts **zero** calibration-season (`22025`) games and zero initial-holdout (`22026` through 2026-11-04) games. Schedule repair fixed cutoffs but did not attach publication evidence to rows skipped during injury recovery.

A bounded replay candidate set exists: **409 eligible `22025` games** were previously blocked by `game_start_unknown` and have `reports_seen > 0` in the existing recovery receipts. This is a candidate population, not a claimed repair count. Replay should be restricted to those cached cases, followed by this same acceptance check. Do not restart the full injury job.

## Measured eligibility and start coverage

These are SQL counts using the exact predicates in `src/models/features/nba/training_loader.py:490-499`; they are **not** a claim that the full feature pipeline was built successfully.

| Season | Role | Eligible player rows | Eligible games | Schedule starts used | Missing consumer starts |
|---|---:|---:|---:|---:|---:|
| `22023` | fit | 23,364 | 1,227 | 1,227 | 0 |
| `22024` | fit | 22,885 | 1,225 | 1,225 | 0 |
| `22025` | calibration | 23,898 | 1,278 | 1,278 | 0 |
| `22026` through 2026-11-04 | initial holdout | 1,962 | 104 | 104 | 0 |

Combined fit population: **46,249 player rows / 2,452 games**. Calibration population: **23,898 player rows / 1,278 games**.

The consumer start precedence in `training_loader.py:248-265` selected schedule starts for all 3,834 eligible games; timing fallback was used for zero, and no schedule-present game produced a missing consumer start.

## Actual public injury-selector evidence

`load_training_injury_rows(...)` was executed against the local mirror with the real public selector from `src/models/features/nba/injury_context.py:524-655`. The inspected raw population was bounded to **66,648 rows** across the four seasonal date sets.

| Season | Accepted items | Accepted games | Game coverage | Accepted team-games | Team-game coverage |
|---|---:|---:|---:|---:|---:|
| `22023` | 7,774 | 799 / 1,227 | 65.12% | 1,577 / 2,454 | 64.26% |
| `22024` | 9,789 | 923 / 1,225 | 75.35% | 1,838 / 2,450 | 75.02% |
| `22025` | 0 | 0 / 1,278 | 0.00% | 0 / 2,556 | 0.00% |
| `22026` through 2026-11-04 | 0 | 0 / 104 | 0.00% | 0 / 208 | 0.00% |

A deterministic 12-game sample (maximum three games per season) exercised the same public loader. Positive controls in `22023` and `22024` returned accepted capture IDs and pre-cutoff provider times. Prior `game_start_unknown` controls now had valid schedule starts but still returned no accepted injury items, confirming that schedule synchronization alone does not add missing publication evidence.

The ordinary loader intentionally keeps team/opponent absence aggregates unknown; accepted reports contain listed-player statuses, not proof of complete team editions. Missing rows remain unknown and must not be interpreted as healthy.

## Cached replay scope evidence

Eligible games previously blocked by `game_start_unknown` with `reports_seen > 0`:

- `22023`: 354
- `22024`: 203
- `22025`: **409**
- `22026` through 2026-11-04: 0

Total candidate games across inspected seasons: 966. For the immediate baseline gate, the `22025` set is decisive. The receipts do not establish that all 409 will become accepted, so the acceptance check must be rerun after a bounded cached replay.

## Guardrails and scope

- Target: local `gameflow_local` through `LOCAL_DATABASE_URL`.
- Verified read-only transaction setting: `transaction_read_only=on`.
- Verified timeouts: `statement_timeout=15s`, `lock_timeout=3s`.
- No database writes, external API requests, training, model loading, full feature-season build, calibration, or backtest were performed.
- Official schedule receipt: `logs/receipts/nba_official_schedule_fixer/historical_execute.json` (`selected=3959`, `local_verified=3959`, completed 2026-11-07T21:47:49Z).

## Evidence

- `logs/receipts/nba_training_input_acceptance_20261107/acceptance_results.json`
- `logs/receipts/nba_training_input_acceptance_20261107/queries.sql`
- `src/models/features/nba/training_loader.py`
- `src/models/features/nba/injury_context.py`
