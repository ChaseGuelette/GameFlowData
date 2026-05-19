# MLB Backtest + CLV Operator Runbook

> Current as of 2026-05-18 after quote-clean artifact, dense CLV snapshot, linking, and decision-policy changes.

## Purpose

This runbook explains how to run the current MLB `batter_hits` backtest/CLV workflow after the recent changes.

Primary goals:

- Generate future-grade `bets.csv` artifacts with exact selected quote metadata.
- Use the dense CLV snapshot table for CLV/timing validation.
- Avoid relying on old weak `13:30 ET` assumptions except for legacy comparisons.
- Support fixed-time, game-relative, and slate-based decision policies.
- Keep CLV, diagnosis, and dropout audit interpretation separated.

## Critical invariants

- Do not use Gaussian CDF probabilities. Probabilities must come from empirical CDF/sample logic.
- Do not deploy global conformal recalibration offsets.
- Do not use post-decision or post-commence odds.
- Treat `selected_decision_time` as the simulated/model job run time.
- Treat `selected_snapshot_time` as the actual selected quote snapshot at/before that decision time.
- Promotion/feature expansion requires both valid backtest artifacts and decision-grade CLV evidence.

## Important files

### Backtesting

```text
src/backtesting/mlb/run_mlb_sweep.py
src/backtesting/bet_simulator.py
src/backtesting/mlb/line_selection.py
```

### CLV / timing / diagnosis

```text
scripts/analyze_mlb_batter_hits_clv.py
scripts/diagnose_mlb_clv_failure_modes.py
scripts/link_mlb_clv_snapshots.py
scripts/scrape_mlb_clv_snapshots.py
scripts/audit_mlb_quote_clean_dropout.py
```

### DB migrations

```text
database/migrations/030_mlb_clv_snapshot_table.sql
database/migrations/031_mlb_clv_snapshot_linking.sql
```

### Combined wrapper

```text
scripts/run_mlb_quote_clean_audit_suite.py
```

## Current status of wrapper tooling

A combined post-sweep wrapper for:

```text
optional dense linker -> dropout/timing audit -> CLV analysis -> CLV failure diagnosis -> summary manifest
```

has been implemented:

```text
scripts/run_mlb_quote_clean_audit_suite.py
```

It runs the individual scripts in this order:

1. `scripts/link_mlb_clv_snapshots.py`
   - only when `--run-linker` is supplied
2. `scripts/audit_mlb_quote_clean_dropout.py`
   - unless `--skip-dropout-audit` is supplied
3. `scripts/analyze_mlb_batter_hits_clv.py`
4. `scripts/diagnose_mlb_clv_failure_modes.py`

It intentionally does **not** run `src/backtesting/mlb/run_mlb_sweep.py`. Keep the expensive backtest sweep explicit so decision-time policy comparisons stay preregistered and reviewable.

## Dense CLV snapshot table

The dense table is:

```text
public.mlb_player_props_clv_snapshots
```

It stores Odds API historical/player-prop snapshots for CLV/timing validation without bloating `mlb_raw_player_props`.

Key columns from migration 030:

```text
api_game_id
odds_api_event_id
player_id
api_player_name
bookmaker
market_key
outcome_label
line
odds_american
commence_time
home_team
away_team
snapshot_time
requested_snapshot_time
market_last_update
bookmaker_last_update
scrape_reason
target_offset_minutes
```

Migration 031 adds linking columns:

```text
game_id
linked_player_name
game_link_method
player_link_method
linked_at
```

For future CLV work, prefer this dense table over `mlb_raw_player_props` once it is linked.

Reason:

- Dense table was built specifically for CLV/timing windows.
- It includes T-60/T-30/T-15/T-5 and fixed decision grids.
- It avoids growing the raw props ingestion table.
- It is easier to reason about coverage/failure modes.

Exception:

- Use `mlb_raw_player_props` only for legacy comparison or if dense table linking/coverage is known incomplete.

## Step 0: Let the dense scrape finish

The dense scrape command used in the other terminal was:

```powershell
.\venv\Scripts\python.exe scripts\scrape_mlb_clv_snapshots.py --start-date 2026-04-13 --end-date 2026-05-17 --markets batter_hits --request-sleep-seconds 0.20
```

Before CLV analysis, confirm the scrape has inserted rows and is not still running.

## Step 1: Apply migration 031

Apply:

```text
database/migrations/031_mlb_clv_snapshot_linking.sql
```

This adds `game_id` and linking audit columns to `public.mlb_player_props_clv_snapshots`.

Do not run destructive updates manually. The linker script is idempotent/conservative.

## Step 2: Link dense CLV snapshots

The linker is safe-by-default: preflight mode does not write. Execution requires `--execute` and an explicit `--max-batches`.

Preflight from repo root:

```powershell
.\venv\Scripts\python.exe scripts\link_mlb_clv_snapshots.py --mode preflight --report backtest_results\audits\mlb_clv_snapshot_link_report.md
```

First real smoke test for game linkage only:

```powershell
.\venv\Scripts\python.exe scripts\link_mlb_clv_snapshots.py --execute --max-batches 1 --batch-size 500 --only-games --skip-report
```

If the smoke test updates nonzero rows at acceptable speed, continue in bounded chunks and resume with the last logged `max_id`:

```powershell
.\venv\Scripts\python.exe scripts\link_mlb_clv_snapshots.py --execute --max-batches 100 --batch-size 1000 --only-games --start-id <last_max_id> --skip-report
```

After game linkage is complete, smoke-test player linkage separately:

```powershell
.\venv\Scripts\python.exe scripts\link_mlb_clv_snapshots.py --execute --max-batches 1 --batch-size 500 --only-players --skip-report
```

Expected report path when report mode is used:

```text
backtest_results/audits/mlb_clv_snapshot_link_report.md
```

Review coverage before trusting CLV results:

```text
rows
game_linked
player_linked
fully_linked
top unlinked player names
```

If `player_linked` or `fully_linked` is poor, inspect the top unlinked names before trusting CLV results.

The linker is conservative:

- bounded id-window batches
- exact/normalized name matching only
- ambiguous players remain NULL
- unlinked names are reported instead of guessed

## Step 3: Choose a decision policy for the backtest sweep

The canonical sweep is:

```text
src/backtesting/mlb/run_mlb_sweep.py
```

Quote-clean mode now supports:

```text
--quote-clean
--quote-cutoff-time-et HH:MM
--quote-decision-policy fixed_et|skip_early_fixed_et|relative_to_commence|slate_or_tminus
--quote-relative-minutes N
```

### Policy: `fixed_et`

Legacy/comparison mode.

One decision time per day:

```text
--quote-cutoff-time-et 13:30 --quote-decision-policy fixed_et
```

Use for apples-to-apples comparison with old sweeps.

Risk:

- early games can make the fixed cutoff unrealistic unless separately skipped.

### Policy: `skip_early_fixed_et`

Fixed daily time, but skips games that already started by that time.

Use when testing a realistic single daily job run.

Example:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py `
  --start 2026-04-13 `
  --end 2026-05-17 `
  --stats batter_hits `
  --quote-clean `
  --quote-cutoff-time-et 13:30 `
  --quote-decision-policy skip_early_fixed_et
```

### Policy: `relative_to_commence`

Per-game decision time:

```text
T-N minutes before each game
```

Use for CLV/timing validation and fair comparison across early/night games.

T-60 example:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py `
  --start 2026-04-13 `
  --end 2026-05-17 `
  --stats batter_hits `
  --quote-clean `
  --quote-decision-policy relative_to_commence `
  --quote-relative-minutes 60
```

T-30 example:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py `
  --start 2026-04-13 `
  --end 2026-05-17 `
  --stats batter_hits `
  --quote-clean `
  --quote-decision-policy relative_to_commence `
  --quote-relative-minutes 30
```

### Policy: `slate_or_tminus`

Combined slate + game-relative fallback.

Current policy:

```text
Morning slate: 09:30 ET for games before 15:00 ET
Main slate:    13:30 ET for games 15:00-19:00 ET
Night slate:   17:30 ET for games after 19:00 ET
Fallback:      if slate time is at/after commence, use T-minus N minutes
```

Recommended first future-realistic policy:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py `
  --start 2026-04-13 `
  --end 2026-05-17 `
  --stats batter_hits `
  --quote-clean `
  --quote-decision-policy slate_or_tminus `
  --quote-relative-minutes 60
```

## Step 4: Run preregistered multi-time sensitivity sweeps

Recommended preregistered decision policies/times:

```text
fixed_et 09:30
fixed_et 10:30
fixed_et 11:30
fixed_et 12:30
fixed_et 13:30
fixed_et 15:30
fixed_et 17:30
relative_to_commence T-60
relative_to_commence T-30
relative_to_commence T-15
slate_or_tminus T-60 fallback
```

Compare:

```text
bet counts
ROI
CLV
same-book coverage
+15/+30/+60 stability
early-game dropout
bookmaker concentration
```

Do not pick the highest-profit time after the fact without labeling it exploratory. For promotion decisions, use preregistered policy comparisons.

## Step 5: Verify new `bets.csv` columns

After a sweep, inspect the config output dir:

```text
backtest_results/<sweep>/<config>/bets.csv
```

Future-grade `bets.csv` should include:

```text
selected_decision_time
selected_snapshot_time
selected_market_last_update
selected_bookmaker_last_update
selected_bookmaker
selected_line
selected_price
selected_side
over_snapshot_time
under_snapshot_time
over_bookmaker
under_bookmaker
over_market_last_update
under_market_last_update
over_bookmaker_last_update
under_bookmaker_last_update
```

Meaning:

```text
selected_decision_time = simulated/model job run time
selected_snapshot_time = selected quote snapshot at/before decision time
```

For new sweeps, do not rely on `--assume-bet-time-et` unless intentionally debugging old artifacts.

## Step 6: Run dropout / timing artifact audit

Run the dropout audit script on each sweep/config as before.

Example shape:

```powershell
.\venv\Scripts\python.exe scripts\audit_mlb_quote_clean_dropout.py `
  --sweep-dir backtest_results\<sweep_dir> `
  --output-dir backtest_results\audits\<audit_dir>
```

This audit is about whether the backtest artifact itself is temporally/denominator valid.

It is separate from CLV.

## Step 7: Run CLV analysis using dense table

Preferred future command:

```powershell
.\venv\Scripts\python.exe scripts\analyze_mlb_batter_hits_clv.py `
  --bets-csv backtest_results\<sweep_dir>\<config_dir>\bets.csv `
  --output-dir backtest_results\audits\clv_<label> `
  --snapshots-table mlb_player_props_clv_snapshots
```

Legacy/raw-table comparison:

```powershell
.\venv\Scripts\python.exe scripts\analyze_mlb_batter_hits_clv.py `
  --bets-csv backtest_results\<sweep_dir>\<config_dir>\bets.csv `
  --output-dir backtest_results\audits\clv_<label>_raw `
  --snapshots-table mlb_raw_player_props
```

Old artifact fallback only:

```powershell
--assume-bet-time-et 13:30
```

Do not use that for new promotion-grade artifacts unless intentionally testing legacy assumptions.

CLV output files include:

```text
clv_matches.csv
clv_summary.csv
clv_unmatched_reasons.csv
clv_timing_stability.csv
clv_by_bookmaker.csv
clv_by_edge_bin.csv
clv_by_plus_odds_band.csv
phase1b_decision.csv
phase1b_clv_summary.md
raw_snapshots_used.csv
```

## Step 8: Run CLV failure-mode diagnosis

```powershell
.\venv\Scripts\python.exe scripts\diagnose_mlb_clv_failure_modes.py `
  --clv-output-dir backtest_results\audits\clv_<label> `
  --output-dir backtest_results\audits\clv_diag_<label>
```

Review:

```text
decision_label
failure_modes
reasons
overall.mean_clv_implied_prob
overall.mean_clv_ci_low
overall.edge_clv_spearman
overall.edge_clv_ci_low
timing_horizons_present
timing_horizon_coverage_pct
top_unmatched_reasons
bookmaker concentration
same-book share
```

## Decision gates

### Measurement-quality gate

Before judging the model, check measurement quality:

```text
unmatched_rate <= 20%
same_book_share >= 60%
+15 coverage present/useful
+30 coverage present/useful
+60 coverage present/useful
bookmaker top concentration explained or reduced
```

If this fails, the decision is data/timing unresolved, not model failure.

### Model-quality gate

After measurement quality passes:

```text
mean CLV implied probability >= +1.5 pp
mean CLV CI low > 0
Spearman(edge, CLV) CI low > 0
n_scored >= 200
n_blocks >= 25
```

If this fails after measurement passes, then the model/policy is not validated.

## Current recommended workflow

1. Finish dense scrape/backfill.
2. Apply migration 031.
3. Run `scripts/link_mlb_clv_snapshots.py`.
4. Review linking report.
5. Keep forward live close snapshots flowing through `src/orchestration/mlb_lines_job.py --dense-clv-close`:
   - live scraper writes `close_t_minus_30` rows into `mlb_player_props_clv_snapshots`
   - the job records the dense table `MAX(id)` before scraping
   - after scraping, it links only newly inserted rows with bounded dense game/player linkers
   - Railway scheduler calls this via `run_mlb_pregame_30min_props` every 10 minutes from 10 AM-11 PM ET, filtered to games about 30±5 minutes from commence
6. Run `slate_or_tminus` backtest sweep first.
7. Run `relative_to_commence` T-60 and T-30 comparison sweeps.
8. Run dropout audit.
9. Run CLV against `mlb_player_props_clv_snapshots`.
10. Run CLV diagnosis.
11. Compare policies on both ROI and CLV/timing stability.

## Should future backtests always use the dense table?

For CLV/timing validation: yes, prefer the dense table.

For quote selection inside the backtest: currently no, unless/until line selection is explicitly changed to source from the dense table.

Current division:

```text
Backtest selection source: existing quote-clean line selection path, currently backed by mlb_raw_player_props.
CLV/timing source: dense mlb_player_props_clv_snapshots table.
```

Future desirable improvement:

```text
Add a line-source mode so backtest quote selection can also use mlb_player_props_clv_snapshots for dates/windows where dense data is available.
```

This now exists via:

```text
src/backtesting/mlb/run_mlb_sweep.py --line-source mlb_player_props_clv_snapshots
scripts/audit_mlb_quote_clean_dropout.py --line-source mlb_player_props_clv_snapshots
```

Use the dense table as both line-selection source and CLV source once migration 031 has been applied and `scripts/link_mlb_clv_snapshots.py` reports acceptable `fully_linked` coverage.

## Combined wrapper

Script:

```text
scripts/run_mlb_quote_clean_audit_suite.py
```

It accepts a completed sweep output directory and runs the post-sweep validation lane:

1. optional dense snapshot linker with `--run-linker`
2. dropout/timing audit
3. CLV analysis with `--snapshots-table mlb_player_props_clv_snapshots`
4. CLV failure diagnosis
5. summary markdown/CSV/JSON manifest

It does not launch the backtest sweep. Run `src/backtesting/mlb/run_mlb_sweep.py` first, then point this wrapper at the resulting `backtest_results/<sweep_dir>`.

Example:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py `
  --sweep-output-dir backtest_results\<sweep_dir> `
  --output-dir backtest_results\audits\suite_<label> `
  --model-dir src\models\mlb\artifacts `
  --start 2026-04-13 `
  --end 2026-05-17 `
  --stats batter_hits `
  --quote-decision-policy slate_or_tminus `
  --quote-relative-minutes 60 `
  --line-source mlb_player_props_clv_snapshots `
  --snapshots-table mlb_player_props_clv_snapshots
```

Recommended wrapper defaults:

```text
snapshots_table = mlb_player_props_clv_snapshots
require_selected_decision_time = true
require_selected_snapshot_time = true
fail_if_dense_link_rate_low = true
```

## Quick command checklist

```powershell
# 1. Link dense snapshots: start with a bounded smoke test, then continue by --start-id after reviewing output
.\venv\Scripts\python.exe scripts\link_mlb_clv_snapshots.py --execute --max-batches 1 --batch-size 500 --only-games --skip-report

# 2. Run realistic slate/tminus sweep
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py `
  --start 2026-04-13 `
  --end 2026-05-17 `
  --stats batter_hits `
  --quote-clean `
  --quote-decision-policy slate_or_tminus `
  --quote-relative-minutes 60

# 3. Run CLV against dense table
.\venv\Scripts\python.exe scripts\analyze_mlb_batter_hits_clv.py `
  --bets-csv backtest_results\<sweep_dir>\<config_dir>\bets.csv `
  --output-dir backtest_results\audits\clv_<label> `
  --snapshots-table mlb_player_props_clv_snapshots

# 4. Diagnose CLV failure modes
.\venv\Scripts\python.exe scripts\diagnose_mlb_clv_failure_modes.py `
  --clv-output-dir backtest_results\audits\clv_<label> `
  --output-dir backtest_results\audits\clv_diag_<label>
```
