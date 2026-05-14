# MLB Pitcher K Hook Ablation Hardening Report

> Plan: `.hermes/plans/mlb-pitcher-k-hook-ablation-hardening-2026-05-13.md`

## Current status

- Phase 0: approved and encoded in the plan file.
- Phase 1: quote-clean backtest path implemented; smoke validated; 2026 hook fixed-config quote-clean runs completed.
- Shadow testing: not started and still requires a separate later approval gate.

## Hook model under discussion

The hook model in this lane is the stripped-down single-feature MLB pitcher strikeout ablation:

- Artifact: `src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657`
- Feature candidate: `team_starter_deep_start_rate_l30`
- Meaning: pitcher-side / own-team recent deep-start leash tendency, computed from the starter's own team prior games only.
- Not this: the rejected full predicted-IP feature bundle.
- Not this: the rejected all-L30-hook bundle.

The clean no-leakage static baseline remains:

- Artifact: `src/models/mlb/artifacts/mlb_run_20260513_111207`

## Phase 1 quote-clean production rule

The production-equivalent quote-clean backtest rule implemented for `run_mlb_sweep.py --quote-clean` is:

1. Use a fixed ET historical inference cutoff per slate/date, currently `--quote-cutoff-time-et 13:30`.
2. Keep only `mlb_raw_player_props.snapshot_time <= cutoff`.
3. Exclude non-executable books/platforms:
   - `novig`
   - `betonlineag`
   - `dabble_us_dfs`
   - `betr_us_dfs`
   - `pick6`
   - `prizepicks`
   - `underdog`
4. For each `player_id/game_id/market_key/bookmaker/line/outcome_label`, keep the latest snapshot at/before cutoff.
5. Require paired Over and Under odds at the same bookmaker+line.
6. Select the lowest-vig line per `player_id/game_id/market_key`.
7. Preserve quote audit columns in predictions:
   - `selected_snapshot_time`
   - `over_snapshot_time`
   - `under_snapshot_time`

## Implementation status

Implemented in:

- `src/backtesting/mlb/run_mlb_sweep.py`

Added CLI flags:

```text
--quote-clean
--quote-cutoff-time-et HH:MM
```

Default behavior remains legacy unless `--quote-clean` is passed.

Important fix included:

- The sweep's sharpest-line selection previously keyed by `player_id/market_key` and ignored `game_id` in both standard and fast precompute paths.
- Production selects lines per `player_id/game_id/market_key`.
- This was corrected to avoid cross-game contamination, especially for doubleheaders / same-player multi-game cases.

Validation passed:

```text
python3 -m py_compile src/backtesting/mlb/run_mlb_sweep.py
```

PowerShell/local-DB execution note:

- WSL `localhost` did not reach the usable local GameFlow Postgres context.
- Native Windows PowerShell from `C:\Users\Chase\Projects\GameFlowData` with `.\venv\Scripts\python.exe` successfully reaches `gameflow_local` on Windows localhost.

## Smoke validation

Smoke command shape:

```text
.\venv\Scripts\python.exe src/backtesting/mlb/run_mlb_sweep.py --local --start 2026-04-13 --end 2026-04-13 --stats pitcher_strikeouts --model-dir src/models/mlb/artifacts/mlb_run_20260513_111207 --tau 0.75 --z-max 0.25 --max-weight 0.65 --edge 0.02 --kelly 0.125 --quote-clean --quote-cutoff-time-et 13:30 --output-dir backtest_results/quote_clean_smoke_static_20260513
```

Smoke result:

- Date: `2026-04-13`
- Predictions generated: 20
- Quote-clean precomputed rows: 14
- Bets: 3
- ROI: +97.86%, not interpreted as evidence because this is a one-day plumbing smoke.
- Output directory: `backtest_results/quote_clean_smoke_static_20260513`

Audit verification:

- `predictions.csv` has `selected_snapshot_time`, `over_snapshot_time`, and `under_snapshot_time`.
- All 14 smoke prediction rows had non-null values for those audit columns.

## 2026 hook fixed-config quote-clean runs

Window:

- Backtest: `2026-04-13` through `2026-05-10`
- Model artifact: `src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657`
- Config: `tau=0.75`, `z_max=0.25`, `max_weight=0.65`, `edge=0.02`
- Quote cutoff: `13:30 ET`
- Quote mode: `--quote-clean`

### Kelly sizing

Output directory:

- `backtest_results/quote_clean_2026_hook_deep_start_l30_fixed_20260513`

Results:

| Metric | Value |
|---|---:|
| Predictions | 752 |
| Quote-clean rows | 565 |
| Bets | 274 |
| Wins | 163 |
| Losses | 109 |
| Hit rate | 59.93% |
| ROI | +23.22% |
| Profit | +$11,778.14 |
| Staked | $50,727.01 |
| Sharpe | 2.052 |
| Max drawdown | 7.68% |

Side split:

| Side | Bets | Hit rate | ROI | Profit |
|---|---:|---:|---:|---:|
| Under | 121 | 64.46% | +26.33% | +$4,494.54 |
| Over | 153 | 55.56% | +21.38% | +$7,283.60 |

Audit verification:

- `predictions.csv` rows: 565
- `bets.csv` rows: 274
- All 565 prediction rows had non-null `selected_snapshot_time`, `over_snapshot_time`, and `under_snapshot_time`.

### Flat $100 sizing

Output directory:

- `backtest_results/quote_clean_2026_hook_deep_start_l30_fixed_flat100_20260513`

Results:

| Metric | Value |
|---|---:|
| Predictions | 752 |
| Quote-clean rows | 565 |
| Bets | 274 |
| Wins | 163 |
| Losses | 109 |
| Hit rate | 59.93% |
| ROI | +16.42% |
| Profit | +$4,466.81 |
| Staked | $27,200.00 |
| Sharpe | 2.204 |
| Max drawdown | 4.87% |

Side split:

| Side | Bets | Hit rate | ROI | Profit |
|---|---:|---:|---:|---:|
| Under | 121 | 64.46% | +25.85% | +$3,127.89 |
| Over | 153 | 55.56% | +8.75% | +$1,338.92 |

Audit verification:

- `predictions.csv` rows: 565
- `bets.csv` rows: 274
- All 565 prediction rows had non-null `selected_snapshot_time`, `over_snapshot_time`, and `under_snapshot_time`.

## Interpretation caveats

- These are hook-only quote-clean runs, not a full decision yet.
- The point estimates are still likely hot; do not interpret +16–23% ROI as expected long-run live ROI.
- Flat-stake sanity is directionally positive, which is better than a Kelly-only artifact.
- The next required comparison is static vs hook under the same quote-clean rule, same fixed config, same window.
- Promotion should still depend on relative hook-vs-static improvement, paired/overlap analysis, CLV/shadow design, and bootstrap uncertainty.

## Next validation needed

1. Run the matching static quote-clean fixed-config reruns for the same `2026-04-13` → `2026-05-10` window.
2. Run paired/overlap analysis:
   - overlap bets where both models agree,
   - overlap games where they differ,
   - unique-to-hook marginal selections,
   - unique-to-static marginal selections.
3. Repeat on the independent 2025 validation artifact/window if Phase 1 remains promising.
4. Estimate block-bootstrap uncertainty before any shadow launch.
5. Do not start shadow testing without separate explicit approval.
