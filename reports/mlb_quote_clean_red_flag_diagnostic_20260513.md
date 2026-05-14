# MLB Quote-Clean Red-Flag Diagnostic — 2026 Static vs Hook

Date: 2026-05-13

## Bottom line

Do not treat the quote-clean 2026 April results as validation.

The quote-clean implementation is useful instrumentation, but the resulting +13% to +16% flat ROI and very low drawdowns are still too smooth/hot for a promotion-grade conclusion. The diagnostics support Chase's concern: line availability and snapshot timing are now first-class confounders, not solved problems.

## What changed after the red flag

After Chase flagged that quote-clean should not dramatically improve ROI, we stopped promotion-style interpretation and ran diagnostics only.

Already-launched outputs from before the stop were kept only as evidence:

- Static quote-clean fixed config
- Hook quote-clean fixed config
- Paired/overlap flat-stake analysis

No shadow-test or promotion recommendation follows from these results.

## Models / artifacts

Static baseline:

- `src/models/mlb/artifacts/mlb_run_20260513_111207`

Hook candidate:

- `src/models/mlb/artifacts/ip_ablation_hook_deep_start_l30/mlb_run_20260513_130657`
- This is not a one-feature-only model.
- It is the normal pitcher K model with the ablation variant isolating the single hook/leash candidate `team_starter_deep_start_rate_l30` versus the static/no-L30 baseline.

## Fixed config / window

- Window: `2026-04-13` through `2026-05-10`
- Config: `tau=0.75`, `z_max=0.25`, `max_weight=0.65`, `edge=0.02`
- Flat sanity: `$100` per bet
- Quote cutoff: `13:30 ET`

## Static vs hook under quote-clean, flat $100

| Arm | Bets | Hit rate | ROI | Profit | Sharpe | Max DD |
|---|---:|---:|---:|---:|---:|---:|
| Static | 252 | 58.80% | +13.69% | +$3,450.52 | 1.855 | 12.52% |
| Hook | 274 | 59.93% | +16.30% | +$4,466.81 | 2.204 | 4.87% |

Interpretation:

- Hook is only +2.61 pp ROI over static on flat staking in this quote-clean window.
- The absolute ROI is implausibly high for both arms.
- The hook's very low flat max drawdown remains a red flag, not a strength.

## Legacy/no-quote-clean under current code, flat $100

These were rerun after the red flag using the current code path with `--quote-clean` disabled, so the comparison isolates the quote-clean filter more cleanly than older sweep artifacts.

| Arm | Mode | Bets | Hit rate | ROI | Profit | Max DD |
|---|---|---:|---:|---:|---:|---:|
| Static | legacy/current-code | 246 | 59.10% | +16.62% | +$4,023 | 7.9% |
| Static | quote-clean | 252 | 58.80% | +13.69% | +$3,451 | 12.5% |
| Hook | legacy/current-code | 268 | 57.70% | +13.65% | +$3,618 | 6.5% |
| Hook | quote-clean | 274 | 59.93% | +16.30% | +$4,467 | 4.9% |

Interpretation:

- Static does not inflate under quote-clean in this current-code rerun; it gets worse by about 2.9 pp flat ROI.
- Hook improves by about 2.7 pp flat ROI.
- The original concern is still valid because both quote-clean arms remain unrealistically high/smooth, but the current-code apples-to-apples result is more nuanced than “quote-clean inflated both equally.”

## Dropout diagnostics

Both arms generated 752 predictions.

Quote-clean rows:

- Static: 565
- Hook: 565

Dropped predictions:

- Static: 187 / 752 = 24.87%
- Hook: 187 / 752 = 24.87%
- Dropped-set overlap: 187 / 187, Jaccard = 1.0

Interpretation:

- Static and hook are evaluated on the same quote-clean sample, so sample mismatch between arms is not causing the relative hook-vs-static difference.
- But 24.87% prediction dropout is still a major methodology problem.
- The dropped set is selected by market/line availability before 13:30 ET, not by baseball randomness.

Legacy fallback on the 187 dropped predictions:

| Arm | Dropped predictions with legacy fallback rows | Flat bets | Flat ROI | Kelly bets | Kelly ROI |
|---|---:|---:|---:|---:|---:|
| Static | 4 | 3 | -47.06% | 3 | -100.00% |
| Hook | 4 | 2 | -20.59% | 2 | -76.94% |

Interpretation:

- Only 4 of the 187 dropped prediction keys had even a legacy fallback row in the current line table.
- The tiny fallback sample lost badly; too small for inference, but directionally supports the concern that line availability may filter out bad/hard-to-price spots.
- This is not proof of leakage, but it is enough to block validation claims.

## Side splits

Quote-clean flat $100 side split:

| Arm | Side | Bets | Hit rate | ROI | Profit |
|---|---|---:|---:|---:|---:|
| Static | Under | 94 | 67.02% | +29.10% | +$2,735.48 |
| Static | Over | 158 | 53.85% | +4.53% | +$715.04 |
| Hook | Under | 121 | 64.46% | +25.85% | +$3,127.89 |
| Hook | Over | 153 | 56.29% | +8.75% | +$1,338.92 |

Interpretation:

- The huge Under edge is not hook-specific; static also has a very large Under edge.
- The baseline model/window is already strongly Under-profitable.
- Hook's incremental contribution is not “it discovered all the Under edge.”
- Hook appears to add more Over improvement than Under improvement versus static in this window, but the whole window is hot and confounded.

## Paired / overlap analysis, quote-clean flat $100

Exact bet overlap is same date/player/game/stat/side/line/bookmaker/odds.

| Bucket | Bets | ROI | Profit | Hit rate |
|---|---:|---:|---:|---:|
| Exact overlap | 212 | +20.92% | +$4,435.34 | 62.56% |
| Static-only exact | 40 | -24.62% | -$984.82 | 38.46% |
| Hook-only exact | 62 | +0.51% | +$31.46 | 50.82% |

Counts:

- Static bets: 252
- Hook bets: 274
- Exact overlap bets: 212
- Static-only bets: 40
- Hook-only bets: 62
- Common player-games: 213
- Static unique player-games: 39
- Hook unique player-games: 61

Interpretation:

- Overlap bets are identical and highly profitable; this is baseline/window signal, not hook signal.
- Hook’s main relative win in this window is avoiding bad static-only bets, not making hugely profitable hook-only bets.
- Hook-only marginal selections are approximately breakeven flat (+0.51% ROI), not a strong standalone signal.
- This weakens the causal “hook feature creates new edge” story.

## Snapshot timing diagnostics

`mlb_game_schedule` has `game_time_utc`, so snapshot-to-game-start deltas were computed after the initial diagnostic missed that column.

Quote-clean flat $100 ROI by selected snapshot time to game start:

| Arm | Bucket | Bets | ROI | Hit rate | Profit |
|---|---|---:|---:|---:|---:|
| Static | after start unexpected | 1 | -100.00% | 0.00% | -$100.00 |
| Static | 0–1h to start | 34 | +28.81% | 64.71% | +$979.39 |
| Static | 1–3h to start | 39 | -4.98% | 48.72% | -$194.39 |
| Static | 3–6h to start | 82 | +21.93% | 63.41% | +$1,798.58 |
| Static | 6–12h to start | 96 | +10.07% | 57.45% | +$966.95 |
| Hook | after start unexpected | 1 | -100.00% | 0.00% | -$100.00 |
| Hook | 0–1h to start | 36 | +27.61% | 63.89% | +$994.09 |
| Hook | 1–3h to start | 39 | +6.08% | 53.85% | +$236.99 |
| Hook | 3–6h to start | 88 | +21.62% | 63.22% | +$1,902.81 |
| Hook | 6–12h to start | 110 | +13.03% | 58.72% | +$1,432.92 |

Selected snapshot time relative to 13:30 cutoff:

- Most bets came from 13:00–13:30 ET snapshots:
  - Static: 188 bets, +14.28% ROI
  - Hook: 207 bets, +17.35% ROI
- Older 08:00–10:00 ET snapshots were weak/negative:
  - Static: 16 bets, -15.50% ROI
  - Hook: 17 bets, +1.80% ROI

Interpretation:

- The huge ROI is not only from stale early-morning lines; most profits come from snapshots close to the 13:30 cutoff.
- However, there is one impossible/invalid `after_start_unexpected` bet in each arm. That must be inspected before trusting the quote-clean implementation.
- Snapshot timing does not fully explain the inflated/smooth equity curve.

## Current root-cause hypotheses

1. Market/window effect:
   - April 2026 pitcher K under/edge behavior may be a hot structural window.
   - Static overlap is already extremely profitable.

2. Quote availability selection:
   - 25% of predictions disappear due to no quote-clean line by cutoff.
   - The dropped set is identical across arms, but may still be an easier/priced subset.

3. Edge-threshold/sample interaction:
   - Quote-clean changes selected lines/odds enough to shift which bets clear `edge=0.02`.
   - The feature’s relative gain may be more about selection and avoiding bad static-only bets than new profitable hook-only bets.

4. Potential timestamp/timezone issue:
   - One selected bet appears after game start in both arms.
   - Needs row-level inspection.

5. Not proven but still possible:
   - leakage or calibration/test-window issue.
   - Current evidence does not isolate leakage, but the smooth drawdown and high overlap ROI require more checks.

## Files written

- `reports/mlb_quote_clean_red_flag_diagnostics_20260513.json`
- `reports/mlb_quote_clean_dropped_predictions_20260513.csv`
- `reports/mlb_quote_clean_snapshot_delta_bets_20260513.csv`
- `reports/mlb_quote_clean_snapshot_cutoff_bucket_summary_20260513.json`
- `reports/mlb_quote_clean_snapshot_game_start_delta_summary_20260513.json`
- `reports/mlb_quote_clean_snapshot_game_start_delta_bets_20260513.csv`
- `reports/mlb_quote_clean_2026_static_vs_hook_overlap_20260513.csv`
- `reports/mlb_quote_clean_2026_static_vs_hook_overlap_summary_20260513.json`

## Recommended next investigation before any validation claim

1. Inspect the single after-start selected quote row and determine if it is timezone parsing, game-time data quality, or a real post-start quote.
2. Compare quote-clean vs legacy selected lines on exact overlapping candidate rows to see whether quote-clean systematically chooses softer prices or different lines.
3. Run the same quote-clean fixed-config diagnostics on the independent 2025 validation window.
4. Run a date-block bootstrap on flat-stake daily P&L, but only after timestamp and quote-availability concerns are resolved.
5. Add dropout accounting and snapshot-to-start delta summaries as required outputs of any future quote-clean backtest.

## Decision

No promotion, no shadow approval, no “hook validated” claim.

Current status: quote-clean path is instrumented, but the 2026 April result remains suspicious and requires root-cause investigation.
