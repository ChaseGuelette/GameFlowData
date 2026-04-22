> Part of [[Handoffs]]

**Date**: April 21, 2026 at 9:31 PM

## Summary

Implemented sample-size awareness for calibration Discord alerts. The calibration monitor was firing "Significant Drift" (critical/red) on ~44 bets from a 2-day-old playoff model, because tight thresholds (3% quantile gap, 3% ECE) plus tiny per-stat samples (~10 bets) caused random noise to trigger 15+ alerts. Now, with < 75 bets, thresholds are relaxed and severity is capped at "warning" — no more false critical alarms.

## What Was Done

- **`src/paper_trading/calibration_monitor.py`**:
  - Added `LOW_CONFIDENCE_THRESHOLD = 75` constant
  - Added relaxed thresholds for low-confidence windows: quantile gap 8% (vs 3%), ECE 0.08 (vs 0.03), bias 10% (vs 4%), edge gap 15pp (vs 8pp)
  - Added `low_confidence` property on `CalibrationMetrics` dataclass
  - Capped `severity` at `"warning"` max when `n_bets < 75` — "critical" is now unreachable with small samples
  - `compute_calibration_drift()` temporarily swaps to relaxed thresholds for small samples (try/finally for safety)
  - CLI `__main__` output shows "(LOW CONFIDENCE — metrics may be noisy)" caveat

- **`src/discord_bot/alerts.py`**:
  - `_build_calibration_embed()` now differentiates low-confidence warnings: title shows "Early Signal (Low Sample)" instead of "Drift Detected"
  - Appends sample-size warning to description: "Low sample size (N bets) — metrics may be noisy. Need ~75+ bets for reliable calibration."
  - "Significant Drift" (critical/red) title is now only reachable with 75+ bets

## Decisions Made

- **75-bet threshold**: Chosen as the minimum for reliable calibration. With 5 quantiles x 3 stats + ECE + edge buckets, need enough bets that per-stat-quantile buckets have 10+ observations. 75 gives ~15/stat for 5 stats.
- **Relaxed thresholds (not disabled)**: Rather than suppressing all alerts below 75, we widen thresholds so genuinely large deviations still surface as warnings. A 20% quantile gap on 40 bets is worth flagging; a 4% gap is noise.
- **Severity cap approach**: The `severity` property caps at "warning" for low confidence, rather than filtering alerts. This preserves the alert list for diagnostic value while preventing false critical escalation.

## Blockers and Open Questions

- None. This is a self-contained monitoring improvement.

## Recommended Next Steps

1. **Verify in production**: Wait for the next calibration check to fire on Railway (~9:15 AM ET tomorrow). Confirm it shows "Early Signal (Low Sample)" with amber color instead of critical red.
2. **Once 75+ bets accumulate**: The relaxed thresholds will automatically switch back to tight ones. Monitor that the first "normal confidence" alert is accurate.
3. **Consider the same pattern for MLB**: MLB paper trader calibration will face the same issue as it ramps up. The code is sport-agnostic (uses `paper_bets` table), so it already benefits.

## Files to Read on Resume

- [[handoff-040]] — this handoff
- `src/paper_trading/calibration_monitor.py` — calibration drift monitor with low-confidence logic
- `src/discord_bot/alerts.py` — Discord embed builder with low-confidence title/description
- [[Execution-Plan]] — overall project roadmap
