> Part of [[Handoffs]]

**Date**: April 10, 2026 at 12:37 PM

## Summary

Fixed both the MLB and NBA paper traders to align with how the daily runner and edge refresh job compute edges. The MLB paper trader was placing ~1 bet/day (should be 20-50) due to a conservative single BL blender. The NBA paper trader was betting far less than the user's 10-15 personal bets/day due to redundant BL re-blending and aggressive sanity checks that the dashboard no longer applies after 4:15 PM.

## What Was Done

- **`src/paper_trading/mlb_paper_trader.py`** — Fixed BL config mismatch
  - Removed single conservative blender (tau=0.5, z_max=1.0, max_weight=0.50)
  - Now builds per-stat blenders from `STAT_BL_CONFIGS` (same source as daily runner)
  - Falls back to `DEFAULT_BL_CONFIG` for unknown stats
  - Import: swapped `BLConfig` out, added `DEFAULT_BL_CONFIG, STAT_BL_CONFIGS`
  - `_bl_blenders: dict` + `_default_bl_blender` replace the old single `_bl_blender`

- **`src/paper_trading/paper_trader.py`** — Fixed NBA paper trader over-filtering
  - Removed: `_bl_blender`, `_load_samples_for_date()`, gzip/numpy imports, `MAX_Q50_DIVERGENCE`, `L5_ABOVE_LINE_MARGIN` constants
  - Now reads stored BL values (`bl_over_edge`, `bl_under_edge`, `bl_over_prob`, `bl_under_prob`) from `daily_predictions` table
  - Falls back to raw `over_edge`/`under_edge` when BL columns are NULL
  - Kept `bl_tau`/`bl_z_max` dataclass fields for backward compat with `place_bets.py`

- **`brain/Models/NBA-Model.md`** — Documented paper trader fix
- **`brain/Models/MLB-Model.md`** — Documented paper trader BL config fix

## Decisions Made

- **Use stored BL values (NBA paper trader)**: Rather than re-blending from MC samples, read `bl_over_edge`/`bl_under_edge` directly from DB. This ensures paper trader uses the same values that determine `is_recommended` on the dashboard.

- **Remove sanity checks from NBA paper trader**: `edge_refresh_job.py` overwrites `is_recommended` at 4:15 PM WITHOUT L5_ABOVE_LINE and Q50_DIVERGENCE checks. Dashboard shows these picks. Paper trader should not apply a stricter filter than the dashboard.

- **Per-stat blenders from STAT_BL_CONFIGS (MLB)**: The single source of truth already exists in `mlb_stat_config.py`. Paper trader now reads from it instead of maintaining its own blender config.

## Blockers and Open Questions

- **Bet count verification**: Need to watch next Railway MLB inference run logs to confirm bet count increases to ~20-50/day (from ~1/day).
- **NBA paper trader bet count**: Need to verify next NBA inference run (12:15 PM ET) places ~10-15 bets rather than the previous low count.
- **Tests**: `tests/test_paper_trader.py` uses `bl_tau=None` + mocked DataFrames without `bl_over_edge` column → falls back to raw edges → still passes. Worth running `pytest` to confirm.

## Recommended Next Steps

1. **Deploy to Railway** — Both paper traders are now fixed locally; push and watch next inference run logs
2. **Run Python tests** — `python -m pytest tests/test_paper_trader.py` to confirm backward compat holds
3. **NBA calibration check** — Next check due Apr 13 (model will be 21 days old, hitting 3-week limit)
4. **Stripe integration** — Subscribe page, Customer Portal, webhook route, DB columns still TODO

## Files to Read on Resume

- [[MLB-Model]] — MLB model status + paper trader fix docs
- [[NBA-Model]] — NBA model status + paper trader fix docs
- `src/paper_trading/paper_trader.py` — NBA paper trader (now uses stored BL values)
- `src/paper_trading/mlb_paper_trader.py` — MLB paper trader (now uses per-stat BL)
- `src/orchestration/edge_refresh_job.py` — Understand why stored values are authoritative
