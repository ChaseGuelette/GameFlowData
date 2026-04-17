# Handoff 012 — Kalshi Audit + BL Config Fixes

> Part of [[Handoffs]]

**Date**: April 16, 2026

---

## Summary

Full Kalshi bet audit and pre-live readiness pass. Built `scripts/audit_kalshi_bets.py` (3-part: settlement reconciliation, slippage estimation, adjusted P&L). Discovered and fixed 3 bugs across the Kalshi stack: DNP resolution bug in both paper and live traders, wrong BL configs in `kalshi_edge.py` (single hardcoded config for all sports/stats), and missing edge-sort in live trader's greedy allocation. Confirmed 0 real settlement mismatches. Kalshi is AMM-driven — no slippage risk. System is ready to go live once a few days of playoff-config paper trading are validated.

---

## What Was Done

- **`scripts/audit_kalshi_bets.py` created** — 3-part audit script with CLI flags (`--paper-only`, `--skip-api`, `--sport`, `--date-from`, `--date-to`, `--csv-out`). Settlement reconciliation via Kalshi API, slippage via orderbook walking, adjusted P&L. NaN bug fixed in `check_mismatch` (was `is None`, now `pd.isna()`).
- **DNP bug fixed in `kalshi_paper_trader.py`** — `_fetch_actuals()` added `AND s.min > 0` to NBA stats query. DNP players created zero-stat rows that incorrectly resolved NO bets as wins. ~$91 phantom P&L identified from 4 real affected bets.
- **Same DNP bug fixed in `kalshi_live_trader.py`** — separate `_fetch_actuals()` implementation, same fix applied.
- **Edge-sort fix in `kalshi_live_trader.py`** — `select_trades()` second pass now sorts candidates by `fee_adjusted_edge` descending before greedy allocation, matching paper trader behavior exactly.
- **BL config fix in `kalshi_edge.py`** — was hardcoded `BLConfig(tau=0.5, z_max=1.0)` for all sports/stats. Now: NBA uses `NBA_PLAYOFF_MODE` env var (playoff: `tau=0.9, z_max=0.25, mw=0.8`; regular: `tau=0.5, z_max=1.0, mw=0.5`); MLB uses per-stat `STAT_BL_CONFIGS` from `mlb_stat_config.py` (pitcher K: `tau=0.9/z_max=0.25/mw=0.8`, batter hits: `tau=0.75/z_max=1.0/mw=0.8`, batter hrr: `tau=0.9/z_max=0.25/mw=0.65`).
- **Per-stat direction restriction added to both traders** — `kalshi_paper_trader.py` and `kalshi_live_trader.py` now check `MLB_STATS[stat]["allowed_directions"]` after side selection for MLB bets. Blocks `pitcher_strikeouts YES` (was losing -$256 on 238 bets at 13.4% win rate).
- **Confirmed `NBA_PLAYOFF_MODE=true` is set on Railway** — playoff config active for both dashboard and Kalshi paper trader.
- **Confirmed `KALSHI_LIVE_TRADING_ENABLED` is NOT set** — paper trading only. Live trading requires explicit env var addition.

---

## Decisions Made

- **Kalshi AMM = no slippage risk**: Player prop markets are AMM-driven. The `yes_price` IS the fill price. Orderbook endpoint only shows user limit orders (always empty). Slippage section of audit is structurally N/A.
- **Wrong BL config was suppressing volume**: The conservative config (tau=0.5) was pulling `bl_model_prob` toward market prior. For NBA playoff mode, this understated NO edges by ~5-10pp, causing borderline bets to miss the 15% threshold and sizing to be ~30-60% too small on placed bets. 88-92% of bets ended up as 1-contract overflow.
- **Direction fix stops pitcher K YES bets**: Previously 238 overflow bets on pitcher K YES, 13.4% win rate, -$256. Direction restriction in `MLB_STATS` now enforced at both daily runner and trader level.
- **Do NOT go live yet**: Paper trade the correct playoff BL config for a few days first. Starting tonight's `kalshi_refresh_job`, `bl_model_prob` in `kalshi_markets` will use correct configs. Validate with `/check-kalshi` before setting `KALSHI_LIVE_TRADING_ENABLED=true`.

---

## Blockers and Open Questions

- **Live trading validation pending**: Need 2-3 days of paper bets computed with correct BL configs before going live. Check with `/check-kalshi` to confirm edge quality.
- **batter_hrr BL sweep still pending** (from Session 33/Step 1.9): backfill `batter_hits_runs_rbis` odds, run linker, run sweep, promote if ROI > 0% and Z > 1.5.

---

## Recommended Next Steps

1. **Monitor Kalshi paper bets tonight and tomorrow** — first run with correct playoff BL config for NBA, correct per-stat configs for MLB. Run `/check-kalshi` to validate edge quality.
2. **Once 2-3 days look good, set `KALSHI_LIVE_TRADING_ENABLED=true`** on Railway to go live. Fund the Kalshi account first.
3. **Complete batter_hrr promotion (Step 1.9)**: backfill odds, run linker, sweep, promote.

---

## Files to Read on Resume

- [[handoff-012]] — this file
- `src/models/kalshi_edge.py` — BL config fix (lines 228-247)
- `src/paper_trading/kalshi_paper_trader.py` — direction fix (lines ~307-315)
- `src/paper_trading/kalshi_live_trader.py` — edge-sort fix (line ~468), direction fix, DNP fix
- `src/models/mlb/mlb_stat_config.py` — STAT_BL_CONFIGS and allowed_directions source of truth
