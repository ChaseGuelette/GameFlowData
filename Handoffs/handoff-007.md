> Part of [[Handoffs]]

**Date**: April 16, 2026 at 4:45 PM

## Summary

Session focused on Phase 9.2: rebuilding the Polymarket scraper to ingest all market categories instead of sport-specific filtering (which was broken — Polymarket's tag filter returns 0 events). Also completed the batter_rbis OVER-only config change from the pre-session plan. The scraper is now live on Railway with 70,651 markets stored across 7 categories, running hourly.

## What Was Done

### batter_rbis OVER-only Config (pre-compaction, commit dae0a45)
- `src/models/mlb/mlb_stat_config.py`:
  - `batter_rbis`: `edge_threshold` 0.12 → 0.08, added `allowed_directions: ["over"]`
  - BL config: `z_max` 0.25 → 0.5, `max_weight` 0.80 → 0.65
- Matches pitcher K playbook: restrict direction + tune BL params
- Backtest rationale: UNDER was bleeding -$9,503 (-21.3% ROI); OVER was +79.9% ROI, 5.61 Sharpe, 120 bets/2mo

### Phase 9 Strategic Pivot
- Confirmed Polymarket signed exclusive MLB deal (March 2026) — player prop coverage is thin
- Pivoted Phase 9 target: ALL categories — season-long futures, NRFI, game moneylines, politics, crypto, economics, weather, culture
- Updated Execution Plan Phase 9 goal section to reflect pivot (player props dropped)

### Phase 9.2: Polymarket Scraper Full Rewrite
- **Root cause discovered**: Polymarket's sport tag filter (e.g., `tag_id=100026` for MLB) returns 0 events — has been broken all along. This is why `polymarket_markets` was always empty.
- **Fix**: Always scrape all events (`tag_id=None`), then classify by detecting tag IDs + slug/title keywords post-fetch

**Bugs fixed in scraper**:
1. `clobTokenIds` is a JSON string, not a list — fixed with `json.loads()` before iterating
2. CLOB API `/midpoints` POST returns 400 for all requests — switched to Gamma API's `outcomePrices` field (already in response, 78% of markets have prices)
3. Schema created 69K new rows every hourly run (`condition_id + snapshot_time` unique key) — migrated to `condition_id` only, one row per market, upserted in place
4. Row-by-row inserts took 30+ minutes for 69K markets — switched to batch executemany 500/chunk (~2 min total)
5. Parse loop had a `continue` bug that skipped sport-specific events in all-categories mode — simplified logic

**New DB migrations applied**:
- `polymarket_add_category`: added `category text` column, made `sport` nullable, added category index
- `polymarket_upsert_on_condition_id`: truncated old data, dropped `(condition_id, snapshot_time)` unique key, added `(condition_id)` unique key

**Files modified**:
- `src/scrapers/polymarket/polymarket_utils.py`: Added `_SPORT_TAG_IDS` inverted lookup, `_NRFI_PATTERN`, `_SEASON_FUTURE_PATTERNS`, updated `detect_market_type()` with `is_sports` param + NRFI/season_future types, added `_CATEGORY_KEYWORDS` dict, new `detect_category(event)` function
- `src/scrapers/polymarket/polymarket_market_scraper.py`: Full rewrite — all-categories mode, Gamma API prices, batch upserts, category detection, `--all` CLI flag, `by_category`/`by_market_type` summary stats
- `src/orchestration/arb_scan_job.py`: Added `mode: str = "sport"` param, `--mode all` CLI arg, routes to `scrape_and_store(sport=None)` in all-categories mode
- `src/orchestration/scheduler.py`: Added `run_arb_scan_all_categories()` (hourly, 9:30AM–11:30PM ET), removed `run_arb_scan_nba()` (NBA season over), kept MLB every-10-min scan

**Result**: 70,651 markets across 7 categories (sports: 6,008, politics: 12,365, crypto: 3,199, weather: 1,681, culture: 904, economics: 299, other: 45,104). 54,247 have prices from Gamma API (78%). Running live on Railway.

### Deployment
- Committed: `bac117d` ("Fix Polymarket scraper: all-categories mode, price from Gamma API, batch upserts")
- Pushed to `origin/main` — Railway redeployed

## Decisions Made

- **All-categories always, sport filtering post-hoc**: Polymarket's tag filter is broken. The only reliable approach is to scrape everything and classify after. This is actually better — we get non-sports markets for free.
- **Prices from Gamma API only**: The CLOB `/midpoints` endpoint is broken for batch requests. Gamma API's `outcomePrices` field provides prices for 78% of markets with zero extra API calls. Sufficient for arb detection.
- **One row per market, upserted**: Storing one row per market updated in place is the correct design for a live market database. The snapshot-per-run approach would have created 1.66M rows/day.
- **Removed NBA arb scan from scheduler**: NBA season is essentially over for 2026. No need to scan sport-specific NBA markets. The all-categories job covers all sports.
- **batter_rbis OVER-only**: Same playbook as pitcher K — restrict to the only profitable direction. UNDER was a clear structural loser (market pricing anchors better on UNDER for counting stats with rare events like RBIs).

## Blockers and Open Questions

- **arb_opportunities table is still empty**: The arb scanner (Steps 9.3/9.4) hasn't been built yet — market matching between Polymarket and Kalshi is the next piece. Data is flowing into `polymarket_markets`, but the matcher hasn't been wired up.
- **45,104 "other" category markets**: These are Polymarket markets that don't match any known keyword cluster. Could contain valid opportunities. Worth reviewing a sample to see what's in there.
- **NBA calibration check overdue**: Was due April 13, now 3 days overdue. Model is 24 days old (past the 21-day trigger). Apr 10 check was +10.9% ROI overall but REB UNDER at -15.1% ROI.

## Recommended Next Steps

1. **Phase 9.3: Game-level matcher** (highest priority for arb revenue) — match Kalshi ↔ Polymarket on: (a) season-long futures by team name normalization + event type, (b) NRFI by game date + team pair, (c) game moneylines/totals by (team1, team2, date, market_type). Read `src/arbitrage/market_matcher.py` and `src/scrapers/kalshi/kalshi_market_scraper.py` to understand existing Kalshi data format.
2. **NBA calibration check** — model is now 24 days old (3-week trigger exceeded). Run `/check-calibration`. Pay attention to REB UNDER trend.
3. **Step 1.9 batter_hrr**: Backfill `batter_hits_runs_rbis` odds 2023-2025, run linker backfill, run BL sweep, promote if ROI > 0% with Z > 1.5.
4. **Phase 9.4: Non-sports matcher** — fuzzy question text matching between Kalshi and Polymarket binary markets. The politics/crypto/economics categories are where the most persistent gaps will be.

## Files to Read on Resume

- [[handoff-007]] (this file — start here)
- [[Execution-Plan]] — Phase 9 steps 9.3-9.6 are the active build queue
- `src/arbitrage/market_matcher.py` — existing matcher code to understand what needs to expand
- `src/scrapers/polymarket/polymarket_market_scraper.py` — the newly rewritten scraper
- `src/scrapers/polymarket/polymarket_utils.py` — category detection + market type detection
