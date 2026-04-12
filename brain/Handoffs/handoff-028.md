> Part of [[Handoffs]]

**Date**: April 12, 2026 at 01:14 PM

## Summary

Built the full Polymarket-Kalshi Arbitrage Scanner (Phase 9, Step 9.1). This is a new pipeline that scrapes Polymarket prediction markets, matches them against Kalshi and sportsbook data, and detects pure arbs, soft arbs, and sportsbook mispricings. All infrastructure is live on Railway with Discord alerts routing to a dedicated `#arb-opportunities` channel.

---

## What Was Done

### New Files Created
- `src/scrapers/polymarket/__init__.py` — empty package init
- `src/arbitrage/__init__.py` — empty package init
- `src/scrapers/polymarket/polymarket_utils.py` — market type detection, stat normalization, question parsing (`POLYMARKET_SPORT_TAGS`, `POLY_STAT_MAP`, `detect_market_type`, `parse_player_prop`, `parse_game_market`, `normalize_poly_stat`, `polymarket_fee`)
- `src/scrapers/polymarket/polymarket_client.py` — HTTP client for Gamma API + CLOB API (no auth needed, rate limiting, retries)
- `src/scrapers/polymarket/polymarket_market_scraper.py` — 4-step scrape pipeline; reuses `normalize_player`/`link_player` from `kalshi_market_scraper.py`
- `src/arbitrage/market_matcher.py` — `MarketMatcher` class: Kalshi↔Poly matching (3-level: exact→near→fuzzy) + Poly↔sportsbook comparison
- `src/arbitrage/arb_scanner.py` — `ArbScanner` class: detects pure arbs (net margin > 0), soft arbs (≥5% discrepancy), sportsbook mispricings (≥8%), stores to DB
- `src/orchestration/arb_scan_job.py` — CLI orchestrator (--sport, --dry-run, --skip-discord, --skip-scrape, --date)

### Files Modified
- `src/discord_bot/alerts.py` — Added `_build_arb_alert_embed`, `send_arb_alert`, `send_arb_alert_sync`; orange=pure arb, yellow=soft, blue=sportsbook mispricing; routes to `DISCORD_CHANNEL_ARB` → `DISCORD_CHANNEL_KALSHI` → fallback
- `src/orchestration/scheduler.py` — Added `run_arb_scan_nba` (CronTrigger 11AM-11PM ET, */10 min offset :05) and `run_arb_scan_mlb` (12PM-11PM ET, */10 min offset :05); added `"arb_scan_job.py": "Arb Scanner"` to JOB_NAMES

### Database
- Migration `create_polymarket_arb_tables` applied via Supabase:
  - `polymarket_markets` — stores scraped Polymarket data (condition_id, token IDs, sport, market type, player linking, pricing snapshot)
  - `arb_opportunities` — stores detected arbs (all 3 types, all pricing fields, net margin, fees, estimated profit, status='detected')
  - Indexes on both tables for sport+snapshot, market type, player_id+stat

### Railway
- `DISCORD_CHANNEL_ARB=1492934576467611700` added via Railway MCP (channel: `#arb-opportunities`)

---

## Decisions Made

- **Soft arb threshold split**: stored at ≥5% discrepancy, Discord-alerted at ≥8% to reduce noise
- **Sportsbook comparison uses `over_prob`**: `sportsbook_prob` column doesn't exist in either `daily_predictions` or `mlb_daily_predictions` — confirmed via schema query
- **MLB uses `stat` column**: both NBA and MLB prediction tables use `stat` (not `stat_type`) — fixed query in `_load_sportsbook_props`
- **Reuse kalshi_market_scraper.py**: `normalize_player`, `link_player`, `build_player_cache` imported directly; no refactoring needed
- **Polymarket fees = 0**: currently free to trade; `polymarket_fee()` returns 0.0 as a named placeholder for future changes
- **Arb jobs offset by 5 min**: scheduler runs at :05/:15/:25 etc. (Kalshi refresh runs at :00/:10/:20) to use fresh Kalshi data

---

## Blockers and Open Questions

- **Migration 023 still pending**: `mlb_batters_latest` and `mlb_pitchers_latest` DB views for MLB Stats Vault page need to be applied in Supabase dashboard (carried over from Session 27)
- **Polymarket sport tag IDs**: hardcoded (nba=100029, mlb=100026) based on common knowledge — verify on first dry-run that events are actually returned
- **Arb scanner not yet dry-run tested**: first Railway deploy will be the first real test of Polymarket API connectivity and player name matching accuracy. Should run `--dry-run` before relying on results.

---

## Recommended Next Steps

1. **Verify arb scanner on Railway** — After deploying, run `python src/orchestration/arb_scan_job.py --sport nba --dry-run` in Railway console to confirm Polymarket API reachability and matching accuracy. Check first 20-30 matched markets for false positives.

2. **Apply migration 023** — Go to Supabase dashboard SQL editor and apply `database/migrations/023_mlb_stats_vault_views.sql` to create the MLB Stats Vault views. This unblocks the MLB Stats Vault dashboard page.

3. **NBA calibration check (due Apr 13)** — Model is 18 days old. Run the calibration health check per `check-calibration` invariants. REB UNDER (-15.1% ROI) and 15%+ edge bucket (-3.8% ROI) are the main concerns.

4. **Kalshi live trading validation** — The NO-only overhaul was deployed in Session 26 but still in 2-3 day validation window. Confirm live bets are flowing and check Bot Tracker page.

5. **Phase 3: Stripe monetization** — All steps not started. This is the next major business milestone.

---

## Files to Read on Resume

- [[handoff-028]] — this file (session context)
- `brain/Execution-Plan.md` — overall progress, Phase 9 added this session
- `src/orchestration/arb_scan_job.py` — main CLI for the new arb scanner
- `src/arbitrage/market_matcher.py` — cross-platform matching logic
- `database/migrations/023_mlb_stats_vault_views.sql` — pending migration to apply
