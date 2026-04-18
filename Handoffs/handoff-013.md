> Part of [[Handoffs]]

# Session 13 Handoff

**Date**: April 17, 2026

## Summary
This session completed the Kalshi non-sports market scraping infrastructure, wiring up economics (KXGDP, KXFED, KXCPI) and crypto (KXBTC, KXBTCD, KXETH, KXETHD, KXDOGE, KXXRP) series into the existing arb scanner pipeline. 1,044 markets are now stored in the DB with sport=NULL and being read by the non-sports arb scanner. The end-to-end pipeline is verified working, though 0 matches are produced due to question wording differences between Kalshi and Polymarket.

## What Was Done
- **Created** `src/orchestration/kalshi_nonsports_refresh_job.py` — standalone scrape job, exits gracefully without Kalshi creds
- **Modified** `src/scrapers/kalshi/kalshi_client.py` — added `list_events()` and `list_all_events()` methods (events endpoint returns series_ticker unlike markets)
- **Modified** `src/scrapers/kalshi/kalshi_discovery.py` — fixed to use /events endpoint, fixed parameter name bug (max_markets→max_events)
- **Modified** `src/scrapers/kalshi/kalshi_utils.py` — added `KALSHI_NON_SPORTS_SERIES` with 9 series (3 economics, 6 crypto)
- **Modified** `src/scrapers/kalshi/kalshi_market_scraper.py` — added `scrape_non_sports_and_store()` function
- **Modified** `src/orchestration/scheduler.py` — added `run_kalshi_nonsports_refresh()` function + scheduled job every 10 min, 11AM-11PM ET + JOB_NAMES entry
- **Modified** `src/arbitrage/market_matcher.py` — added keyword pre-filter in `match_non_sports_markets()` reducing 27,711 Poly markets to ~2,900 before SequenceMatcher
- **Applied DB migration** `make_kalshi_markets_sport_nullable` — dropped NOT NULL on `kalshi_markets.sport`
- **Verified**: 1,044 Kalshi non-sports markets in DB; arb scanner reads them; scan completes in ~2.5 min

## Decisions Made
- **sport=NULL for non-sports**: Required migration to drop NOT NULL on kalshi_markets.sport. Correct approach since `_load_kalshi_non_sports()` filters `sport IS NULL OR sport = ''`.
- **Keyword pre-filter**: Added before SequenceMatcher to cut 27,711 Poly → ~2,900 Poly markets. Keywords: gdp, cpi, inflation, federal funds, fomc, bitcoin, btc, ethereum, eth, dogecoin, doge, ripple, xrp, crypto.
- **Accept 0 matches for now**: Kalshi/Polymarket phrase questions differently ("Will real GDP increase by more than 2.5%?" vs "Will US GDP grow above 2% in Q1?"). SequenceMatcher 0.80 threshold too strict. Infrastructure is correct; matching logic is future work.
- **10-min refresh cadence**: Same as NBA/MLB Kalshi refresh jobs — keeps all Kalshi scraping on the same schedule for simplicity.

## Blockers and Open Questions
- **0 non-sports matches**: Question wording mismatch between platforms. Need to either lower SequenceMatcher threshold to 0.50-0.60, or build explicit series→Polymarket slug mapping. The latter is more reliable but requires manual mapping per series.
- **Slow SequenceMatcher**: ~2.5 min for 3M comparisons even after pre-filter. Further optimization: group Kalshi by category, match each group against corresponding Poly category only. Would reduce to ~500k comparisons.
- **Arb paper trader resolution**: `arb_paper_trader.py` built but not fully tested — moneyline resolution (join mlb_game_schedule to determine winner) needs verification.

## Recommended Next Steps
1. **Deploy to Railway** — push all scheduler changes so `kalshi_nonsports_refresh` runs in production every 10 min
2. **Improve non-sports matching** — lower threshold to 0.50-0.60, or build KXGDP/KXFED/KXCPI → Polymarket slug explicit mapping
3. **Arb paper trader resolution** — verify moneyline resolution logic in `arb_paper_trader.py` against actual game results
4. **Fix game-level arb bugs** — cross-date matching, cross-market-type matching, in-play contamination (per the Step 9.5 plan)

## Files to Read on Resume
- [[market_matcher]] → `src/arbitrage/market_matcher.py` — non-sports matching, keyword pre-filter
- [[kalshi_market_scraper]] → `src/scrapers/kalshi/kalshi_market_scraper.py` — scrape_non_sports_and_store
- [[arb_scan_job]] → `src/orchestration/arb_scan_job.py` — full pipeline integration
- [[scheduler]] → `src/orchestration/scheduler.py` — all job scheduling
