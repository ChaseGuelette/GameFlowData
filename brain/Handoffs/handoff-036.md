> Part of [[Handoffs]]

# Session 036 Handoff

**Date**: April 19, 2026 at 01:45 PM

## Summary
Implemented the Kalshi Elections + Politics non-sports arb expansion — growing the scraper from ~200 macro markets to 6,439 markets across 649 Elections series and 332 Politics series discovered dynamically at runtime. Resolved a significant performance issue (16M comparisons → manageable via volume/liquidity filters), then improved match quality through candidate name disambiguation, reducing false positives from 364 → 144 matched pairs. Full validation confirmed the pipeline works end-to-end; deployment to Railway is still pending.

## What Was Done

### Code Changes
- **MODIFIED** `src/scrapers/kalshi/kalshi_utils.py`:
  - Added `_CAT_ELECTIONS` and `_CAT_POLITICS` entries to `KALSHI_NON_SPORTS_SERIES` with `mode: "category_scrape"`
  - Added corresponding entries to `KALSHI_SERIES_POLY_CONFIG` with `fallback_threshold: 0.65`, `min_kalshi_volume`, and `min_poly_liquidity` per-config overrides
  - Elections: `min_kalshi_volume=5000`, `min_poly_liquidity=50000` (Poly politics is top-heavy — liq>1000 still returns 3,544 markets; only liq>50000 reduces to 283)
  - Politics: `min_kalshi_volume=500`, `min_poly_liquidity=5000`

- **MODIFIED** `src/scrapers/kalshi/kalshi_market_scraper.py`:
  - Added Phase A: pre-discovery block — calls `list_all_events(status="open")` once, groups series by Kalshi category, builds `category_series_map`
  - Refactored loop into `mode == "category_scrape"` (iterates discovered series, 0.1s pacing, stores `series_ticker = config_key`) and `mode == "static"` (existing macro behavior, unchanged)
  - Extracted shared market-parsing into `_parse_and_append_market()` helper

- **MODIFIED** `src/arbitrage/market_matcher.py`:
  - Added `series_ticker` to SELECT in `_load_kalshi_non_sports()`
  - Grouping uses stored `series_ticker` (via `row.get("series_ticker") or ticker.split("-")[0]`)
  - `_load_poly_non_sports()` gains `min_liquidity` parameter for per-config overrides
  - Per-config `min_kalshi_volume` and `min_poly_liquidity` applied in matching loop
  - Added `_extract_candidate()` helper (regex: `will (.+?) win`) and candidate disambiguation in fuzzy fallback:
    - Both sides have a candidate → require name similarity ≥ 0.65 (not 0.5 — Korean/Spanish romanization shares too many chars at 0.5)
    - One side has "Will X win?" and the other doesn't → structural mismatch → reject
    - Neither has candidate → proceed with fuzzy score

- **CREATED** `scripts/inspect_nonsports_matches.py`:
  - Diagnostic script showing actual matched question pairs + prices + scores
  - Supports `--series`, `--top N`, `--sort margin|score`, `--min-score` flags
  - Score distribution histogram at the bottom

### Validation Results
- **Scraper dry-run**: 4,769 elections + 1,470 politics + 200 macro = **6,439 total markets** parsed
- **Arb scan dry-run**: Completed in ~2 min (was DNF/30+ min before volume filters)
  - `[_CAT_POLITICS]` 1,252 Kalshi × 662 Poly
  - `[_CAT_ELECTIONS]` 1,133 Kalshi × 276 Poly
  - **361 non-sports matched pairs** → **105 pure arbs, 174 soft arbs** found
- **After candidate disambiguation**: 144 matches (364 → 144, 60% false positive reduction)

## Decisions Made

- **Dynamic discovery instead of hardcoding**: 970+ election/politics series can't be hardcoded. Use `list_all_events(status="open")` at scrape time to discover current open series, grouped by Kalshi category. Markets stored with `series_ticker = config_key` so matcher groups all elections under one config entry.

- **Volume/liquidity thresholds are about arb viability, not just performance**: Low-volume Kalshi markets and low-liquidity Poly markets aren't actionable for arb anyway. `min_kalshi_volume=5000` and `min_poly_liquidity=50000` keep only meaningful markets while reducing compute by 80%.

- **Candidate name similarity threshold 0.65, not 0.50**: Korean and Spanish-language names romanized to English share enough common characters (ch, on, ng, etc.) that "Chong Won-o" vs "Kang Hoon-sik" scores 0.50 — exactly at the old threshold. 0.65 correctly rejects these while still passing "Joe Biden" vs "Joseph Biden" (~0.76).

- **Structural mismatch check**: "Will the 2028 presidential election occur?" (Kalshi, 93c) was matching every "Will [candidate] win the 2028 US Presidential Election?" on Poly because the candidate regex returned None for the occurrence-style question. When exactly one side has a "Will X win?" pattern and the other doesn't, the questions are structurally different — reject regardless of surface similarity.

- **OpenCode server was not running**: Attempted GLM handoff for implementation but the OpenCode server wasn't started. MEMORY.md updated with the correct pattern: `--attach` fails with misleading "File not found: [prompt text]" error when server is down. Implemented directly instead.

## Remaining False Positives (Known Issues)

1. **Same-race, different placement** — "Will Roberto Sánchez Palomino finish 2nd?" vs "Will Roberto Chiabra finish 2nd?" — both "Roberto" but different candidates. The structured extractor doesn't distinguish candidate names; SequenceMatcher scores them ~0.85 due to "Roberto" + "2nd" + "Peruvian presidential" overlap. Fix: extend candidate check to handle "finish Nth" patterns.

2. **Different verb, same person** — "Will Marco Rubio receive a presidential pardon?" vs "Will Marco Rubio announce a presidential run?" — same person, unrelated questions. Both pass candidate check (name matches). Fix: extract action verb and require similarity.

3. **GDP country mismatch** — Structured extractor scores US GDP "Will real GDP increase by more than 2.5% in Q1 2026?" against Mexico/Eurozone GDP questions as `score=1.0` because it extracts (value=2.5, direction=above, period=Q1-2026) from both and finds exact match — but ignores the country. Fix: add country extraction to `non_sports_extractor.py` and include in match score.

## Not Yet Done

- **Not deployed to Railway** — all changes are local only. Need to push to Railway so the 10-min refresh job picks up Elections/Politics markets.
- **Finance/Markets category** — S&P 500, Nasdaq, VIX markets on Kalshi. High priority: Poly has equity markets with good liquidity and Kalshi is active here.
- **Entertainment category** — Oscars, Emmys, box office. Kalshi has 333 series, Poly has 348 culture markets. Low priority but easy win once infrastructure is proven.
- **SCOTUS rulings category** — Kalshi and Poly both cover Supreme Court case outcomes. Binary yes/no format, good structural alignment.
- **Science/Tech and Companies categories** — Medium priority. AI milestones, CEO exits, acquisitions. Kalshi ~142 combined series, Poly has coverage in "other" category.

## Recommended Next Steps

1. **Deploy to Railway** — push the kalshi_utils + kalshi_market_scraper + market_matcher changes. Verify the 10-min nonsports refresh job picks up elections/politics markets in logs.
2. **Fix GDP country mismatch** — add country extraction to `src/arbitrage/non_sports_extractor.py`. The KXGDP extractor should include the country/region in the structured fields and require it to match.
3. **Add Finance/Markets category** (`_CAT_FINANCE`) — highest incremental value after elections. Research Kalshi finance series tickers via `list_all_events` filtered by Finance category.
4. **Tune thresholds after first Railway cycle** — check actual match counts with real DB data (vs dry-run). Adjust `min_kalshi_volume` / `min_poly_liquidity` if matches are too sparse or too many.
5. **Add same-race placement disambiguation** — extend `_extract_candidate()` to handle "finish Nth place" patterns and check placement (1st vs 2nd) as part of match validation.

## Files to Read on Resume

- `src/scrapers/kalshi/kalshi_utils.py` — `KALSHI_NON_SPORTS_SERIES` and `KALSHI_SERIES_POLY_CONFIG` (new entries at bottom of each dict)
- `src/scrapers/kalshi/kalshi_market_scraper.py` — `scrape_non_sports_and_store()` (~lines 688-780)
- `src/arbitrage/market_matcher.py` — `_extract_candidate()` (~line 65), `match_non_sports_markets()` (~lines 510-610)
- `scripts/inspect_nonsports_matches.py` — diagnostic tool for match quality inspection
- [[non-sports-arb-market-structure]] — Kalshi universe audit + known structural limitations
