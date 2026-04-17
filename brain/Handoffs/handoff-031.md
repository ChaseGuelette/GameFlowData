> Part of [[Handoffs]]

**Date**: April 16, 2026 at 11:36 PM

## Summary

Completed the full Kalshi Discovery + Arb Scanner dry-run (Phase 9.3 activation). Ran the discovery script locally to enumerate real Kalshi series tickers via the `/events` API (the `/markets` endpoint lacks `series_ticker` — a key API surface discovery). Populated `KALSHI_GAME_SERIES` with 27 confirmed series, fixed 4 bugs discovered during end-to-end testing, and got 29 clean pure arbs from 203 matched game-level pairs after adding a `MIN_KALSHI_BID=3c` filter to exclude stale 1c season future placeholder prices.

---

## What Was Done

- **Discovered API surface mismatch**: Kalshi `/markets` endpoint does NOT include `series_ticker`. Pivoted `kalshi_discovery.py` to use `/events` endpoint. Enumerated 5,000 events → 200 sports series confirmed.
- **Populated `KALSHI_GAME_SERIES`** in `src/scrapers/kalshi/kalshi_utils.py` with 27 real series:
  - MLB game: KXMLBGAME, KXMLBRFI, KXMLBTOTAL, KXMLBSPREAD, KXMLBTEAMTOTAL, and 3 F5 variants
  - MLB futures: WS/AL/NL champions + 6 division winners
  - NBA game: moneyline, spread, total, team total, H1/H2 halftime (6 sub-markets)
  - NBA playoffs: series winner, series games, series score, series spread
  - NHL: 5 playoff/futures series
- **Resolved HRR TODOs** in `kalshi_utils.py` — confirmed `KXMLBHRR` is live (8 events, 312 markets), removed all TODO comments
- **Fixed dry-run formatter crash** in `src/scrapers/kalshi/kalshi_market_scraper.py` — game markets have `player_name=None`, which caused `TypeError` in the f-string format spec. Added conditional display (props show player/stat/line; game markets show market_type + teams).
- **Applied DB migration**: `kalshi_markets.line` made nullable — game markets have no line value; NOT NULL constraint was blocking all inserts.
- **Fixed UTC date boundary** in `src/arbitrage/market_matcher.py` `_load_kalshi_game_markets`: late-night scrapes (10 PM ET = 2 AM UTC next day) stored snapshots on `target_date + 1`. Fixed by checking `+1` first in the 4-day lookback loop.
- **Ran live MLB scrape**: 1,442 markets stored in DB (props + all game series).
- **Ran arb scanner dry-run**: 203 matched Kalshi-Poly pairs → initially 175 pure arbs (inflated by 1c futures), 29 after filter.
- **Added `MIN_KALSHI_BID=3c` filter** in `src/arbitrage/arb_scanner.py`: derived YES bid (`kalshi_yes_bid`) and NO bid (`100 - kalshi_yes_ask`) per direction; skip direction if bid < 3c. Eliminated stale season-future placeholders. The 29 remaining pure arbs are all in realistic 34–68c price range.
- **2 commits pushed to main**: `6e05713` (discovery + game pipeline) and `6d4af4d` (bid filter)
- **694 tests pass**

---

## Decisions Made

- **Use `/events` not `/markets` for series discovery**: The Kalshi markets list API does not expose `series_ticker`. The events API is the correct enumeration surface for series classification. This is non-obvious and not documented clearly — recorded in Scrapers.md.
- **MIN_KALSHI_BID=3c threshold**: A YES bid below 3c (or derived NO bid < 3c via `100 - ask`) signals no real orderbook — the price is a stale placeholder with no actual buyer. This filters out season futures where most teams are priced at 1c with zero depth. 3c chosen as minimum meaningful bid signal.
- **Soft arbs (126) not filtered for now**: The 126 soft arbs likely have a mirror issue (99c Kalshi prices on futures, opposite direction). Deprioritized because soft arbs are not being executed in production yet — no paper trader, no alerts. Revisit when arb paper trader (Step 9.5) is built.
- **Game-level arb scan scope**: Only MLB and NBA game markets matched against Polymarket. NHL added to `KALSHI_GAME_SERIES` but Polymarket coverage of NHL is minimal — may produce zero matches.

---

## Blockers and Open Questions

- **Prop matching still 0**: 2,704 Kalshi props × 56 Poly props → 0 matches. Not investigated this session. Low priority (Phase 9 strategy dropped player props).
- **Soft arb 99c mirror issue**: Season futures where Kalshi prices most teams at 99c (= 1c NO) with no real ask orderbook. These inflate soft arb counts just as 1c YES prices inflated pure arb counts. No filter applied yet.
- **Display bug in arb scan output**: Game-level arbs show `0.0` for team names in the dry-run printout. The underlying data is correct in `ArbOpportunity.extra["team1/team2"]` — the formatter in `arb_scan_job.py` is not extracting them for display. Makes it hard to visually audit which specific matchups are triggering arbs.
- **batter_hrr BL sweep pending**: Step 1.9 still in progress — needs odds backfill + linker + sweep before promotion.
- **NBA model check due Apr 13** (was 3 days ago — OVERDUE). Model is 24 days old. Needs calibration check.

---

## Recommended Next Steps

1. **Fix game-level arb display bug** (30 min) — In `src/orchestration/arb_scan_job.py`, update the dry-run output formatter to pull `team1`/`team2` from `opp.extra` and print them. Without this you can't visually audit whether the 29 pure arbs are real opportunities or matching errors.

2. **NBA model calibration check** (overdue — was due Apr 13) — Run `/check-calibration` to assess 24-day-old model. REB UNDER was -15.1% ROI in the Apr 10 check. Check if still concerning and whether retrain trigger has been crossed.

3. **Run arb scan job live (not dry-run)** — After confirming display bug is fixed and matches look correct, run without `--dry-run` to actually store opportunities in the `arb_opportunities` table. The Railway hourly job should already be doing this, but verify a non-zero count is appearing in the DB.

4. **Build arb paper trader (Step 9.5)** — `arb_paper_bets` 2-leg table, `ArbPaperTrader` class to simulate P&L on pure arb fills. Needed to validate whether the 29 pure arbs are genuinely executable or just theoretical.

5. **batter_hrr BL sweep** (Step 1.9) — Backfill `batter_hits_runs_rbis` Odds API props 2023–2025, run linker, run sweep. Commands are ready in prior session notes.

---

## Files to Read on Resume

- [[Execution-Plan]] — Steps 1.9 (batter_hrr pending) and 9.3–9.6 (current arb scanner status)
- [[handoff-031]] — This handoff (current state of game-level arb pipeline)
- [[Scrapers]] — Updated with KALSHI_GAME_SERIES details and /events API note
- [[Kalshi-Integration-Design]] — Full Kalshi arb scanner design and decisions
