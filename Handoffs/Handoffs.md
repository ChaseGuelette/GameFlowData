# Handoffs

> Part of [[BRAIN-INDEX]]

Session continuity notes. Read the latest handoff at the start of every session.

## Session History

- [[handoff-001]] — April 12, 2026 — MLB pitcher K UNDER-only + backtest direction filter. Sweeps run, edge=0.08 locked in for early season.
- [[handoff-002]] — April 12, 2026 — MLB Kalshi volume investigation, pitcher_outs fix, batter_hrr (H+R+RBI) model groundwork across 6 files, plan doc created.
- [[handoff-003]] — April 15, 2026 — Migration 023 applied (Stats Vault live). MLB paper trader bet-count diagnosis + 3 fixes: min_odds -200→-500, date dropdown RPC, paper trader redesigned to mirror Model Picks exactly via is_recommended flag.
- [[handoff-004]] — April 15, 2026 — bet_reasoning JSONB on paper bets (model context in Discord), daily summary fixes (timezone/data/timing), greedy bet sizing, analysis embed clarity overhaul, high-edge alert liquidity filter.
- [[handoff-005]] — April 15, 2026 — batter_hrr model trained (bias 0.9950, ECE passes). MLB lineup + roster scrapers built and scheduled. BL sweep unblocked: batter_hits_runs_rbis backfill commands ready, sweep code fixed. Next: run backfill → linker → sweep → promote.
- [[handoff-006]] — April 16, 2026 — Dashboard filter redesign (10 controls → 4 + popover). Sportsbook 0.5 lines bug fixed (bookmaker:line dedup key). MLB analysis modal shows all batting stats + binary model framing. MLB Ask AI fully implemented + enabled (opponent pitcher, park factors, rolling avgs).
- [[handoff-007]] — April 16, 2026 — Phase 9.2 complete: Polymarket scraper rebuilt for all-categories mode (sport tag filter was broken). 70,651 markets in DB across 7 categories. batter_rbis switched to OVER-only. Hourly all-categories job live on Railway.
- [[handoff-008]] — April 16, 2026 — Kalshi go-live prep: 3 code fixes (SUPPORTED_STATS, MLB_STAT_RESOLUTION list format, exposure default 0.90), 9 Railway env vars set, analysis script rewritten (14 sections), /check-kalshi skill created, Discord balance bug fixed, overflow embed expanded.
