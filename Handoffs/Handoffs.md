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
- [[handoff-009]] — April 16, 2026 — Phase 9.3+9.4 complete: Cross-platform market matcher built. `team_normalizer.py`, `kalshi_discovery.py`, `match_game_markets()` (frozenset), `match_non_sports_markets()` (SequenceMatcher). DB migration applied. KALSHI_GAME_SERIES stub in place (needs discovery run to populate). 694 tests pass.
- [[handoff-010]] — April 16, 2026 — Status-check session. Confirmed 694 tests pass. Execution plan updated (9.3+9.4 marked completed). Pending: run kalshi_discovery.py locally to populate KALSHI_GAME_SERIES.
- [[handoff-011]] — April 16, 2026 — MLB paper trader audit: 4 bugs fixed (allowed_directions enforcement in daily runner + paper trader, stat filter added, frontend perStatConfig corrected). 232 wrong-direction bets identified (-$13,633 PnL). Two eras in mlb_paper_bets established. First clean production run is Apr 17.
- [[handoff-012]] — April 16, 2026 — Kalshi audit + pre-live readiness. Audit script built. 3 bugs fixed: DNP resolution (both traders), wrong BL configs in kalshi_edge.py (now per-stat MLB + playoff-aware NBA), missing edge-sort in live trader. Direction restrictions added to both traders. KALSHI_LIVE_TRADING_ENABLED not set — paper trading correct configs starting tonight. Ready to go live after 2-3 day validation.
- [[handoff-013]] - Session 13: Kalshi non-sports scraping complete
- [[handoff-014]] — April 20, 2026 — NBA playoff model v2: minutes trend ratio features, retrained model, new BL config (tau=0.9/z=1.0/edge=0.15), structural bet filters (no reb over <=2.5, no ast over). +16.7% filtered ROI on 277 bets.
