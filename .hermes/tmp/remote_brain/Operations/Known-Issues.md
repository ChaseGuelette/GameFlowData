---
title: Known Issues
type: operations
domain: operations
status: active
owner: Chase
tags: [known-issues, maintenance, gbrain]
---

# Known Issues

> Part of [[Operations]]

## Current Status
Active issue inventory with possible stale entries. Before acting on any issue or claiming it is fixed, verify against current tests, logs, code, or GBrain handoffs.


## Active Bugs
- **Railway Lines Scraper intermittent linker failures** — `lines_job.py` succeeds often but intermittently fails in `src/processing/nba_linker_local.py incremental` at `Linking Props (Incremental)`. Recent failures had 10k-20k unlinked rows before failing; current remote last-30-day unlinked count later reached 0 after successful runs. Immediate work item: deploy the `src/orchestration/lines_job.py` stderr-tail logging patch, capture the next real traceback, then decide whether to reduce batch/window size, target latest scrape rows only, or plan a concurrent index. See [[handoff-088]].
- `test_finds_latest_run_directory` failing — expects `run_*` prefix but code now expects `nba_run_*`
- **MLB paper bets disabled** (`--skip-bets`) — `batter_total_bases` and `batter_runs_scored` trained with at_bats leakage. Re-enable after retraining.

## Recently Fixed (Session 15)
- **Railway MLB daily stats failing**: Supavisor strips `-c` startup params → role-level 8s timeout killed batting/pitching rolling average queries. Fix: explicit `SET statement_timeout = '120000'` in `fetch_batter_season_games()` and `fetch_pitcher_season_games()`.
- **MLB bets not resolving**: Zero 2026 game stats in DB — schedule existed but all games stuck at "Scheduled". Backfilled locally, 946 bets resolved.
- **pitcher_outs column mismatch**: Mapped to `"outs"` but actual column is `"outs_recorded"` in `mlb_paper_trader.py`.
- **MLB Discord P&L missing**: `mlb_daily_stats_job.py` never sent post-resolution P&L summary. Added `_send_mlb_pnl_summary()`.

## Technical Debt
- `raw_player_props_combined` at **67M+ rows** — queries take 9-14s. Needs archiving or partitioning.
- In-memory rate limiting on `/api/ask` — won't work multi-instance (needs Redis)
- No pagination on history/performance pages
- DFS/heatmap tables use horizontal scroll on mobile (should be card layouts)
- AI chat not persisted across modal close
- No CI/CD — deploys are manual git push

## GBrain Retrieval / Sync Issues
- `stale_pages` remains high in GBrain health (67 on 2026-05-13) while embeddings, doctor, and orphans are clean. Treat as unresolved metadata/retrieval-quality risk until audited; future eval misses should check whether stale pages outrank canonical facts/lessons/decisions.
- GBrain source routing during manual import/sync is suspect: `gbrain import /home/chase/GameFlowBrain --no-embed` placed new pages in source `default` despite `.gbrain-source=gameflow`; later `gbrain sync --source gameflow` emitted `Page not found ... (source=default)` messages while final DB state was clean. This is not harmless just because final DB state passed; investigate before relying on manual import/sync error noise as a normal workflow.
- **Query-time source isolation is fixed only in local carried GBrain patch:** 2026-05-13 Batch 1 code vectorization found that `gbrain query --source gameflow-code-modeling-core ...` did not isolate results to the requested source; results still included `gameflow-code-mlb-pilot`. Root-cause inspection found `--source` resolved into CLI context, but `query` did not pass `ctx.sourceId` into `hybridSearch`, `hybridSearch` omitted `sourceId` from `searchOpts`, and Postgres/PGLite keyword/vector SQL lacked `p.source_id` filtering. Local patch branch `fix/source-scoped-query-retrieval` in `/home/chase/gbrain` fixes this and passed regression/E2E checks, but it is not upstreamed; keep it as Chase's private carried patch.
- **Hybrid query weakness:** Natural-language GBrain hybrid queries still miss non-verbatim lesson and code-location prompts even when keyword/symbol retrieval works. Example after Batch 2: natural-language queries for MLB backtest ROI / `run_mlb_sweep` aggregation / edge-threshold application often returned no results, while source-scoped keyword and symbol lookups for `roi`, `edge_threshold`, `model_dir`, `n_samples`, and `force_features` returned useful code chunks. Treat hard-facts/lesson-list/keyword route as mandatory for recommendations and prefer keyword/symbol lookups for code-location questions until hybrid retrieval improves.
- `gbrain link` cross-source resolution gap: Batch 1 and Batch 2 graph hygiene required manual links from `gameflow` plan pages to isolated code-source pages. Do not normalize ad-hoc direct `links` table inserts. Use a validated cross-source link helper or explicit source-qualified SQL until the CLI supports source-qualified endpoints.
- Frozen lesson evals after Batch 2 passed 5/5 under the enforced Hard-Facts / Critical-Invariants / lesson-tag workflow. Code pages did not pollute final authority, but eval/meta pages and recent handoffs can appear high in diagnostics. Treat this as “currently robust under workflow,” not proof that hybrid query alone is sufficient.

## Deferred Issues (from ISSUES.md)
4 of 43 total issues remain deferred:
1. **ISS-017** — Ratio column names say "l15" but compute L3/L5 (deferred to next retrain)
2. **ISS-018** — Pre-game inference requires game row to exist (needs new metadata source)
3. **ISS-020** — `validate_features=False` disables XGBoost safety (blocked on pandas 3.0 compat)
4. **ISS-023** — Stage 2 dedup keeps one stat per player per game (needs correlation-aware Kelly)

## Performance Bottlenecks
- DFS `get_dfs_lines` RPC: 9-14s on 67M+ row table
- `authenticated` role 8s timeout (workaround: SECURITY DEFINER with 30s override)

See full issue tracker: `ISSUES.md` at project root.

#issues #operations #bugs
