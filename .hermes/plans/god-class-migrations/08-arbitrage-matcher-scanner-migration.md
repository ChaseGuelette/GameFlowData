# Arbitrage Matcher / Scanner Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to change arbitrage thresholds, matching confidence, paper-trading behavior, scraping cadence, or DB schemas.

**Goal:** Rebuild the Polymarket ↔ Kalshi arbitrage pipeline into explicit components for market loading, normalization, matching, verification, opportunity calculation, storage, alerting, and paper-trading handoff.

**Architecture:** Preserve `ArbScanner`, `MarketMatcher`, `arb_scan_job.py`, and `ArbPaperTrader` call paths as compatibility facades. Extract pure match-key/normalization/scoring/opportunity math first, then source loaders and storage adapters. Keep behavior-preserving extraction separate from any future matching-quality improvements.

**Tech Stack:** Python, SQLAlchemy, pandas where used by adjacent paper-trading code, dataclasses, Polymarket/Kalshi scraper outputs, pytest.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- `infrastructure/railway-setup`

Applied facts/invariants:

1. Arbitrage scan jobs run inside the same Railway worker as production sports/Kalshi jobs; schedule/runtime changes are production-impacting.
2. No DB DDL or index work belongs in this structural migration.
3. Python backend uses Postgres-role DB access; dashboard consumers may use RLS-constrained paths.
4. Kalshi unsupported-stat and NO-side model lessons apply indirectly when arbitrage results feed paper/live workflows; do not mix strategy changes into matching extraction.
5. Non-sports scrape is intentionally separate from scan-only jobs and may handle 70k+ Polymarket markets; do not add broad re-scrapes to scan paths.

---

## Executive diagnosis

The arbitrage pipeline is smaller than feature-store/training god classes but has dense, high-risk responsibility mixing:

- `src/arbitrage/market_matcher.py`
  - 944 total lines
  - 748 non-comment LOC
  - `MarketMatcher`: 704 class lines, 12 methods
  - player props, game markets, non-sports matching, DB loaders, verified links, normalization helpers in one module
- `src/arbitrage/arb_scanner.py`
  - 455 total lines
  - 365 non-comment LOC
  - `ArbScanner`: 380 class lines, 4 methods
  - matching orchestration, arb detection, storage, pending-link queueing in one class
- `src/orchestration/arb_scan_job.py`
  - 302 total lines
  - 244 non-comment LOC
  - `run(...)`: 145 lines
  - scraping, matching/scanning, alerts, paper trading, CLI modes in one job
- `src/arbitrage/team_normalizer.py`
  - 355 total lines
  - 263 non-comment LOC
  - team alias normalization plus team extraction from slugs/questions
- `src/arbitrage/non_sports_extractor.py`
  - 435 total lines
  - 333 non-comment LOC
  - ticker/question field extraction plus structured scoring

The key risk is false-positive market matching. A single matching or scoring change can create fake arbitrage opportunities and bad paper/live evidence.

---

## Current ownership problems

### 1. `MarketMatcher` owns source loading and three matching domains

Current methods:

- `match_kalshi_markets(...)`: player props
- `match_game_markets(...)`: game-level markets
- `match_non_sports_markets(...)`: politics/crypto/economics/etc.
- `_load_kalshi_props(...)`, `_load_poly_props(...)`
- `_load_kalshi_game_markets(...)`, `_load_poly_game_markets(...)`
- `_load_kalshi_non_sports(...)`, `_load_poly_non_sports(...)`
- `_build_verified_matches(...)`
- `_load_approved_links(...)`
- `_load_kalshi_by_tickers(...)`, `_load_poly_by_condition_ids(...)`

Why this is wrong:

- DB snapshot loading and match scoring are inseparable.
- Player-prop exact/near/fuzzy logic shares a module with non-sports structured/fuzzy matching.
- Verified-link handling is buried inside non-sports matching.
- False-positive mitigations are comments and inline branches rather than testable policies.

Target owners:

- `src/arbitrage/sources/kalshi_source.py`
- `src/arbitrage/sources/polymarket_source.py`
- `src/arbitrage/matching/player_props.py`
- `src/arbitrage/matching/game_markets.py`
- `src/arbitrage/matching/non_sports.py`
- `src/arbitrage/matching/verified_links.py`

---

### 2. Match keys and confidence policy are implicit

Current player-prop matching:

- exact `(player_id, stat_type, line)`
- near line variants within tolerance
- fuzzy normalized player-name fallback with threshold `>= 0.80`
- seen-polymarket de-dup by condition ID

Current game-level matching:

- frozenset team key + market type + game date
- totals also rounded to nearest 0.5
- fallback totals matching within `NEAR_LINE_TOLERANCE`
- Kalshi ticker date preferred over poly slug date

Current non-sports matching:

- series-specific config from `KALSHI_SERIES_POLY_CONFIG`
- volume/liquidity filters
- keyword filtering
- structured extraction of `(price, direction, period)`
- fuzzy fallback threshold from config/default
- candidate-name disambiguation threshold 0.65
- placement disambiguation
- verified links appended

Target owners:

- `src/arbitrage/matching/keys.py`
- `src/arbitrage/matching/confidence.py`
- `src/arbitrage/matching/non_sports_rules.py`

Tests:

- exact/near/fuzzy player-prop fixture matches.
- cross-date game market false positive blocked.
- totals line tolerance behavior preserved.
- non-sports candidate/placement false-positive guards preserved.

---

### 3. Team normalization is duplicated across project domains

Current arbitrage normalizer:

- `src/arbitrage/team_normalizer.py`

Other project normalizers exist in:

- `src/processing/nba_linker_local.py`
- `src/processing/mlb/mlb_linker.py`
- `src/processing/mlb/mlb_linker_local.py`
- `src/processing/ncaab/ncaab_linker.py`

Why this is risky:

- Arbitrage matching depends on canonical team abbreviations.
- But linker normalizers are not necessarily safe to unify in one pass because they may have source-specific aliases.

Target owner for this lane:

- Keep arbitrage team normalization inside `src/arbitrage/normalization/teams.py` first.
- Do not merge with processing linkers until a separate alias audit exists.

---

### 4. `ArbScanner` owns matching orchestration, opportunity math, and storage

Current methods:

- `scan(...)`: match sources, detect arbs, sort, store, return result
- `_detect_kalshi_arbs(...)`: opportunity math and live/game-start filter
- `_store_opportunities(...)`: DB insert/upsert
- `_store_pending_links(...)`: DB queue for fuzzy non-sports review

Why this is wrong:

- Pure opportunity math is mixed with DB writes.
- Game-start stale-price filter lives inside opportunity detection.
- Pending-link review queue is part of non-sports matching, not scanner math.

Target owners:

- `src/arbitrage/opportunity/calculator.py`
- `src/arbitrage/opportunity/fees.py`
- `src/arbitrage/opportunity/game_state.py`
- `src/arbitrage/storage/opportunities.py`
- `src/arbitrage/storage/pending_links.py`

---

### 5. `arb_scan_job.py` owns too many pipeline steps

Current `run(...)` owns:

- pending paper-bet resolution
- optional Polymarket scrape
- scrape-only mode
- matching/scanning
- dry-run printing
- paper-trading detected opportunities
- Discord alerts
- CLI mode semantics

Why this is wrong:

- Job-level CLI should orchestrate a typed pipeline, not embed step behavior.
- Scrape-only vs scan-only vs all-categories behavior is schedule-critical.
- Paper-trading and alerts are side effects that should be adapters.

Target owners:

- `src/arbitrage/pipeline.py`
- `src/arbitrage/cli.py`
- `src/arbitrage/alerts.py`
- `src/arbitrage/paper_handoff.py`

Compatibility:

- `src/orchestration/arb_scan_job.py` remains the scheduler script entry point.

---

### 6. Non-sports extraction is a separate hidden rules engine

Current `non_sports_extractor.py` owns:

- price parsing
- percent parsing
- month/quarter/year parsing
- ticker date/price parsing
- direction parsing
- GDP country parsing
- `MarketFields`
- `extract_kalshi(...)`
- `extract_poly(...)`
- `_periods_match(...)`
- `match_score(...)`

Why this is wrong:

- Field extraction rules are critical to avoiding false positives.
- They should remain pure and heavily fixture-tested.
- Matching code should depend on extractor interfaces, not inline extraction details.

Target owner:

- Keep file initially, but move under `src/arbitrage/matching/non_sports_extractor.py` only after fixture coverage exists.

---

## Target design by responsibility

### A. `arbitrage/contracts.py`

Dataclasses:

- `MatchedMarket`
- `ArbOpportunity`
- `ScanResult`
- `MarketSnapshot`
- `MatchDecision`
- `ScanRequest`

### B. `arbitrage/sources/*.py`

DB snapshot loaders for Kalshi and Polymarket, separated by domain.

### C. `arbitrage/matching/*.py`

Pure-ish matching engines:

- `player_props.py`
- `game_markets.py`
- `non_sports.py`
- `verified_links.py`
- `keys.py`
- `confidence.py`

### D. `arbitrage/normalization/*.py`

Team/name/date/line normalization used only by arbitrage initially.

### E. `arbitrage/opportunity/*.py`

Arb calculation, fee application, stale game filtering.

### F. `arbitrage/storage/*.py`

Opportunity and pending-link persistence.

### G. `arbitrage/pipeline.py`

Orchestrates scrape/match/scan/store/alert/paper-trade flow.

### H. Compatibility facades

Keep:

- `MarketMatcher.match_kalshi_markets(...)`
- `MarketMatcher.match_game_markets(...)`
- `MarketMatcher.match_non_sports_markets(...)`
- `ArbScanner.scan(...)`
- `arb_scan_job.run(...)`

---

## Refactor phases

### Phase 0: Characterization and fixture inventory

Objective: Lock current matching and opportunity semantics before extraction.

Files:

- Create: `tests/test_arbitrage_inventory.py`
- Create: `tests/fixtures/arbitrage/` fixture JSON files as needed

Tests:

- public imports/classes work.
- current helper functions are importable.
- player-prop exact/near/fuzzy match fixture behavior characterized.
- game-level same-teams different-date false positive blocked.
- non-sports structured/fuzzy/candidate/placement behavior characterized.
- current `ArbScanner._detect_kalshi_arbs` pure and soft arb outputs characterized.

Validation:

`venv/Scripts/python.exe -m pytest tests/test_arbitrage_inventory.py -q`

---

### Phase 1: Extract contracts

Objective: Move dataclasses/types without behavior change.

Files:

- Create: `src/arbitrage/contracts.py`
- Modify: `market_matcher.py`, `arb_scanner.py` to import/re-export dataclasses
- Create: `tests/test_arbitrage_contracts.py`

Tests:

- old import paths still work.
- dataclass fields/defaults preserved.
- serialization-friendly fields remain stable for storage/alerts.

---

### Phase 2: Extract pure normalization and match-key helpers

Objective: Move helpers that do not touch DB.

Files:

- Create: `src/arbitrage/normalization/__init__.py`
- Create: `src/arbitrage/normalization/teams.py`
- Create: `src/arbitrage/normalization/text.py`
- Create: `src/arbitrage/matching/keys.py`
- Create: `tests/test_arbitrage_normalization.py`
- Create: `tests/test_arbitrage_match_keys.py`

Tests:

- team aliases match current arbitrage normalizer.
- ticker date/time extractors preserve behavior.
- line variants preserve tolerance behavior.
- game key includes game date.

Non-goal:

- Do not unify with NBA/MLB processing linkers yet.

---

### Phase 3: Extract player-prop matcher

Objective: Move player prop matching out of `MarketMatcher`.

Files:

- Create: `src/arbitrage/matching/player_props.py`
- Create: `tests/test_arbitrage_player_prop_matching.py`
- Modify: `MarketMatcher.match_kalshi_markets` to delegate.

Tests:

- exact player_id/stat/line fixture.
- near-line fixture.
- fuzzy name fallback fixture.
- duplicate Polymarket condition is not matched twice.

---

### Phase 4: Extract game-market matcher

Objective: Move game-level matching and date/team/total behavior.

Files:

- Create: `src/arbitrage/matching/game_markets.py`
- Create: `tests/test_arbitrage_game_market_matching.py`
- Modify: `MarketMatcher.match_game_markets` to delegate.

Tests:

- moneyline same-team/date match.
- totals line tolerance match.
- different-date same-teams no match.
- resolved game date preference preserved.

---

### Phase 5: Extract non-sports matcher and verified-link service

Objective: Split structured/fuzzy non-sports matching from DB loading.

Files:

- Create: `src/arbitrage/matching/non_sports.py`
- Create: `src/arbitrage/matching/verified_links.py`
- Create: `tests/test_arbitrage_non_sports_matching.py`
- Modify: `MarketMatcher.match_non_sports_markets` to delegate.

Tests:

- series config scoping preserved.
- volume/liquidity filters preserved.
- keyword filtering preserved.
- candidate-name false positives rejected.
- placement disambiguation preserved.
- verified links appended with live prices.

---

### Phase 6: Extract source loaders

Objective: Move DB queries behind source adapters after matchers are pure-tested.

Files:

- Create: `src/arbitrage/sources/__init__.py`
- Create: `src/arbitrage/sources/kalshi_source.py`
- Create: `src/arbitrage/sources/polymarket_source.py`
- Create: `tests/test_arbitrage_sources_sql.py`
- Modify: `MarketMatcher` to compose source adapters.

Tests:

- most-recent snapshot query behavior preserved.
- Polymarket player-prop 3-day lookback preserved.
- Kalshi game-market target_date±window behavior preserved.
- non-sports category/liquidity loading preserved.

DB safety:

- No schema changes.
- No new broad queries beyond current scrape/scan behavior.

---

### Phase 7: Extract opportunity calculator and storage

Objective: Move arb math and DB writes out of `ArbScanner`.

Files:

- Create: `src/arbitrage/opportunity/__init__.py`
- Create: `src/arbitrage/opportunity/calculator.py`
- Create: `src/arbitrage/opportunity/game_state.py`
- Create: `src/arbitrage/storage/opportunities.py`
- Create: `src/arbitrage/storage/pending_links.py`
- Create: `tests/test_arbitrage_opportunity_calculator.py`
- Create: `tests/test_arbitrage_storage_sql.py`

Tests:

- pure and soft arb calculations preserved.
- Kalshi/Polymarket fees preserved.
- game-start stale filter preserved.
- DB insert/update columns preserved.
- pending-link storage preserved for fuzzy non-sports.

---

### Phase 8: Extract pipeline and CLI adapters

Objective: Shrink `arb_scan_job.py` to CLI + pipeline call.

Files:

- Create: `src/arbitrage/pipeline.py`
- Create: `src/arbitrage/cli.py`
- Create: `src/arbitrage/alerts.py`
- Create: `src/arbitrage/paper_handoff.py`
- Create: `tests/test_arbitrage_pipeline.py`
- Modify: `src/orchestration/arb_scan_job.py`

Tests:

- scrape-only exits after scrape.
- skip-scrape scan path preserved.
- skip-paper behavior preserved.
- dry-run print path preserved.
- alert sending can be skipped.
- paper resolution and placement failures remain non-fatal where current behavior is non-fatal.

---

### Phase 9: Anti-regrowth guards

Recommended thresholds after extraction:

- `market_matcher.py` under 300 non-comment LOC.
- `arb_scanner.py` under 220 non-comment LOC.
- `arb_scan_job.py` under 120 non-comment LOC.

Guards:

- no DB queries in pure matching modules.
- no opportunity DB writes in calculator module.
- no scrape execution in scanner class.
- no fuzzy-threshold constants duplicated across modules.

---

## Files likely touched

Existing:

- `src/arbitrage/market_matcher.py`
- `src/arbitrage/arb_scanner.py`
- `src/arbitrage/team_normalizer.py`
- `src/arbitrage/non_sports_extractor.py`
- `src/orchestration/arb_scan_job.py`
- `src/paper_trading/arb_paper_trader.py` only as handoff adapter later

New:

- `src/arbitrage/contracts.py`
- `src/arbitrage/sources/*.py`
- `src/arbitrage/matching/*.py`
- `src/arbitrage/normalization/*.py`
- `src/arbitrage/opportunity/*.py`
- `src/arbitrage/storage/*.py`
- `src/arbitrage/pipeline.py`
- `src/arbitrage/cli.py`
- `src/arbitrage/alerts.py`
- `src/arbitrage/paper_handoff.py`

Tests:

- `tests/test_arbitrage_inventory.py`
- `tests/test_arbitrage_contracts.py`
- `tests/test_arbitrage_normalization.py`
- `tests/test_arbitrage_match_keys.py`
- `tests/test_arbitrage_player_prop_matching.py`
- `tests/test_arbitrage_game_market_matching.py`
- `tests/test_arbitrage_non_sports_matching.py`
- `tests/test_arbitrage_sources_sql.py`
- `tests/test_arbitrage_opportunity_calculator.py`
- `tests/test_arbitrage_storage_sql.py`
- `tests/test_arbitrage_pipeline.py`

---

## Validation commands

Inventory/contract baseline:

`venv/Scripts/python.exe -m pytest tests/test_arbitrage_inventory.py tests/test_arbitrage_contracts.py -q`

Pure matching phases:

`venv/Scripts/python.exe -m pytest tests/test_arbitrage_normalization.py tests/test_arbitrage_match_keys.py tests/test_arbitrage_player_prop_matching.py tests/test_arbitrage_game_market_matching.py tests/test_arbitrage_non_sports_matching.py -q`

Source/storage phases:

`venv/Scripts/python.exe -m pytest tests/test_arbitrage_sources_sql.py tests/test_arbitrage_storage_sql.py -q`

Opportunity/pipeline phases:

`venv/Scripts/python.exe -m pytest tests/test_arbitrage_opportunity_calculator.py tests/test_arbitrage_pipeline.py -q`

Lane-wide:

`venv/Scripts/python.exe -m pytest tests -k "arbitrage or arb_" -q`

Compile:

`venv/Scripts/python.exe -m py_compile src/arbitrage/*.py src/arbitrage/sources/*.py src/arbitrage/matching/*.py src/arbitrage/normalization/*.py src/arbitrage/opportunity/*.py src/arbitrage/storage/*.py src/orchestration/arb_scan_job.py`

Diff hygiene:

`git diff --check -- src/arbitrage src/orchestration/arb_scan_job.py tests .hermes/plans/god-class-migrations/08-arbitrage-matcher-scanner-migration.md`

---

## Risk controls / non-goals

Non-goals:

- Do not change arb thresholds.
- Do not change matching confidence thresholds.
- Do not change non-sports series config.
- Do not change scrape cadence or scheduler jobs.
- Do not change DB schema.
- Do not auto-approve fuzzy links.
- Do not unify all project team normalizers in this lane.
- Do not change paper-trading stake/placement behavior.
- Do not add live trading execution.

Hard rules:

- False-positive prevention is more important than recall during extraction.
- Existing CLI modes remain stable.
- Scrape-only and skip-scrape semantics remain stable.
- Storage writes remain idempotent/current behavior.
- Any matching-quality improvement must be a separate behavior-changing PR with before/after fixtures.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A fuzzy match is actually a dashboard-reviewed pending link, not an opportunity input.
2. A verified link source has different freshness semantics than live source loaders.
3. A market type has hidden line/date/team matching policy.
4. A normalized team alias differs from sportsbook/linker aliases.
5. A non-sports series requires custom extraction not covered by generic fields.
6. A paper-trading handoff consumes incidental opportunity columns.
7. A Discord/dashboard consumer depends on output schema/order.
8. A DB loader has a time-window assumption not represented in tests.
9. A behavior-changing false-positive fix appears; split it from extraction.
10. A parity guard is needed between old and new matcher outputs before deleting facade logic.

Progress log entries must distinguish: contract extracted, pure matcher created, source adapter introduced, scanner delegates, storage adapter introduced, pipeline delegates, old duplicate removed, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with pure contracts and normalization/match-key fixtures:

1. Add `tests/test_arbitrage_inventory.py` with import and fixture characterization.
2. Extract `contracts.py` while re-exporting old dataclasses.
3. Extract normalization/text/key helpers with parity tests.
4. Do not move DB loaders, scanner storage, scrape orchestration, or paper-trading handoff yet.

This creates a safe seam around false-positive prevention before touching source loading or opportunity storage.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- AST/method inventory for `arb_scanner.py`, `market_matcher.py`, `team_normalizer.py`, `non_sports_extractor.py`, and `arb_scan_job.py`.
- Targeted reads of player-prop, game-level, and non-sports matching code.
- Targeted reads of `ArbScanner.scan(...)`, `_detect_kalshi_arbs(...)`, and `arb_scan_job.run(...)`.
- Callsite scan across `src`, `scripts`, and `tests` for arbitrage symbols and team normalizers.
- GBrain hard facts, critical invariants, and Railway setup.

Current status:

- Documentation only.
- No production code changed.
- No scrape, scan, DB query, alert, or paper-trading action run.

---

## Done when

- Market loading, matching, opportunity calculation, storage, alerting, and paper handoff have separate owners.
- Player-prop, game-level, and non-sports matchers are fixture-tested.
- False-positive guards are explicit and covered.
- `MarketMatcher`, `ArbScanner`, and `arb_scan_job.py` are compatibility facades.
- Existing scheduler/CLI behavior remains stable unless a separate approved schedule or strategy change is made.
