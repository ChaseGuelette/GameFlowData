# NBA Data Readiness Audit — Combined Assessment

**Date:** 2026-09-24  
**Status:** Complete read-only audit; remediation not started  
**Overall verdict:** **RED — not ready for a dependable 2026–27 NBA customer pick path**  
**Plan authority:** `docs/operations/nba-core-product-restart-plan.md`

## Executive assessment

The historical NBA data foundation is substantially better than a simple “stale system” diagnosis would suggest. Core basic box scores, dimensions, rolling averages, player IDs, and historical prediction samples are present; key feature contracts are centralized; prior-game basic rolling logic correctly uses season-scoped `shift(1)`; and 277 focused tests across the four audit lanes passed.

The restart is nevertheless blocked by a connected set of season-rollover, feature-parity, linkage, and temporal-integrity defects:

1. Production-safe NBA ingestion and scheduled inference still hard-code the completed 2025–26 season (`22025`).
2. Opening-night player discovery depends on recent derived rows rather than durable roster/schedule truth, has no target-date upper bound, and cannot reliably handle rookies, signings, trades, or the offseason gap.
3. Single-date and date-range feature modes do not match the stable feature contract or serving semantics.
4. Current future props and game lines are not linked to internal NBA game IDs, so they cannot flow into inference and edge selection.
5. Training, backtesting, production inference, edge refresh, and injury context do not share a reproducible decision-time contract. Multiple paths can use later or post-commence information.
6. Prediction outputs do not preserve the selected quote, decision timestamp, artifact identity, or revision history needed to reproduce a displayed recommendation.
7. The dashboard reads the giant append-only raw props table directly with weak bounds and client-side deduplication.

This is not primarily a “run the scrapers and retrain” problem. The minimum safe sequence is to repair season/slate identity, roster/player discovery, feature-mode parity, quote/linkage contracts, and temporal integrity before customer-facing picks or meaningful model training resume.

## Audit artifacts

- Source data: `docs/operations/audits/nba-source-data-audit-2026-09-24.md`
- Derived features: `docs/operations/audits/nba-derived-feature-audit-2026-09-24.md`
- Odds and linkage: `docs/operations/audits/nba-odds-linkage-audit-2026-09-24.md`
- Temporal integrity: `docs/operations/audits/nba-temporal-integrity-audit-2026-09-24.md`

The lane reports contain exact SQL, commands, paths, functions, bounded counts, caveats, and evidence timestamps.

## Evidence quality

### Verified strengths

- Core historical NBA team and player game keys are duplicate-free in the audited windows.
- Basic box-score and basic rolling tables are populated through the 2025–26 Finals.
- Basic rolling generation is grouped by entity and season and applies `shift(1)`.
- The eight-minute rule is correctly applied to basic rolling-stat values while schedule counters still count appearances.
- Stable feature contracts currently contain 33 minutes, 31 points-rate, 30 rebounds-rate, 30 assists-rate, and 32 threes-rate features, with 74 unique features.
- Training and single-player loaders can emit the full 74-feature stable contract in bounded historical smokes.
- Player IDs were fully linked in the bounded current future-prop sample.
- The bounded current quote sample had paired Over/Under rows.
- The latest historical prediction run had complete prediction-to-sample linkage with 10,000 samples per row.
- Empirical Monte Carlo probability semantics remain present in the backend.

### Important limitations

- There is no real 2026–27 game-performance slate yet, so first-game insertion, derived refresh, feature variation, and inference output cannot be certified against live new-season results.
- Railway deployment identity was not independently verified; `job_executions` proves activity, not the exact deployed commit.
- Paid source-provider availability and credentials were not exercised.
- No production data, schema, deployment, model, or index was changed.
- Counts against giant odds tables were bounded by IDs/dates; no broad full-table count was attempted.

## Readiness by layer

| Layer | Status | Assessment |
|---|---|---|
| Historical basic source data | **AMBER** | Structurally sound, but season rollover is hard-coded and opening-night proof is absent. |
| Schedule/roster/slate identity | **RED** | No durable NBA schedule/roster table; player discovery is derived-row based and unsafe at season open. |
| Advanced source coverage | **AMBER/RED** | Incomplete and workstation-only; 4,213 audited basic rows lacked advanced rows, including 230 active-player rows. |
| Basic rolling features | **GREEN historically** | Season-scoped prior-game logic and eight-minute behavior passed. |
| Advanced rolling features | **AMBER** | Advanced rolls do not apply the documented eight-minute mask. |
| Feature contract parity | **RED** | Date/date-range modes omit four same-position injury features and differ in defaults/spread semantics. |
| Season-opening feature behavior | **RED** | Prior-season rows can leak into opening-night inference; player discovery fails real-time cold start. |
| Prop ingestion | **AMBER** | Active and paired in the sample, but scraping far-future events at high cadence creates major bloat. |
| Player linkage | **GREEN in bounded sample** | Current sampled prop rows had player IDs. |
| Game/team linkage | **RED** | 0/500,169 newest prop rows had internal `game_id`; team assignment logic can select the home team for away players. |
| Quote temporal safety | **RED** | No unified decision-time and pre-commence selection contract. |
| Injury temporal safety | **RED for historical use** | Latest same-day status is selected without observation-time cutoff or version identity. |
| Backtest integrity | **RED** | Fixed date cutoff without commence guard; selected quote/decision metadata is not saved. |
| Prediction reproducibility | **RED** | Quote IDs/timestamps, decision time, artifact identity, and revision history are missing. |
| Automation observability | **AMBER** | Jobs run, but no-op success is not distinguished from output success and stored failures are truncated. |
| Frontend quote consumption | **RED** | Direct weakly bounded raw-table reads and client-side deduplication are not safe. |
| 2026–27 model readiness | **DEFERRED** | Existing models are templates only; no useful new-season retrain is justified yet. |

## Consolidated P0 blockers

### P0-1 — Season rollover is not implemented safely

Evidence:

- `src/scrapers/nba_cdn_scraper.py` hard-codes `DEFAULT_SEASON_ID = "22025"` and `DEFAULT_SEASON = "2025-26"`.
- `src/models/features/nba/inference_loader.py` returns `season_id = "22025"` for scheduled context.
- Existing tests encode the old value.

Impact:

- New-season box scores, rolling cohorts, diagnostics, and scheduled inference can be mislabeled as 2025–26.

Required outcome:

- One tested season resolver derived from authoritative schedule/game dates and used consistently by ingestion, processing, feature construction, persistence, and tests.

### P0-2 — Opening-night player and roster discovery is not viable

Evidence:

- `DailyPredictionRunner._get_players_for_games()` discovers players from recent `player_average_game_stats` rows.
- It requires rows within the prior 30 days and has no upper bound at the target date.
- Historical opener probing selected future rows; real opening night would have no current-season rows after the offseason gap.

Impact:

- Established players, rookies, signings, and pre-first-game trades can be omitted or attached to stale teams.

Required outcome:

- Define authoritative future-slate and roster/player eligibility sources as of the decision time. Join historical derived values only after candidate identity is established.

### P0-3 — Feature modes are not contract- or semantics-equivalent

Evidence:

- Date and date-range loaders omit:
  - `team_out_same_pos_count`
  - `team_out_same_pos_min_sum`
  - `team_out_same_pos_usg_sum`
  - `team_out_same_pos_starter_sum`
- Date-range defaults differ from other modes.
- Date-range spread direction differs.
- Date-range loading can silently continue after partial chunk failures.

Impact:

- Future artifacts using the stable contract can fail at prediction time or evaluate against different semantics than serving.

Required outcome:

- One cross-mode parity contract with identical ordered feature membership, defaults, directionality, completeness behavior, and season-opening policy.

### P0-4 — Season-boundary feature behavior is accidental

Evidence:

- Derived rolling generation correctly resets by season.
- Single-player source queries retrieve the latest row before the target date without a season boundary.
- In the sampled prior opener, 36/37 qualifying players resolved to a previous-season row.

Impact:

- Opening-night predictions silently carry prior-season values even though the source pipeline models a season reset.

Required outcome:

- Choose and encode one explicit cold-start policy:
  - strict current-season defaults;
  - approved prior-season carryover features with indicators;
  - or a documented hybrid.

The policy must be identical in training, historical replay, scheduled inference, and production serving.

### P0-5 — Current props cannot join to internal NBA games

Evidence:

- 0/500,169 newest bounded prop rows had internal `game_id`.
- 0/100,001 newest bounded game-line rows had `nba_game_id`.
- The incremental lookup covers only a short future window while the scraper is collecting much farther-future events.
- The linker attempts `stats.nba.com` before CDN fallback and can assign the home team ID to away-player props.

Impact:

- Current props cannot enter the `(player_id, game_id, stat)` inference/edge path.

Required outcome:

- CDN-only bounded future schedule lookup, exact matchup/date linkage, and fail-closed team assignment. No guessed home-team fallback.

### P0-6 — No unified decision-time contract exists

Affected surfaces:

- training line features;
- injury features;
- date-range/backtest quotes;
- production inference quotes;
- edge refresh quotes;
- saved prediction state.

Evidence:

- Production inference ranks latest matching quotes without decision or commence cutoffs.
- Edge refresh uses a recent-24-hour filter but lacks a pre-commence guard.
- NBA backtests use a fixed date-time cutoff without a commence guard.
- Training game-line features aggregate without a timestamp cutoff.
- Historical injuries select same-day latest records without observation-time semantics.

Impact:

- Training and replay can learn from later information.
- Displayed recommendations can change after the intended decision time or after tipoff.
- Existing historical ROI evidence is not promotion-grade.

Required outcome:

Define one timezone-aware `decision_time` policy and require:

- effective quote time `<= decision_time`;
- snapshot time `<= decision_time`;
- quote/update time `< commence_time`;
- paired Over/Under selection from a reproducible quote state;
- injury observation time `<= decision_time` for historical construction;
- no new recommendation or paper bet after the cutoff.

### P0-7 — Displayed decisions cannot be reproduced

Evidence:

- Predictions do not preserve selected raw quote IDs, quote timestamps, market/book updates, decision time, artifact ID/hash, feature as-of time, or refresh-run ID.
- Prediction upserts overwrite the only row for `(prediction_date, player_id, game_id, stat)`.
- Backtest outputs omit artifact and temporal-policy identity.

Impact:

- A customer-visible edge cannot be reconstructed after raw data or a refresh changes.

Required outcome:

- Persist immutable decision evidence or append-only revisions with exact model, feature, quote, time, and policy identity.

### P0-8 — Frontend raw-prop consumption is unsafe

Evidence:

- Dashboard paths query `raw_player_props_combined` directly without one server-side latest-paired pregame quote contract.
- Analysis history is deduplicated/sorted client-side.
- Sportsbook availability reads have weak bounds.
- A bounded quote reconstruction query timed out.

Impact:

- Pages can show stale/incomplete rows, hit PostgREST limits, disagree with backend recommendation semantics, or become slow as the raw table grows.

Required outcome:

- Replace raw-table browser reads with one bounded authenticated API/RPC/view contract returning latest valid paired quotes for explicit keys.

## Consolidated P1 findings

1. Advanced game-stat coverage is incomplete and Railway correctly cannot repair it because advanced scraping remains workstation-only.
2. `game_id_map` is stale/incomplete and must not be assumed to be a current authority without a consumer audit.
3. 170 injury rows are unlinked to players.
4. Advanced rolling features do not apply the documented eight-minute rule.
5. Line-context features were all-zero/default in tested local date slates and are temporally unsafe where present.
6. Only three books appeared in the current bounded prop sample.
7. Combo predictions lacked selected lines and edges in the latest historical inference run.
8. Far-future props are scraped every ten minutes, producing about 500,000 bounded rows for three opening-night events well before they are actionable.
9. Lines and edge-refresh runtimes consume much of or exceed their cadence.
10. Automation status does not distinguish a healthy no-op from a run that produced expected rows.
11. Stored daily-job failure excerpts are too truncated to identify the failed step.
12. Raw props permit anonymous reads, broader than the authenticated-client boundary.
13. Local position history trails remote by one day; this affects local training truth, not remote production truth.

## Parent analysis: what needs to happen

### Decision 1 — Separate “system ready” from “model ready”

The data and frontend systems can be repaired before new models are available. Customer-facing recommendations should remain disabled or clearly non-production until a future candidate is trained and certified. Existing artifacts may be used only for loader/schema smoke tests and shadow plumbing—not as trusted 2026–27 picks.

### Decision 2 — Do not retrain now

There is no new 2026–27 result data. Retraining the same architecture against the same uncorrected history would only create newer artifact timestamps while preserving:

- line-feature leakage;
- injury as-of leakage;
- unclear late-season/rest-game cohort policy;
- backtest quote-integrity defects;
- season-opening semantic mismatch.

Near-term model work should be limited to extracting templates, proving the harness can smoke-run, and defining the clean future dataset/policy.

### Decision 3 — Define the historical cohort before future training

Playoffs/play-in should be explicitly separated from regular-season training rather than mixed by convenience. The late regular season should not be excluded by an arbitrary “last X games” rule. Profile and preregister a deterministic rule using evidence such as:

- starter participation rate;
- starter minutes and minute volatility;
- inactive/rest designations;
- rotation continuity;
- team elimination/seed-lock context if available without leakage;
- distribution shift versus the main regular season.

The output should be a reproducible inclusion/exclusion manifest, not a manual date list hidden in a command.

### Decision 4 — Build around opening-night reality

The first operational model problem is not calibration; it is identity and cold start. We need to know:

- which games exist;
- which players are eligible for each team;
- which players changed teams;
- which historical values are allowed to carry across seasons;
- which values must default;
- how those policies are represented in training.

Until that contract exists, an apparently successful inference run can silently omit the exact players customers care about.

## Recommended remediation program

### Work Package 1 — Season/slate/roster foundation

**Goal:** Make a 2026–27 slate and eligible-player set deterministic before any prediction work.

Code-only first slice:

- implement/test one NBA season resolver;
- remove literal `22025`/`2025-26` from active ingestion and scheduled inference;
- define the canonical future-slate source;
- define the canonical roster/player-eligibility source;
- add target-time upper bounds;
- add fixtures for opener, rookie, signing, trade, and first-game cases;
- add a read-only preseason canary.

No remote data mutation is required for the first slice.

### Work Package 2 — Decision-time and evidence contract

**Goal:** Establish one temporal policy before repairing individual quote consumers.

Specify and test:

- timezone-aware `decision_time`;
- commence guard;
- effective quote timestamp policy;
- paired quote identity;
- injury observation timestamp/version;
- append-only decision/revision identity;
- artifact and feature-as-of identity;
- cutoff behavior for inference, edge refresh, paper capture, historical replay, and frontend display.

This is a contract/specification and focused code-test slice before any historical backfill or retraining.

### Work Package 3 — Cross-mode feature parity and cold start

**Goal:** Make training, date, date-range, and single-player inference produce one semantic feature contract.

Required repairs:

- add the four same-position fields to date/date-range modes;
- align missing-source defaults;
- align spread direction;
- fail closed on incomplete date-range chunks;
- encode the chosen season-opening carryover/default policy;
- decide and test the eight-minute rule for advanced rolling values;
- add parameterized parity tests across all modes.

### Work Package 4 — CDN-only game/team linkage

**Goal:** Convert current future props into safely linked game/player/team quote candidates.

Required repairs:

- remove the `stats.nba.com` attempt from the Railway linker path;
- derive a bounded future schedule window from observed prop commence dates;
- link exact normalized matchup/date pairs;
- never infer away-player team identity from the home team;
- leave ambiguous rows unlinked;
- add one-event dry-run/read-only evidence before any approved write;
- separately decide whether stale `game_id_map` remains necessary.

### Work Package 5 — Quote-safe inference, refresh, and historical replay

**Goal:** Apply the decision-time contract to every active consumer.

Required repairs:

- line features in training and serving;
- production selected lines;
- edge refresh;
- injury context;
- NBA backtest quote selection;
- saved quote and decision metadata;
- post-commence negative tests;
- stop new recommendations after cutoff.

Existing artifacts that used unsafe line/injury features remain templates only.

### Work Package 6 — Frontend bounded consumer

**Goal:** Remove direct browser dependency on the giant raw props table.

Required repairs:

- create/use one bounded authenticated quote consumer;
- return latest valid paired pregame quotes by explicit player/game/market keys;
- make frontend probabilities and edges use persisted backend semantics rather than independent quantile interpolation;
- add row limits, freshness states, empty states, and stale-state handling;
- remove anonymous raw-table access after an explicit RLS review/approval;
- hide unsupported routes while the MVP is restored.

### Work Package 7 — Automation observability and canary

**Goal:** Prove output, not merely process completion.

Add per-run metrics:

- events found;
- games inserted;
- player rows inserted;
- rolling rows upserted;
- props fetched;
- rows linked;
- predictions generated;
- predictions with valid paired lines;
- recommendations produced;
- explicit no-op reason.

Run a read-only preseason canary, followed by a one-slate shadow path once real 2026–27 games are available.

## Approval boundaries

### Can proceed as code/test/spec work without DB mutation approval

- season resolver and boundary tests;
- player-discovery/roster contract design;
- cross-mode parity tests and code fixes;
- decision-time contract and negative tests;
- linker dry-run/read-only code path;
- bounded frontend consumer code;
- richer job metrics;
- report/docs updates.

### Requires Chase approval before execution

- remote or local backfills/updates;
- injury relinking writes;
- `game_id_map` repairs;
- advanced-stat catch-up;
- schema changes or new schedule/roster tables;
- RLS changes;
- indexes or keys;
- broad local sync;
- deployment;
- long training, sweep, or historical replay.

## Recommended immediate next action

Start with **Work Package 1: Season/slate/roster foundation**, while drafting the **Work Package 2 decision-time contract** in parallel. These two contracts govern almost every downstream repair and prevent reworking feature, quote, and frontend code twice.

The first implementation should be code/tests only and should not mutate production data. After scoped tests pass, run a read-only preseason canary and present the evidence before requesting any production write or deployment approval.

## Spot verification performed by the parent agent

After receiving the lane reports, the parent agent independently:

- read all four report files;
- confirmed the reports exist at the documented paths;
- ran `git diff --check` on the new plan/audit paths successfully;
- spot-checked the target-date-unbounded player-discovery query in `src/models/daily_runner.py`;
- spot-checked latest-snapshot query patterns in NBA backtest/refresh paths;
- confirmed the four same-position feature names occur in contracts/training/injury paths but not the date/date-range loader files;
- confirmed dashboard references to `raw_player_props_combined` remain in protected dashboard and analysis paths.

The detailed database counts remain delegated read-only SQL evidence and are preserved verbatim in the lane reports and their referenced SQL transcripts.
