# NBA Core Product Restart Plan — 2026–27 Season

## Training-policy research — November 3, 2026

Read the [historical model/policy research](nba-training-policy-research-2026-11-03.md) before choosing the next training baseline. It distinguishes old owner decisions from incidental defaults, identifies March train/calibration overlap and a playoff calendar-year/OOS contradiction, and proposes explicit cohort, cold-start, trade and source policies. Proposed windows/policies are not approved. Current recovery and Slice 8 coding completion are reconciled in [the readiness assessment](nba-training-readiness-reconciliation-2026-11-03.md); older operational checkpoints below are historical and must not restart completed work. No new training, DB work or implementation is authorized by this research.

## Data/incident checkpoint — November 2, 2026 UTC

The props sync completed and [checks 1–2](nba-post-sync-checks-2026-11-02.md) were executed: bounded transfer parity passed; historical coverage/linkage and archive inclusion still have blockers. Railway authentication is restored. The owner's CDN catch-up is verified through November 1 (90 games), but derived averages remain at June 13 because the remote Daily Stats pipeline has not completed. The existing linker's bounded `--limit`/`--dry-run` repair is applied locally and passes six existing tests, 45 controlled checks and zero introduced Pyright errors; it is not committed or deployed. A read-only latest-1,000 preview proposed zero fills, but future-fixture identities remain unverified, not certified wrong or historically reconciled. The duplicate-tool draft is paused/unintegrated. Follow [the incident evidence](nba-reconciliation-incident-2026-11-02.md); focus next on restoring and verifying the existing remote scraper/derived/linker/sync automation, not assigning routine manual jobs to Chase. No agent remote write, deployment, archive transfer, pruning or training was performed. Historical S1/S2 gates remain separate.

## Resume checkpoint — November 1, 2026

Wrap-up: Chase confirmed **coding slices 1–2 completed** and **coding slice 3 in progress** in `repair/nba-flat-replay`; do not relaunch that lane. Source integration of slices 1–2 is still pending: main commit `10d9c22` contains docs and sync changes but no slice 1–2 source/helpers/tests. Preserve the verified `nba-cli-preflight` worktree and reconcile its CLI changes with slice 3 before any separately approved integration. Historical S1–S5 remains a distinct gate sequence.

Slices 1–2 are scoped-implementation verified in `repair/nba-cli-preflight`: slice 2 passed independent spec/quality review, 72 controlled checks and all 117 existing scoped tests after separately owner-approved fixture adaptations; no new cases or changed assertions. This lane remains uncommitted/unintegrated; real-data and platform limitations are recorded separately. Slice 4's pure primitives were separately approved and merged into main as `f81170f`, without consumer wiring or historical policy activation. Read [slice 2 evidence](nba-artifact-slice2-2026-11-01.md), the [artifact wiki](../understanding/nba-artifact-identity.md) and [all eight coding slices](../understanding/nba-training-evaluation-slices.md). S1 changes, model runs, this lane's integration and later slices remain gated.

Chase is repairing the failed NBA sync in another chat. Do not restart or compete with it. The October 16 waiting instructions below are historical; the unattended S2 audit stopped at a failed sync, not a completed data audit.

Read [next steps and parallel work](nba-restart-next-steps-2026-11-01.md) for this chat's decision summary. After a verified stable sync: complete S1 real-data acceptance and S2 source evidence, obtain explicit policy approval, then separately scope S3 historical parity, S4 quote-clean replay and S5 owner-launched training. S1 integration remains separately gated.

Chase has redirected this lane toward coding readiness: mirror the canonical MLB lifecycle/harness contracts and complete NBA training/evaluation plumbing while the other chat repairs sync. Read the [coding readiness plan](nba-coding-readiness-plan-2026-11-01.md), which supersedes the earlier documentation-first recommendation. Recommended first source slice: DB-free training/replay CLI, dry-run and preflight; then artifact gates, flat-first replay, policy-ready shared selectors, sweeps, audits and lifecycle orchestration. Exact multi-file scopes still require approval; historical source policy and real-data validation remain gated. Keep chat decisions and unresolved approvals in linked Markdown summaries for Chase.

## Kick off immediately — current handoff, October 16, 2026

- You: let the already-running [NBA-only sync](nba-sync-inventory-2026-10-16.md) finish; do not start a second/all-sports sync. Real-data readiness remains open until its completion summary is verified.
- Other chat: S1 is delivered with the owner-approved `_run_date` extension; preserve its worktree pending [coordinator review/acceptance](nba-s1-coordinator-review-2026-10-16.md). No integration yet.
- This chat: own S2 read-only source/timestamp research, policy recommendations and independent review. No concurrent writes to S1's files.
- You: review the S1 coordinator packet, [wiki](../understanding/nba-rebuild.md) and [research report](nba-historical-readiness-2026-10-16.md) while independent review progresses. Do not relaunch S1 or training.

### Current checkpoint, overriding stale next-action text

I’m now arranging an unattended read-only audit: wait for the successful NBA sync, verify the historical evidence, and leave Chase a reviewable summary before any implementation decision.

Chase authorized three-minute local sync checks and the full S2 audit while away. Follow [watch status](nba-s2-watch-status-2026-10-16.md) and [audit progress](nba-s2-audit-progress-2026-10-16.md); the return summary will distinguish research completion from unresolved source/policy signoff. Every review section starts with a plain-English purpose/conclusion. No S3/training/repair/integration authorization is implied.

S1 independent existing checks and controlled probes passed; real-data/artifact acceptance awaits the running sync. S2 [source-clock research](nba-s2-source-clock-review-2026-10-16.md) is published; refreshed-data reconciliation and policy approval remain open. Do not interpret source-only progress as training or integration readiness.

- Layer 1 season resolver is committed as `23f335c`. Layer 2 schedule code/migration/preflight is committed in `d478458`; no longer describe it as uncommitted. Migration application, Railway canary, ingest/deployment and production readiness were not verified by the October 16 historical-data package.
- Three initial agents plus a targeted follow-up completed: data sample, current-code blockers and reconciled repair specification. The 12 missing sampled advanced/rolling keys were all zero-minute DNPs; all 202 sampled appearances with at least five minutes were covered. This does not certify whole historical periods.
- Historical training readiness and current-slate publication readiness are separate gates. Source/contract/cohort repairs can justify a historical retrain without waiting for new-season results; frontend/Engineering OS and current-roster deployment are not automatic training dependencies.
- References below to fixtures/tests are verification requirements, not permission to create new unit tests. Chase's current instruction is no new unit tests; preserve existing coverage and use bounded real-path/data checks.
- This is a documentation/research checkpoint, not blanket approval for source edits, DB work, training, deployment or promotion. Chase can review packets asynchronously; material decisions and implementation remain explicitly gated.

**Status:** Active — initial September audit and October sampled verification complete; historical training readiness still blocked by source/policy/parity/completeness work; rebuild Layers 1–4 remain the approved product design direction
**Effective date:** 2026-09-24
**Last reconciled:** 2026-10-16
**Owner:** Chase
**Primary goal:** Restore a dependable NBA data/model pipeline and ship the minimum viable customer frontend.

## Session resume anchor

When resuming GameFlow work, read this file after:

1. `AGENTS.md`
2. the latest GBrain handoff
3. `operations/critical-invariants`
4. `models/nba-model`

Current direction:

- Engineering OS is paused. Do not expand, certify, or promote MVP 1+ unless Chase explicitly reactivates it.
- Preserve the existing read-only Engineering OS deployment if it remains harmless; do not spend core-product time dismantling it.
- NBA core-product readiness is the active priority.
- The September 24 audit was performed before new-season performance evidence was available. Do not treat that dated observation as a current source-data check; verify actual coverage for any proposed run.
- Existing production models are outdated reference artifacts. Use them as templates for feature contracts, training configuration, artifact structure, and runtime integration—not as assumed production candidates.
- Do not retrain every existing model merely against unchanged history. Training resumes when an approved data/feature-contract/cohort repair or sufficient new-season evidence gives the run a meaningful purpose, with frozen splits and a trustworthy validation path.
- The combined readiness assessment is `docs/operations/nba-data-readiness-audit-2026-09-24.md`; its four lane reports live under `docs/operations/audits/`.
- Chase approved rebuild Layers 1–4 below as the design direction. Implement each layer as a separately reviewed slice.

## Active scope

1. Audit NBA source data, derived features, odds data, and temporal integrity.
2. Repair the minimum daily data path needed for NBA inference and the frontend.
3. Extract the incumbent model contracts and training templates.
4. Define a clean historical training cohort for future runs.
5. Restore a minimum viable NBA frontend.
6. Run safe pipeline/inference shadow checks; no live-money promotion is implied.

## Explicit non-goals

- Engineering OS expansion or certification.
- New model architectures or speculative feature families.
- Immediate full retraining using unchanged historical data.
- Treating old model ROI or calibration claims as current readiness evidence.
- Stripe activation, DFS, Ask AI, secondary sports, or broad dashboard restoration.
- Broad database cleanup, unbounded backfills, or opportunistic indexing.
- Advanced `stats.nba.com` scraping on Railway.

## Governing invariants

- Never deploy global conformal recalibration offsets.
- Q10 behavior is edge-bearing; do not blindly recalibrate it.
- Probabilities use empirical CDF: `(samples > line).mean()`.
- Preserve quantile monotonicity.
- Rolling features use prior games only (`shift(1)`).
- Railway NBA stats collection remains CDN-only.
- Never run non-concurrent `CREATE INDEX` on `raw_player_props_combined`.
- Main-context agents do not call Supabase MCP directly; database work uses the isolated SQL-runner pattern.
- Long training, sweeps, and broad backfills are Chase-launched after dry-run/preflight.

---

## Approved rebuild direction — Layers 1–4

**Decision date:** 2026-09-24  
**Owner approval:** Chase approved the four-layer direction and separate implementation slices.

The order is deliberate. Season identity and durable slate/roster authority must exist before feature parity tests can encode correct opening-night behavior.

### Layer 1 — Season identity and rollover

**Goal:** Remove stale season literals and establish one runtime authority for NBA season identity.

Required outcomes:

- implement one tested NBA season resolver;
- remove active `22025` / `2025-26` literals from CDN ingestion and scheduled inference;
- derive season identity from authoritative game/schedule context rather than the newest stored derived row;
- add boundary tests for preseason, regular season, playoffs/play-in where applicable, and the following season;
- keep historical training CLI defaults separate from runtime resolution—a historical default is not automatically a production rollover bug;
- preserve CDN-only Railway behavior.

This is the first implementation slice and should require code/tests only, with no production data mutation.

### Layer 2 — Durable NBA schedule authority

**Goal:** Make game/slate identity deterministic and independent of sportsbook-event discovery or recent player rows.

Approved direction:

- add a durable `nba_game_schedule`-style table after schema/source review;
- use an authoritative Railway-safe source, likely an NBA CDN schedule endpoint, but verify the exact endpoint and field semantics before locking the schema;
- upsert mutable current state idempotently because game times and statuses can change;
- make schedule records the authority for internal game ID, season, teams, game date, commence time, and status;
- link sportsbook events to the schedule rather than treating sportsbook events as the game authority;
- build a read-only preseason schedule canary before any schema write or broad persistence run.

Minimum conceptual fields:

- `game_id` primary key;
- `season_id` and season label;
- `game_date`;
- `commence_time_utc`;
- `home_team_id` and `away_team_id`;
- game `status`;
- optional venue identifier;
- source identifier and source update time;
- scrape/ingestion timestamps.

The current-state table may be paired with history later if schedule-change replay proves necessary. Do not overbuild a history subsystem before a concrete temporal requirement is established.

### Layer 3 — Historical roster authority, eligibility, and opening-night gate

**Goal:** Identify eligible players from current slate/roster truth instead of recent derived-stat rows.

Approved direction:

- add an effective-dated or daily-snapshot roster table, conceptually `nba_roster_snapshots`;
- do **not** build only a mutable `nba_current_roster` table that erases trade/signing/waiver history;
- verify a dependable Railway-safe roster source before committing the schema or ingestion path;
- define player eligibility from scheduled teams plus the latest valid roster snapshot as of the decision time;
- use injuries/availability and starter information as additional context rather than using recent derived rows as identity truth;
- join rolling/history features only after candidate identity and team membership are established;
- add opener, rookie, offseason signing, trade-before-first-appearance, waived player, and stale-roster fixtures.

Minimum conceptual roster fields:

- roster snapshot/effective date;
- `team_id`;
- `player_id` and player name;
- position/position group;
- roster or availability status;
- source identifier and source update time;
- scrape/ingestion time.

#### Opening-night blocker policy

The blocker is a readiness/publication gate, not a blanket prohibition on generating predictions.

Gate 1 — identity readiness:

- block customer-facing output if the schedule is missing/stale, the roster snapshot is missing/stale, teams cannot be mapped, or game/player identity is ambiguous.

Gate 2 — feature-source policy:

- never silently select the latest row across a season boundary;
- assign every feature family one explicit policy: current-season only, prior-season carryover allowed, carryover with decay/indicator, neutral default, or prediction-blocking when unavailable;
- keep that policy identical across training, historical replay, date/date-range construction, and scheduled inference.

Likely starting hypotheses to validate:

- player skill/rate history may be carryable with an explicit carryover indicator;
- minutes, starter role, and team-context features are high-risk after roster/coaching/rotation changes;
- team/opponent context should use current-season values or explicit priors;
- injuries and availability must be current decision-time information;
- schedule/rest/back-to-back context must come from the current schedule;
- prop/game lines must be current-game and decision-time valid.

Gate 3 — publication readiness:

- permit shadow predictions for diagnostics when safe;
- prevent customer recommendations when required identity is stale, required features exceed an approved default/carryover threshold, cross-season sourcing violates policy, quote integrity fails, or the model is not approved for the cold-start cohort.

### Layer 4 — Cross-mode feature-contract parity

**Goal:** Make training, single-date, date-range, and single-player inference produce one ordered and semantically equivalent contract.

Do not fix this only by adding the four currently missing same-position injury fields to two loaders. That would repair the immediate symptom while preserving independent implementations that can drift again.

Required outcomes:

- define one canonical feature-row schema;
- centralize reusable injury, context, default, directionality, and cold-start builders;
- make each loader use those shared builders where practical;
- add the four missing same-position fields to date/date-range behavior through the shared path;
- align missing-source defaults and team-directional spread semantics;
- make incomplete date-range construction fail closed or return explicit completeness metadata;
- preserve exact ordered feature membership expected by artifact manifests;
- separate compatibility-computable features from future-training-eligible features.

Required parameterized parity fixtures:

- normal in-season player;
- opening-night returning player;
- rookie;
- offseason signing;
- traded player before first appearance for the new team;
- injured player;
- player absent from the current roster;
- missing schedule;
- stale roster snapshot;
- unavailable feature source/default behavior;
- home/away spread direction;
- partial date-range source failure.

Parity tests must assert more than column presence. They must check ordered membership, values/defaults, sign/direction, as-of behavior, season policy, and completeness behavior.

---

## MLB decisions and lessons to reuse

Most MLB control-plane and data-integrity decisions translate to NBA. MLB-specific role, feature, and model-shape decisions do not.

### Directly reusable decisions

1. **Durable schedule authority.** MLB production starts from `mlb_game_schedule`; NBA should start from its own schedule authority rather than recent player rows or sportsbook events.
2. **Historical roster snapshots.** Reuse the daily/effective snapshot pattern, not a current-only mutable table.
3. **Separate identity from history.** Roster/schedule determine who belongs to the slate; derived history enriches those players afterward.
4. **Strict point-in-time joins.** Historical sources use strict as-of predicates rather than eventual end-of-season/latest-row values.
5. **One shared quote-selection contract.** Adapt `src/backtesting/mlb/line_selection.py` semantics: explicit decision time, pre-commence guards, paired Over/Under selection, same-snapshot identity, and returned quote timestamps.
6. **Centralized feature contracts.** Contract membership, compatibility, and training eligibility are owned centrally and verified against artifact manifests.
7. **Point-in-time advanced-stat history.** Season aggregates used historically need dated snapshots and strict prior-time selection.
8. **Compatibility versus training eligibility.** Features may remain computable for old artifacts while being excluded from future training.
9. **Artifact/lifecycle evidence.** Exact artifact identity, quote-clean OOS evidence, flat-stake risk metrics, independent validation, forward paper evidence, and owner approval remain required.
10. **Output-based job health.** Job success must report rows/events/predictions/recommendations and an explicit no-op reason, not only process exit status.

### MLB patterns to adapt rather than copy

- MLB uses probable pitchers and confirmed batting orders; NBA eligibility requires roster, injury, starter, and rotation-aware handling.
- MLB has a structured nine-player lineup; an NBA lineup table should not be added until a reliable timestamped source and concrete consumer need are verified.
- NBA minutes × rate decomposition remains the active architecture; MLB direct-stat model shapes are not a reason to redesign it.
- NBA cold start is especially sensitive to minutes, starter role, trades, coaching, and rotation changes; carryover must be decided per feature family.
- NBA historical cohort policy must separately address play-in/playoffs and distorted late-regular-season rotations.

### MLB implementation warning

Do not blindly copy the current MLB batter-discovery code. Although MLB has schedule and active-roster tables, `MLBDailyPredictionRunner._get_batters_for_games()` still discovers batters from recent derived batting rows before applying lineup filtering. That is the same class of anti-pattern currently blocking NBA opening-night discovery. Reuse the schedule/roster architecture and temporal lessons, not every legacy implementation detail.

### Decisions that do not transfer directly

- probable-pitcher eligibility;
- batting-order filtering;
- pitcher/batter direct-stat model architecture;
- park/weather/bullpen feature conclusions;
- MLB 26-man roster semantics;
- MLB-specific feature-family outcomes;
- MLB edge thresholds, BL settings, or artifact promotion decisions.

### Relevant prior lessons/invariants

- Feature selection is not an ablation.
- A centralized feature list does not prove semantic parity.
- Raw timestamps do not guarantee temporal integrity; downstream predicates must enforce as-of behavior.
- Source coverage must exist in training periods before a feature family is interpreted.
- Implausibly strong backtests trigger methodology review rather than promotion.
- Probabilities remain empirical CDF.
- Global conformal recalibration remains prohibited.
- Advanced NBA scraping remains off Railway.
- Old NBA artifacts are compatibility/training templates, not 2026–27 promotion candidates.

---

## Workstream A — NBA data-readiness audit

**Status: complete.** Four read-only audit lanes were run in parallel and combined into `docs/operations/nba-data-readiness-audit-2026-09-24.md`.

### A1. Source-data audit

Audit current schema, freshness, historical coverage, keys, duplicates, nulls, and consumer paths for:

- schedules and games;
- teams, players, and rosters;
- box scores and player game stats;
- average/rolling source tables;
- injuries and availability;
- lineups where retained;
- job execution evidence for ingestion and processing.

Classify each source as:

- ready;
- stale but repairable;
- incomplete historical coverage;
- schema/consumer mismatch;
- unavailable or intentionally deferred.

### A2. Derived-feature audit

Audit the stable NBA feature system and all supported modes:

- training;
- single-date inference;
- date-range inference;
- single-player inference.

Verify the current contracts:

- minutes: 33 features;
- points rate: 31;
- rebounds rate: 30;
- assists rate: 30;
- threes rate: 32;
- 74 unique features overall.

Check for:

- missing contract columns;
- all-zero/default feature families;
- train/serve semantic mismatch;
- stale derived dates;
- current-season cold-start behavior;
- traded/newly rostered player behavior;
- rolling-window leakage;
- minimum-eight-minute filtering behavior;
- schedule counters versus rolling-stat inclusion.

### A3. Odds and linking audit

Audit the NBA odds path required for a customer-facing pick:

- event discovery;
- player props ingestion;
- player/game/stat linkage;
- paired Over/Under availability;
- bookmaker and market coverage;
- selected line/price persistence;
- raw-table query safety;
- inference-to-quote-to-edge linkage;
- current frontend query compatibility.

All large-table queries must be keyed and date-bounded. No DDL or index work is authorized by this audit.

### A4. Temporal-integrity audit

Trace the actual training, backtest, inference, and edge-refresh paths and verify:

- stats/features are available before the target game;
- rolling features exclude the current game;
- injuries/lineups are as-of the decision time where used;
- quote selection is at or before the decision time and before commence;
- no latest-row query leaks post-decision odds;
- game/player joins cannot cross slates or seasons;
- saved artifacts contain enough timestamps/IDs to reproduce decisions.

Raw timestamp columns alone do not prove temporal integrity; the downstream query path must enforce the cutoff.

### A5. Combined audit output

Write:

- `docs/operations/nba-data-readiness-audit-2026-09-24.md`

The combined report must include:

- executive readiness verdict;
- evidence and caveats from each lane;
- readiness by layer: source, derived, odds/linkage, temporal, automation, frontend consumption;
- blockers ranked P0/P1/P2;
- repairs that are safe and bounded;
- repairs requiring Chase approval;
- exact next work package;
- explicit unknowns where production access or evidence is unavailable.

---

## Workstream B — Minimum viable frontend

The frontend should expose only dependable NBA product surfaces.

### Required MVP

- authentication and account access;
- NBA current-picks page;
- pick cards with player, game, stat, line, direction, sportsbook price, model probability, edge, recommendation state, and freshness;
- basic pick detail with recent history and line context;
- responsive desktop/mobile behavior;
- explicit empty, stale, loading, and error states;
- paper/user-bet capture only if its backend path passes the audit;
- basic history/performance only if backed by verified records;
- RLS and authenticated-client boundaries.

### Hide or defer

- DFS;
- Ask AI;
- secondary sports unless already harmless and isolated;
- advanced analysis panels;
- subscription enforcement;
- public track-record sharing;
- complex admin pages;
- any route depending on uncertified or retired data paths.

### Frontend gate

A signed-in user must be able to see current NBA picks, understand freshness, inspect a pick, and record/track a bet without encountering retired or misleading features.

---

## Workstream C — Model-template and future-training readiness

### C1. Treat incumbent artifacts as templates

For each existing NBA model generation, extract only:

- feature contract and feature order;
- minutes/rate decomposition configuration;
- training and calibration command shape;
- hyperparameters and seeds;
- artifact directory contract;
- production loader expectations;
- inference output schema;
- recommendation-policy integration.

Do not spend time attempting to re-certify the old artifacts for 2026–27 production. Functional load checks are still useful to document runtime contracts, but old performance is not a promotion argument.

### C2. Define the clean historical cohort

Before the next training run, produce an evidence-backed inclusion/exclusion policy covering:

- regular season versus playoffs/play-in;
- the late-regular-season period where starters may be rested or minutes become non-representative;
- games with abnormal rotations or missing availability context;
- traded players and team-context transitions;
- short appearances and the established eight-minute rolling-stat rule;
- season-boundary and cold-start behavior.

Do not choose an arbitrary “last X games” exclusion without profiling the data. Compare minutes distributions, starter participation, inactive/rest labels where available, and rotation stability by date. Then freeze a deterministic rule that can be reproduced in training and evaluation.

### C3. No unchanged-history timestamp-only retrain

Do not retrain every model against unchanged historical data merely to generate newer timestamps. The September audit's lack-of-new-season-data observation is historical, not a new live-data assertion. A material approved historical repair can justify the first meaningful retrain once its readiness gates pass.

Near-term model work is limited to:

- proving the training harness still works with a tiny smoke run or fixture;
- verifying feature contracts and artifact output shape;
- preparing reproducible future commands/configs;
- defining the clean historical dataset;
- deciding the minimum new-season sample needed before each meaningful run.

### C4. Future run trigger

A full training run should have a declared purpose, such as:

- enough 2026–27 games exist to update calibration or evaluate season shift;
- a verified data repair changes the historical training cohort materially;
- a contract/feature bug invalidates the old training data;
- the clean cohort definition differs enough from the old run to justify retraining.

Every future run remains Chase-launched and must preserve exact artifact identity, calibration boundaries, quote-clean OOS evaluation, and forward-paper gates.

---

## Execution sequence

### Milestone 1 — Read-only truth gathering

- Complete the four audit lanes. **Complete 2026-09-24.**
- Produce the combined data-readiness report. **Complete 2026-09-24.**
- Inventory the minimum frontend routes and backend contracts.
- Extract incumbent model template/configuration contracts.

### Milestone 2 — Adjudicate and approve repairs

- Rank P0/P1/P2 blockers.
- Separate code repairs, data catch-up, deployment fixes, and owner-gated DB/backfill work.
- Choose the smallest path that restores one dependable NBA daily slate.

### Milestone 3 — Restore historical training/replay and the minimum daily data path

- Repair source/derived/odds/linking blockers in dependency order.
- Historical-training lane: S1 completeness, S2 observation/source policy, shared historical feature parity and quote-clean replay, then a meaningful owner-launched baseline. See the reviewed repair spec.
- Publication lane: retain Layer 1 → Layer 2 → Layer 3 → Layer 4 dependencies for season/slate/roster/current-serving readiness; verify the real inference/frontend consumers before customer activation.
- Do not hold a valid historical retrain behind unrelated frontend work, and do not treat a successful retrain as authorization to publish picks.
- Keep production mutations, broad backfills, and DDL separately approved.

### Milestone 4 — Restore the frontend MVP

- Hide unsupported routes.
- Connect the NBA picks experience to verified current data.
- Pass lint, typecheck, tests, production build, preview deployment, and a real signed-in user path.

### Milestone 5 — Preseason/early-season operation

- Run ingestion, feature, inference, and frontend checks in shadow mode.
- Gather 2026–27 evidence.
- Monitor cold-start, roster-change, and minutes-distribution behavior.
- Decide when a meaningful first training/calibration run is justified.

---

## Current next action

Other chat: **S1 — completeness handling** is delivered in its isolated worktree, including Chase's approved `_run_date` extension. Preserve it pending [coordinator acceptance](nba-s1-coordinator-review-2026-10-16.md); do not relaunch or integrate. Layer 1 and Layer 2 source preparation already landed in Git; do not rebuild them from the old September next-action note.

This chat: own S2 source/timestamp evidence and proposed historical policies while Chase reads/reviews. S1 and read-only S2 can proceed in parallel without conflicting writes. No decision-time rule, injury fallback, cohort split or broader implementation is silently approved.

You: review the wiki/evidence, approve material policies, and launch training only after the frozen historical readiness/validation gates pass.

Implementation boundaries remain:

- one layer per reviewable slice;
- no schema migration, remote write, deployment, broad backfill, index, or long model run without separate approval;
- preserve the approved Layer 1 → Layer 2 → Layer 3 → Layer 4 dependency order for product publication; historical training work follows the separately scoped S1-S5 gates;
- verify each layer through its real consumers before moving to the next.
