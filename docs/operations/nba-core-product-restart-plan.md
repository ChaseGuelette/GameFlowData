# NBA Core Product Restart Plan — 2026–27 Season

## Next session priority — reduce incoming NBA quote volume (November 12, 2026)

Implementation follow-up: Chase approved [the eight-market change-only capture scope](../../.hermes/plans/nba-quote-change-only-capture-2026-11-12.md); it is integrated locally in main, uncommitted. Thirty-minute capture slots preserve full injury/game-line runs without duplicate scrapes; active combo coverage remains. Immutable quote/capture receipts, coordinated current/T-60/paper/dashboard readers and withdrawn-offer edge-refresh handling are delivered. Independent reviews, actual disposable-Postgres persistence/permissions/cutoff checks, guarded public probes, Ruff/compile and integrated frontend build passed; existing suite remains 40 passed/4 baseline failures plus 4 passing lines/resilience checks. Source was pushed as `65611ff6`. Chase then approved applying the production migration and rejected a new activation variable: a local follow-up makes change-only capture the normal live path, with no environment toggle. Publish that small follow-up before claiming automatic suppression is active. No production write, paid scrape, model run or deletion occurred. Coordinate receipt retention and rollback; flag-off alone does not roll back receipt-authoritative consumers. Preserve the concurrently active direct-rebounds lane.

Chase explicitly chose [inference quote-volume reduction](nba-inference-quote-volume-reduction-2026-11-12.md) as this session's next work item. Narrow markets/books/pregame event scope before considering unchanged-state capture suppression; keep PTS/REB/AST plus model-input threes and paired Over/Under evidence. The proposal is not approval to delete data, alter schedules or deploy. The Lines Job incident is a T-60 materializer row-cap overflow, not the historical linker SQL/lock failure; one game has 11,484 qualifying snapshots. Preserve separately owned retention/scheduler and direct-rebounds work. [Promotion/paper-policy source delivery](nba-baseline-paper-policy-2026-11-12.md) is local and uncommitted; no artifact switch or deployment was performed. Canonical GBrain handoff `handoffs/handoff-154` and Execution Plan were committed/pushed at `e41c7f23`, synced and read back after Tailscale reauthentication; embeddings are complete and orphan pages are zero. The local draft is superseded by that canonical handoff.

## Predictive-quality discussion — November 12, 2026

The [frozen-direct versus tuning-only betting review](nba-direct-reb-fixed-vs-retuned-sweep-review-2026-11-12.md) verifies both owner-run sweeps and analyses (20 cells each, correct inputs, identical controls). Both direct models fail to establish a betting upgrade: all raw cells lose and tuning-only loses all cells. Retuned/reselected testing is owner-running; leave it undisturbed. No duplicate runs or expanded threshold/trial search is proposed. Conditional-error diagnosis within the existing architecture can proceed independently while the third run finishes.

Chase wants improved baseline predictive quality rather than another broad tuning search. [The discussion summary](nba-predictive-quality-direction-2026-11-12.md) separates unfinished family/injury proposals from new recommendations. Chase completed the isolated direct-total rebounds benchmark; [the verified result](nba-direct-reb-benchmark.md) shows a modest mixed forecast improvement (about 1.02% lower mean pinball, lower Brier/RMSE, slightly worse median MAE), still trailing the market on Brier. Do not repeat fixed training. The separate [saved-window rebounds-only sweep and post-analysis](nba-direct-reb-sweep.md), plus [optional direct-model retuning/reselection](nba-direct-reb-retune.md), are implemented, integrated and offline-verified; full new runs remain owner-launched. The sweep reuses frozen quotes/forecasts and canonical betting machinery without modifying the original sweep or production architecture. Return focus to error-guided improvements of existing minutes/rate models after command handoff. No new source acquisition or promotion is authorized.

## Repaired retune sweep and diagnostic complete — mixed result, no promotion

Both owner-run candidate jobs completed: `nba_rate_pinball_feature_match_holdout_sweep_10_nov2_v1` and `nba_rate_pinball_feature_match_error_stages_nov2_v1`. [Matched comparison](nba-pinball-retune-sweep-diagnostic-review.md) verifies 10/10 cells and both diagnostic/reference receipts; quotes/outcomes/settings match the original control. At frozen 5% edge, raw improves from -8.11% to -5.80% ROI but remains losing; fixed BL falls from +2.49% to +0.42%. The candidate's +7.29% BL maximum has only 32 bets. Rates/combined losses change little; rebounds show the clearest modest matched-score improvement, while raw probabilities still trail the market and selection amplification persists. Preserve this candidate as exploratory, retain the original methodological control, and do not promote or repeat training/sweeps for completeness. October 21–November 2 is already-used discovery data; further targeted development needs a new bounded hypothesis, not another broad trial increase by default. No new run or production change was made by the analysis.

## Repaired rate retune completed and artifact-verified

The owner completed `nba_rate_pinball_feature_match/nba_run_20261110_092318`. [Review](nba-rate-pinball-run-20261110-092318-review.md): all saved rate estimator parameters match the retained configs, including regularization and early stopping; selected features/copula match the original control, and minutes learned boosters/config/offsets remain unchanged. Completion/sanity and manifest hashes pass. Calibration is broadly similar, with no demonstrated betting or forecast-quality improvement over the control yet. Next is the matched raw/fixed-BL replay against `game_market_free/nba_run_20261108_150945`, not another retrain based on the saved checks. The earlier pre-repair run remains exploratory evidence. No replay or promotion was performed during the artifact analysis; discovery-window and historical-provenance limitations remain.

## Tuning parameter repair delivered

Chase approved the repair; regularization/early-stopping parity is implemented in main, uncommitted. [Delivery and verification](../../.hermes/plans/nba-tuning-parameter-parity.md): 48 existing offline tests, real-method controlled parameter/feature probes, lint/syntax checks and independent delta review passed. Legacy config defaults are preserved and incompatible pre-fix studies cannot resume. Completed control/candidate artifacts remain unchanged; no replacement training, replay or promotion has occurred. This supersedes the repair-pending recommendation below, not the limitations of the existing candidate.

## Rate pinball retrain completed; parameter handoff defects found

The owner completed `nba_rate_pinball_feature_match/nba_run_20261109_220136`; see [the artifact review](nba-rate-pinball-run-20261109-220136-review.md). The pinball objective and opt-in feature-matching code are implemented, superseding older proposed labels. Selected lists/copula parameters match the control exactly; minutes boosters/config/offsets and population receipts are unchanged. Calibration is broadly similar, with no downstream profitability evidence yet. The analysis found two pre-existing trial/final-fit mismatches: `reg_alpha`/`reg_lambda` are dropped from the final config, and trials remove early stopping while final fitting enables it. Preserve this exploratory artifact; repair/verify the effective parameter contract before calling a further run a faithful Optuna-winner refit. No repair, rerun, sweep or promotion was authorized or executed by this analysis.

## Error-stage v4 completed and reviewed — November 9, 2026 UTC

The owner-run v4 diagnostic is complete; this supersedes the pending-run instructions below. See [the verified v4 review](nba-error-stage-v4-review.md). All 1,579 eligible player-games are oracle-valid; output hashes and exported probability scores were checked, and the run records reference/RNG reproduction. Rate-oracle sensitivity exceeds minutes sensitivity for PTS/REB/AST, but every learned component already beats its L5 point baseline; hindsight gains include realized outcome noise and are not attainable alpha. Raw probabilities trail the market and the gap widens after selection; fixed BL narrows the gap without establishing independent model edge. Recommended next development comparison: a separately approved pinball-tuned rate challenger with minutes/features/sampler/betting settings fixed. The Optuna plan remains proposed, other-chat repair ownership is preserved, and no training or promotion is authorized. October 21–November 2 remains discovery data; historical readiness is unverified.

## Error-stage diagnostic delivered; full v2 run pending — November 8, 2026

The [diagnostic usage guide](nba-error-stage-diagnostics.md) is the current owner-run entrypoint. The runner and helper are implemented in the main checkout, uncommitted; original populated controlled integration and 18 existing checks passed. The first real v1 attempt stopped at actual-minute acquisition because the date-wide outcome population exceeded the feature-row cap. A separate lane has repaired both outcome-row and outcome-game caps without relaxing feature caps; its local loader receipt records 2,803 rows across 104 games with read-only/timeout guards. That receipt's `FAIL` verdict refers to the superseded 100-game outcome limit, not a completed v2 run. Current v2 preflight and Ruff pass. Preserve v1 incomplete evidence and the other lane's repair; no complete diagnostic report exists yet. Next: Chase launches the fresh v2 command, then analyze its component/oracle/probability/selection evidence to choose one targeted development experiment. Do not rebuild the runner, repeat the completed family ablations, or call preflight a real-data diagnosis. The separate Optuna pinball-objective plan remains proposed in the inspected file; coordinate ownership before changing the tuner.

## Signal-development priority clarified — November 8, 2026

Chase's goal is stronger betting signal, not serial minor confirmations. The [four-ablation review's updated next-action section](nba-four-ablation-review-2026-11-08.md#priority-clarification-signal-development-versus-confirmation) separates new-window validation from model improvement. Combined short-form/matchup removal is an optional modest-information interaction test, not the main expected breakthrough. The proposed development path is a bounded component/market-relative error diagnosis that selects one controlled learning experiment; a stat-specific retune is plausible, but the current outer Optuna objective selects on coverage gap and must not be mistaken for pinball optimization. Preserve fixed-reference comparisons, fitting-only chronology and the separate validation/promotion track. No diagnostic job, tuner change or training is authorized by this clarification.

## All four family ablations reviewed — November 8, 2026

The owner completed the game-market-free control and its short-form, opponent-matchup and prop-market exclusions, each with ten matched raw/fixed-BL sweep cells. See [the deep four-ablation review](nba-four-ablation-review-2026-11-08.md) and its numerical evidence. Keep prop-market inputs: their removal materially worsens common-cohort forecast scores and expands losing bets. No-short-form is a lean challenger, not a certified profitability improvement; matchup exclusion is mixed. Retain the game-line-free control while original game-line timing remains unverified. No raw setting is profitable and no new model is promotion-certified. Proposed next work is a fixed later-window comparison of the control and lean challenger, or one separately approved combined short-form/matchup exclusion with props retained. This supersedes earlier unimplemented/unrun first-wave instructions below; do not launch duplicate fits or rebuild the controls.

## Feature-family ablation controls delivered — November 8, 2026

Chase explicitly approved the general NBA feature-family setup, not the previous-day injury variant. The [owner-run setup](nba-feature-ablation-setup.md) is implemented and integrated into main, uncommitted: six disjoint families, exact/family include/exclude controls for ordinary full training, per-quantile forced-inclusion guarantees, artifact conformance and lifecycle threading. Independent spec/quality reviews passed after concrete repairs; main-checkout validation passed 108 existing tests, guarded populated/negative probes, five dry-runs, real baseline-artifact compatibility, scoped Ruff/syntax and diff checks. No training, sweep, DB/API operation, deployment, commit or push was launched. This supersedes the controls-only proposed/unimplemented status below. The existing null-cutoff reference still uses the direct training recipes; lifecycle cutoff gates remain unchanged. Concurrent sweep/Railway edits were preserved.

## First no-injury sweep reviewed — November 8, 2026

The [first sweep review](nba-first-sweep-review-2026-11-08.md) reconciles all 95 saved ledgers for October 21–November 2. Every raw/no-BL threshold lost money; the best BL ROI cell with at least 100 total bets returned +4.17% on 271 bets, driven by unders/REB over only 12 betting dates. Historical readiness remains unverified and the window is now discovery data, not untouched finalist evidence. Next proposed training work is a game-line-free control, followed by fixed-parameter short-form, opponent-matchup and prop-market exclusions. Generic family controls still need approval/implementation; no new runs or promotion are authorized. The logged correlation warning does not indicate independent sweep sampling: recorded copula parameters were enabled and global offsets were disabled.

## Feature-family ablation discussion — November 8, 2026

The first no-injury baseline has finalized at `src/models/artifacts/no_injury_baseline/nba_run_20261108_122129`; saved completion/sanity metadata, injury exclusion and incumbent-parameter equality were checked. This is not profitability or promotion certification. See [the ablation proposal](nba-feature-family-ablation-direction-2026-11-08.md) for current training-control gaps, reusable lifecycle stages, actual selected-feature counts and the recommended family experiments. General family controls and further training/replays are proposed, not approved or launched. Preserve the completed baseline and separately owned injury/Railway work.

## Injury-feature baseline direction — November 8, 2026

Follow-up [RapidAPI clock validation](nba-rapidapi-clock-validation-2026-11-08.md) compared retained raw records with official reports: `reportTime` is not a reliable publication clock, but many statuses match usable earlier reports. A previous-day snapshot experiment is proposed, not approved or implemented. Keep the no-injury training lane independent; do not enable a blanket time-map or restart broad recovery from this research.

Chase approved leaving historical injury features out of the first baseline while source repair proceeds separately. See [the decision summary](nba-injury-baseline-direction-2026-11-08.md) for the distinction between unsupported publication evidence and late injury news, consistent feature exclusion across artifact stages, and preserving already-placed decisions. The exclusion is implemented and the owner-launched baseline has finalized; selected lists contain no injury-family features. See the newer ablation discussion above for the exact artifact and verification limits. Existing split/fixed-parameter policy and independent backend/job ownership remain unchanged; no deployment or promotion is authorized.

## Owner-prioritized next work — November 7, 2026

The **two headline work items** are now:

1. **Paper-betting backend repair:** consistent manual NBA settlement first, then immutable automatic entries, fail-closed pregame eligibility and retained model/quote/evaluation identity. Chase approved first qualifying pregame entry per player/game/stat (no replacement or opposite-side addition), fixed $10 automatic paper stakes and minimal additive provenance. Manual amounts and legacy history stay unchanged.
2. **Production job recovery:** finish the existing Daily Stats repair and close the four recorded follow-ups: schedule/frontend retrieval verification, props scrape/link failures, local advanced-input/inference readiness, and meaningful edge refresh rather than missing-sample no-ops.

Use the [backend and job-readiness plan](../../.hermes/plans/nba-paper-backend-and-job-readiness-2026-11-07.md) for scope, dependencies and acceptance gates. Policy choices are approved; implementation, migration execution, job launches and deployment remain separately authorized. Do not confuse this paper-betting backend with the concurrent historical backtest-harness repair below, and do not launch duplicate work against that lane.

Frontend cleanup is delivered in `914e2b9a`: Props/Record/History, default My Bets, date filters, NBA/MLB Model History selection, DFS UI removal, awaited save confirmation and honest schedule failure states. Production build/TypeScript, controlled frontend checks and independent reviews passed. This does not certify the backend settlement cycle or prove current inference/source readiness. Historical injury acceptance and training gates remain separate; do not restart completed frontend or injury-recovery tooling.

## Backtest harness check — November 7, 2026

The [current harness check and repair](nba-backtest-harness-check-2026-11-07.md) records the completed owner-requested drawdown, valid-quantile calibration and daily-Sharpe fixes plus compact-table replay wiring. Historical prices come from the existing exact-response cache, not fabricated odds or the giant raw table. All 43 affected tests passed; 44 supported-stat quote pairs matched through direct/prefetched/native readers in a real local pilot. Exact-window local/remote compact parity also passed. Full holdout evaluation, richer predictive metrics and a bounded loader-only CLI remain separate; no model fitting or full replay was launched.

## Props T-60 implementation — November 5, 2026

The owner-approved T-60 props slice is integrated into the main checkout and its four-table remote migration is applied/read back. Use the [T-60 runbook](nba-props-t60-runbook.md) for the separate one-time API backfill, daily inference table/materializer, game-level eligibility, bounded cleanup and validation limitations. The original bulk scraper and old raw/archive tables are unchanged. No paid scrape, broad backfill, local sync, commit/push or deployment was run. Historical source population and the daily production cutover remain pending; do not treat the empty new tables as training-ready data. The [investigation](nba-compact-prop-lines-proposal-2026-11-05.md) retains the original evidence. Concurrent injury work was preserved.

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
