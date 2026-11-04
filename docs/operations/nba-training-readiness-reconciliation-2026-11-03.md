# NBA training readiness reconciliation — November 3, 2026

Follow-up: [historical model and policy research](nba-training-policy-research-2026-11-03.md) adds code-backed March train/calibration-overlap evidence, a playoff calendar-year/OOS contradiction, and proposed policies/windows. The owner checkpoint below supersedes its proposed date splits; other research recommendations are not automatically approved.

## Owner direction — November 3, 2026

**November 4 acquisition correction (latest):** Chase clarified that injury scraping must use historical game IDs/dates independently of props. The corrected raw-history manifest has 3,781 games and 567 request dates; six responses are reused and 561 requests remain. The [actual dry-run passed and the scrape is ready for owner launch](nba-injury-recovery-launch-2026-11-04.md). The props-based acquisition block below is superseded; props availability and strict injury feature acceptance remain downstream training issues. No full scrape was launched by the agent.

**November 4 full-cohort follow-up:** the [recovery wrapper is implemented](nba-injury-recovery-launch-2026-11-04.md), but its canonical export found zero retained fitting games in both 22023 and 22024 (1,230 candidates each) under the approved `raw_player_props_combined` linkage filter. Calibration has 1,321 retained games. These are now whole-export counts, superseding the earlier sample-only uncertainty for this filter; archive availability/cause is still unknown. The real dry-run correctly blocks instead of collecting calibration only. No bulk API job or DB repair was launched.

**November 4 follow-up:** Chase approved and the agent implemented/exercised the API-first file-only pilot; see [delivery](nba-injury-pilot-delivery-2026-11-04.md) and [full-scrape scope](../../.hermes/plans/2026-11-04-nba-injury-full-scrape.md). Capture/replay mechanics pass, but the actual audit is SOURCE_BLOCKED: the bounded three-date cohort yielded no prop-linked target games among 17 candidates, and publication/edition/completeness semantics remain unverified. This does not establish whole-season absence or a fresh linker defect. The November 3 research-only hold below is superseded for pilot Tasks A–C only. Full collection, native adapter integration, DB repair and training were not launched.

Execution breakdown: [pretraining hardening and bounded data scope](nba-pretraining-hardening-scope-2026-11-03.md). It separates code work, read-only coverage investigation and conditional owner-launched collection; effort estimates exclude model jobs and deferred cold-start design.

- Fit on 2023–24 and 2024–25; calibrate on 2025–26; use 2026–27 for an initial test and/or later calibration. The exact current-season use and any subsequent untouched test boundary remain to be chosen. Do not reuse the research proposal's within-2025–26 development/test partitions as approved policy.
- Start with fixed incumbent hyperparameters; also consider a separately identified fully retuned challenger. Consideration is not authorization to launch tuning. Keep tuning/feature selection within chronological training folds; do not use the protected current-season test to select settings.
- Retain individual history across trades; keep the latest valid strictly pregame position until a newer valid pregame observation is available. Team/opponent/injury context follows the target game's correct team. No future/current-roster backdating.
- Chase considers the previously discussed structural-zero rules settled; verify their intended handling without reopening that policy as a new research question. Cold-start rules are explicitly deferred; do not silently freeze a new cold-start policy to proceed.
- Injury rescraping is an acceptable possible remedy if needed. First identify an actual scoped coverage/edition defect; no bulk scrape, paid call or DB write is authorized here.

Injury investigation and API pilot completed: [bounded injury usability verdict and approved recovery direction](nba-injury-usability-investigation-2026-11-03.md). Chase approved the existing basketball injury API as the primary scraper. Three raw requests returned 324 records: known malformed statuses originate upstream, 75 records carry game dates different from request dates that our writer does not preserve separately, and exact pre-cutoff edition/completeness evidence remains unestablished. No within-response deduplication loss occurred in this pilot; do not attribute the observed deficiencies solely to the unique key. Next establish the API's historical edition-selection/clock semantics before bulk collection or acceptance. Official PDFs remain validation/fallback evidence, not a replacement scraper. No injury source/model code or DB changes were made.

Remaining non-cold-start work: apply the owner-approved training exclusion below rather than repairing every missing-linkage game; retain provenance/as-of pairing checks for remaining rows, establish injury edition/omission semantics, and run the bounded actual local feature check. Chronology enforcement, whole-date inner splits, training-policy threading and acquisition safeguards are implemented, reviewed and merged in `4ffdf065`; do not reopen those completed code items. See [integration scope/status](nba-pretraining-hardening-scope-2026-11-03.md), [bounded source investigation](nba-pretraining-bounded-data-investigation-2026-11-03.md), and the separately progressing [linkage recovery](nba-props-linkage-recovery-2026-11-03.md). Existing stats recovery and accepted Slice 8 coding remain complete. Current-season data used to adjust calibration cannot also count as untouched test evidence. Preserve the no-global-offset invariant. No model job is launched by this checkpoint.

Injury follow-up scope: [API-first recovery pilot plan](../../.hermes/plans/2026-11-03-nba-injury-api-recovery-pilot.md). Research and scoping only; Chase explicitly paused implementation. The plan separates file-only capture/normalization from evidence-gated historical feature integration and DB repair. No documented intraday selector was found on the provider's rendered date endpoint.

### Owner-approved missing-linkage training exclusion — later November 3 checkpoint

- Chase approved dropping games without linked props from the training set instead of further linkage recovery. The six historical misses were sampled games, not a whole-cohort defect count. The recheck also found the October 26 recent-season miss; archive-versus-linkage cause remains unproven and is not a reason to launch another repair here.
- Implemented in the native historical fitting path (`TrainingFeatureRequest`, role `training`): retain a target game only if `raw_player_props_combined` has an exact matching `game_id` with a nonnull `player_id`. A missing `team_id` alone does not exclude the game. Missing games are removed before native feature-source acquisition, not from stored stats, rolling histories or the database.
- Presence is not quote acceptance. Partially linked games still pass through unchanged per-player quote, clock, pairing, provenance and injury checks. Calibration, evaluation/replay, controlled evidence and legacy no-policy loading are unchanged. Other source/SQL errors are fatal; empty or missing-season fitting populations still fail existing chronology gates.
- Lookup uses batches of at most 100 exact game IDs, the configured statement timeout and rollback. Original population consumes the acquisition budget before filtering. Row/date exclusion accounting is retained with reason `no_linked_player_props`; counts and exact excluded games/dates/seasons are saved as `execution.training_linkage_exclusion` in run configuration and artifact provenance.
- Verification: 49 existing tests, scoped Ruff and whitespace checks passed. A guarded controlled probe exercised the actual loader and feature assembler, exclusion/date accounting, unaffected non-fitting paths, fatal timeout/overflow/source errors, empty-population rejection, batching and run-config persistence. Independent scoped specification/quality review returned PASS and separately checked compilation/whitespace. No live training-population count or model fit was performed. Source changes remain local/uncommitted.
- Exact SELECT recheck receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-exact-linkage-recheck-20261103/`. Implementation spec/probe/receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-training-linkage-exclusion/`.

## Evidence and scope

Readiness assessment, not launch approval or a new DB audit. Sources: GBrain handoffs 137/138, execution-plan and critical-invariants; the local records linked below. Local main was clean at `ab1e4e47` during resume. Older dirty-worktree/merge descriptions are historical, not live state.

## Reconciled verdict

- **Recovery complete for the audited window:** advanced catch-up, averages, approved position-dependent exclusions, and the October 3–November 2 support mirror. Do not repeat these jobs without a concrete defect.
- **Slice 8 approved coding scope complete:** shared historical consumers and replay/sweep/audit/attachment wiring are merged. Controlled tests do not establish real-source acceptance. Do not reopen accepted 8B/8C/8D.
- **Exact training dataset not yet certified:** final populated local training-feature check has not run. The support mirror does not certify every historical season or decision-time quote/injury source.
- **Experiment policy partially settled:** season-level fit/calibration direction and trade-position continuity are recorded above. Current-season evaluation versus calibration, remaining family/source semantics and deferred cold-start rules are not yet fully frozen. The conservative current-season-only proposal is not implicitly approved.
- **Source acceptance still pending:** bounded canonical triage is complete; it does not establish whole-cohort quote provenance or injury-edition completeness. Sample linkage/schedule repairs in the parallel lane do not certify the entire historical population.
- **Launch route implemented:** historical training CLI and lifecycle policy forwarding are integrated and controlled-path verified. Real populated local acceptance and an approved frozen configuration remain prerequisites to owner launch.
- **Resource guard implemented:** cumulative date/game/player-game limits, budget-plus-one initial fetch, transaction-local timeout, overflow rejection and identity binding are merged. Actual run limits must still be set explicitly; downstream source cost is not globally bounded by this guard.

## Confirmed versus proposed

Confirmed: T-60, strict missing/ambiguous-source blocking, no diagnostic fallback, target/rolling minutes 5/8, and valid strictly earlier position snapshots for position-dependent aggregates. Preserve raw stats and ordinary rolling averages.

Recommended next step, not owner approval: focused policy/evidence work rather than architectural redesign or another broad data rebuild. Establish candidate cohort/windows and family rules, reconcile source coverage, freeze feasible choices, perform bounded real-consumer acceptance, then prepare/dry-run the owner-launched baseline. Unsupported selected policy or launch behavior may need a narrow follow-up implementation.

## Relevant prior lessons/invariants

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: source rows and successful copying do not prove the consuming query is decision-time safe.
- `lessons/cheap-baseline-before-architecture`: keep the first baseline simple; no architecture expansion without evidence.
- `operations/critical-invariants`: empirical CDF, no global conformal offsets, preserve Q10 edge, isolated SQL and owner-launched long runs.

## Source records

- [Slice 8 completion](nba-slice8-completion-status.md)
- [Owner decisions](nba-s8-completion-decisions-2026-11-02.md)
- [Family proposal](nba-s8-family-policy-proposal-2026-11-02.md)
- [Data recovery/readiness](nba-position-repair-and-retraining-readiness-2026-11-03.md)
- [Operator workflow](../understanding/nba-s8-operator-workflow.md)

No DB query, source repair, model run, launch approval, commit, push or deployment accompanies this assessment.
