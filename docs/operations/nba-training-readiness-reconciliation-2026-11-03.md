# NBA training readiness reconciliation — November 3, 2026

Follow-up: [historical model and policy research](nba-training-policy-research-2026-11-03.md) adds code-backed March train/calibration-overlap evidence, a playoff calendar-year/OOS contradiction, and proposed policies/windows. The owner checkpoint below supersedes its proposed date splits; other research recommendations are not automatically approved.

## Owner direction — November 3, 2026

Execution breakdown: [pretraining hardening and bounded data scope](nba-pretraining-hardening-scope-2026-11-03.md). It separates code work, read-only coverage investigation and conditional owner-launched collection; effort estimates exclude model jobs and deferred cold-start design.

- Fit on 2023–24 and 2024–25; calibrate on 2025–26; use 2026–27 for an initial test and/or later calibration. The exact current-season use and any subsequent untouched test boundary remain to be chosen. Do not reuse the research proposal's within-2025–26 development/test partitions as approved policy.
- Start with fixed incumbent hyperparameters; also consider a separately identified fully retuned challenger. Consideration is not authorization to launch tuning. Keep tuning/feature selection within chronological training folds; do not use the protected current-season test to select settings.
- Retain individual history across trades; keep the latest valid strictly pregame position until a newer valid pregame observation is available. Team/opponent/injury context follows the target game's correct team. No future/current-roster backdating.
- Chase considers the previously discussed structural-zero rules settled; verify their intended handling without reopening that policy as a new research question. Cold-start rules are explicitly deferred; do not silently freeze a new cold-start policy to proceed.
- Injury rescraping is an acceptable possible remedy if needed. First identify an actual scoped coverage/edition defect; no bulk scrape, paid call or DB write is authorized here.

Remaining non-cold-start work: enforce and verify nonoverlapping actual fit/calibration/test populations (including inner tuning/selection folds); verify training-entrypoint policy threading; add separately approved bounded acquisition safeguards; accept quote provenance/as-of pairing and injury edition/omission semantics; then run the bounded actual local feature check. Existing stats recovery and accepted Slice 8 coding remain complete. Current-season data used to adjust calibration cannot also count as untouched test evidence. Preserve the no-global-offset invariant. No model job is launched by this checkpoint.

## Evidence and scope

Readiness assessment, not launch approval or a new DB audit. Sources: GBrain handoffs 137/138, execution-plan and critical-invariants; the local records linked below. Local main was clean at `ab1e4e47` during resume. Older dirty-worktree/merge descriptions are historical, not live state.

## Reconciled verdict

- **Recovery complete for the audited window:** advanced catch-up, averages, approved position-dependent exclusions, and the October 3–November 2 support mirror. Do not repeat these jobs without a concrete defect.
- **Slice 8 approved coding scope complete:** shared historical consumers and replay/sweep/audit/attachment wiring are merged. Controlled tests do not establish real-source acceptance. Do not reopen accepted 8B/8C/8D.
- **Exact training dataset not yet certified:** final populated local training-feature check has not run. The support mirror does not certify every historical season or decision-time quote/injury source.
- **Experiment policy partially settled:** season-level fit/calibration direction and trade-position continuity are recorded above. Current-season evaluation versus calibration, remaining family/source semantics and deferred cold-start rules are not yet fully frozen. The conservative current-season-only proposal is not implicitly approved.
- **Source evidence remains distinct from policy:** recorded quote-route and injury-edition/completeness gaps are not resolved by approving a policy or attaching metadata. Existing external evidence may help; no fresh audit or proof of absence was performed here.
- **Launch route needs explicit resolution:** the documented lifecycle historical mode currently supports artifact attachment, not training. This is separate from shared FeatureStore training support. Baseline preparation must verify the actual trainer receives the frozen policy; do not silently substitute the legacy route.
- **Resource guard remains a separate approval-gated follow-up:** explicit date/game/player-game limits, budget-plus-one initial population fetch, timeout, overflow failure and identity binding before real acceptance.

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
