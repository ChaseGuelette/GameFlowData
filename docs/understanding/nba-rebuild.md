# NBA rebuild wiki: data to trustworthy retraining

## Batch D — observation and quote primitives

Chase approved Batch D / coding slice 4, its pairing/duplicate contract and its merge into main from `repair/nba-observation-primitives` (`b2fb86f`). Implementation and bounded controlled verification are complete; real-data acceptance and consumer integration remain open. This is not historical repair stage S4. Read the [decision and evidence record](../operations/nba-batch-d-observation-primitives.md). Coordinator reruns passed the 77-assertion worker probe, adversarial and review-resolution probes, 10 existing inventory tests, lint/compile/type checks and file-scope checks. No new unit tests were created.

Think of this boundary as a checker, not a historical researcher. The caller supplies when a decision happens, when the game starts, what each source clock means and which snapshots are compatible. The checker explains whether each observation qualifies and whether a genuine Over/Under pair exists. It does not choose a T-minus policy or repair unknown source history.

Two prices from the same book are not necessarily a pair: game, player, market/period/variant, line and explicit source/snapshot identity must agree. Exact duplicates collapse with their provenance retained; contradictory versions block the group rather than letting input order choose a price. An older complete pair can remain in the eligible inventory without silently becoming a fallback bet. Selecting one pair requires an explicit caller rule.

Controlled checks can prove these mechanics, not that historical rows carry trustworthy clocks. S2 evidence and later consumer integration are still required. No production SQL, injury/cohort policy, training or S1 changes are part of this slice.

## Current coding lane — November 1, 2026

We can prepare execution plumbing without waiting for refreshed data. [CLI/preflight slice 1](../operations/nba-cli-preflight-slice1-2026-11-01.md) is implemented and independently reviewed in a separate worktree; all 117 existing scoped tests pass after owner-approved fixture reconciliation. Integration is still owner-gated. Another chat owns sync repair and S1 remains unintegrated. Read the [eight-slice overview](nba-training-evaluation-slices.md), [preflight explainer](nba-training-preflight.md) and [coding backlog](../operations/nba-coding-readiness-plan-2026-11-01.md). This is not real-data acceptance. The October running-sync notes below preserve the earlier checkpoint.

## Kick off immediately

- You: let the already-running [NBA-only refresh](../operations/nba-sync-inventory-2026-10-16.md) finish; no duplicate sync. Its completion summary is required before real-data acceptance.
- Other chat: S1 is delivered in its isolated worktree; preserve it pending the [coordinator review](../operations/nba-s1-coordinator-review-2026-10-16.md). The owner-approved nested `_run_date` extension is included; no integration yet.
- This chat: own S2 source/timestamp research and review the S1 packet; no conflicting source edits.
- You: read the coordinator S1 review first, then this wiki and the [research report](../operations/nba-historical-readiness-2026-10-16.md). Independent review need not pause while you read; implementation/integration approvals remain explicit.

Owner: Chase. Working guide dated October 16, 2026.
Status: NBA-only sync running; S1 implemented in a separate worktree with coordinator-rerun existing tests/lint and independently rerun controlled probes passing. Real-data/artifact acceptance remains open. Main source is unchanged; no integration or model-quality certification.

## The shortest useful path

Historical data -> as-of feature rows -> frozen cohort/splits -> training artifact
-> load and predict -> quote-clean replay -> flat-stake evaluation -> paper evidence.

Current-slate schedule/roster readiness and the customer frontend are a separate publication gate. They block historical training only where the historical cohort, features, or replay actually depend on them. This separation does not waive the restart plan's season-policy and feature-parity requirements.

## Two different meanings of ready

| Gate | Evidence needed | What passing permits |
| --- | --- | --- |
| Ready to retrain | Covered source seasons/local mirror, correct keys and labels, prior-game/as-of features, explicit cohort and season rules, real feature variation, fixed train/calibration/OOS splits, bounded trainer/artifact smoke | Chase launches one meaningful baseline run |
| Ready to evaluate | Exact artifact loads; replay features match training semantics; quotes were available at the decision and before tip; missing quotes/dropouts are explained; saved outputs identify artifact/config/window | Interpret model and flat-stake historical results |
| Ready to publish | Fresh schedule/roster identity, opener/carryover policy, complete feature contract, valid current quotes, approved model, shadow/paper and frontend-consumer evidence | Separate owner decision; never implied by training success |

A repaired historical cohort or feature contract can justify a historical retrain without waiting for new-season results. Retraining unchanged history solely for a newer artifact timestamp is not useful.

## First work packages

### A. Historical data and feature readiness

Revalidate the September audit findings against current code and bounded source/local-mirror evidence. Check:

- source date/season coverage and game/player identity;
- labels, missingness, duplicates, non-default feature variation;
- rolling sources excluding the target game;
- market/injury/context fields obeying a declared historical as-of cutoff;
- regular-season/play-in/playoff and late-season cohort rules;
- ordered feature membership and training/replay semantic parity.

Deliverable: a short pass/blocker/unknown matrix with the smallest repair list and exact evidence dates. No broad backfill, migration, or new architecture.

### B. Training and evaluation path, in parallel

Find the actual NBA trainer and replay consumers. Use the current MLB lifecycle, quote selector and quote-clean sweep as references, not its deprecated legacy harness. Before a long run, verify a bounded path can emit/load an artifact and replay a historical slice. Any expensive smoke is prepared for Chase to launch.

Deliverable: exact commands, artifact contract, split/decision-time policy, and a bounded repair proposal. Do not interpret returns until quote selection and dropout accounting are trustworthy.

### C. First baseline, after the historical gates

Freeze data/cohort, feature contract, settings and time splits. Chase launches the run. Record its exact artifact and resolved configuration. While it trains, finish downstream reporting/CLV plumbing that does not change those frozen contracts. Check the produced artifact through the real loader before running protected OOS evaluation.

Start raw/no-BL and flat-stake. Report model diagnostics plus profit, ROI, volume, Sharpe, drawdown, sides, quote coverage and dropout. Use BL selection only for finalists. Kelly/ranking is a separate optional lane, not a blanket prerequisite for flat-stake approval.

## First discovery results

Code inspection on October 16 confirmed these repair targets. Sampled local DB verification and a metadata-only follow-up also ran; neither certifies whole-period data or runtime/model readiness:

| Target | Observed behavior | Why it matters |
| --- | --- | --- |
| `src/models/features/nba/training_loader.py:371-398` | Game lines use unrestricted `MAX`; prop lines use a calendar-date/pre-commence guard but no declared decision-time cutoff | A training feature can reflect information unavailable when the intended bet would have been placed |
| `src/models/features/nba/date_range_loader.py:243-310` | Injury context uses same-day reports/latest IDs without an observation-time cutoff | Historical replay cannot yet establish what was actually known at the decision |
| `src/backtesting/backtest_harness.py:475-604` | Single-day and bulk-prefetch paths use a fixed 23:30 clock cutoff and independently rank each outcome before combining odds | Neither inspected query enforces quote-before-tip or same-snapshot Over/Under pairing; both sibling paths need review |

The sampled 22025 missing advanced/rolling rows were all zero-minute DNPs: **202/202** appearances meeting the current five-minute training threshold were covered. This is not a backfill blocker. It also does not erase the September audit's different, broader coverage questions.

The coordinator confirmed basic incremental rolling masks prior performances below eight minutes while the advanced path does not apply that mask. A rolling row on a short/DNP appearance is not itself a violation: row identity and schedule counters are different from which performances contribute to its values.

Quote timestamp columns exist in the local metadata, but historical availability still needs validation. Injury `report_time` is text; its timezone, meaning and retained-history semantics remain unresolved. No production source/schema status was checked.

Routing discovered by file-only agents:

- NBA trainer: `src/models/train_pipeline.py`.
- NBA replay CLI: `src/backtesting/run_backtest.py`; pass the exact model directory rather than rely on latest-run discovery.
- MLB orchestration reference: `src/backtesting/mlb/run_mlb_sweep.py` and the current YAML lifecycle.
- MLB quote-contract reference: `src/backtesting/mlb/line_selection.py`, not the legacy MLB harness.

### Next package boundary

1. Other chat: S1 code is delivered; preserve the worktree pending coordinator acceptance. No other source work shares those files and no integration is authorized.
2. This chat: S2 representative data/source semantics and decision-time recommendations. No silent assumption that schema columns establish historical availability.
3. Later approved packages: shared historical feature policy/parity, quote-clean direct/prefetch replay, exact-artifact smoke, then a meaningful owner-launched baseline.

### Why completeness comes first

If one feature chunk or inference date crashes and the runner quietly scores the rest, the reported performance describes a selected subset, not the requested window. S1 makes that failure explicit. It does not repair quote leakage or prove profitability; those remain separate gates.

An outer fatal boundary is not enough if `_run_date` swallows the predictor's error first. Chase approved removing that nested suppression. This differs from treating a legitimate empty slate or zero bets as an error: those remain valid nonfatal outcomes.

### Why source clocks need separate labels

The same `snapshot_time` column can mean a historical request cutoff or a live scraper's local clock. Historical requests have a documented at-or-before bound, conditional on provenance; live props currently label later event fetches with an earlier invocation time. A report-cycle label is also not a complete injury observation history. See the [source-clock review](../operations/nba-s2-source-clock-review-2026-10-16.md) for evidence and unresolved policies; do not infer either blanket safety or a blanket rescrape requirement.

## Delegation map

- Orchestrator: dependency order, evidence reconciliation, scoped work packets, independent review, wiki maintenance.
- Short internal subagents: bounded read-only file questions with exact paths and stop conditions.
- SQL runner: separate, bounded SELECT-only data checks after tables/windows are identified.
- Separate chats/worktrees: longer implementation packages with non-overlapping ownership, or interactive platform work.
- Chase: policy/implementation approvals, long training/sweep launches, DB writes, deployment and integration decisions.

Every implementation packet must state allowed files, non-goals, dependencies, validation, expected result, and when to stop. Two agents must not edit the same checkout concurrently.

## Working agreement

- No new unit tests. Preserve existing tests; verify with relevant existing checks, real-path smoke/integration runs, bounded data audits and lint.
- Chat stays concise. This wiki carries the explanations.
- Save reusable, proven procedures in the relevant Hermes skill; keep temporary run status in evidence/handoffs.
- No automatic commit/push, migration, ingest, deploy, promotion, or long model run.
- Each active work document starts with a bounded kickoff/ownership section. Keep findings, proposals and approved decisions distinct; a document being ready to read is not approval to execute all its stages.

## Terms worth knowing

- As-of: information available at the declared decision time, not the latest row now.
- Cohort: the deterministic set of games/player appearances allowed into a run.
- Calibration window: data used to adjust model outputs; it cannot also be untouched evaluation data.
- OOS: out-of-sample data held beyond training/calibration and protected from selection.
- Quote-clean replay: simulated decisions using real eligible historical quotes, not future or synthetic best prices.
- Dropout: predictions lacking usable inputs/quotes or otherwise not reaching a bet; explain why.
- Artifact: the actual saved model plus feature/config/window identity, not merely a directory label.

## Relevant prior lessons/invariants

Timestamps alone do not prove as-of safety; the consuming queries must enforce it. Strong backtest returns do not prove the harness is correct. Keep probabilities empirical, preserve Q10 behavior, prohibit global conformal offsets, and keep advanced NBA scraping off Railway.

GBrain references: operations/critical-invariants; lessons/raw-timestamps-do-not-guarantee-temporal-integrity; lessons/quote-clean-clv-before-feature-work; models/nba-model; models/mlb-model.

## Read next

- [S1 coordinator review and real-data acceptance gate](../operations/nba-s1-coordinator-review-2026-10-16.md)
- [S2 source-clock evidence and policy gates](../operations/nba-s2-source-clock-review-2026-10-16.md)
- [Remote NBA-only sync inventory and authority correction](../operations/nba-sync-inventory-2026-10-16.md)
- [Repair specification and immediate S1 handoff](../operations/nba-historical-repair-spec-2026-10-16.md)
- [Three-agent historical-readiness tracker](../operations/nba-historical-readiness-2026-10-16.md)
- [Approved restart plan](../operations/nba-core-product-restart-plan.md)
- [Layer 2 preflight, September 24 evidence](../operations/nba-layer2-preflight-2026-09-24.md)

The restart plan and preflight contain historical status. Current code, live data and actual execution must be checked before marking a gate passed.
