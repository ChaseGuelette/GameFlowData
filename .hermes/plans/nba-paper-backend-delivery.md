# NBA paper backend MVP delivery

## Authorization and scope
Chase authorized completing the entire backend paper repair branch on November 7, 2026: functional MVP and solid results before aggressive hardening. Implement Slices A and B from the source plan at `C:/Users/Chase/Projects/GameFlowData/.hermes/plans/nba-paper-backend-and-job-readiness-2026-11-07.md` (read-only context, untracked in main). This authorization covers code and controlled validation, not commit/push/merge, production DB mutation, live placement, deployment, training, or job recovery JOB-01..04. Do not ask again to implement.

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-paper-backend`
Branch: `repair/nba-paper-backend`; base `914e2b9a08af19504d3ef0bd22ef56b752cf6a66`.
Main checkout and backtest/historical-source work are protected. Do not copy source from other dirty worktrees.

## Allowed edit scope
- `src/paper_trading/paper_trader.py`
- `src/paper_trading/user_bet_resolver.py`
- `src/paper_trading/nba_settlement.py` (new only if useful)
- `src/paper_trading/nba_paper_entry.py` (new only if useful)
- `src/orchestration/resolve_user_paper_bets.py`
- `src/orchestration/daily_stats_job.py` only settlement invocation/summary
- `src/orchestration/scheduler.py` only settlement invocation/summary (avoid rescheduling if both callers can safely share idempotent policy)
- `src/models/prediction_store.py`, `src/models/daily_runner.py`, `src/orchestration/inference_job.py`, `src/orchestration/edge_refresh_job.py` only necessary propagation of selected quote/artifact metadata and paper caller integration. No model/feature/edge formula changes.
- One additive migration in existing `database/migrations/` naming convention; prepare only, do not execute.
- This delivery record, including rollout/rollback commands and validation receipts.
No test edits/new unit tests, dependency changes, frontend changes, broad refactoring, new frameworks or job recovery changes. New small helper allowed only when directly needed by above behaviors.

## Slice A: one NBA settlement policy
- Reuse the same exact-game rules for manual NBA entrypoints and automatic paper bets. Existing UserBetResolver may resolve all supported NBA user bets (real and paper); preserve that scope unless source callers prove otherwise. The standalone paper resolver must remain paper-only for its own invocation. Both must exclude MLB/unsupported stat families; preserve existing standalone MLB behavior.
- Known final played result => won/lost/push. Explicit final nonparticipation => cancelled, zero P&L. Missing/incomplete/ambiguous data stays pending with reason. Zero minutes by itself is not authoritative DNP. Do not cancel lookup misses.
- Prefer stored game ID or prediction identity. Legacy player/date fallback requires one unique matching fixture; never silently choose among conflicts. Require per-game final/completeness evidence actually available in real schemas. Do not invent a column or trust one team row for an entire date. Use bounded queries.
- Require all components for combo results. Null player/unsupported stat/ambiguous game stays pending.
- Conditional update: pending status plus captured unchanged identity/line/side/odds/stake; persist status/actual/P&L/resolved_at together. Repeated and concurrent invocations must not double-settle or overwrite user edits. Return real updated counts, distinguish pending/blocked/failure, propagate actual errors rather than healthy wrapper summaries.
- Preserve next-morning scheduling and MLB; prefer minimal shared callable over scheduler overhaul.

## Slice B: automatic entry, settlement, accounting
- First eligible pregame recommendation per player/game/stat/evaluation run wins. Freeze line/side/odds/stake/probability/placed_at; no later replacement/opposite side. Preserve current placement windows and frequent refresh `--skip-paper`; no new T-60 schedule.
- $10 fixed stakes for NEW automatic evaluation-period entries only. Kelly helper may remain diagnostic. Preserve manual stakes and all historical rows. Evaluation identity should be explicit and stable across reruns, not newly generated on every placement call.
- Reject missing/stale/conflicting/started schedule or quote evidence, recheck at insertion boundary. Replace raw giant-table start scan with existing authoritative schedule/current-quote path. No invented numeric TTL beyond existing eligibility semantics.
- Capture selected coherent quote identity (book/game/line/prices/timestamps/decision time) and immutable model/artifact/evaluation identity from actual producers. Do not relabel latest quote as the prediction's quote or infer artifact from mutable prediction FK. Trace producer metadata and propagate minimally. If existing artifact helper exists, reuse it without model loading. Do not fabricate legacy provenance.
- Minimal additive schema fields/snapshot payload after live schema receipt is available; migration remains unapplied. Avoid new framework. Ensure normal producer->store->selection->placement can yield populated candidates after prerequisite migration, not a forever-blocking adapter.
- Return inserted-row counts using actual DB outcomes. Implement duplicate protection sufficient for concurrent callers (existing constraints + bounded locking or minimal index); migration must preserve legacy history. No broad odds indexes.
- Same settlement semantics as Slice A. Recompute daily/cumulative summaries from actual ledger when settling older dates; won/lost stake denominator, pushes/cancellations excluded. Preserve historical stakes, do not rewrite results or fabricate history.

## Verification budget
Interpreter: `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`
Offline guard: `C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-implementation-worker/scripts/run_nba_offline_checks.py`
Run: interpreter guard WORKTREE pytest tests/test_paper_trader.py
Coordinator baseline: 25 passed, zero live I/O/model attempts before writes.
Inspect existing fixtures first. Some success mocks omit game/final evidence and will conflict with the new safety contract; do not weaken production validation or edit tests unapproved. Report exact fixtures requiring correction to coordinator, continue independent implementation.
Use only a few short controlled actual-public-path probes for populated manual settlement, missing/partial/DNP, repeated insertion/opposite side, start rejection and CAS/rerun semantics. Reuse existing scratch `paper_readiness_probe.py` if available, otherwise one lean probe in own scratch directory. Controlled data must be labeled controlled, never production evidence.
Run scoped Ruff and syntax checks, review exact diff/new files. Do not run real DB/network/inference/model loads; SQL preflight is owned by separate read-only runner.

## Delivery gates
- [x] isolated branch and offline baseline
- [x] Slice A implemented and controlled caller path verified
- [x] Slice B implemented incl producer metadata, prepared additive migration and ledger summaries
- [ ] existing suite compatibility; named fixture approvals if necessary
- [x] populated public caller probes + material failure cases (controlled, not production)
- [x] independent spec and quality review, fixes and rerun (subsequent main-overlap check recorded below)
- [x] rollout instructions and exact remaining live gates
No production readiness claim until migration + deployed placement-to-settlement cycle is separately approved and actually verified.

## Implementation receipt — November 7, 2026

Status: implemented, independently reviewed and offline exercised; uncommitted and not deployed. The isolated SQL runner performed bounded production SELECT-only checks; implementation/probes made no live DB/network/job/model calls. No migrations, commits, pushes or merges were performed. No tests were edited or added. Scheduler/placement windows and frequent-refresh `--skip-paper` remain unchanged.

### Exact changed files (relative to this worktree)

- `src/paper_trading/paper_trader.py`
- `src/paper_trading/user_bet_resolver.py`
- `src/paper_trading/nba_settlement.py` (new)
- `src/paper_trading/nba_paper_entry.py` (new)
- `src/models/daily_runner.py`
- `src/models/prediction_store.py`
- `src/orchestration/inference_job.py`
- `src/orchestration/edge_refresh_job.py`
- `src/orchestration/resolve_user_paper_bets.py`
- `src/orchestration/daily_stats_job.py` (settlement invocation/summary only)
- `database/migrations/20261107_nba_paper_backend.sql` (new, unapplied)
- `.hermes/plans/nba-paper-backend-delivery.md`

### Source/schema decisions

- Used the actual receipt at `C:/Users/Chase/AppData/Local/hermes/cache/scratch/paper-backend-schema.md` before preparing DDL. No assumed `is_final`, sport, game-ID or quote-ID fields were added to existing lookup SQL.
- Manual prop-card producer (`dashboard/src/lib/hooks/useUserBets.ts`) stores prediction/player/date/stat identity and preserves manual terms; manual/CSV unlinked players remain pending. General `UserBetResolver` keeps real + paper NBA scope; standalone resolver passes `paper_only=True`. Its existing MLB resolver is isolated to its three existing MLB stat families and otherwise unchanged.
- Shared exact-game settlement checks both reciprocal team rows, matching dates, W/L consistent with non-tied scores, at least 240 team minutes each, player participation and all requested stat components. Existing source trace: CDN scraper explicitly checks `gameStatus == 3` before insertion; unified team scraper uses LeagueGameFinder W/L. Schedule final status corroborates results; an observed live/conflicting/postponed/cancelled schedule blocks. A schedule snapshot observed before tipoff is not treated as a contradiction to complete final team results, so next-morning settlement does not depend on the later schedule-refresh job. Historical games without persisted schedule use the reciprocal final-result evidence. This policy needs bounded real-data review before rollout.
- `did_not_play` alone is insufficient because old ingestion derives it from zero minutes. Cancellation additionally requires an explicit DNP/DND/NWT/inactive comment and no conflicting positive points/rebounds/assists/minutes. Missing rows, zero minutes alone, partial combos, null players and ambiguous legacy fixtures remain pending with reasons. Pending status plus the entire captured ledger JSON protects identity/terms against concurrent edits; only `RETURNING` rows count as resolved.
- Actual quote path is `nba_player_props_inference` -> paired same-snapshot quotes -> lowest-vig selected book -> empirical/BL calculations -> `PredictionStore` JSONB -> paper candidate -> insertion recheck. Edge refresh now uses the same compact reader, not independently selected raw-table sides. Quote rows, staging IDs, prices, source/availability clocks, read/decision times and effective BL config are preserved.
- Actual model path is inference/drift producer -> existing `validate_artifact_bundle` (hash-only) -> loaded pipeline identity -> prediction model snapshot -> immutable paper entry. Existing non-manifest bundles retain the explicit `legacy_unverified` label while recording actual file hashes; no legacy training provenance is invented. Sample digests prevent a later refresh from attaching old artifact metadata to mismatched separately stored samples. Re-derived combos retain component artifact identities.
- New entries use stable evaluation ID `nba-paper-flat10-v1-20261107`, fixed $10 and diagnostic Kelly fraction 0. Existing entries are never updated by placement. The additive partial unique index prevents same-run/player/game/stat opposite-side or concurrent duplicates; existing legacy uniqueness remains as an additional conservative constraint (it may suppress same-date entries across distinct runs/games rather than rewrite history).
- Selection and placement both require fresh persisted schedule and the exact captured current quotes. Existing 90-minute current-day / 48-hour future-day schedule semantics and 48-hour quote-observation eligibility are reused, with no new TTL. Selected fixture/quote row locks and insertion-time wall-clock check reject unknown/started/conflicting evidence. No HTTP fallback is used for placement.
- Daily/cumulative summaries rebuild from actual ledger, serialized by an advisory transaction lock. Won/lost stakes form the denominator; pushes/cancellations do not. Older settlements update later cumulative totals, and an empty pending rerun still repairs a previously failed summary write. Historical stakes/results are not rewritten.

### Verification receipts

Commands below were run from this worktree, with the exact interpreter and guard specified above. Final code validation: November 7, 2026, approximately 14:42 local tool log time.

1. Baseline and final existing suite:

   `"C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe" "C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-implementation-worker/scripts/run_nba_offline_checks.py" "C:/Users/Chase/Projects/GameFlowData-worktrees/nba-paper-backend" pytest tests/test_paper_trader.py`

   Baseline: **25 passed**. Final: **21 passed, 4 failed**; zero live connection/model-load/model-fit attempts. The suite is NOT claimed green. Exact incompatible fixtures, unchanged:

   - `TestPaperTraderBetSelection::test_select_bets_over_direction` (assertion line 137)
   - `TestPaperTraderBetSelection::test_select_bets_under_direction` (assertion line 180)
   - `TestResolveAllPending::test_resolve_all_pending_processes_dates_with_stats` (call line 485)
   - `TestResolveAllPending::test_resolve_all_pending_multiple_dates` (call line 580)

   Selection fixtures need actual producer quote/model snapshots and eligible schedule/quote boundary responses. Settlement fixtures need `captured` ledger JSON, an exact-game identity/result response with both final-team/participation evidence, and actual update `RETURNING` behavior rather than positional old date-wide stats mocks. They currently raise `Settlement reader requires captured ledger identity`. Coordinator may seek approval to correct those exact fixture inputs; assertions/test count need not be weakened. No such test-edit approval was assumed here.

2. Populated controlled public paths:

   `"C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe" "C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-implementation-worker/scripts/run_nba_offline_checks.py" "C:/Users/Chase/Projects/GameFlowData-worktrees/nba-paper-backend" probe "C:/Users/Chase/AppData/Local/hermes/profiles/codex-spark-worker/cache/scratch/paper-backend-probe/probe.py"`

   **PASS**, exit 0, zero live I/O/model attempts. Adapted the earlier audit probe into this worker's isolated scratch lane; no repository tests added. Real `DailyPredictionRunner.run_for_date`, coherent quote selection, empirical BL, `PredictionStore.store_predictions`, `PaperTrader.select_bets/place_bets`, edge refresh, both settlement classes and standalone paper wrapper ran with controlled source/DB boundaries. One populated recommendation/insert, then zero rerun/opposite-side inserts with original terms unchanged; start/missing/conflicting quote rejected; mismatched sample artifact metadata cleared. Win/loss/push, DNP cancellation, contradictory DNP/zero minutes/missing/partial/ambiguous/null-player/live-game pending, unique legacy fixture, superseded pregame schedule, CAS edit protection and lookup-error propagation passed.

   The actual aggregate/UPSERT SQL also executed against a disposable in-memory SQLite ledger: late older-date settlement changed the later cumulative P&L to 25 and bankroll to 10025, preserved historical 30/20 stakes, and excluded 80 push/90 cancellation stakes from the denominator. PostgreSQL JSONB/CAS/index/locking/concurrent execution was NOT executed or certified. Controlled artifact files are explicitly non-loadable hash-only fixtures, not real trained model evidence.

3. Scoped lint:

   `"C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe" -m ruff check src/paper_trading/paper_trader.py src/paper_trading/user_bet_resolver.py src/paper_trading/nba_settlement.py src/paper_trading/nba_paper_entry.py src/models/daily_runner.py src/models/prediction_store.py src/orchestration/inference_job.py src/orchestration/edge_refresh_job.py src/orchestration/resolve_user_paper_bets.py src/orchestration/daily_stats_job.py`

   **All checks passed**.

4. Scoped syntax (bash; bytecode goes to worker scratch):

   `PYTHONPYCACHEPREFIX="$TMPDIR/paper-backend-pycache" "C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe" -m compileall -q src/paper_trading/paper_trader.py src/paper_trading/user_bet_resolver.py src/paper_trading/nba_settlement.py src/paper_trading/nba_paper_entry.py src/models/daily_runner.py src/models/prediction_store.py src/orchestration/inference_job.py src/orchestration/edge_refresh_job.py src/orchestration/resolve_user_paper_bets.py src/orchestration/daily_stats_job.py`

   **PASS**, exit 0. `git diff --check` also passed. Tracked diffs and all three new source/SQL files were explicitly self-reviewed. Git status contains only the exact allowed files above; tests/scheduler/frontend/source-main worktrees were not modified.

### Rollout / rollback — commands are instructions, not executed

1. Coordinator closes independent spec/quality + SQL review and the named fixture gate. Verify receipt against actual final team/minute/DNP records and prepared SQL; review partial index and retained legacy uniqueness. Keep JOB-01..04 and stats recovery with their owners.
2. Obtain separate migration/deployment permission. Quiesce automatic placement during rollout, preserve scheduled `--skip-paper`, and apply migration BEFORE deploying code that references the new prediction columns. In an already-authenticated operator shell using approved PG connection settings (no credentials in command text):

   `psql --set=ON_ERROR_STOP=1 --file="C:/Users/Chase/Projects/GameFlowData-worktrees/nba-paper-backend/database/migrations/20261107_nba_paper_backend.sql"`

   Migration is transactional and timeout-bounded. Confirm exact added columns/index/constraint afterward; do not blindly repeat an already applied migration. No schema change is applied by Python startup. Existing prediction rows remain missing provenance until a real producer run; do not backfill made-up metadata.
3. Deploy the independently reviewed revision only after separate commit/integration/deploy authorization. Verify actual deployed commit, producer metadata and schedule/current-quote freshness, not merely job registration. The controlled script does not prove deployed artifacts, upstream readiness or real coverage.
4. Separately approve a tiny manual-paper + automatic smoke in existing windows. Read back frozen entry terms/evaluation/snapshots; rerun for no-op and concurrent index/CAS checks; verify the actual final result and Performance/History accounting. A read-only standalone preview after permission uses `python src/orchestration/resolve_user_paper_bets.py --dry-run`; it touches the real DB but does not update rows. The wrapper normalizes legacy MLB hypothetical counts into `would_resolve`. Do not run broad jobs/training as part of this handoff.
5. Rollback: stop placement first (inference `NBA_TRADING_ENABLED=false`, keep edge refresh `--skip-paper`), redeploy the prior approved application revision only with settlement/placement disabled, and retain additive columns/index/snapshots. Do NOT run the old upsert/resolver against accepted new entries. No down migration or ledger deletion is needed for a code rollback. If the migration itself fails, its transaction rolls back; investigate rather than retrying with disabled checks. Historical repair/deletion requires a separate evidenced scope.

Remaining production-only gates: approved migration/readback; deployed artifact + schedule/quote/input readiness; actual PostgreSQL query/CAS/concurrency behavior; authorized real manual/automatic placement-to-final-settlement cycle with dashboard readback. Remaining non-production gate: approved correction of the four existing success fixtures. No production-readiness claim.

## Coordinator verification and concurrent-main reconciliation

- Independently reran the controlled populated producer/placement/settlement/accounting probe: PASS, zero guarded live I/O/model attempts. Scoped Ruff passed. Independently reproduced the existing-suite result: 21 passed, 4 failed against the unchanged fixture contracts listed above. The narrow fixture-edit approval prompt timed out, so tests remain untouched and the suite is not green.
- Independent spec review found one blocker: standalone NBA dry-run totals omitted `total_would_resolve`. Fixed the wrapper to expose hypothetical totals, normalize legacy MLB preview reporting and print `Would resolve N` without claiming committed writes. Actual wrapper/CLI controlled regression passed with two NBA + one MLB hypothetical resolutions and zero committed resolutions. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/paper-backend-preview-probe.py`.
- Independent spec-close and quality reviews subsequently passed. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/paper-backend-quality-review.md`. Prediction/sample atomic persistence, additional prediction-row locking and migration rerun convenience remain optional hardening, not MVP implementation scope.
- Main advanced concurrently to `4d3c1135112ed52ad93f4d98b885fb96e9aea50b` (production job recovery); this branch's committed base remains `914e2b9a`. The only source overlap is `edge_refresh_job.py`. Preserved main's compact quote route via the shared reader and incorporated its exact compact-table game-time fallback, removed DFS step and nonproductive-output warnings into this worktree. No merge/rebase/commit was run and no main files were modified. Future integration must preserve all other files from the production-job commit, not replace main with this older branch checkout.
- Bounded real-source acceptance: the isolated runner exercised the actual `_outcome` read-only path against one settled automatic bet from November 6, 2026 (game `0022600029`). Exact prediction identity, reciprocal final team scores/W-L, numeric player minutes, both team-minute values of 240 and final schedule matched the contract; the helper reproduced the stored winning status and actual value. No DNP or pending row was observed in that game, so those remain controlled-only coverage. Eight production execute calls included two failed probes for the known unapplied columns; each failed transaction was abandoned and guards reverified. No ledger mutation occurred. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/paper-backend-final-source-check.md`.
- Subsequent independent overlap-only review: PASS. `backfill_game_times()` matches main's landed fix, DFS removal/nonproductive warnings are preserved, shared coherent quote selection and `--skip-paper` remain intact. Scoped Ruff/diff checks and the controlled public-path probe passed again with zero guarded live I/O/model attempts. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/paper-backend-overlap-review.md`. These checks are not production deployment or placement receipts.

## Next owner gate

Approve correcting only the mock inputs/adapters in the four named existing tests, preserving all outcome assertions and test count, then rerun the existing suite. Code implementation and independent review are complete; full test-suite acceptance is not. Commit/integration, migration execution, deployment and live paper smoke remain separate approvals. Suggested eventual commit subject: `fix: unify NBA settlement and freeze flat-unit paper entries`.

## Commit/merge authorization

Chase subsequently requested: "Please merge and committ all slices." This authorizes committing and locally merging both reviewed paper-backend slices A/B, including this delivery record and the prepared migration. It does not include unrelated active work, test-fixture edits, push, migration execution or deployment. Preserve the documented 21-pass/4-fail suite result; source publication does not turn that into full acceptance. Integration verification is recorded in the merge commit message and Git ancestry.
