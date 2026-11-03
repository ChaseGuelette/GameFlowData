# NBA position repair and retraining-readiness gates — November 3, 2026

## Latest checkpoint — final mirror verified, training-data check next

Chase completed the bounded opponent-allowed generation and the final support transfer. The actual receipt `logs/sync/nba_support_final_execute_20261103.json` was independently read and programmatically verified: `status=success`, `phase=complete`, no failed tables, all 13 planned tables confirmed, and 31,293 selected/exported/staged/copied/committed/read-back rows agree. The October 3–November 2 selection includes 616 opponent-allowed rows, 2,887 basic and basic-average player rows, 2,887 advanced-average rows, 2,885 raw advanced rows, and 214 team/basic-average rows each. Raw advanced/basic count differences are not by themselves evidence of missing played-player keys.

The mirror is complete for this recovery window. **The final actual local training-feature check has NOT run; retraining has NOT been launched or authorized by this checkpoint.** Resume with that check, not another scrape/average/sync cycle. Apply Chase's approved drop-unknown-position rule below; do not reopen historical reconstruction as a prerequisite for the excluded cohort. All 53 unique players in that missing-history cohort were matched to both external 2026–27 rookie lists; see [research](nba-position-cohort-rookie-check-2026-11-03.md).

Source caveat: the initial validity/exclusion fixes are in concurrent commit `0cfb6a5d`. The subsequent materialized incremental-query rewrite remains a local change. Its six controlled SQL cases pass, but its actual fetch still exceeded the eight-second diagnostic limit and its estimated plan cost increased to 5,436,467.19. Do not call it a verified optimization or infer owner-job failure from that diagnostic cap: Chase subsequently reported the real backfill complete, and the final transfer read back 616 generated rows. Preserve the local diff for a separate review; wrap-up does not revert or publish it. Latest receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/final_materialized_position_drop_verification_20261103T151411Z.json`.

## Scope and permissions

Chase approved the local position-guard fix, read-only contamination audit, bounded historical-repair preparation and readiness assessment. Source edits are not deployment, data repair or training execution approval. Chase launches data jobs. Do not commit/push/deploy, start backfills, pause/restart schedules, or start training under this approval. Slice 8 remains owned in another worktree; do not modify its code or choose its historical policy here. Skill reorganization remains deferred.

## Owner-confirmed interim position policy

Chase approved excluding player/game contributions with no established valid pre-game position from position-dependent queries. This is query-time exclusion, not deletion of raw player/team stats. Use the most recent eligible **valid** position snapshot strictly before the game; malformed blank/unknown primary positions and inconsistent G/W/B mappings are not valid evidence. Ordinary position-independent averages remain unchanged.

Later games automatically become eligible once a valid earlier snapshot exists and the regular derivation runs. Today's snapshot does not retroactively qualify earlier games; legitimate historical corrections require replay of affected dates. The resulting position aggregates intentionally cover the known-position cohort, not every opponent player. Minimal impact is an owner hypothesis, not a measured result.

This decision supersedes the proposed 41-key retrospective repair as the active next step. The 190 no-evidence keys are excluded under the approved rule, not a reason to keep requesting a historical policy decision. It does not authorize database deletion, historical reconstruction, job execution, deployment or training.

## Verified and reported progress

- Owner's local advanced catch-up saved all 15 audited November 2 games (`0022600158` through `0022600172`) with zero reported failures.
- Actual canonical `count_missing_advanced_pairs` returned zero through November 2, season `22026`, covering positive-minute player possessions and required team advanced fields. Read-only receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_advanced_gate_20261103.json`.
- Chase reports the November 2 averages update completed. Exact output-key verification is a separate read-only audit, not established solely by that report.
- Earlier support transfer succeeded: all 13 tables, 29,889 selected rows committed/read back. Receipt: `logs/sync/nba_support_post_averages_execute.json`. That transfer preceded the latest advanced catch-up/averages and must not be presented as current parity afterward.
- Railway deployment `913f146a-d66c-438a-933c-f55510895a48` at `04cdb08a` was observed SUCCESS/RUNNING. Its scheduler registered 9 AM Daily Stats and 9:30 AM retry. The old deployment's 9 AM run failed at DFS entry resolution; scheduler callback success did not mean data-job success. The new local position guard is NOT included in this deployment.

## Local source correction — implemented, not deployed

File: `src/scrapers/update_player_position_history.py`.

Both the single-date and quarterly-backfill SQL now accept only `G`, `G-F`, `F-G`, `F`, `C`, `C-F`, `F-C` before ranking/counting. B is assigned explicitly only to `C`, `C-F`, `F-C`; unknown values no longer default to B. Existing valid mappings, tie ordering, date windows and keyed UPSERT behavior are unchanged. This prevents new invalid classifications; it does not remove or correct any already stored bad snapshots.

Validation: five unchanged existing tests pass with real psycopg2 connections prohibited; actual SQL capture from both functions confirms the allowlist and explicit B mapping; scoped Ruff, compilation and whitespace checks pass. No new/modified unit tests or real snapshot-write probes. These are offline code checks, not deployed production-output acceptance.

## Approved drop-unknown derivation implementation

The same valid, strictly pre-game position lookup is now used by:

- `src/processing/backfill_opponent_allowed_incremental.py` — active incremental opponent history.
- `src/processing/backfill_opponent_allowed.py` — manual/full opponent history; removed the synthetic `U` fallback.
- `src/scrapers/update_league_position_averages.py` — current league-position aggregates.
- `src/processing/backfill_league_priors.py` — historical league-prior snapshots.

Each filters known `(primary_position, position_group)` pairs inside the ordered as-of lookup, before `LIMIT 1`, and uses an inner lateral join. No eligible valid snapshot means no contributor row. An invalid newer snapshot does not hide an older valid one. Same-day/future snapshots remain ineligible. Existing shift(1), rolling windows, required advanced guards and numerical aggregates are unchanged.

All 74 unchanged existing tests across the four affected suites pass with real DB connections forbidden, including a rerun after the incremental query-shape correction below. Captured SQL from all four public derivation functions confirms the shared eligibility contract. Scoped Ruff, compilation and whitespace checks pass. No tests/fixtures were added or modified. During this lane's work, concurrent integration commit `0cfb6a5d` incorporated the initial five-file eligibility changes; this lane did not create that commit. The subsequent incremental-query performance correction remains uncommitted. No job, deployment, deletion or historical reconstruction was launched by this lane.

The production incremental SQL was executed in PostgreSQL against clearly synthetic `VALUES` inputs, replacing table identifiers only: all six cases passed in 0.682 seconds (pre-position game excluded, later game after valid position included, same-day/future excluded, blank-primary/B excluded, mismatched G/B excluded, older valid C/B retained despite a newer invalid snapshot). This is controlled execution, not actual production row-count evidence.

The actual populated October 20–November 2 public fetch/pandas path was attempted read-only and canceled at the eight-second diagnostic limit. The strictly smaller October 20-only public-path smoke also timed out at the same limit. Neither yielded production row counts or rolling results, so real-data runtime is NOT verified. A bounded read-only `EXPLAIN` comparison of baseline and changed SQL is now being prepared before any query-shape repair. No larger retry, timeout increase, data job or index operation was launched. Receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/position_drop_validation_receipt_20261103T145635Z.json` and `opening_night_allowed_smoke_20261103T150325937844Z.json` in the same directory.

The read-only `EXPLAIN` comparison then isolated the cost increase to the repeated valid-position lateral lookup (estimated cost 15.83 per lookup versus 1.54 without the validity filter); the existing large advanced-table scan was common to both plans. The incremental query now materializes the relevant team-game history, deduplicates required `(player_id, game_date)` position keys, computes each valid position lookup once, then joins those positions back for the unchanged sums. Relevant-season history and shifted rolling semantics are preserved. No index/DDL or increased timeout was introduced. Final controlled and full-window read-only execution of this corrected shape is pending; earlier timeout and controlled-pass receipts must not be mistaken for acceptance of the new source bytes.

The separately owned slice-8 feature-consumer source is untouched. Its owner should apply the same valid-position/excluded-cohort contract when verifying the selected training/inference path; this implementation does not claim those independently owned consumer queries were changed.

## Earlier historical reconstruction proposal — superseded, not executed

Audited October 20–November 2 missing-position cohort: 231 player/game keys across 53 players. Of these, 41 keys across nine players have retained usable positions from earlier games; 190 keys have no qualifying prior usable evidence. The nine players also have earlier no-evidence keys; no player is fully recoverable across the whole missing cohort.

Candidate groups among those 41 keys: 23 W, 14 G, four B. Invalid source rows in this missing cohort included 142 distinct prior advanced records with empty-string positions; these must not become B classifications. This source-row count is NOT a count of already contaminated snapshots. A separate stored-snapshot audit is required.

Exact candidate scope and evidence bounds are frozen in [candidate manifest](nba-position-repair-candidates-2026-11-03.json). It contains only the 41 candidate target keys and nine players, with proposed snapshot dates one day before each target game. It is NOT an executable backfill or historical-acceptance receipt.

The following requirements apply only if Chase separately revives this optional reconstruction proposal; they are not blockers to the approved drop-unknown query path. Before any historical write:

1. Obtain approval of retrospective reconstruction as a source-policy choice; retained historical game dates do not prove original operational ingestion/availability.
2. Recompute each candidate using the actual updater's `[snapshot_date - one year, snapshot_date]` window. The audit classified using `[target_date - one year, target_date)`; moving snapshot date to target minus one day changes the lower bound by one day, so require expected group/confidence readback rather than assuming identical output.
3. Recheck current eligible snapshots and conflicts; concurrent routine jobs can change state after this receipt. Preserve valid existing snapshots.
4. Restrict execution to exact approved player/snapshot pairs. The existing `--date` command updates all qualifying players, so it is NOT an executor for this player-scoped manifest. Do not pass unsupported player-ID flags or silently broaden the repair to all players. Prepare the exact restricted SQL/executor only after the policy and write scope are approved.
5. Use no target-game/later positions, current roster backdating, fuzzy assignments, or unknown-to-B fallback.
6. Verify exact written rows and strict `snapshot_date < target_game_date`. Remaining no-evidence keys stay unresolved unless the owner approves another genuinely historical source or explicit missingness/cohort policy.

## Opponent-allowed implications

`fetch_raw_allowed_stats_incremental` now uses an inner as-of lateral lookup (`snapshot_date < game_date`) restricted to valid primary-position/group pairs. Unknown-position contributions are dropped before team/position aggregation, not merely after rolling metrics or from a player's final training row. Consequently output totals intentionally cover the eligible-position cohort; do not present them as full-roster totals or assume the impact is negligible without measurement.

The interim source/cohort policy is now approved: omit unknown/invalid-position contributions before aggregation, including malformed stored snapshots. Historical cleanup/reconstruction is not required to use that policy. The local query correction must pass its scoped checks before owner launch; production requires separate deployment. The direct runner supports a bounded target date range while preserving relevant-season history for shifted rolling computation. Existing owner command (not launched by this lane):

    .\venv\Scripts\python.exe -X utf8 src\processing\backfill_opponent_allowed_incremental.py --start-date 2026-10-20 --end-date 2026-11-02

Repair routine automation by deploying reviewed source and then checking real stages, first retained failure and actual canonical outputs. Keep Railway CDN-only; advanced scraping remains local. Do not run the full wrapper as an emergency catch-up shortcut while its unrelated linker/schema gates are open.

## Retraining verdict

NOT YET CERTIFIED for an unspecified training cohort. Finishing this current-season recovery is necessary for that affected cohort, but does not certify all historical training/calibration/test seasons.

Clear the data gate with:

- Exact intended train/calibration/evaluation dates. The interim missing-position treatment is approved above; quantify the excluded cohort and aggregate-contributor effects rather than requiring every historical position to be recovered.
- Source/derived key and required-value coverage, past-only semantics, and no invalid/default-only position/opponent features throughout that intended cohort.
- Updated bounded support transfer after final canonical repair, with exact selected-row readback. Raw prop/game-line sources retain their separate paths; verify as-of quote coverage if the chosen training features consume them.
- A short populated execution through the actual selected training-feature loader, including missing-source behavior and feature variation. If the retrain uses slice 8's new consumer, verify that specific integration with its owner; do not require unrelated roadmap completion or overwrite their worktree.
- Explicit training launch approval and an owner-run command. Passing this data gate is not promotion or live-model approval; post-training out-of-sample evaluation remains separate.

Current-season gaps are not a blanket ban on an unaffected, separately validated historical cohort. Conversely, successful copy or green unit tests do not certify the proposed cohort. Railway automation health is a separate reliability gate, not a reason to block offline training from an already validated frozen local dataset.

## Relevant prior lessons/invariants

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: temporal safety must hold through the consuming query, not just source date columns.
- `operations/critical-invariants`: rolling statistics retain eight-minute/shifted-history semantics, advanced scraping stays local, Railway remains CDN-only, and long runs are owner-launched.

## Completed read-only audit

The stored-snapshot audit found 5,850 empty-primary-position rows among 8,714 snapshots dated October 3–November 3. Current-season as-of selection for October 20–November 2 selected an empty-primary snapshot for 1,095 player/game keys; another 231 lacked any eligible snapshot. These are separate groups. The new query rejects malformed snapshots without deleting them and can fall back to an older valid snapshot.

November 2 coverage was independently verified: zero missing player basic/advanced average keys across 332 positive-minute keys; zero missing team-average keys across 30 team/game keys; zero missing advanced source rows/possessions, mapped player advanced values or required team fields. Exact read-only receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_position_audit_20261103_092323/`.

No database repair was launched by this lane. The approved drop-unknown SQL passed controlled PostgreSQL execution; full-window real-path validation timed out at the diagnostic cap as recorded above. Read-only validation is separate from a writing backfill or deployment.
