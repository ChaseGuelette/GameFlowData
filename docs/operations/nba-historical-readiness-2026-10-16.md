# NBA historical-readiness package — October 16, 2026

## Kick off immediately

- You: let the already-running [NBA-only refresh](nba-sync-inventory-2026-10-16.md) finish; no duplicate/all-sports sync. No real-data S1 validation while targets are being rewritten.
- Other chat: S1 is delivered, including the owner-approved nested `_run_date` change. Preserve the worktree pending [coordinator acceptance](nba-s1-coordinator-review-2026-10-16.md). No other source package is ready to launch.
- This chat: own next-stage read-only source/timestamp research and review; do not edit the S1 files concurrently.
- You: read the S1 coordinator review, then the [wiki](../understanding/nba-rebuild.md). Read this report to challenge evidence and decisions; do not relaunch the delivered implementation while acceptance is open.

Status: initial local data evidence is stale-local-only; remote metadata preflight complete; owner-run NBA full sync in progress. S1 is implemented in its separate worktree with coordinator-rerun existing tests/lint and independently rerun controlled probes passing; real-data/artifact acceptance remains open. No integration or historical training-readiness certification.
Coordinator: this chat. Delegation batch: `deleg_167d9016`.

Follow-up source-only S2 research: [clock evidence and policy gates](nba-s2-source-clock-review-2026-10-16.md). Historical request time has a conditional conservative bound; mixed live clocks and injury-history availability still require validation. No post-sync row-coverage result is claimed.

## Ownership and tracking

| Lane | Agent ID | State | Bounded output | Stop condition |
| --- | --- | --- | --- | --- |
| A: historical data | `sa-0-8b6ac719` | Report reviewed; accepted as sampled evidence only | Actual local SELECT-only sampled coverage, exact queries/counts, unknowns | Nine SELECT queries completed; no whole-period certification |
| B: retraining blockers | `sa-1-abbdc6ff` | Report reviewed; six scoped defects accepted | Current training/date-range/injury code blocker matrix | File-only; no SQL or implementation |
| C: repair spec | `sa-2-ca76844e` | Draft reconciled into reviewed repository spec | S1 exact scope; S2-S5 explicit gates | Launch only S1 via separate owner handoff; no blanket implementation approval |
| A follow-up | `sa-0-95680220` | Report reviewed and accepted (`deleg_baa5778b`) | All 12 missing keys classified; source metadata inventoried | Six SELECTs, exact sampled IDs; no raw-odds row scan |
| Coordinator | This chat | Research/spec package complete; next research owned here | Evidence reconciled; wiki/spec updated | Source/policy/long-job gates remain explicit |

A and B are independent. C can draft from known code issues now, but its conclusions cannot substitute for A/B evidence. Running three agents does not make their tasks independent at acceptance time.

## Scope and guardrails

- Local training database only for A. No silent production fallback.
- Candidate diagnostic seasons: `22023`, `22024`, `22025`; these are sample coverage checks, not a chosen training/calibration/OOS split.
- Exact tables: `player_game_stats`, `player_game_advanced_stats`, `player_average_game_stats`, `player_average_advanced_stats`.
- Schema/index discovery before keyed data checks; one connection, read-only transaction, bounded query/lock timeouts.
- No raw-odds scans, sync, backfill, migration, source edits, deployment, training, or new unit tests.
- Credentials may be used through the existing runtime configuration; never read or print credential-file contents or connection URLs.
- Agents write distinct scratch artifacts. Only the coordinator edits repository documentation.

## Returned artifacts

Scratch directory:
`C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-readiness-20261016-170947/`

- `data-verification.json`: exact query text, params, bounded results and failures.
- `data-verification.md`: local evidence and limits.
- `retraining-blockers.md`: current-code evidence and owner decisions.
- `repair-spec-draft.md`: provisional sequenced repair packages.
- `data-followup.json` / `data-followup.md`: exact missing-key classification and source-family metadata.

The coordinator read the reports, reviewed the probe/query shapes and parsed the recorded follow-up results. Durable reviewed findings are in this report and the [reviewed repair specification](nba-historical-repair-spec-2026-10-16.md). Scratch evidence is temporary. A proposed JSON-archive script was denied and was not retried; this documentation update does not claim that a durable raw-query archive exists.

## Reviewed results and corrections

### A: data sample

The configured local training connection confirmed a read-only transaction, 10-second statement timeout and one-second lock timeout. Nine SELECT queries completed; no raw odds or production mutation was performed.

| Season bucket | Sample | Basic source keys | Advanced / basic rolling / advanced rolling matches |
| --- | --- | --- | --- |
| `22023` | Ten regular-season games on 2024-04-14 | 233 | 233 / 233 / 233 |
| `22024` | Ten regular-season games on 2025-04-13 | 199 | 199 / 199 / 199 |
| `22025` | Ten playoff games, 2026-05-24 through 2026-06-13 | 300 | 288 / 288 / 288 |

No sampled duplicate source keys or null core labels/minutes were found. Follow-up classified **all 12 missing keys as zero-minute DNPs**. All **202/202** sampled appearances with at least five minutes had advanced source, basic rolling and advanced rolling rows; all **182/182** at least eight minutes were also covered. Do not recommend a backfill for these DNP keys or treat them as evidence of missing eligible training rows.

Follow-up minute bins: zero minutes 76 keys (64 covered, 12 DNPs missing); between zero and five minutes 22/22 covered; five to under eight minutes 20/20 covered; at least eight minutes 182/182 covered. Six read-only SELECTs were executed with the same one-connection/timeouts policy. These are the exact ten sampled playoff games, not full historical coverage.

Metadata-only follow-up found typed quote/update/commence timestamps in both local odds families and `report_date` plus textual `report_time` on injuries. This does not prove timestamp semantics or historical version retention. `public.games` and `public.nba_game_schedule` were absent locally; production was not checked and no migration/sync was performed.

Review limits/corrections:

- Samples are concentrated at season ends and differ in game type. They are not representative historical-cohort certification or apples-to-apples season comparisons.
- Sample selection grouped all rows in each season before `LIMIT 10`. Its output was limited and execution timeout-protected, but it was not strictly input-bounded; the subsequent coverage joins were keyed to the sampled IDs. Do not repeat that selection shape for larger tables.
- A rolling row attached to a short/DNP performance does not prove that performance contributed to its rolling values. The incremental basic producer actually masks prior appearances below eight minutes while preserving row/schedule counters (`populate_average_stats_incremental.py:245-319`). Its advanced counterpart shifts without that mask (`:326-347`), confirming a producer-policy mismatch independent of row existence.

### B: current code

Six reviewed issues: unrestricted training game-line selection, prop features without a decision cutoff, injury history without observation-time selection, training/range spread-sign divergence, feature/default divergence, and partial date-range returns after failed chunks. The coordinator directly verified the partial-return branch at `date_range_loader.py:330-367`; logs are not a completeness gate.

### C: spec review

The original scratch draft covered both direct and bulk replay paths but was not accepted unchanged. The [reviewed repository spec](nba-historical-repair-spec-2026-10-16.md) incorporates these corrections and retains unresolved source/policy gates:

1. No default 18:30 decision policy inferred from an old code comment; freeze the strategy policy explicitly.
2. Do not claim A validated market/injury timestamps: A's initial authorization covered four stats tables only. The completed follow-up inventoried source metadata; metadata alone still does not prove historical availability semantics.
3. Add B's ordered schema/default, spread-direction and failed-chunk acceptance requirements explicitly.
4. The completed classification found only zero-minute DNPs in the missing sample. No backfill or cohort exclusion is justified from those keys.
5. Do not require spread and total to occupy one physical row or share a cross-market snapshot; validate the actual normalized row shape and select each required market deterministically under the same decision cutoff.
6. Separate trainable-row eligibility (currently five minutes in the inspected loaders) from eight-minute rolling-contribution policy.

## Decisions already made

1. Separate historical retraining readiness from customer-publication readiness.
2. Do not make roster/frontend/Engineering OS work a training dependency unless the actual historical inputs require it.
3. Spec creation is authorized; implementation is not authorized by this package.
4. No new unit tests. Preserve existing tests and verify later implementation with scoped existing checks and real CLI/data paths.
5. Do not treat sample coverage as certification of every historical row.
6. S1 is the first bounded implementation handoff: fail closed on query-chunk and unexpected per-date execution failures, without changing feature or odds policy.
7. Documentation/research can advance while Chase reviews completed packets; research recommendations do not silently become approved source or betting policies.

## Decisions pending evidence

- How representative cohort coverage and actual feature variation look across opening/middle/end regular-season windows.
- Which historical decision-time and injury-availability policies can the retained data support?
- What training/calibration/OOS split should be frozen?
- Whether Chase has launched S1; its implementation/integration and validation evidence are not yet present.

## What these lanes teach

Documentation-only continuation: file-tool edits were confirmed and the new links point to documents read here. No scripts, new DB queries, tests, `git diff --check` or evidence-archive retry ran during that continuation. Repository docs were updated; remote GBrain was not changed.

Data verification asks whether the required information is present in the training database.
Blocker review asks whether the code uses it correctly.
The repair spec turns verified gaps into a bounded change another agent can implement.
None of the three, alone, establishes model profitability or permission to publish picks.

See the [NBA rebuild wiki](../understanding/nba-rebuild.md) for the overall data-to-training flow.
