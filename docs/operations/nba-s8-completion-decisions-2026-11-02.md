# Slice 8 completion: owner decisions and execution gates

## Authority and workspace

Owner: Chase. Decisions obtained through the live clarification form on November 2, 2026 (Eastern).

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-completion`
Branch: `repair/nba-slice8-completion`
Integrated base: `e2f049be83107920b20fc318f5384a6d7ceb0436`.

This record supplements the [Slice 8 umbrella plan](../../.hermes/plans/2026-11-02-nba-slice8-historical-integration-validation.md), [source-policy matrix](nba-s8-source-policy-matrix-2026-11-02.md), and [implementation checkpoint](nba-s8-implementation-2026-11-02.md). It supersedes earlier unanswered-clarification wording only for the explicit decisions below. It is not a claim that all 8A policies or data gates are resolved.

## Confirmed decisions

- Proceed with remaining Slice 8 implementation and bounded acceptance work, preserving the established model, numerical owners, ordered feature schema, production defaults and no-new-unit-test/fixture-edit constraints.
- Decision time: game-relative T-60 minutes.
- Strictly block missing or ambiguous sources, injury history and commence; no diagnostic fallback.
- Preserve target eligibility at 5 minutes and prior-game rolling contribution at 8 minutes.
- Preserve coherent paired quotes, explicit provenance and unchanged production behavior.
- Review and incorporate only necessary historical S1 fail-closed changes in this isolated lane. Do not commit, merge into main, push or deploy. The original S1 worktree remains read-only.
- Inspect evidence and propose exact cohort/split windows and feature-family policies for Chase's approval. This authorizes discovery and recommendations, not inferred approval of their values.

## Pending owner decisions and evidence

- Exact train/calibration/protected-OOS/independent-validation windows.
- Per-family season/carryover, cold-start and team-transition policies.
- Specific authenticated source classes, acceptable clock/freshness semantics and evidence of complete injury history/omission treatment.
- Exact manifested artifact and sufficient bounded source window for actual feature/model/quote acceptance.
- Frozen final baseline configuration and Chase's launch. Do not launch long training/sweeps from the agent.

No policy choice may be selected using retrospective ROI. Unknown source facts remain unknown; owner policy approval cannot establish absent provenance or injury history.

## Work sequence

Current family recommendation: [family-policy proposal](nba-s8-family-policy-proposal-2026-11-02.md). A second clarification form requested approval of that proposal and a choice between implementation-with-blocked-data-gates versus evidence-repair-first. The form timed out with both questions unanswered. No policy or scope answer is inferred.

1. Read-only source-readiness SQL, bounded file discovery and independent S1 diff review run in parallel.
2. Reconcile findings into one exact policy/family/window approval packet and narrow implementation scopes. Do not add generic scaffolding in place of missing semantics.
3. Implement shared feature/injury consumer integration, then shared direct/prefetch quote replay, with existing suites and a few actual-path probes.
4. Implement bounded verifier using existing feature, artifact, predictor, quote, sweep, audit and lifecycle owners, not duplicate implementations.
5. Validate actual bounded data only where source evidence and approvals support it. A blocked outcome is not accepted Slice 8D.
6. Freeze/dry-run the baseline only after prerequisites pass; Chase launches it. Historical acceptance and production promotion remain separate.

## Relevant prior lessons/invariants

- `operations/critical-invariants`: empirical CDF; no global offsets; preserve Q10 edge and quantile order; temporal integrity; isolated SQL; owner-launched long jobs.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: actual consumer queries and artifact path must prove as-of semantics.
- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`: metadata/index-aware bounded reads; timeout is unknown, not zero.
- `lessons/cheap-baseline-before-architecture`: preserve minutes × rate and start raw/no-BL, flat-stake.

## Current verification

Fresh main Git status confirms the separate automation edits remain dirty and untouched. Slices 1–7 and partial 8 are already integrated at the new worktree's base. No source implementation, tests, DB mutation, archive access, model execution or long baseline launch has yet occurred in this completion lane.

### Current remote metadata evidence

The first SQL worker failed with provider HTTP 400 before executing SQL. Its clean replacement completed a single remote `information_schema.columns` SELECT for exact names `raw_player_props_combined`, `raw_game_lines`, and `nba_injuries`.

- `raw_player_props_combined` has 19 columns, including quote clocks and bookmaker identity, but no explicit producer/ingestion-route/job/batch provenance column. Bookmaker identity does not authenticate historical versus live ingestion.
- The other two exact names returned no rows; that is a name mismatch, not proof that game-line/injury sources are absent. Parent source tracing identified actual owners `raw_game_lines_staging` and `rapidapi_injuries`, and a follow-up isolated metadata check uses those names.
- Schema absence alone cannot rule out external authenticated ingestion receipts. None has yet been identified or accepted in this lane; do not label existing rows authenticated from their timestamps alone.

Exact successful query:

```sql
SELECT table_schema, table_name, ordinal_position, column_name,
       data_type, udt_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('raw_player_props_combined', 'raw_game_lines', 'nba_injuries')
ORDER BY table_name, ordinal_position;
```

### Follow-up remote evidence

The isolated runner inspected columns/indexes for the actual `raw_game_lines_staging`, `rapidapi_injuries` and `game_id_map` tables, then an indexed April 12 injury sample capped at 10 rows, with a 10-second statement timeout.

- Game lines have nullable `commence_time` and quote/update clocks but no explicit historical-route provenance. A raw quote commence field is a candidate observation, not automatically an authoritative conflict-free schedule.
- `game_id_map` has mapping source/time but no commence timestamp.
- Injuries have `report_date`, text `report_time`, `scraped_at`, and row IDs, but no report-edition ID, timezone or completeness marker. The unique key `(report_date, team, player, status, reason)` excludes report-time/scrape-time and collapses repeated editions.
- The capped sample returned exactly 10 rows, IDs 575986–575995, all report-time `06PM` and scrape-time `2026-04-12 12:01:00.414213+00`. Statuses were eight Questionable, one Out and one Probable. This proves row shape only; no whole-date completeness claim.
- Exact modeling windows cannot honestly be proposed as executable from this metadata/sample. External authenticated receipts could be a recovery route, but none is identified here. Strict blocking remains in force.

### S1 implementation checkpoint

Necessary S1 semantics were hand-ported only at the three exception sites described in [the inclusion scope](nba-s8-s1-inclusion-scope-2026-11-02.md). No stale harness copy was used. Parent verification: 31 affected existing tests passed, scoped Ruff, compilation and whitespace passed. Dotenv was disabled, DB URL environment variables removed and networking forbidden during the tests. These are offline compatibility checks, not real source/model acceptance.

Final independent specification/quality review and controlled failure-path execution passed. Parent read the actual receipt and independently verified the reviewed source hashes match current files:

- `date_range_loader.py`: `4d6a411646eeca2744eea3eda47c67767288bfa3f94b04657503b7951f196a5e`
- `backtest_harness.py`: `e051dba1bbe77d46dadcf0a62dd9445676216e8f413ddac683741ceb1030b956`

Controlled real-class probes proved: failed second feature chunk causes zero concatenations; predictor failure reaches the caller; date failure prevents actuals, voids, simulation, metrics and returned results; legitimate empty inputs remain no-ops. Logging was enabled and captured, nonempty and sanitized. No network or credential-file-open attempts occurred. SQL constants and protected combo/sample helpers matched the unchanged base. No real data/model run was performed. Optional same-config Pyright comparison was not performed; no typing-clean claim is made.

Main concurrently advanced to `a07cd1e2`; scoped comparison confirmed no changes to the two S1 target files relative to the isolated lane's `e2f049be` base. The concurrent application commit is not this lane's integration or authorization.
