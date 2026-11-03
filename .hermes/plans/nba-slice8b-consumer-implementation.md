# Executable implementation scope: NBA Slice 8B consumers

## Authorization and purpose

Chase explicitly requests completion of all remaining Slice 8 work. Proceed with consumer implementation, not another approval loop or another generic policy framework. Exact project policy/family/window approvals and real source acceptance remain separate; no unanswered approval is inferred. Implement parameterized consumers against explicit `HistoricalPolicy` and refuse unresolved/unsupported authorities. Do not choose or freeze a modeling split, source provenance or carryover mode in project configuration.

Base: `b24a69f36dc7f7d51aef3a78381af209e6a9530a`, includes accepted S1. Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-consumers`. Existing affected-suite baseline independently run by coordinator: 118 passed. Runtime: `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`.

## Exact writable scope

- `src/models/features/nba/requests.py`
- `src/models/feature_store.py` (explicit historical request propagation only)
- `src/models/features/nba/training_loader.py`
- `src/models/features/nba/date_batch_loader.py`
- `src/models/features/nba/date_range_loader.py` (preserve integrated S1 fail-closed behavior)
- `src/models/features/nba/line_sources.py`
- `src/models/features/nba/injury_context.py`
- New shared actual consumer owner `src/models/features/nba/historical_loader.py` if needed; no second parallel framework.
- New `docs/understanding/nba-s8-feature-consumers.md` and `docs/operations/nba-s8-feature-consumer-evidence.md`.

Existing `historical_policy.py`, `historical_observation_adapters.py`, observation/quote primitives, canonical contracts/transforms/defaults are read-only dependencies. Do not redesign their schemas. If an actual blocking API defect prevents required behavior, report the exact smallest expansion before editing those protected files.

## Behavioral contract

1. Legacy/default calls remain compatible and unchanged. Historical behavior is explicit opt-in; do not silently change production inference/scheduler/Railway.
2. Propagate exact immutable policy identity through facade, training, date and date-range requests, including per-season/per-date delegation. Validate policy readiness and canonical required feature-family coverage before acquisition. Unsupported configured modes must explicitly fail, never become legacy fallback.
3. Implement one actual historical feature assembly path shared across training/date/range. Reuse existing source/transform owners and ordered feature contracts; no duplicate feature math, new model features or changed artifact order. It is not enough to tag already-defaulted legacy frames as historical.
4. Resolve exact per-game context from authoritative timezone-aware commence with explicit evidence, never game-date inference or latest raw-quote commence. Use the policy-derived decision rule. Preserve game/player/team identities and raw observation references.
5. Enforce explicit cohort and family season/carryover semantics. Implement only modes whose semantics are executable and evidenced; unsupported decay/neutral/transition policies fail explicitly, not silently approximated. Preserve target5 versus prior-game rolling contribution8 distinction. Preserve producer shift semantics: do not blindly change <= to <; verify producer lineage and source rows before defaults. Do not invent evidence that existing derived rolling rows were produced with a policy.
6. Historical line and injury values must consume the existing observation adapters and pure quote selectors with source-specific raw clocks, per-row provenance and completeness. Market features and executable quotes are distinct. Never assume a mixed raw-table row came from historical scraping based on `snapshot_time` or a policy-wide interpreter string. No insertion-as-availability inference. No naive timezone guessing or latest-ID-as-edition rule. Missing injury rows are not healthy, missing quote rows are not zero features.
7. If current source schema cannot prove required history/route/lineage, emit an exact structured blocked result/error preserving source/family/game/policy identities; do not return healthy feature rows. Implement the actual integration and rejected-population accounting, not a successful stub, all-empty pass, arbitrary exception wrapper or another policy validator.
8. Any supplied authority/evidence boundary needed for source interpretation must be small, explicit, immutable and source-record-specific; reuse existing dataclasses rather than build a generic registry/schema. Do not fabricate a real evidence file or designate a controlled fixture as authoritative. Record precisely what trusted caller inputs mean and why raw current DB sources remain blocked.
9. For the same explicit input game/player/policy, training/date/range must have identical ordered feature membership, values, missingness, line/injury/rolling lineage and readiness. Fatal query/chunk errors propagate; legitimate successful empty populations stay distinct from blocked input.

## Protected/non-goals

No tests/fixture edits or new unit tests. No DB queries/writes, credentials, archive work, sync/backfill, network calls, model deserialization/training/sweeps, commits/push/merge/deploy, production behavior, math changes (empirical CDF/Q10/quantiles/MC/copula/BL/calibration), replay harness/lifecycle edits or whole-file S1 copies. No executable project policy YAML or baseline with unapproved dates.

## Verification and delivery

Read actual signatures and consumers before editing. First short discovery then implementation; no prolonged rediscovery. Use file tools and V4A patch for existing files, write_file for new files. Write the wiki alongside code, distinguishing implemented/controlled verified/real-data blocked.

Run the 8 existing feature suites listed in the umbrella plan using the established project venv, offline with dotenv loading explicitly disabled, DB env removed and network guarded. Run scoped Ruff/compile/diff whitespace. Use at most one small disposable probe (absolute Hermes scratch path) with actual consumer public constructors and controlled DB boundary rows to demonstrate policy propagation, equal historical outputs or identical blocked outcomes across all modes, missing/ambiguous evidence handling, and legacy compatibility. Controlled rows are labeled controlled, never real source acceptance. Do not build an exhaustive scratch matrix.

Stop after completing this 8B allowlist and leave concise exact command/results, remaining real-source limitations and source hashes in the evidence document. Signal the historical shared owner API and ready sections promptly so the coordinator can prepare 8C. Do not implement 8C/D/E yourself; those are sequential downstream consumers after review.
