# NBA Slice 8D operator workflow

## State and ownership

The historical operator path is implemented and controlled-execution verified in `nba-slice8-consumers`. Source is uncommitted/unmerged. Chase owns source validity and real-data acceptance. No live database connection, trained-model loading, training, data repair, deployment or publication occurred in the implementation check.

8D code integration is not the original umbrella plan's D1–D4 real-data acceptance. An attached policy, matching file hashes or an integrity PASS does not certify historical authenticity, database freshness, profitable results or model promotion.

## One existing command chain

`replay CLI / attach-mode lifecycle -> policy + metadata attachment -> native FeatureStore -> historical harness -> shared sweep inputs -> sweep export -> evidence audit -> report-only decision`

No second verifier, simulator or policy format was added. The standalone `scripts/verify_nba_historical_readiness.py` proposal remains deferred; existing commands provide the operator surface.

## Inputs

Both `src/backtesting/run_backtest.py` and `python -m src.backtesting.nba.run_nba_sweep` now accept:

- `--historical-policy PATH`: JSON consumed by the existing `policy_from_json` validator. Required authorities/families must be execution-ready; no domain values are inferred.
- `--historical-source-metadata PATH`: optional JSON sidecar in the existing `PostgresHistoricalSources` format. Requires policy. When omitted, metadata must be embedded in the actual source rows.
- Historical execution requires `--local`. Replay also requires explicit `--flat-bet-size` and an exact nonlegacy manifested artifact directory, not directory discovery.
- Sweep retains `--diagnostic-history`; it is acknowledgement, not historical approval. Existing required sweep controls remain required.

Historical evaluation dates must lie inside one declared OOS/independent-validation policy window and start at or after the attached artifact's actual exclusive calibration cutoff. Artifact training/calibration provenance is not rewritten or retrospectively certified by the evaluation attachment.

The policy file's bytes, canonical policy payload/content ID, metadata mode, optional sidecar bytes/hash and absolute paths are frozen in the attachment. Source file changes are checked before acquisition and completion. File hashes do not establish DB snapshot identity or freshness; no watermark is fabricated.

Metadata shape and real source facts remain defined in [the native source contract](../operations/nba-s8-native-source-contract.md). Unknown/missing facts remain errors, not default healthy values.

## Lifecycle YAML extension

Add only this optional section to an otherwise valid existing attach-mode lifecycle YAML:

```yaml
historical:
  policy: path/to/owner-approved-policy.json
  source_metadata: path/to/owner-maintained-metadata.json
```

The paths above are placeholders, not supplied data. `source_metadata` may be omitted for embedded metadata. Lifecycle-relative paths resolve against the repository root. Direct CLI relative paths resolve against its working directory; lifecycle adapters pass resolved absolute paths.

The historical route currently supports `artifact.mode: attach` only. Historical training fails configuration validation rather than launching a trainer that ignores the policy. Existing nonhistorical training configuration remains unchanged.

The lifecycle resolved configuration includes a frozen historical receipt. Stage inputs include the receipt and historical implementation hashes. A changed policy/sidecar at the same path invalidates reuse; loading changed inputs into an existing run root can produce the existing explicit configuration-conflict refusal. Use a new experiment/run identity rather than overwriting old evidence.

## Planning and execution boundaries

- Replay `--dry-run` / `--preflight`: artifact/filesystem/config validation only; no engine/model runtime or output directory.
- Sweep `--dry-run`: same no-runtime/no-output guarantee.
- Lifecycle `--dry-run`: preserves its established behavior of writing isolated planning files to `<run-root>_dry_run`; executes no child jobs. It is not a zero-file-write command.
- Lifecycle `--status --run-root PATH`: reads recorded lifecycle-owned state only; does not reopen policy/metadata or contact the DB to certify current health.
- Removing `--dry-run` executes the selected command. Real replay/long jobs remain owner-launched and require actual data/artifact/bounds approval.

Use the existing `scripts/run_nba_model_lifecycle.py --config PATH --dry-run` and `--status --run-root PATH` interfaces; there is no new alternate lifecycle CLI. A full production baseline YAML was intentionally not created because owner windows/artifact/data acceptance are not frozen here.

## Evidence and compatibility

Historical sweep runs use manifest/plan schema version 2. Nonhistorical runs retain version 1; the auditor accepts both through explicit strict branches.

The v2 extension binds:

- canonical historical attachment in fixed config and source-policy identity;
- the added historical implementation file hashes;
- a structured `historical_quote_ledger.json` containing every acquired outcome occurrence, including rejected and duplicate outcomes;
- ledger input digest, output file hash, per-date occurrence/pair counts, policy identity and adapter summary.

The auditor validates saved data only: it does not follow recorded source paths, authenticate upstream facts or query the DB. Existing timing, matching, coverage, metric and PASS/FAIL/BLOCKED meanings were not weakened. Requests for unsupported or unproven historical readiness remain BLOCKED even when serialization and integrity succeed.

Historical feature/context/provenance dataclasses and enum values are projected to inert exact JSON at the sweep evidence boundary. Decimal values retain explicit decimal tags. Numerical feature values, probabilities, sample/combo math and settlement owners are unchanged.

The sweep passes historical kwargs to feature acquisition and the effective feature population to quote prefetch, preserving 8C's exclusion/minutes-filtering parity. Missing required acquired dates or incompatible feature receipts fail explicitly. A shared acquisition is reused across cells; no per-cell source reselection occurs.

## Bounds still requiring owner acceptance

Reader batch size is not a global player cap. Existing game/quote caps are not a total feature-query budget. The original verifier proposal's full pre-acquisition player/resource caps are not implemented by this operator-wiring tranche. Before a real D2/D3 smoke, establish approved bounds or scope that additional control separately; do not infer bounded SQL reads from a one-day command alone.

## Verified November 3, 2026

- 55 existing tests passed across replay CLI, harness, line-source and legacy sweep suites. The legacy sweep suite is compatibility evidence, not proof of the new shared-input sweep.
- The actual replay CLI and actual sweep CLI -> producer -> audit exporter -> lifecycle decision chain passed on one date, one stat and one raw/flat cell with controlled DB/model boundaries. The fixture artifact was produced by the real manifest writer but contains explicitly dummy, never-deserialized payloads; it is not a trained model.
- Normal native source reader/assembler/harness execution was retained. Reused prior controlled source producers, including an OUT player without a target box score and one rejected late quote pair.
- Replay/sweep dry-runs passed with engine/model entrypoints forbidden and no output directories created.
- Malformed policy, metadata without policy, remote historical target and unsupported historical training were rejected.
- Verified lifecycle reuse launched zero new children. Same-path metadata drift withdrew current success. Corrupted ledger bytes failed the real auditor. Requested historical readiness remained BLOCKED.
- An existing producer-exported v1 bundle still passed the auditor.
- Scoped Ruff, compilation and whitespace checks passed. 8B/8C source fingerprints and protected sweep numerical/identity ASTs remained unchanged.

Disposable proof: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8d/acceptance.py`; result `receipt.json`; source fingerprints `final-source-hashes.json`. Scratch can expire. The durable [completion record](../operations/nba-slice8-completion-status.md) preserves outcomes and limits.
