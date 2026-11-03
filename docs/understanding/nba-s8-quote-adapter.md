# NBA Slice 8C historical quote adapter

## State

8C replay code mechanics are complete under the November 3 lean closeout scope: the shared pure adapter is wired into direct/prefetched replay through `HistoricalReplayQuotes` and the native source reader. Controlled public-path verification passed; real data validity remains Chase's lane. All output remains `decision_grade: false`.

Earlier independent acceptance covered the pure adapter at SHA-256 `1c84d179ff922dff410da906ab4c1f8bad4daf1741f7f76032d7ab3e708bd7a2`, not the later native/harness integration. The closeout is coordinator-verified, not a new independent review. See [the current delivery record](../operations/nba-slice8-completion-status.md).

Source: `src/backtesting/nba/historical_quote_adapter.py`.

## Public API

`adapt_historical_quotes(rows, *, contexts, policy)` accepts either a DataFrame or iterable of outcome mappings, an explicit mapping of exact game IDs to existing `HistoricalGameContext` objects, and the exact immutable `HistoricalPolicy`.

It returns `HistoricalQuoteResult(pairs, ledger, summary)`. Every input occurrence has a ledger row. Every eligible coherent Over/Under pair remains available for existing downstream line shopping; no ranker chooses bets here.

## Input contract

Required quote identity/value fields:

- `game_id`, `player_id`, `market_key`, `period`, `variant`, `bookmaker`, `line`, `side`;
- `source_namespace`, `source_record_id`, `snapshot_compatibility_key`;
- `odds_format`, `price`;
- `source_class`, `provenance` as `SourceProvenance`.

Explicit clocks: `snapshot_time`, source-class-specific `historical_requested_at` or `local_observed_at`, and required `market_updated_at`/`bookmaker_updated_at`. Optional raw `commence_time` must agree with the supplied authoritative context if present. Naive timestamps are not localized. Insertion time never establishes availability.

The provenance object's `provenance_evidence_ref` must match the policy's same-named field exactly. This is a binding to an upstream assertion, not proof that the upstream evidence exists or is complete. Current mixed raw-table rows must not simply be tagged as historical by a caller. Current tables also lack some of the required period/variant/snapshot-route authority. Real acquisition requires a separately reviewed source boundary.

## Boundaries and accounting

- Quote clocks at or after decision/commence are rejected in this historical adapter, without changing the generic observation primitive's inclusive semantics.
- All occurrences reach the pure quote selector before adapter vetoes are applied; an invalid-price occurrence cannot disappear before same-snapshot conflict detection.
- Reused source IDs with contradictory semantic content or snapshot clocks are source-integrity blocked, not ordinary no-edge/dropout.
- Missing/mismatched provenance and pandas-null source fields produce measured blocked rows rather than nullable-boolean exceptions.
- Exact integral player IDs normalize to Python integers for deterministic merge-compatible output; opaque string IDs remain distinct and unchanged.
- Only the explicit 8–10 ASCII-digit NBA padding seam can map to an exact supplied ten-digit context. Raw game tokens and mapping identity remain in the ledger; arbitrary opaque IDs or whitespace are not repaired.
- Side metadata includes semantic/source references, observation payload, canonical snapshot-clock values and deterministically ordered raw occurrences. Ledger occurrence indices preserve acquisition order separately.

## Controlled verification

One disposable real-public-constructor probe used entirely labeled controlled policy/context/source inputs. It proved coherent pairing, iterable/DataFrame and reverse-order pair parity, nullable source-field blocking, policy-evidence mismatch blocking, exact cutoff rejection, conflict-before-filtering, duplicate retention, reused-identity/snapshot conflicts and the explicit padding seam. No DB/network/model/artifact execution or permanent unit tests were added.

Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8c-acceptance-receipt.json`. Scratch may expire; this page preserves the decisive scope and limitations.

## Replay seam now implemented (controlled mechanics)

`HistoricalReplayQuotes` in `src/backtesting/nba/historical_replay.py` is the shared acquisition/adapter owner used by the harness's explicit historical branches in direct and prefetch mode. It enforces request markets/books, explicit route authority, game/candidate caps and context agreement. The normal harness uses `PostgresHistoricalSources.quote_rows` with the same reader's contexts/routes and explicit embedded/sidecar metadata. Its raw-table fallback does not invent absent authority. Missing or ambiguous evidence blocks rather than becoming authenticated quotes. No real native query was launched during verification.

Explicit controlled quote rows can exercise the real feature/harness path, with decision-grade always false. Exact pair/clock metadata survives the existing edge merge; the existing math, sample/combo checks and bet selection remain unchanged. Numeric conversion happens only at the existing calculator boundary with finite/round-trip checks; exact original values remain metadata. Unsupported odds formats block. Quote ledgers can be exported as structured JSON without lossy Decimal conversion.

The November 3 closeout reran the actual-harness and native-reader probes plus 21 selected existing tests. Pair/ledger and prediction/edge parity, empirical probability, metadata, sanitized query errors, protected numerical ASTs, public run/export, exact serialization and candidate-cap propagation passed. Older 216-test evidence above this closeout's scope is historical, not a new full-suite run.

### Effective game population

`BacktestHarness.run()` supplies `prefetched_features` to `_prefetch_all_lines`. Historical prefetch applies the same `player_avg_min_l5 >= min_minutes_avg` filter as direct replay, then passes the resulting game IDs to `HistoricalReplayQuotes.for_date`. This avoids fetching irrelevant configured games and incorrectly hitting acquisition limits. Callers that invoke `_prefetch_all_lines` directly should supply the same feature mapping when they want feature-population parity; omitting it explicitly retains all configured contexts in range.

A missing historical prefetched date is an execution error. A present empty quote frame is different: it can represent a measured no-eligible-pair result. Neither query/cap failures nor missing required source metadata may be converted into a healthy empty result.

## Not completed by these modules

Real source authenticity/coverage and live PostgreSQL/schema validation, CLI/artifact-policy threading, lifecycle integration, bounded real-data acceptance, frozen project windows and the owner baseline launch remain separate work. The native reader architecture is implemented; successful controlled execution is not evidence that real sources satisfy its contract. Default legacy production behavior is preserved; it is not relabeled quote-clean by these opt-in mechanics.
