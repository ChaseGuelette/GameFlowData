# NBA Slice 8B historical feature consumers

Status: minimal native source path implemented and exercised through public APIs and replay with controlled DB/model boundaries. Real-data coverage/acceptance and integration approval remain separate. Source is uncommitted.

## Why a separate path?

Legacy feature SQL coalesces missing inputs and selects date-level injury/market rows. Those production defaults remain untouched. An explicit historical request must instead carry the exact immutable HistoricalPolicy plus source-record-specific evidence. Policy approval does not prove that a raw mixed-table quote was scraped historically or that a derived rolling row used that policy.

## Shared owner

Training, date and range delegate to historical_loader.HistoricalFeatureLoader. Normal historical requests acquire real source rows through `historical_sources.PostgresHistoricalSources`; no caller-built evidence tuple or controlled flag is needed. Caller evidence remains controlled-only. Normal frames declare `historical_readiness=source_integrated`, controlled frames `controlled_only`; both retain `decision_grade=False` pending real-data acceptance.

The native reader acquires raw rolling, advanced, team, positional, market and injury sources, binds actual commence contexts and constructs the existing immutable source types. Explicit source metadata may be embedded in each row or supplied in a sidecar; clocks, lineage and completeness are never invented. Missing records/metadata block specifically instead of producing healthy empty frames. See [the owner data contract](../operations/nba-s8-native-source-contract.md).

The evidence boundary is an assertion by a trusted upstream source reader, not cryptographic certification. Controlled inputs demonstrate mechanics only. Derived snapshots must attest exact policy/producer lineage and prior-game contribution boundaries before the existing transforms can apply. A target with five minutes is not the same rule as an eight-minute rolling contribution; neither threshold is inferred from the other.

Controlled inputs must also supply exact per-team injury roster membership and explicit market-route authority (bookmaker/period/variant). Omitted roster rows and unsupported missing/omission policies block; market selection cannot silently substitute an unauthorized quarter/alternate route. Legacy ratio/schedule defaults are blocked before existing transforms. Only current-season controlled carryover is currently supported; other modes fail explicitly. Multiple contexts for a game and conflicting reuse of source records are rejected. Date-range output retains per-date accounting, including empty dates; unused evidence is separately recorded.

Coordinator's real-public-constructor probe produced matching training/date/range frames with 74 ordered features, preserved empty-date accounting, blocked normal caller attestations before any query, and verified roster/default guards and policy-owned rate thresholds. This is controlled mechanics only; [the evidence record](../operations/nba-s8-feature-consumer-evidence.md) gives the limitations.

## Protected behavior

No production defaults, source producers, model math, tests, fixtures, lifecycle, schema or project policy configuration are changed. Native contexts/routes/raw quotes now feed the shared direct/prefetch replay owner without changing protected probability/selection math. Existing same-game shifted rolling semantics are preserved only with explicit prior-contribution evidence; blindly replacing <= with < would discard the intended pre-game row.

## Real source limitations

Raw quote timestamps alone do not establish route/snapshot compatibility; injury dates/IDs do not prove edition completeness; derived rows need contribution lineage. These source facts are explicit inputs to the implemented reader, not reasons to leave code unwired. Chase owns data repair/coverage. No live-source acceptance, long model job or launch is claimed. The normal source path passed populated date/training/range and actual replay checks with only DB/model boundaries controlled, including OUT-player weights without target-game box scores and precise missing-metadata failures.
