# Executable Slice 8C independent section: historical quote adapter

## Boundary and approval

User authorizes remaining Slice 8 implementation. This section is independent of the active 8B writer: consume only already-integrated immutable `HistoricalPolicy`, `HistoricalGameContext`, historical observation adapters and quote primitives. Do not read/import the unfinished shared historical feature loader. Full harness/CLI integration follows after 8B interfaces freeze.

Worktree `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-consumers`, base `b24a69f3`.

## Sole writable files

- New `src/backtesting/nba/historical_quote_adapter.py`
- New `docs/understanding/nba-s8-quote-adapter.md`

No other source/config/docs/test files may be changed. Existing policy/observation/quote primitives read-only. No SQL/network/credentials/model/DB/source repair/Git writes. No tests or fixtures may be added/edited.

## Deliver actual shared adapter

Create one reusable raw-outcome-row -> temporal observation -> paired quote/ledger adapter for later direct and prefetch callers. Read actual constructors first. Inputs: raw DataFrame/records, explicit per-game `HistoricalGameContext` mapping and exact `HistoricalPolicy`. Source-specific evidence must be explicit per-row and traceable to its raw source record; do not interpret policy-wide interpreter selection as proof that every current-table row came from historical ingestion.

- Preserve raw IDs/clocks/provenance and source row occurrence accounting, including rejected, duplicate and conflicting rows. Do not prefilter conflicting duplicates before the pure selector sees them.
- Require policy readiness for the canonical feature-family set and context.policy_content_id == policy.content_id; no unresolved or mismatched policy may yield eligible rows. Unsupported source classes remain unknown, never assumed historical.
- Use `evaluate_market_observation` and existing `QuoteInput`/`select_quote_pairs(tuple(...), ranker=None)`; don't duplicate pairing or generic observation math. Preserve every eligible complete pair for existing downstream line shopping. No ranker/no best-book/math changes.
- Strict historical cutoff contract from the Slice8 acceptance plan: quotes at or after decision or commence are rejected; do not modify inclusive generic primitive. Enforce caller-policy freshness/max age and market/book clock requirements without making up timestamps. Supplied historical-request upper bound and source namespace/class must be authenticated by the source input boundary, not inferred from snapshot_time. Missing provenance, context, period/variant, snapshot key, clock or identity yields a measured blocked/rejected occurrence, not a placeholder accepted quote.
- No insertion-as-availability and no naive timezone localization. Preserve raw missingness and reason codes. Unexpected programming/contract errors propagate instead of masquerading as no quotes; malformed source records should have explicit per-occurrence rejection.
- For NBA padded/unpadded IDs, provide one explicit narrow mapping seam shared by future callers. Match an 8–10 ASCII-decimal source token to an explicitly supplied canonical 10-digit game context only when left-zero padding produces that exact context ID; preserve original raw token in evidence. Reject ambiguous mapping/context collisions, don't normalize arbitrary opaque IDs or silently repair whitespace. Record mapping provenance. If actual IDs require another rule, report rather than invent it.
- Pair result frame must retain the existing downstream columns player_id, game_id, market_key, line, bookmaker, over_odds, under_odds, while preserving merge-compatible input player-ID types. Add exact pair/snapshot/schema identity, policy/context identity and side-specific clocks/source refs as metadata. Do not use float conversion that changes line/ID identity or drops odds validation.
- Ledger frame preserves every raw occurrence and observation/quote reasons via stable enum values/existing observation_payload. Include eligible/rejected/duplicate counts and no-eligible/dropout vs blocked distinction. No all-empty 'pass' after missing required evidence.
- Deterministic direct/prefetch parity: same input occurrences/context/policy produce same paired rows and ledger; avoid order-dependent accidental identities. Global occurrence index remains evidence, not a source ID.

## Existing real-source facts

Current props table has staging_id/player/game/market/book/side/price/line, commence_time and snapshot/update/insert clocks, but lacks explicit source route/period/variant/snapshot compatibility authority. Historical scrape_progress.json tracks attempted event/request timestamps, including empty/422 responses, not row-level response receipts. Thus raw current-table rows are NOT automatically authenticated. Controlled rows with explicitly supplied evidence may prove adapter mechanics only; do not fabricate or persist real source evidence/config.

## Validation and output

Use existing observation/quote suites if present (discover exact test names under tests). Add no permanent tests. One small disposable probe at an explicit absolute Hermes scratch path may exercise a coherent pair, missing counterpart, equality/late clocks, conflict/duplicate, unknown source, exact ID mapping and parity through the real adapter. No exhaustive scratch framework. Scoped Ruff/compile. Existing project venv is C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe. Disable dotenv explicitly and guard network before project imports. Write wiki with exact public API, stable row contract and distinction between controlled verification and unaccepted real sources. Max35 tool turns/15 minutes. Return exact API, modified files and commands/results. Stop without touching harness or 8B files.
