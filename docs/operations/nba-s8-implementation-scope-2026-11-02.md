# NBA Slice 8 implementation — sequential scope

## Kick off immediately

Chase authorized remaining Slice 8 implementation, no new unit tests, lean verification and concurrent educational wikis. This scope covers the first functional section only; do not launch a broad writer.

This chat: implement and review the pure historical policy and observation adapters in the isolated `repair/nba-historical-integration` worktree based on `bf7a3e9`. Other chat: owns completed, uncommitted Slice 7 and active automation/reconciliation. You: retain control over unresolved historical policy values and long baseline execution.

## Dependency state

Slice 7 is SPEC PASS / QUALITY APPROVED but uncommitted in `nba-lifecycle`; main does not contain it. Historical S1 is still dirty in its separate worktree. Do not copy, merge or modify either dependency in this section. These pure components do not need their source. Later loader/replay integration requires explicit S1 overlap reconciliation, and lifecycle integration requires the final Slice 7 dependency base or an explicitly approved fingerprinted read-only snapshot.

## Section 1 allowed writes

- Create `src/models/features/nba/historical_policy.py`.
- Create `src/models/features/nba/historical_observation_adapters.py`.

All other source/config/test files are read-only. No policy YAML, request/loader/harness changes, commits, SQL, network, deserialization, source repairs, tests/fixtures or new test framework.

## Functional requirements

Read actual observation_policy.py, quote_selection.py and requests.py before implementing. Use stdlib immutable records and existing primitive APIs. No guessed imports or dependencies.

Policy module:

- Strict versioned, serializable, immutable caller-supplied historical policy with canonical content identity. Reject bool-as-number, nonfinite settings, duplicate keys/IDs, malformed dates/clocks, mutable nested authority and silently repaired identifiers.
- Explicit decision rule supports game-relative T-minus or America/New_York slate clock with explicitly configured early-game behavior. No implicit numeric decision-time defaults; reject ambiguous or nonexistent wall times rather than guessing a DST fold.
- Per-game context preserves exact game ID/date, authoritative timezone-aware commence and provenance reference, computes decision from the supplied rule, requires decision strictly before commence, and binds policy identity.
- Represent unresolved required cohort/split, injury, source and per-family carryover values as structured blocking reasons. A policy can be inspected/hashed while blocked but cannot pass an execution-readiness check. No string-only claim of approval makes data verified.
- Distinguish invalid policy contracts (ValueError subtype) from valid-but-unresolved policy (typed blocking exception with reasons).
- JSON payload loading/roundtrip must preserve absent versus explicit blocked state; never create executable choices for Chase.

Observation adapters:

- Implement actual pure mapping of explicit caller source-class/record ID/provenance/clock evidence to existing ClockEvidence, AvailabilityInterpretation and ObservationDecision through evaluate_observation.
- Support authenticated historical-request upper-bound interpretation and local observed-at interpretation as separately named source classes; live props invocation-start is unsupported, never treated as historical request.
- Historical requested time and market-update evidence must both pass decision/commence. Required missing clocks stay missing/blocked; no insertion fallback. Book update requirement must be explicit, not defaulted from null data.
- Injury observed-at establishes only local availability; textual report cycle is retained provenance, never parsed by timezone guess. Unknown completeness/omission cannot become healthy.
- Preserve raw clocks and opaque source IDs. Contradictory/malformed contracts raise; unavailable/unknown source produces structured primitive rejection. Do not equate bookmaker with ingestion provenance or synthesize IDs.
- Use primitive equality boundaries: availability/cutoff <= decision and strictly < commence. No unsupported stricter semantics.

## Verification budget

Use project venv absolute interpreter `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`. Scoped Ruff and compile; one short real API smoke with explicit controlled inputs checks policy roundtrip/hash, DST-aware decision resolution, blocked policy rejection, historical before/after-decision observation, unknown source and injury local-observation provenance. At most one disposable probe file in a unique `nba-slice8-section1` Hermes scratch directory. No exhaustive mutation suite. Run existing affected feature/primitive suite only if such files actually exist, otherwise report exact missing suite; do not invent tests.

## Stop and report

Return exact source files, public APIs, actual verification commands/results and unresolved seams. Do not call section 1 complete Slice 8B: feature loaders, replay, lifecycle and real-data acceptance remain separate sections. No policy selections, data readiness or promotion claims.
