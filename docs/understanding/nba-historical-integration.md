# NBA Historical Integration: Slice 8B–8E

## Kick off immediately

Pure policy/context and observation adapters are implemented in the isolated Slice 8 worktree, with controlled checks passing after repairs. Consumer integration is not implemented or accepted yet. The purpose of the remaining work is to carry an owner-approved historical policy through the existing NBA feature, replay, audit, and lifecycle path without changing the model.

- **This chat — code/docs:** implement the narrowly approved 8B–8D seams and keep this integration wiki aligned with the actual contracts.
- **Other chat — reconciliation/Slice 7 integration:** finish source reconciliation and accept/integrate the final Slice 7 lifecycle contract before lifecycle wiring is frozen.
- **You — policy/baseline gates:** approve unresolved policy values, authorize bounded real-data gates, and launch the exact frozen baseline when it is ready.

The governing scope is `.hermes/plans/2026-11-02-nba-slice8-historical-integration-validation.md`. The policy concepts and remaining owner choices are explained in `docs/understanding/nba-historical-decision-policy.md`.

## The integration chain

```text
explicit policy
  → per-game decision context
  → source interpretation
  → shared line/injury features
  → paired direct/prefetch replay
  → bounded verifier
  → lifecycle handoff
```

Each arrow is an identity and evidence boundary. A later stage may consume only an approved, explicit result from the prior stage; it must not fill an unknown with a convenient default.

## 1. Explicit policy

The historical path begins with one versioned policy identity covering decision time, cohort and splits, source interpreters, missing-evidence behavior, feature-family carryover, minutes rules, and exact windows.

A missing or incomplete policy must not become decision-grade through defaults. Unresolved values remain **blocked** until approved. Policy choices are based on source semantics and bounded coverage evidence, not retrospective ROI.

Legacy production behavior remains the default for existing callers. The historical path is opt-in and identity-bound. Slice 8 must not silently route production inference, scheduling, or Railway jobs through historical behavior.

## 2. Per-game decision context

Training, single-date, date-range, and replay consumers must resolve the same context for each game:

- exact policy identity;
- decision cutoff and timezone rule;
- authoritative commence boundary;
- game/player identity;
- accepted source interpreters;
- cohort, split, and season/carryover treatment;
- readiness and completeness state.

This removes date-level cutoff drift. Commence is never inferred from game date, and separate consumers may not invent separate decision times.

## 3. Source interpretation

Source adapters translate raw quote or injury evidence into the existing observation decisions. They preserve raw clocks, provenance, record IDs, cutoff evidence, and rejection reasons.

Requested historical time, provider snapshot time, market update time, invocation start, parse time, insertion time, decision time, and commence are not interchangeable. An interpretation approved for a historical source must not leak onto a live source with different clock semantics.

Unknown provenance, missing history, ambiguous history, or unprovable timing produces an unresolved/blocked policy result. It does not become an empty row, a zero feature, or the latest known record.

## 4. Shared line and injury features

The approved context must produce matching ordered features across training, date, and date-range paths. Parity includes values, defaults or missingness, spread direction, line and injury provenance, completeness, and carryover behavior.

Historical integration preserves existing feature names, order, artifact compatibility, four same-position injury fields, and strictly prior rolling joins. It does not redesign rolling-stat producers or alter the distinction between target eligibility and prior-game contribution thresholds.

The model remains unchanged: the minutes × rate architecture, quantile behavior, empirical-CDF probabilities, Q10 edge, Monte Carlo, calibration posture, and artifact feature schema are protected.

## 5. Paired direct/prefetch replay

Direct and prefetched replay must call one temporal eligibility and quote-pair contract. Over and Under are paired only when player, game, market, book, line, source, and snapshot identity are compatible.

The integration preserves opaque source IDs and records why candidates were rejected. Independently choosing the newest Over and newest Under is prohibited because those sides may never have existed together.

For the same bounded input, direct and prefetch paths must agree on eligible rows, selected pairs, and ledger evidence. Existing edge math, chosen-side behavior, one-bet-per-player mechanics, simulation, and staking remain unchanged.

## 6. Result states are not interchangeable

- **Policy unresolved/blocked:** required timing, provenance, commence, injury, feature, or quote evidence cannot support decision-grade interpretation.
- **Execution error:** a query, chunk, loader, inference, or orchestration operation failed. No partial success-shaped aggregate may be published.
- **Legitimate no-op:** a successfully acquired population truly has no games or targets. No-feature/no-prediction results require an explicit expected cause; query or model failures cannot masquerade as empty populations. No eligible quotes is measured quote dropout, not evidence of no model edge.

Missing quotes are measured source dropout, not proof of “no model edge.” Diagnostic-only behavior must be explicitly named and cannot count as historical acceptance or promotion evidence.

## 7. Bounded verifier

The verifier is a read-only orchestrator, not a repair tool or second implementation of feature loading, prediction, quote selection, simulation, or auditing. It calls the real owners with exact policy, artifact, dates, local target, caps, and a new output directory.

Validation increases in controlled bounds:

1. static and DB-free contract checks;
2. bounded source-readiness receipts;
3. one-day real feature/model/quote smoke;
4. three-to-seven-day direct/prefetch replay parity;
5. complete bounded sweep, audit, report-only lifecycle, and invalidation checks after Slice 7 integration.

Controlled evidence and real-data evidence remain separate. Passing controlled cases proves mechanics; it does not prove historical data readiness, model quality, or profitability.

## 8. Lifecycle handoff

Only after the bounded gates pass should the project freeze an exact lifecycle configuration. It names the approved policy, exact manifested artifact or approved training output, windows, sources, bookmakers, seed, sample count, risk settings, local target, readiness receipt, and new output root. It contains no placeholders, `latest`, inferred dates, or artifact discovery.

Agents may validate and dry-run the frozen handoff. You launch training or protected-OOS replay when separately approved, then review the persisted evidence for Confirm/Shelf/Exclude and deployment posture.

Historical acceptance remains separate from customer publication, paper/live promotion, deployment, production database changes, and production-default changes.

## What completion means

Code integration alone is not Slice 8 acceptance. Completion requires policy approval, source readiness for the selected window, feature parity, paired replay parity, bounded real-data evidence, successful audit/lifecycle identity checks, and an owner-launched frozen baseline.

Current status: **pure contract section implemented and controlled checks pass; final independent acceptance, loader/replay/lifecycle wiring, owner policy choices and real-data acceptance remain open**. The partial source checkpoint `1f16d2b` is merged into main as `041c013`; Slice 7 is merged as `83c121b`. See `docs/operations/nba-s8-implementation-2026-11-02.md` for exact evidence and the next owner decisions. Git integration does not close the historical acceptance gates.
