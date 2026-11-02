# NBA saved-evidence audits — coding slice 6

## Purpose

Slice 5 saves the results of comparing settings against shared prediction/sample inputs. Slice 6 checks those saved results before another tool treats them as usable evidence.

The auditor is artifact-only: no database calls, model loading, prediction regeneration, training or replay. It cannot turn diagnostic history into promotion-grade evidence.

## The four different populations

- Acquisition predictions: counts recorded while generating the original shared inputs.
- Candidate rows: pre-selection rows in `all_bookmaker_edges.csv`. Several books/lines can describe the same player/game/stat opportunity.
- Selected predictions: rows in `predictions.csv`, after the harness chose its side/stat/quote. This is not the full acquisition population.
- Placed bets: rows in `bets.csv`. Some selected predictions never become wagers.

Those populations need different denominators. A missing bet is not automatically a missing quote or a model failure. Reasons not persisted by the producer remain unmeasurable.

## Approved scope

Implementation/review status is maintained in the [slice-6 evidence record](../operations/nba-evidence-slice6-2026-11-01.md). The approved implementation plan is `.hermes/plans/2026-11-01_213948-nba-slice6-evidence-audits.md` in the slice-6 worktree.

- Verify supported bundle layout, hashes, ordered CSV schemas, actual row counts, cell/date accounting and saved configuration commitments.
- Check occurrence-consuming candidate → selected → bet correspondence without choosing a new side, book or tie-break.
- Describe concentration by side, stat, book, game and player, with explicit count/stake denominators.
- Describe saved date outcomes, sample/quote status and recorded exclusions.
- Check quote ordering only when explicit, timezone-aware observations support it.

No new probability, BL, simulator, calibration, settlement or ranking math is added. Existing tests/fixtures and producer source are protected.

## Integrity is not historical truth

A byte hash proves consistency with its recorded commitment, not honesty or correct execution. The original raw features, sample arrays and RNG state are not fully persisted in this format. The auditor cannot reconstruct the full shared-input identity or prove that recorded settings actually reached execution.

Likewise, timestamps existing in a CSV do not prove a historical query used the correct as-of policy. Missing/naive clocks stay blocked; no synthetic bet time, ET localization, source-age policy or T-minus rule is invented.

Closing snapshots, +15/+30/+60 movement and source-verified quote-clean dropout are not supported by this first saved-output version. They require separately approved observations/adapters. Nice ROI does not override missing or contradictory evidence.

## Status vocabulary

- PASS: the requested, supported saved-evidence check was evaluated and conformed.
- FAIL: supported evidence contradicts the declared contract, an applicable timing order is violated, or execution/report I/O fails.
- BLOCKED: a required observation, supported completed input or safe resource/path condition is unavailable.
- UNSUPPORTED: this version cannot perform the capability.
- NOT_REQUESTED: an available check was not requested.

Report completion only means the report was written/read back successfully. It is not an audit pass, data acceptance or promotion decision. `decision_grade` stays false and historical readiness stays unverified.

## Interface

The approved command is `python -m src.backtesting.nba.evidence --sweep-dir PATH`, with an optional explicit `--checks` list and `--output-dir`.

Without an output directory, results go to stdout only. Explicit output uses a fresh exclusive destination outside the source sweep/model evidence; it never overwrites a prior report. See the delivery record for actually exercised commands and final API details.

Output checks use ordinary path/identity observations before and after writes, plus input fingerprint checks before finalization. An observed `output_path_changed` or `source_changed_during_audit` produces a nonzero result and prevents successful publication. Failed output remains incomplete; a redirected unsafe path is not used for corrective writes.

These are detection checks, not locks. Chase approved the simpler local-tool contract instead of Windows-specific race prevention. A concurrent directory replacement can cause writes before detection; a transient change between observations may be missed. Do not concurrently move/replace the input or output directories. This is not an adversarial filesystem security boundary.

Exit codes: 0 when all requested checks pass; 1 for a failure; 2 for blocked/unsupported requested work without a higher-priority failure. A default audit may correctly return 2 when saved timing observations are missing. Integrity-only success still does not imply decision-grade evidence.

## Read next

- [Eight coding slices](nba-training-evaluation-slices.md)
- [Shared-input sweep explanation](nba-shared-input-sweep.md)
- [Slice-6 decisions and verification](../operations/nba-evidence-slice6-2026-11-01.md)
