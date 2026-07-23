# Flat-First BL Discovery and Separate Kelly Certification Plan

**Date:** 2026-07-22

## Goal

Use MLB model ablations to find the most profitable robust Black-Litterman policy under flat staking. Treat Kelly sizing as a separate, optional certification lane after a flat policy is approved.

## Lane A — Flat-staking discovery and approval

1. Attach the completed artifact; do not retrain.
2. Run a broad quote-clean BL sweep with flat $100 stakes, dense CLV snapshots, preferred-book-first routing, and lower post-BL edge thresholds.
3. Use the existing sweep engine and `sweep_summary.csv`; do not build a neighbor-stability analyzer or cached volume-scan system.
4. Require an adequate sample, positive ROI, acceptable drawdown, and valid quote/timing/dropout evidence.
5. Compare qualified cells by total profit, ROI, Sharpe, drawdown, and bet count against a no-BL control.
6. Certify the best defensible candidate as `FLAT_STAKING_APPROVED`. Ranker and edge-monotonicity results may be reported, but they must not block flat approval.
7. Freeze the approved artifact, BL parameters, edge threshold, directions, routing, timing, and flat stake for forward paper evaluation.

## Lane B — Optional Kelly certification

Only a flat-approved policy enters this lane.

1. Require positive edge-to-CLV or edge-to-quality ranking with confidence intervals and monotonic edge buckets.
2. Compare flat staking against conservative capped Kelly on the same policy and evidence window.
3. Measure profit, return on capital, maximum drawdown, stake concentration, and performance by edge bucket.
4. Validate capped Kelly in forward paper before any live sizing change.
5. Classify the result as `CAPPED_KELLY_APPROVED`, `FLAT_ONLY`, `KELLY_UNDERPOWERED`, or `KELLY_REJECTED`.
6. Kelly failure must never revoke an existing flat-staking approval.

## Immediate next step

Create and dry-run a flat-only broad BL discovery lifecycle config for the completed Batter Hits platoon plus contact-quality artifact. Reuse existing tooling, do not retrain, and do not start the long sweep until Chase reviews the config.

## Implementation note

The lifecycle decision contract currently conflates flat approval with Kelly-readiness by making positive ranker CI and edge-bucket monotonicity mandatory for finalist certification. Before flat finalist certification, split those outcomes so flat approval can succeed independently and Kelly remains a second-stage decision.
