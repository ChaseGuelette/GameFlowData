---
title: Handoff 109 — Batter hits artifact correction and Pitcher K ablation plan
type: handoff
domain: handoffs
status: completed
owner: Chase
effective_date: 2026-06-21
tags: [handoff, mlb, batter_hits, pitcher_strikeouts, clv, ablation]
---

# Handoff 109 — Batter hits artifact correction and Pitcher K ablation plan

> Part of [[Handoffs]]

**Date**: 2026-06-21 22:51 EDT

## Summary

This session completed a local dense-CLV audit run for MLB `batter_hits`, but then caught a critical interpretation issue: the sweep used the default production artifact directory, not the newly trained `platoon + contact_quality` / forced-feature-family artifact from [[handoff-093]]. Therefore the audit result is valid for the current production artifact only, not for the four-feature/ranker-winner model. The session also produced a Pitcher K two-track ablation plan in `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md` for the next `pitcher_strikeouts` improvement loop after [[handoff-108]].

## What Was Done

- Ran the preferred-book-first local dense CLV audit over `2026-05-18..2026-06-21` for `batter_hits` using:
  - `--line-source mlb_player_props_clv_snapshots`
  - `--quote-decision-policy slate_or_tminus`
  - `--quote-relative-minutes 60`
  - `--book-routing-policy preferred_book_first`
  - `--model-dir src\models\mlb\artifacts\production`
- Audit artifacts were written under:
  - `backtest_results/mlb_batter_hits_dense_quote_clean_slate_tminus60_preferred_book_20260518_20260621/`
  - `backtest_results/mlb_batter_hits_dense_quote_clean_slate_tminus60_preferred_book_20260518_20260621_audit_suite/`
- The audit suite reported:
  - overall gate status: FAIL
  - dropout audit: PASS
  - dense table adequate: yes
  - preferred-book routing worked; ESPNBet/ProphetX concentration was not the blocker
  - config 03 (`no_BL`, edge `0.15`) had positive ROI and positive mean CLV but failed edge-ranking CI
- After inspecting `src/models/mlb/artifacts/production/batter_hits_negbin_meta.json`, the artifact did not include the expected [[handoff-093]] feature-family names such as platoon/contact_quality. This means the run was not the intended independent-window validation of the newly trained four-feature/ranker-winner model.
- Created local helper script `.hermes/tmp/local_dense_clv_audit_probe.py` because PowerShell `python -c` inline SQL probes are too brittle.
- Created `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md` with:
  - Track A load-bearing force-exclude family tests for current Pitcher K baseline
  - Track B high-value feature-family force-include tests, starting with `phase3b_downside`
  - focused BL sweep pattern for each newly trained artifact directory
  - audit/ranker commands
  - train/serve feature coverage gate
  - paired-bet diagnostic requirement
  - promotion gates and non-goals

## Decisions Made

- The `batter_hits` audit from this session must be labeled as a production-artifact audit, not a four-feature/ranker-winner validation.
- Future feature-family evals must verify artifact identity before interpretation:
  - train the feature-family model;
  - sweep with that exact newly trained artifact directory;
  - run audit/ranker on that sweep;
  - inspect model metadata for expected forced-included/new-family features before saying the feature family passed or failed.
- The [[handoff-093]] `no_prop_line + force_include platoon + contact_quality` winner remains untested on this independent dense-CLV window until rerun against the correct artifact directory.
- Pitcher K remains flat-paper only after [[handoff-108]]; no live/Kelly/Kalshi promotion from the dense-CLV audit alone.
- The next Pitcher K improvement loop should start with controlled family ablations, not new architecture, broad scraping, or live deployment.

## Blockers and Open Questions

- Need to locate or regenerate the actual `batter_hits` four-feature/ranker-winner artifact from [[handoff-093]] before rerunning independent-window validation.
- Need to decide whether `.hermes/tmp/local_dense_clv_audit_probe.py` should become a durable script or remain scratch.
- GameFlowData working tree still has unrelated/scratch changes:
  - modified `src/models/feature_store.py` from the NBA feature contract lane;
  - untracked CLV mapping helpers under `.hermes/tmp/`;
  - untracked `reports/mlb_dense_clv_local_residual_game_link_20260518_20260621.md`;
  - untracked `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md`.
- The new Pitcher K plan is a runbook/planning artifact; it has not been executed.

## Recommended Next Steps

1. For `batter_hits`, rerun the independent-window dense CLV validation against the correct newly trained [[handoff-093]] artifact, not `src\models\mlb\artifacts\production` unless that directory is proven to contain the intended feature family.
2. Before any feature-family interpretation, inspect the artifact metadata for expected features and record the artifact directory in the result summary.
3. For `pitcher_strikeouts`, read `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md` and start with Track A dry-runs to map load-bearing baseline families.
4. Do not run live/Kelly/Kalshi from either lane until forward paper and edge-ranking gates pass.
5. Decide whether to promote `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md` and `.hermes/tmp/local_dense_clv_audit_probe.py` into committed project artifacts.

## Files to Read on Resume

- [[handoff-093]]
- [[handoff-108]]
- `docs/development_docs/mlb_pitcher_k_two_track_ablation_plan.md`
- `src/models/mlb/artifacts/production/batter_hits_negbin_meta.json`
- `.hermes/tmp/local_dense_clv_audit_probe.py`
- `backtest_results/mlb_batter_hits_dense_quote_clean_slate_tminus60_preferred_book_20260518_20260621_audit_suite/suite_summary.md`
- `backtest_results/mlb_batter_hits_dense_quote_clean_slate_tminus60_preferred_book_20260518_20260621_audit_suite/suite_manifest.json`
