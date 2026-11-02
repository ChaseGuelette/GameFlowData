# Documentation index

## Kick off immediately — November 1, 2026

NBA slices 1–2 are scoped-implementation verified: slice 2 passed independent spec/quality review, 72 controlled checks and all 117 existing scoped tests after owner-approved fixture adaptations. No new cases or changed assertions. This lane is not committed/integrated; real-data/platform limitations remain explicit. Slice 4's pure primitives were separately approved and merged into main as `f81170f`, without consumer wiring or historical policy activation. Start with the [eight-slice wiki](understanding/nba-training-evaluation-slices.md), [artifact wiki](understanding/nba-artifact-identity.md) and [slice 2 evidence](operations/nba-artifact-slice2-2026-11-01.md). S1 remains isolated. The October monitoring instructions below are historical, not a request to restart the failed watcher or sync.

## Kick off immediately — NBA, October 16, 2026

I’m monitoring the running NBA refresh while Chase is away, then checking the complete S2 evidence checklist without changing source data or model code.

Unattended checkpoint: [watch status](operations/nba-s2-watch-status-2026-10-16.md) and [audit progress](operations/nba-s2-audit-progress-2026-10-16.md). Checks run every three minutes after monitor activation; one coordinator starts only after all NBA tables finish successfully and sync processes exit. The final return summary will be `docs/operations/nba-s2-return-summary-2026-10-16.md`. S3, training, repairs and integration remain unapproved.

- You: let the already-running [NBA-only refresh](operations/nba-sync-inventory-2026-10-16.md) finish; no second/all-sports sync. This chat reviews its completion summary before real-data validation.
- Other chat: S1 is delivered; preserve its isolated worktree pending [coordinator acceptance](operations/nba-s1-coordinator-review-2026-10-16.md), with the owner-approved `_run_date` extension. No integration yet.
- This chat: own read-only S2 source/timestamp research and review of the S1 result; no overlapping source edits.
- You: read the handoff first, then review the wiki/research while independent work progresses. Implementation/policy/long-job approvals remain explicit.

### Read in this order

For the immediate database question, start with the [NBA-only sync inventory](operations/nba-sync-inventory-2026-10-16.md); it tracks pending canonical remote evidence and excludes stale-local conclusions.

For the delivered S1 code, read the [coordinator review](operations/nba-s1-coordinator-review-2026-10-16.md): independent existing-suite/lint results, approved scope extension, fixture-versus-real-data evidence and the sync-dependent acceptance gate.

For S2, read the [source-clock review](operations/nba-s2-source-clock-review-2026-10-16.md): mixed timestamp meanings, the conditional historical-odds bound, injury-provider evidence limits and still-unapproved policy choices. No post-sync data certification yet.

1. [Repair specification](operations/nba-historical-repair-spec-2026-10-16.md) — top kickoff/handoff and S1 acceptance checks now; S2-S5 later as their gates are resolved.
2. [NBA rebuild wiki](understanding/nba-rebuild.md) — the data-to-training mental model, terms and gate explanations.
3. [Historical-readiness report](operations/nba-historical-readiness-2026-10-16.md) — completed agent outputs, corrected DNP interpretation, limitations and open decisions.
4. [Restart plan](operations/nba-core-product-restart-plan.md) — broader product roadmap/background; the current top checkpoint overrides old historical next-action details.

No need to read old audit transcripts or scratch JSON to launch S1. Use them only to inspect a specific claim. This index does not grant blanket implementation authorization.

Status: active documentation map; NBA restart/readiness links reconciled 2026-10-16. The broader repository-scope reconciliation remains dated 2026-08-24.

This repository documents the active NBA/MLB sportsbook product plus the separately deployed read-only Engineering OS. Canonical decisions, lessons, roadmap state, and handoffs live in remote GBrain (`source=gameflow`). Repository documentation explains the checked-in implementation and must not revive retired product lanes.

## Status classes

- **Active:** source-backed setup, operation, or contract documentation.
- **Canonical mirror:** checked-in mirror of an active remote GBrain page.
- **Historical evidence:** preserved result/plan context; missing archived paths are intentional and its commands are not current operating instructions.
- **Agent contract:** always-loaded safety/workflow policy, not a product runbook.

## Root and subsystem guides

| File | Status | Purpose |
|---|---|---|
| [`../README.md`](../README.md) | Active | Product boundary, setup, validation, and retained surfaces |
| [`../AGENTS.md`](../AGENTS.md) | Agent contract | GameFlow safety and context-routing rules |
| [`../CLAUDE.md`](../CLAUDE.md) | Agent contract | Slim mirror for Claude Code users |
| [`../dashboard/README.md`](../dashboard/README.md) | Active | Next.js dashboard setup, env, routes, and validation |
| [`../migrations/README.md`](../migrations/README.md) | Active | Retained schema contracts and archive boundary |
| [`../ops/engineering_os/README.md`](../ops/engineering_os/README.md) | Active | Engineering OS MVP 0 setup, validation, services, and rollback |
| [`../configs/mlb/examples/USAGE_GUIDE.md`](../configs/mlb/examples/USAGE_GUIDE.md) | Active | MLB lifecycle YAML operator workflow |
| [`../configs/mlb/examples/FEATURE_FAMILIES.md`](../configs/mlb/examples/FEATURE_FAMILIES.md) | Active | Test-covered lifecycle family inventory |

## Active operations

- [NBA historical repair specification](operations/nba-historical-repair-spec-2026-10-16.md) — immediate two-file S1 handoff; S2-S5 source/policy/validation gates.
- [NBA historical-readiness report](operations/nba-historical-readiness-2026-10-16.md) — reviewed three-agent package and targeted read-only follow-up, not full-period certification.
- [NBA rebuild wiki](understanding/nba-rebuild.md) — educational guide and explicit ownership for parallel research/implementation/review.
- [NBA 2026–27 core-product restart plan](operations/nba-core-product-restart-plan.md) — current session-resume anchor, scope, sequencing, and approval boundaries.
- [NBA data-readiness audit](operations/nba-data-readiness-audit-2026-09-24.md) — combined source, feature, odds/linkage, and temporal-integrity assessment.
- [NBA audit lane evidence](operations/audits/) — detailed read-only reports with exact queries, paths, tests, and caveats.
- [Daily pipeline automation](daily_pipeline_automation.md) — APScheduler timing and job boundaries.
- [Railway deployment](railway_deployment.md) — one always-on Python scheduler worker.
- [Discord integration](discord_bot_development.md) — retained NBA/MLB alerts and optional commands.
- [Database client](db_client_documentation.md) — remote/local engines and pooling behavior.
- [Historical NBA prop scraper](player_prop_scraper_documentation.md) — manual Odds API backfill.
- [NBA rolling averages](populate_average_stats_documentation.md) — full/incremental point-in-time summaries.
- [MLB FanGraphs scraper](mlb_fangraphs_scraper_documentation.md) — local-only point-in-time advanced snapshots.
- [MLB processing pipeline](mlb_processing_pipeline_documentation.md) — linking, averages, matchup context, and active production profiles.

## Active model references

- [NBA feature store](feature_store_documentation.md)
- [NBA feature-contract catalog](nba_feature_catalog.md)
- [MLB feature-contract catalog](mlb_feature_catalog.md)
- [NBA model pipeline runbook](model_pipeline_runbook.md)
- [MLB lifecycle operator guide](development_docs/mlb_model_lifecycle_usage_guide.md)

Exact contract membership lives in source constants and artifact manifests. Lifecycle docs govern workflow but never authorize long training/sweeps; Chase launches those jobs.

## Engineering OS canonical mirrors

- [Roadmap and usage](operations/gameflow-engineering-os-roadmap-and-usage.md)
- [Technical implementation](operations/gameflow-engineering-os-technical-implementation.md)

MVP 0 remains deployed read-only, but Engineering OS work is paused while NBA core-product readiness is the active priority. Do not resume certification or later MVPs without a new owner decision.

## Historical evidence

- [Engineering OS MVP 0 baseline](operations/evidence/engineering-os-mvp0-baseline.md)
- [Engineering OS MVP 0 certification log](operations/evidence/engineering-os-mvp0-certification.md)
- [Frozen Batter Hits baselines](development_docs/mlb_batter_hits_frozen_baselines.md)
- [Frozen Pitcher K baselines](development_docs/mlb_pitcher_k_frozen_baselines.md)
- [Repository reduction plan](../.hermes/plans/2026-08-24_142813-gameflow-repository-archival-and-reduction.md)

These pages preserve evidence. Paths removed by the reduction are not broken clean-checkout dependencies.

## Retired scope

The active repository does not contain or operate:

- Kalshi or Polymarket trading;
- cross-platform arbitrage;
- NCAAB;
- Bot Tracker, Arb Scanner, or Data Vault dashboard surfaces;
- unsupported MLB stat lanes removed during repository reduction.

Historical GBrain handoffs may discuss those systems. They are archive context, not build instructions.

## Verification contract

For documentation-only changes:

1. every tracked Markdown link must resolve or be an explicit external/GBrain reference;
2. referenced repository paths in active docs must exist;
3. command help/dry-runs should be exercised where safe;
4. `git diff --check` must pass;
5. source-backed counts and route lists must be generated from current code/build output rather than old handoffs.

For a full clean-checkout verification, follow [`../README.md`](../README.md).
