# Broader Project Pruning Candidates — Non-Kalshi

**Audit date:** 2026-07-18

**Mode:** Read-only evidence review; this report is the only file written

**Scope:** Non-Kalshi repository surfaces that may be removed, archived, consolidated, explicitly retained, or held for more evidence. Evidence was drawn from the audit README, completed reports 00/01/02/04/05/06/07/08/09, the current canonical register, migration indexes, tracked-file inventory, and targeted import/call/config/reference checks.

**Excluded:** DB/network/production inspection; secrets; package installation; tests or jobs that execute product behavior; source/config/plan/register/card edits; broad scans of generated artifact trees; and Kalshi-specific decommissioning.

## Kalshi ownership boundary

`12-kalshi-deprecation-and-project-pruning.md` was listed as the dedicated owner in the audit README but was not present in the working tree during this parallel review. It completed afterward and is now the authority for Kalshi-specific disposition. This report intentionally did not classify or duplicate removal of:

- `src/trading/kalshi/`;
- `src/scrapers/kalshi/`;
- Kalshi lifecycle orchestration jobs;
- `/api/kalshi/*`, bot-tracker Kalshi controls, or Kalshi-specific dashboard types/components;
- Kalshi-specific tables, migrations, tests, plans, flags, or artifacts;
- arbitrage surfaces whose removal decision depends on Kalshi/Polymarket product disposition.

Report 06 remains cross-reference evidence for current safety/ownership, not authority for investing in those surfaces. Apply report 12's dependency map before any shared-file cut below.

## Executive disposition

The best pruning opportunities are not speculative dead-code deletions. They are four evidence-backed cuts:

1. **Remove the executable global calibration-offset path** rather than refactor it into another supported production policy. It contradicts the strongest model invariant and has five active consumers.
2. **Move non-production model backups/runs/experiments out of the deployable Git tree** after artifact identity and rollback are made explicit. The targeted tracked inventory found 168,994,105 bytes in named non-production NBA/MLB artifact groups, versus 220,023,222 bytes for the entire tracked checkout.
3. **Archive duplicate local knowledge authorities and stale trackers** (`brain/`, root `Handoffs/`, `ACTIONITEMS.md`, and `CLAUDE.md.backup`) rather than repair them as parallel current sources.
4. **Remove generated/probe residue and consolidate duplicated plans/deployment contracts** instead of adding more documentation around them.

Two tempting deletions should not proceed yet:

- the Lane 02 MLB feature-store compatibility facades/legacy implementations are a deliberate, tested boundary and should be retained;
- the NCAAB lane is detached from the production scheduler but still has coherent source, migrations, docs, and focused tests, so product intent is required before archive/removal.

## Method and tracked inventory

### Authorities read

- `AGENTS.md`.
- `.hermes/audits/tech-debt/README.md` in full.
- Completed reports `00`, `01`, `02`, `04`, `05`, `06`, `07`, `08`, and `09` in full.
- `docs/understanding/tech-debt-register.md` in full.
- `.hermes/plans/god-class-migrations/README.md`.
- `.hermes/plans/god-class-tech-debt-refactor-index-2026-05-18.md`.
- `.hermes/plans/mlb-stat-suite-rebuild/README.md`.
- `.hermes/plans/trading-readiness-fixes-2026-05-26/README.md`.

Reports `03`, `10`, `11`, and `12` were listed in the README but were not present, so they were not treated as completed evidence.

### Mechanical tracked-file baseline

`git ls-files` returned:

- 1,432 tracked files;
- 220,023,222 tracked bytes;
- 379 files / 209,449,975 bytes under `src/`;
- 119 files / 577,272 bytes under `brain/`;
- 21 files / 109,581 bytes under root `Handoffs/`;
- 71 files / 1,203,180 bytes under `.hermes/`;
- 17 files / 973,536 bytes under `notebooks/`.

Targeted artifact accounting found:

- NBA current `production/`: 5 files / 13,520,158 bytes — retain pending manifest hardening;
- NBA `production_playoffs/`: 7 files / 7,870,605 bytes — active runtime references exist;
- NBA old/bad/archived backups: 28 files / 70,841,349 bytes;
- NBA tracked `nba_run_*`: 22 files / 39,588,968 bytes;
- NBA `hybrid_pts_test/`: 6 files / 20,887,809 bytes;
- old NBA run zip: 3,746,096 bytes;
- MLB production: 20 files / 15,842,116 bytes;
- MLB non-production tracked experiments: 24 files / 33,929,883 bytes.

This accounting followed the tracked-file-first rule and did not recursively scan ignored/untracked heavy artifact directories.

## Classification summary

| ID | Classification | Candidate | Priority | Confidence |
|---|---|---|---|---|
| BP-01 | Remove | Global combined-calibration offset writer/loader/application path | Critical | High |
| BP-02 | Archive | Tracked non-production model backups, run copies, and experiment binaries | High | High for repository/deploy cost; Medium for archive destination |
| BP-03 | Archive | Inactive MLB stat artifacts currently mixed into `production/` | High | Medium; required-stat intent needs confirmation |
| BP-04 | Archive | Checked-in `brain/` and root `Handoffs/` as active knowledge surfaces | High | High |
| BP-05 | Archive | Legacy trackers/backups: `ACTIONITEMS.md`, `CLAUDE.md.backup`, `.thoughts.md` | Medium | High for authority drift; Medium for final archive location |
| BP-06 | Consolidate | Overlapping god-class indexes and pre-lane refactor plans | Medium | High |
| BP-07 | Consolidate | Railway schedule docs and dual Vercel configuration modes | High | High for duplication; live Vercel root needs evidence |
| BP-08 | Remove | Tracked generated/test/probe residue | Medium | High, except screenshot provenance |
| BP-09 | Archive | `.hermes/tmp/` operational copies, probes, handoffs, and production log export | High | High |
| BP-10 | Consolidate | Legacy MLB single-config backtest entrypoint/harness | Medium | High |
| BP-11 | Needs evidence | Detached NCAAB product/model lane | Medium | High for detachment; Low for product disposition |
| BP-12 | Needs evidence | Root `claude-flow` npm dependency/lock and residual Claude-flow helpers | Medium | Medium |
| BP-13 | Needs evidence | Root manual pipeline trigger `run_now.py` | Low | Medium |
| BP-14 | Retain | Lane 02 MLB feature-store facades and legacy implementations | Explicit retain | High |
| BP-15 | Retain | Current production artifacts, applied migration history, completed migration evidence, and anti-regrowth tests | Explicit retain | High |

---

## Detailed candidates

### BP-01 — Remove — executable global combined-calibration offset policy

**Why this is a pruning candidate**

The project invariant is not “improve the offset implementation”; it is “never deploy global conformal recalibration offsets.” Keeping a writer, calibrate-only CLI, loader, sample-warping implementation, and implicit consumers preserves an attractive but forbidden switch. Cutting the executable path is safer and smaller than extracting or repairing it.

**Tracked evidence**

- Normal training writes `combined_calibration_offsets.json`: `src/models/train_pipeline.py:705-864`, especially `:840-843`.
- `--calibrate-only` writes the same file into a selected model directory: `src/models/train_pipeline.py:1049-1052,1163-1166`.
- Production inference loads it by file presence: `src/orchestration/inference_job.py:161-174`.
- Edge refresh loads and caches it: `src/orchestration/edge_refresh_job.py:229-283`.
- NBA backtest and sweep load it automatically: `src/backtesting/run_backtest.py:188-198`; `src/backtesting/run_sweep.py:1031-1038`.
- `src/tools/compare_models.py:41-75` treats it as a supported model mode.
- `src/models/monte_carlo.py` owns the loader/application path; report 01 PA-01 records application sites and report 05 MMP-01 confirms production consequences.
- A tracked backup remains at `src/models/artifacts/production_old_20260210/combined_calibration_offsets.json.bak`.

**Blast radius**

- Training CLI and artifact output.
- Monte Carlo constructor/API shape and sample-warping helpers.
- NBA inference, edge refresh, backtest, sweep, and model-comparison tool.
- Any tests/fixtures that assert offset loading or calibrated sample output.
- Historical artifact directories containing the file or backup.

**Dependency checks before cut**

1. Inventory every reference to `combined_calibration_offsets`, including tests and docs.
2. Add a temporary-directory anti-regrowth test proving production, refresh, backtest, and sweep reject or ignore the forbidden file.
3. Decide whether report-only calibration metrics remain useful; preserve reports, not executable offsets.
4. Verify no current production directory contains the file. Report 05 found none in the enumerated current production directory, but deployed state was not inspected.

**Sequence**

1. Characterize all current consumers without training.
2. Remove implicit loader calls from production/backtest/sweep/compare paths.
3. Remove the constructor parameter and sample-warping implementation once callers are gone.
4. Remove the normal writer and `--calibrate-only` artifact-writing mode; keep any approved report-only calibration diagnostics separate.
5. Remove offset artifacts from deployable/archive packages and add a forbidden-artifact validator.
6. Replace any tests whose only purpose is activating offsets with rejection/anti-regrowth tests.

**Rollback/archive need**

Git history and report metadata are sufficient to preserve the rejected experiment. If diagnostic reproducibility is required, archive a schema/example outside deployable model directories; do not preserve a loadable file beside production binaries.

**Why cut beats repair**

Refactoring would legitimize a policy repeatedly shown to hurt ROI and contradict canonical invariants. Removal reduces policy ambiguity and prevents artifact presence from changing runtime behavior.

---

### BP-02 — Archive — non-production model backups, run copies, and experiment binaries from the deployable Git tree

**Why this is a pruning candidate**

The repository is acting as an artifact store, rollback store, experiment archive, and deployment source simultaneously. Named non-production artifact groups account for 168,994,105 tracked bytes. They are included in every clone/build context even when runtime only needs current production and the playoff target.

**Tracked evidence**

NBA groups:

- `src/models/artifacts/production_old_20260210/` and `production_old_20260323/`: 33,275,133 bytes combined.
- `src/models/artifacts/production_bad_20260305/`: 16,543,736 bytes.
- `src/models/artifacts/production_archived_20260305/`: 21,022,480 bytes.
- tracked `src/models/artifacts/nba_run_*`: 39,588,968 bytes.
- `src/models/artifacts/hybrid_pts_test/`: 20,887,809 bytes.
- `src/models/artifacts/run_20260131_112534.zip`: 3,746,096 bytes.

MLB groups:

- non-production tracked artifacts under `src/models/mlb/artifacts/`: 33,929,883 bytes, including `ip_ablation_*`, `validation_2025_*`, named old MLB runs, and a baseline run.

Usage evidence:

- Current runtime explicitly references `production/` and, under `NBA_PLAYOFF_MODE`, `production_playoffs/` (`src/orchestration/scheduler.py:588-589`; `src/paper_trading/calibration_monitor.py:33-39`).
- The old/bad/hybrid directories have no source/config runtime references in the targeted search.
- Some historical tools/docs intentionally reference `production_archived_20260305/` and MLB experiment directories, so immediate deletion would break reproducibility links.
- `scripts/promote_model.py:41-67` can select `nba_run_*` directories, and report 05 shows current resolvers infer identity from directory shape. That makes moving runs unsafe until explicit manifests/arguments replace lexical discovery.

**Blast radius**

- Clean-clone and Railway build size/time.
- Historical comparison commands and reports with literal paths.
- `scripts/promote_model.py` latest-run behavior and backtest default resolvers.
- Reproducibility of MLB frozen-baseline and diagnostic reports.
- Code-plus-model rollback if Git is currently the only artifact store.

**Dependency checks before archive**

1. Implement/validate the manifest identity and completeness boundary from MMP-02/MMP-08.
2. Enumerate literal path references and classify each artifact as active rollback, frozen evidence, invalid/rejected, or disposable duplicate.
3. Verify an archive destination with checksums, immutable identity, access control, and retention; do not merely move binaries to another untracked workstation directory.
4. Confirm Railway/Vercel build inputs need only deployable production targets.
5. Ensure promotion never defaults to a lexically latest directory after runs leave Git.

**Sequence**

1. Keep `production/` and `production_playoffs/` in place.
2. Create authoritative manifests/checksums for retained deployable suites.
3. Copy historical artifacts to the approved archive and verify hashes/readback.
4. Update historical reports to immutable archive IDs or preserve a path-to-archive ledger.
5. Make promotion/backtest require an explicit artifact identity or validated manifest.
6. Remove archived binary copies from the active tree; tighten ignore rules for all run/backup naming families, not only `run_*`/`mlb_run_*`.
7. Verify a clean clone can load only supported production suites and that rollback retrieves a named archived suite.

**Rollback/archive need**

Required. Git history alone is a poor long-term binary artifact registry and may be expensive to rewrite. Do not rewrite history as part of this pruning slice. First remove from the current tree and preserve immutable external copies; history cleanup is a separate owner decision.

**Why cut beats repair**

No refactor makes hundreds of megabytes of duplicate binaries appropriate source files. The repair belongs in artifact identity/storage; once that exists, active-tree copies are redundant.

---

### BP-03 — Archive — inactive MLB stat artifacts mixed into `src/models/mlb/artifacts/production/`

**Why this is a pruning candidate**

The production directory contains 20 files and supports more stat families than current scheduler prose declares active. A permissive loader discovers by filenames and can return partial suites. Leaving inactive files in production expands load ambiguity and deploy size.

**Tracked evidence**

- Scheduler documentation says active MLB models are `pitcher_strikeouts`, `batter_hits`, and `batter_rbis`: `src/orchestration/scheduler.py:698-703`.
- Production also contains home-run, total-bases, runs-scored, and HRR artifacts (`hr_*`, `batter_total_bases_*`, `batter_runs_*`, `batter_hrr_*`).
- `src/models/mlb/mlb_model_suite.py:51-66` maps several of these families and `from_directory` loads by file shape; report 05 MMP-02 records permissive partial loading and absent authoritative manifests.
- `resolve_user_paper_bets.py:60-65` still supports `batter_hrr`, proving that “not in scheduler active-model prose” is not enough to call every extra file dead.
- Kalshi may consume some stat families; that dependency belongs to report 12 and was not adjudicated here.

**Blast radius**

- MLB inference requested-stat behavior.
- Paper/user bet resolution and historical records.
- Kalshi-specific consumers excluded from this report.
- Backtest/sweep defaults and artifact audit expectations.
- Rollback of previously supported stat lanes.

**Dependency checks before archive**

1. Define the required-stat manifest for production and check every non-Kalshi consumer.
2. Apply report 12’s Kalshi dependency map before moving any shared MLB stat artifact.
3. Confirm which stats are supported, parked, experiment-only, or intentionally rollback-ready.
4. Verify historical unresolved bets do not require model binaries; settlement generally needs actuals, not inference models, but this must be checked per consumer.

**Sequence**

1. Add a manifest declaring exactly required production stats.
2. Make inference fail closed for a requested missing stat and ignore undeclared extras.
3. Move undeclared stat artifacts to the same immutable archive used by BP-02.
4. Keep source support for a parked stat only if a product decision says retraining/reactivation is plausible; otherwise prune that source in a separately scoped lane.

**Rollback/archive need**

Required until Chase explicitly retires each model lane. Applied DB migrations and historical bet rows must remain regardless of binary disposition.

**Why cut beats repair**

Repairing the loader is necessary, but retaining undeclared binaries in the deploy target after that repair adds no runtime value. A narrow declared production suite is easier to validate and roll back.

---

### BP-04 — Archive — checked-in `brain/` and root `Handoffs/` as active knowledge surfaces

**Why this is a pruning candidate**

The repository has two local knowledge trees while `AGENTS.md` designates remote `/home/chase/GameFlowBrain`, source `gameflow`, as canonical. Repairing local copies as parallel authorities guarantees future drift.

**Tracked evidence**

- `AGENTS.md:5-13` names remote canonical GameFlowBrain and GBrain-first routing.
- Report 09 AKW-02 shows transition-era pointers still conflict with that authority; AKW-01 shows even the remote execution plan needs reconciliation.
- Tracked inventory: `brain/` contains 119 files / 577,272 bytes; root `Handoffs/` contains 21 files / 109,581 bytes.
- Targeted searches found no active reference from `AGENTS.md`, current `CLAUDE.md`, README, understanding docs, or current plan indexes that requires root `Handoffs/` as a runtime input.
- Reports 00 and 09 identify the checked-in trackers/brain as stale or contradictory, not the current truth.

**Blast radius**

- Human links/bookmarks and old plans that cite local paths.
- Any external agent configuration not tracked in this checkout.
- Historical context available only in local copies if it was never promoted to remote canonical GBrain.
- Git history and documentation links.

**Dependency checks before archive**

1. Compare slugs/checksums against remote canonical GameFlowBrain and identify local-only pages.
2. Salvage only concrete local-only decisions not present remotely; do not bulk-promote stale prose.
3. Validate active tracked links after removal.
4. Confirm no external workstation script reads these paths.

**Sequence**

1. Produce a source-qualified local-only/same/diverged inventory.
2. Promote approved unique evidence to remote canonical pages with normal review.
3. Add an explicit archival marker/forward pointer in a small repository doc if needed.
4. Remove the local trees from the active checkout; rely on Git history for the old snapshot.
5. Add a guard preventing a new checked-in brain mirror from becoming an authority.

**Rollback/archive need**

Git history already archives the current bytes, but a one-time export/checksum ledger is prudent before deletion. Do not create a third in-repo `archive/brain/` copy.

**Why cut beats repair**

Two repaired authorities still diverge. The correct architecture is one remote canonical source plus explicit source-qualified fallbacks, not synchronized prose mirrors in the app repository.

---

### BP-05 — Archive — legacy trackers and assistant backups

**Candidate paths**

- `ACTIONITEMS.md` — 199,612 bytes / 3,154 lines.
- `.thoughts.md` — 80,379 bytes.
- `CLAUDE.md.backup` — 8,482 bytes / 292 lines of stale Solokit workflow.

**Tracked evidence**

- Report 00 classifies `ACTIONITEMS.md` as superseded historical chronology; its top section is March 2026 and repeated lists are duplicates (`00:221-250`).
- Report 09 AKW-09 shows its “Roadmap” title and missing archival banner make it a plausible false authority.
- `CLAUDE.md.backup:5-19` instructs agents to use stale Solokit session management; active `AGENTS.md:73` says not to revive stale Solokit `.session/` state.
- Only stale `brain/Handoffs/handoff-000.md` and `brain/Operations/Project-Root-Files.md` reference `CLAUDE.md.backup` in the targeted tracked search.
- `.thoughts.md` contains old literal training/backtest commands and artifact references but is not named by the current authority map.

**Blast radius**

- Historical archaeology and old command provenance.
- Filename-driven agents and humans currently misrouting to stale instructions.
- Any untracked external workflow that still reads root files by name.

**Dependency checks before archive**

1. Search current tracked contracts and remote canonical pages for live inbound links.
2. Identify any unresolved decision whose only evidence is in these files and promote it selectively.
3. Confirm Git history retention is acceptable.

**Sequence**

1. Add/verify one current forward pointer in `docs/understanding/README.md` or the canonical register workflow.
2. Preserve a final commit/tag containing the historical files.
3. Remove them from the active tree rather than moving them to another obvious root-level “roadmap” path.
4. Add path/link lint so active docs do not point back to them.

**Rollback/archive need**

Git history is sufficient for normal archaeology. If Chase wants browsable history, use one clearly marked external/archive bundle, not active root files.

**Why cut beats repair**

Updating thousands of lines of chronological snapshots into a current tracker duplicates the canonical register/GBrain. Their only valid role is history.

---

### BP-06 — Consolidate — overlapping god-class indexes and pre-lane refactor plans

**Candidate surfaces**

- `.hermes/plans/god-class-tech-debt-refactor-index-2026-05-18.md`.
- Pre-lane plans it points to, especially:
  - `mlb-promotion-backtest-architecture-refactor-2026-05-18.md`;
  - `feature-store-boundary-refactor-2026-05-18.md`;
  - `dashboard-god-route-component-refactor-2026-05-18.md`;
  - `paper-live-trading-policy-refactor-2026-05-18.md` (shared/Kalshi disposition must defer to report 12).

**Tracked evidence**

- The old index defines five broad lanes (`god-class-tech-debt-refactor-index:41-115`).
- The newer canonical lane index defines ten responsibility-specific migrations and current statuses (`god-class-migrations/README.md:26-39`).
- Reports 00/01/07 reconcile current status against the ten-lane index, not the old five-lane priority list.
- Targeted searches found the old plan filenames referenced by the old index but not by the ten lane docs, current understanding docs, brain, or root Handoffs.
- Lane 01 and Lane 02 are complete; Lane 03 is core-complete; the older plans’ generic diagnoses no longer represent current implementation status.

**Blast radius**

- Historical rationale and research notes.
- Old links in untracked/local artifacts.
- Implementers who may accidentally choose an old plan instead of the current lane plan.

**Dependency checks before consolidation**

1. Map unique requirements from each old plan into the current lane plan’s history/expansion checkpoints.
2. Confirm no unique unfinished requirement remains unrepresented.
3. Apply report 12 before consolidating plans whose primary scope is Kalshi/arbitrage.

**Sequence**

1. Add a supersession ledger to the ten-lane index.
2. Copy only unique still-current constraints into the appropriate lane docs with provenance.
3. Mark old plans historical or move them to one clearly archival plan bundle.
4. Keep completed lane plans and progress logs in place as anti-regrowth evidence.

**Rollback/archive need**

Preserve old plan bytes in Git history. No need to keep two live index hierarchies.

**Why consolidate beats repair**

Updating both indexes and both generations of plans creates parallel status authorities. One current lane index with historical provenance is enough.

---

### BP-07 — Consolidate — deployment/schedule documentation and dual Vercel configs

**Tracked evidence**

Schedule documentation:

- `docs/railway_deployment.md:7-18,70-94` claims old schedules/Railway cron semantics.
- `docs/daily_pipeline_automation.md` retains older schedule prose.
- Current source of schedule truth is `src/orchestration/scheduler.py`; the current explainer is `docs/understanding/railway-scheduler.md` (report 04 E-10).

Vercel configuration:

- root `vercel.json:1-6` assumes repository-root deployment and runs `cd dashboard && npm install`.
- `dashboard/vercel.json:1-4` assumes the Vercel project root is already `dashboard/`.
- CI uses `npm ci`, not root config’s `npm install` (report 08 I-06).
- Current Vercel project-root metadata was not queried.

**Classification:** Consolidate, with the final Vercel file selection gated on live project metadata.

**Blast radius**

- Railway operator runbooks and incident response.
- Vercel project import/build behavior.
- Dashboard deployment and lockfile immutability.
- Historical troubleshooting sections that remain useful despite stale schedule claims.

**Dependency checks before consolidation**

1. Read-only inspect Vercel project root/config/install/build metadata without variable values.
2. Inventory unique useful deployment/recovery content in old Railway docs.
3. Ensure current scheduler explainer covers IDs, gates, times, args, and ownership before deleting duplicated schedules.

**Sequence**

1. Make one schedule table mechanically generated/tested from the scheduler/registry.
2. Replace old schedule sections with a pointer; preserve only platform setup/history that remains accurate.
3. Select one Vercel root mode, use immutable `npm ci`, and delete the other config.
4. Update the one deployment owner doc and verify a preview build in a separately approved lane.

**Rollback/archive need**

Git history preserves old instructions. Preserve incident-specific history only where it explains current non-obvious constraints.

**Why consolidate beats repair**

Independent schedule prose and two Vercel root modes will drift again even if both are corrected today.

---

### BP-08 — Remove — tracked generated, test, and probe residue

**High-confidence remove set**

- `.next/trace`, `.next/trace-build` — generated Next traces; dashboard `.gitignore:15-18` already ignores its local `.next/`, while root `.next/` is tracked.
- `GameFlowData.egg-info/*` — generated package metadata; only self-references surfaced.
- `_test_longspec.py` — generic 44-line requests demo unrelated to GameFlow; no tracked references surfaced.
- `backtest_resultsauditstmp_dryrun_selected5/suite_summary.md` — tracked dry-run output under a malformed generated path.
- `tmp_mcp_gbrain_probe.py` — one-off probe that reads active Hermes MCP config and prints tool responses; no tracked callers surfaced.

**Likely remove after provenance check**

- `slate_test.png` — root screenshot with no tracked references.

**Blast radius**

Minimal for source/runtime. Removing `tmp_mcp_gbrain_probe.py` removes a convenient diagnostic, but current Hermes/GBrain skills own the safe source-routing workflow and avoid committing endpoint-sensitive probes.

**Dependency checks before cut**

1. Confirm no CI/package command expects `GameFlowData.egg-info` pre-generated.
2. Confirm screenshot is not externally linked.
3. If MCP probing remains useful, replace it with a documented skill-owned diagnostic that redacts configuration and output; do not retain a project-root one-off script.

**Sequence**

1. Remove files.
2. Add root ignore rules for `.next/`, `*.egg-info/`, malformed audit output roots, and project temp/probe patterns as appropriate.
3. Run tracked-file inventory and clean-clone packaging/build checks in the implementation slice.

**Rollback/archive need**

None beyond Git history. Do not create an archive directory for generated residue.

**Why cut beats repair**

These files have no product ownership. Renaming or documenting generated residue would only make accidental artifacts look intentional.

---

### BP-09 — Archive — `.hermes/tmp/` operational copies, probes, handoffs, and production log export

**Tracked evidence**

`.hermes/tmp/` has 16 tracked files / 311,118 bytes, including:

- a 231,306-byte Railway production log export;
- copied handoffs and a partial `remote_brain/` mirror;
- one-off CLV mapping/sync/probe scripts;
- copied GBrain nightly/weekly service/timer/script artifacts.

Report 08 I-08 independently classifies the tracked Railway log as an avoidable exposure path. Report 09 says remote GameFlowBrain is canonical, so copied handoffs/brain pages are not valid active authorities.

**Blast radius**

- Forensic evidence for old incidents.
- Remote maintenance scripts if these tracked copies are the only source for installed units.
- One-off CLV repair reproducibility.
- Potential operational information/secret exposure in log history.

**Dependency checks before archive**

1. Compare installed remote service/unit scripts to tracked copies; choose a real maintained owner path if still active.
2. Classify each probe/map/sync script as reusable operation, incident-only evidence, or disposable.
3. Run values-redacting secret/PII classification over current log and history; rotate only if actual exposure is found.
4. Verify copied handoffs/pages exist in remote canonical Git.

**Sequence**

1. Promote still-active maintenance scripts to a named `scripts/` or canonical operations owner with tests/runbook.
2. Preserve incident-only evidence in an approved restricted archive if needed.
3. Remove copied brain/handoffs and production logs from the repository.
4. Ignore `.hermes/tmp/` while allowing explicitly reviewed non-temp `.hermes` plans/audits.

**Rollback/archive need**

Required for any unique incident or installed-service provenance. Git history is not an appropriate restricted log archive if sensitive data is found; history remediation/rotation is a separate security decision.

**Why archive beats repair**

A temp directory should not become a second script library, brain mirror, and log store. Reusable assets need real ownership; the rest should leave the active tree.

---

### BP-10 — Consolidate — legacy MLB single-config backtest entrypoint/harness

**Target**

- `src/backtesting/mlb/run_mlb_backtest.py`.
- `src/backtesting/mlb/mlb_backtest_harness.py` after shared constants/test seams are moved.
- legacy-only tests in `tests/test_mlb_backtest_legacy_deprecation.py` and the harness-specific portion of `tests/test_mlb_quote_clean_line_selection.py` after replacement coverage exists.

**Tracked evidence**

- The entrypoint declares itself legacy/debug-only and requires `--allow-legacy`: `run_mlb_backtest.py:35` and `tests/test_mlb_backtest_legacy_deprecation.py:10-44`.
- `src/backtesting/mlb/line_selection.py:3-5` says `run_mlb_sweep.py` is canonical production validation and the harness is retained for single-config/legacy use.
- Completed Lane 01 made the sweep path canonical and promotion-aware; reports 00/01 explicitly reject reopening that migration.
- The harness cannot be deleted immediately: `run_mlb_sweep.py` and `backtest_data_loader.py` import `STAT_ACTUALS`, and quote-clean tests instantiate `MLBBacktestHarness`.

**Blast radius**

- Legacy CLI users and old docs/commands.
- Shared `STAT_ACTUALS` constant consumers.
- Quote-clean line-selection characterization.
- Any one-off debugging workflow not represented by the canonical sweep CLI.

**Dependency checks before consolidation**

1. Inventory current human/operator usage of `--allow-legacy`; no config/scheduler caller surfaced.
2. Move `STAT_ACTUALS` to a neutral contract module.
3. Replace harness-only quote-clean tests with direct shared-service tests.
4. Prove the canonical sweep can express a one-config diagnostic run with equivalent inputs/output metadata.

**Sequence**

1. Extract only shared constants/contracts still imported by canonical code.
2. Add a thin documented one-config mode to the canonical runner if genuinely needed.
3. Migrate tests from harness instantiation to shared line/data/edge services.
4. Remove the legacy entrypoint, then remove the harness when import inventory reaches zero.
5. Delete legacy-deprecation tests whose sole purpose was preserving `--allow-legacy`; replace them with removal/inventory guards.

**Rollback/archive need**

Git history and completed Lane 01 plan preserve behavior. Keep no runnable archived copy in source paths.

**Why consolidate beats repair**

The repository already has a canonical promotion path. Continuing to harden a second explicitly non-promotion entrypoint spends tests and ownership on behavior users should not trust.

---

### BP-11 — Needs evidence — detached NCAAB lane

**Tracked evidence supporting review**

- 23 tracked NCAAB files: three migrations, model/backtest/feature-store code, two orchestration wrappers, five processing/scraper modules, one pipeline doc, one brain page, and four focused tests.
- `src/orchestration/scheduler.py` has no NCAAB registration.
- No NCAAB dashboard route/data consumer surfaced; the only dashboard reference is a public landing card marked `coming_soon` (`dashboard/src/components/landing/SupportedMarkets.tsx:27-31`).
- `src/orchestration/ncaab_daily_stats_job.py:105-158` and `ncaab_lines_job.py` are standalone manual jobs, not dead imports.
- Report 00 classifies the March NCAAB migration/backfill/train item as `Needs-Evidence / Shelf`, not abandoned.

**Why not classify Remove/Archive yet**

A coherent manual/shelved lane can intentionally have no production scheduler. Source age and detachment alone do not prove abandonment. Applied migration files must remain even if product code is retired.

**Evidence needed**

1. Chase product decision: planned, parked with a review date, or retired.
2. Read-only DB migration/table-state check through the SQL-runner lane if disposition depends on existing data.
3. Current test health and dependency/import viability in a separately approved scoped run.
4. Confirmation whether “Coming Soon” is intentional product messaging.
5. Search external/manual job schedulers before declaring entrypoints unused.

**If retired, safe sequence**

1. Remove the public “Coming Soon” claim.
2. Disable/document any external manual schedule.
3. Archive model/scraper/processing/orchestration code and tests together.
4. Retain applied migrations as immutable history; add new migrations only if schema cleanup is separately approved.
5. Preserve a concise retirement record and any data-retention decision.

**Why cutting could beat repair**

If there is no product intent, completing scheduler/inference/dashboard/promotion work would create a fourth active sports lane with no near-term value. If intent remains, retain it explicitly and stop calling detachment debt.

---

### BP-12 — Needs evidence — root `claude-flow` npm dependency and residual Claude-flow helpers

**Tracked evidence**

- Root `package.json:1-5` contains only `claude-flow ^3.0.0-alpha.82`; root `package-lock.json` is 289,191 bytes.
- Railway installs Python requirements; Vercel/CI install under `dashboard/`. Report 08 found no deployment path consuming the root package.
- `docs/token-saving-architecture-rework.md:10-58` says Claude-flow hooks/boilerplate were removed because they consumed context and token budget.
- `.claude/` still contains helpers/statusline logic referring to `.claude-flow`, `agentic-flow`, and optional `@claude-flow/memory`; this means the entire root package cannot be called unused from manifest shape alone.
- No current understanding/README command instructs users to install or run root `claude-flow`.

**Evidence needed**

1. Start the supported Claude Code workflow in a disposable/no-network mode and record which local modules/package binaries are actually resolved.
2. Map each tracked `.claude` helper to active settings/hooks/commands.
3. Compare required packages to what the root lock actually provides; helpers reference packages/npx commands that do not obviously match the sole direct dependency.
4. Confirm Hermes/Codex workflows do not consume root npm state.

**Disposition options**

- If no active hook resolves it: Remove root `package.json`/lock and stale helpers together.
- If only a small helper set is active: Consolidate into an explicitly named agent-tool manifest with exact versions and tests.
- If still intentionally supported: Retain with an owner/readme and stop treating the root package as app/deploy dependency.

**Blast radius**

Claude Code startup/statusline/memory only; not the dashboard app or Railway runtime based on current manifests.

**Rollback/archive need**

Git history is sufficient. Preserve no stale `node_modules` or `.claude-flow` runtime state.

**Why cutting could beat repair**

If obsolete, removing an alpha dependency and 289 KB lock reduces supply-chain and agent-startup ambiguity. If active, consolidation—not silent retention—is the correct outcome.

---

### BP-13 — Needs evidence — root `run_now.py` manual pipeline trigger

**Tracked evidence**

- `run_now.py:1-83` is a standalone manual NBA props/full-lines plus edge-refresh/inference wrapper.
- No scheduler, source, test, README, or docs caller surfaced in targeted tracked searches.
- It invokes real scrape/inference jobs, so it was not executed during this audit.
- The underlying entrypoints remain active scheduler-owned scripts.

**Why not call it dead**

Manual operator CLIs are invoked by humans and often have no import/config references. Lack of callsites is insufficient.

**Evidence needed**

- Chase/operator confirmation of current use.
- Comparison with current runbook commands and `lines_job.py`/`edge_refresh_job.py` flags.
- Static command-parity check showing it preserves current gates, model selection, and failure semantics.

**Disposition**

- If used: move to `scripts/`, document it, add a dry-run/static command test, and name it as the one manual wrapper.
- If unused: Remove it; do not keep a second orchestration policy wrapper.

**Why cutting could beat repair**

An undocumented manual wrapper can silently drift from scheduler safety policy. Either give it explicit ownership or remove it.

---

## Explicit retains

### BP-14 — Retain — Lane 02 MLB feature-store facades and legacy implementations

**Paths**

- `src/models/mlb/mlb_feature_store.py`.
- `src/models/mlb/mlb_batter_feature_store.py`.
- `src/models/mlb/features/legacy_pitcher_feature_store.py`.
- `src/models/mlb/features/legacy_batter_feature_store.py`.

**Evidence**

- Facade docstrings explicitly state that implementation remains behind a thin compatibility boundary while callers migrate.
- At least 14 tracked Python consumers still import the facades, including training, sweep, inference, diagnostics, and focused tests.
- Lane 02 is complete and its inventory tests prevent SQL/helper regrowth.
- Report 01 R-01 rejects “large legacy implementation requires a new lane” as a size-only suspicion.

**Decision**

Retain. Do not rename/move/delete solely because files contain `legacy`. Optional callsite migration may eventually reduce facade use, but deleting the implementation now would break current production/model paths and reopen completed architecture.

**Future removal gate**

Only reconsider after public loaders/transforms own every current behavior, direct import inventory is zero, parity tests cover training/backtest/inference, and Lane 02’s stable boundary is intentionally replaced—not merely renamed.

### BP-15 — Retain — current production and irreversible/history-bearing evidence

Retain unless a separate approved plan proves otherwise:

- `src/models/artifacts/production/` and active `production_playoffs/` until manifest-pinned artifact storage/rollback replaces them.
- `src/models/mlb/artifacts/production/` required-stat subset until BP-03 adjudication and manifest validation.
- Applied migration files under `database/migrations/` and other migration roots, even when a product lane is retired. Removal of live schema requires new forward migrations and DB-safe approval; old migration history is not dead code.
- Completed Lane 01/02 and core-complete Lane 03 plans/progress logs, including anti-regrowth rationale.
- Tests that enforce completed migration boundaries.
- `tests/test_mlb_backtest_legacy_deprecation.py` until BP-10 actually removes the legacy entrypoint; before removal it is a safety guard, not dead-test clutter.
- Model reports/frozen-baseline docs that are the only indexed evidence for archived artifact interpretation, until archive IDs/checksums replace literal paths.

## Tests tied only to removable behavior

| Removal | Current test treatment | Required replacement before deletion |
|---|---|---|
| BP-01 calibration offsets | No focused forbidding test surfaced; any positive loader/application tests must be inventoried | Forbidden-artifact and no-implicit-activation tests across inference/refresh/backtest/sweep |
| BP-10 legacy MLB backtest | `tests/test_mlb_backtest_legacy_deprecation.py`; harness-specific quote-clean test | Canonical one-config/shared-service tests plus static removal guard |
| BP-11 NCAAB, if retired | Four focused NCAAB tests | No replacement for retired product behavior; retain only migration/schema-history validation where needed |
| BP-08 generated residue | No product tests should depend on it | Clean-clone package/build/import checks, not fixture preservation |
| BP-14 facades | Inventory/as-of/feature-store tests are anti-regrowth coverage | Do not delete; replacement parity required before any future boundary change |

## Cross-candidate dependency and sequencing

### Phase 0 — decisions and containment

1. Wait for/read report 12 and apply its shared Kalshi dependency boundaries.
2. Confirm NCAAB product disposition (BP-11).
3. Confirm artifact archive destination, retention, checksums, and rollback owner (BP-02/BP-03).
4. Read-only verify Vercel project root (BP-07) and current Claude helper usage (BP-12).

### Phase 1 — no-regret source-tree hygiene

1. BP-08 generated residue removal and ignore guards.
2. BP-05 archive stale root trackers/backups after link/salvage check.
3. BP-09 promote any truly reusable temp scripts, then remove temp mirrors/logs.
4. BP-06 add supersession mapping and collapse old live plan indexes.

These should be separate commits/slices so rollback does not restore unrelated generated files and stale authorities together.

### Phase 2 — policy and compatibility cuts

1. BP-01 characterize then remove global offset activation/writing.
2. BP-10 extract shared constants/tests, then remove the legacy MLB entrypoint/harness.
3. Keep BP-14 intact.

### Phase 3 — artifact/deployment consolidation

1. Close manifest/consumer validation gaps from reports 01/05.
2. Archive BP-02 non-production binaries with verified hashes/readback.
3. Adjudicate/archive BP-03 undeclared MLB production stats after report 12/shared-consumer checks.
4. Consolidate BP-07 Vercel and Railway docs/config.

### Phase 4 — optional product-lane retirement

If Chase retires NCAAB, remove product claims and executable source/tests together while retaining migration history. If NCAAB remains planned, mark it explicitly parked with an owner/review date and stop treating missing scheduler wiring as accidental debt.

## Rollback principles

- **Source/config removals:** narrow commits; revert by candidate, not one repo-cleanup mega-commit.
- **Artifacts:** verify external archive checksums/readback before source-tree deletion. Do not rewrite Git history in the same slice.
- **Knowledge/docs:** salvage local-only durable facts first; Git history preserves old prose. Do not create new in-repo archive mirrors.
- **Compatibility entrypoints:** replace required shared constants/tests first; add static removal guards after deletion.
- **Migrations/data:** never delete applied migration history as cleanup. Schema/data removal requires a forward migration and separately approved DB-safe process.
- **Security/logs:** if history inspection finds a credential, rotate first; decide history remediation separately.

## Rejected or deferred suspicions

1. **Rejected: files named `legacy_*_feature_store.py` are dead.** They are the active implementation behind tested thin facades.
2. **Rejected: `production_playoffs/` is an old backup.** Scheduler and calibration-monitor runtime code still select it under `NBA_PLAYOFF_MODE`.
3. **Rejected: every old model artifact can be deleted now.** Promotion/backtest resolvers and historical reports still use path/filename identity; archive/manifest work must precede removal.
4. **Rejected: the NCAAB lane is dead because it is not scheduled.** Standalone manual entrypoints, docs, migrations, models, and tests exist; product intent is missing.
5. **Rejected: completed migration plans are clutter.** Lane 01/02/03 progress and anti-regrowth evidence remain current controls. Only overlapping predecessor indexes/plans should be consolidated.
6. **Rejected: all notebooks and `archive/` code should be removed based on age/location.** No usage/knowledge-value review was completed. They remain a future bounded archive inventory, not a confirmed cut.
7. **Deferred to report 12:** Kalshi, bot-tracker Kalshi controls, Kalshi-backed arbitrage, Kalshi-specific tests/flags/plans, and shared files whose only questioned consumer is Kalshi.
8. **Deferred:** broad dashboard route deduplication. Report 07 found serious ownership/auth/data issues, but no redundant non-Kalshi route was proven removable rather than repairable in this pass.
9. **Deferred:** unsupported MLB source families after binary archive. Source contracts may support historical settlement, backtests, or future retraining; binary inactivity alone does not prove source deadness.

## Coverage gaps

- Report 12 and final adjudication report 11 were not present.
- No live Railway/Vercel/GitHub/Supabase state, external scheduler, DB schema, table data, logs, or environment values were inspected.
- No model artifact was deserialized and no deployed image inventory was queried.
- No broad ignored/untracked artifact scan was performed.
- External human use of manual CLIs, notebooks, root `Handoffs/`, and Claude helpers was not observable from tracked references.
- No Git history secret scan or binary-history size analysis was performed.
- No test/build/lint command was run; validation is report-only by instruction.
- The exact external archive technology/retention policy is unknown.
- Root `.claude/`, notebooks, `archive/`, old `docs/development_docs/`, and all 52 tracked scripts were not exhaustively pruned; candidates require the same usage/config evidence standard before promotion.

## Decision queue

### Remove after characterization

- BP-01 global calibration-offset executable path.
- BP-08 generated/test/probe residue.
- BP-10 legacy MLB single-config backtest path after shared imports/tests move.

### Archive after dependency/rollback checks

- BP-02 non-production model binaries.
- BP-03 undeclared MLB production stat binaries.
- BP-04 local brain/handoff mirrors.
- BP-05 stale trackers/backups.
- BP-09 temp operational artifacts.

### Consolidate

- BP-06 plan/index generations.
- BP-07 schedule docs and Vercel configuration.

### Needs Chase/current-state evidence

- BP-11 NCAAB disposition.
- BP-12 root Claude-flow tooling.
- BP-13 manual `run_now.py` ownership.

### Explicitly retain

- BP-14 Lane 02 feature-store facades/implementations.
- BP-15 current production artifacts, applied migrations, completed migration evidence, and anti-regrowth tests until their stated replacement gates pass.

## Validation record

- Report scope is non-Kalshi pruning; Kalshi-specific cuts were excluded and cross-referenced to the dedicated owner.
- Every candidate states classification, evidence, blast radius, dependency checks, sequence, rollback/archive need, and why cutting/consolidating is preferable to repair where applicable.
- Candidates were not inferred dead from names or age alone; runtime/import/config/reference evidence and explicit coverage gaps are recorded.
- No source, test, config, plan, register, card, DB, production, artifact, deployment, package, or secret state was changed.
- Only `.hermes/audits/tech-debt/13-broader-project-pruning-candidates.md` was written.
