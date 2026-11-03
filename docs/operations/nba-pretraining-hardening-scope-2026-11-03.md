# NBA pretraining hardening and bounded data acceptance

Status: owner-approved code items 1 and 3 are implemented and merged into main as `4ffdf065`, from reviewed source commit `974b57ea` on `repair/nba-pretraining-hardening`. Parent validation: 197 existing offline tests, scoped Ruff/compile/whitespace, controlled training plus replay/sweep/audit/lifecycle checks; 96 core tests also passed from the destination main checkout. Independent review: SPEC PASS / QUALITY PASS. Only the owner-approved existing trainer test input setup changed; no new permanent tests or assertions changed. Deployment is not certified by source publication.

Item 2's [bounded read-only investigation](nba-pretraining-bounded-data-investigation-2026-11-03.md) is complete, but real quote/injury source acceptance remains blocked. No scraping, DB writes, real training/model loading, deployment or promotion occurred. The actual local populated source-acceptance check is not complete. Cold-start policy remains deferred. Owner choices are in [readiness reconciliation](nba-training-readiness-reconciliation-2026-11-03.md); background evidence is in [policy research](nba-training-policy-research-2026-11-03.md).

Implementation details and exact checks are in main's [implementation receipt](nba-pretraining-hardening-implementation-2026-11-03.md) and [operator wiki](../understanding/nba-pretraining-hardening.md). The original scope/estimates below are planning history, not a fresh implementation queue or permission to launch jobs.

## 1. Chronological separation — code and controlled validation

- Enforce the intended 2023–24/2024–25 fit population and separate 2025–26 calibration population; initially treat 2026–27 as evaluation.
- Validate actual keys/date ranges, not only CLI season strings; reject overlap and persist exact population boundaries in artifact metadata.
- Inspect date ordering and game/date grouping for inner feature selection, early stopping and optional hyperparameter tuning. `shuffle=False` alone does not prove chronological ordering or game separation.
- Keep fixed-hyperparameter baseline and retuned candidate separately identified. Do not launch either while hardening.
- Known source owners: `src/models/train_pipeline.py`, `src/models/training_preflight.py`, `src/models/quantile_trainer.py`, `src/models/hyperparameter_tuner.py`, and the existing feature-selection owner `src/processing/feature_selection.py`. Final edit allowlist depends on narrow call-path inspection; do not modify every file by default.
- No scraping required for this code work.

## 2. Source eligibility — bounded reads first; conditional code/repair/scraping

- Obtain a keyed, capped, read-only source/coverage inventory for the selected seasons and representative early/late dates; no full raw-odds-table scan. Sampling is triage, not full-cohort certification.
- Distinguish absent rows from missing game/player linkage, insufficient quote clocks/provenance, unpaired outcomes, and unavailable complete injury editions.
- Reuse the active [props-linkage recovery lane](nba-props-linkage-recovery-2026-11-03.md); it has uncommitted source changes and must not be overwritten. Its sampled local/remote parity does not establish historical quote authenticity or all-season coverage.
- Reuse existing eligible data and linking/sync paths where possible. If provenance or edition retention is inadequate, repair capture/parser/receipt behavior BEFORE collecting replacements.
- Any scrape proposal names exact dates/events/markets/cutoffs or injury editions, maximum requests, resume behavior, storage destination and estimated API-credit usage. No general three-season rescrape is implied.
- Official injury PDF ingestion may need targeted parser/identity/receipt work; successful raw-PDF retrieval is not a finished ingestion path.
- Coverage and source acceptance still require whole declared-cohort accounting, performed incrementally under explicit limits after triage.

## 3. Training route and execution bounds — code plus bounded real acceptance

- Thread the explicit historical policy through the actual training entrypoint; lifecycle historical mode is currently attachment-only, so shared FeatureStore support is not sufficient by itself.
- Add the narrowly scoped initial-acquisition date/game/player-game row limits, budget-plus-one overflow detection, initial-query timeout, and limit identity binding. Do not silently truncate.
- Reuse existing feature/source/artifact owners. Identify exact lifecycle adapter/config paths before approving an edit allowlist.
- Validate affected existing suites with real DB/network blocked, then a small authorized read-only local consumer run, and finally a baseline dry-run. No new tests/fixtures without owner approval; no substitute giant scratch framework.
- Launch readiness is conditional on selected source/cohort acceptance and remaining policy choices; completing these tasks does not resolve deferred cold starts or authorize training/promotion.

## Rough effort, not measured runtime

- Chronology plus training-route/resource hardening and scoped tests: approximately **1–2 focused engineering workdays**, assuming reuse of current owners and no substantial interface redesign.
- First bounded data-triage pass: approximately **1–2 hours** of investigation; tighten repair estimates from its receipts.
- If existing data can be accepted/relinked: bounded repair and acceptance may fit within the same work window, depending on owner-run jobs and DB performance.
- If a new robust official-injury ingestion path or quote-provenance capture repair is necessary: allow an additional **1–2 engineering days** as a contingency, not a committed scope.
- Historical collection runtime/API credits, transfer runtime and final cohort verification cannot be estimated honestly before missing task counts and provider behavior are measured. These are additional to engineering effort.
- Training, full hyperparameter search, model sweeps, cold-start redesign and deployment are excluded from these estimates.

## Recommended sequencing and approval boundaries

Run independent code hardening and bounded read-only data triage in parallel after concrete scope approval. Finish capture repairs before any needed rescrape. Chase launches long collection/repair/training jobs; no paid API call, DB write, broad backfill, commit, merge or deployment is inferred from this estimate. First implementation handoff must name exact files/behaviors and validation commands; this document is a scoping checkpoint, not permission for a writer to discover-and-edit broadly.
