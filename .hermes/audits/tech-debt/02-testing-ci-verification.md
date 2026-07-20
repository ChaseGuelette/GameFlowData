# Testing, CI, and Verification Audit

Status: read-only evidence report; findings are candidates pending independent review
Date: 2026-07-18
Scope: `tests/`, dashboard test configuration/tests, `.github/workflows/`, `pyproject.toml`, root/dashboard package manifests, and verification contracts in the god-class plans plus tech-debt audits 00, 01, and 04

## Executive summary

The repository has a substantial Python unit/static-test surface—124 tracked `test_*.py` files and 1,121 mechanically discovered test functions—but CI currently provides weaker evidence than that count suggests. The highest-risk verification gaps are:

1. push CI can auto-modify `src/` and `tests/` and then test a moving branch ref rather than enforcing that the triggering commit was clean;
2. Python coverage is collected with an explicit zero floor, no type-check gate runs, and active pytest/coverage configuration is duplicated across files;
3. the dashboard has no tracked test files or test runner, while CI runs only a placeholder-env production build—no lint, explicit typecheck script, route/component contract test, browser E2E, or RLS/authenticated-flow check;
4. promotion-critical parity and anti-regrowth gates already requested by the Python and scheduler audits are still absent, including empirical-CDF path parity, calibration-offset activation guards, artifact resolver/manifest parity, fast-sweep equivalence, scheduler inventory/CDN-only guards, and MLB lines failure propagation;
5. plan validation commands have drifted: 59 of 103 mechanically extracted planned test paths do not exist. Most belong to intentionally unimplemented lanes, but even completed/core-complete plans retain missing paths;
6. at least one wall-clock/sleep test is scheduler-load sensitive, and optional-capability tests can pass without exercising the capability.

No DB/network call, production action, broad test run, dashboard build, deployment, training, or backtest was performed.

## Method and mechanical inventory

Tracked-file inventories were performed first with `git ls-files`, then targeted files were read.

### Python tests

- 124 tracked `tests/test_*.py` files; 125 tracked Python files under `tests/` including `tests/__init__.py`.
- 23,764 LOC across tracked `tests/*.py`.
- 1,121 test functions, 129 `Test*` classes, 51 fixture definitions, and one parametrized test were mechanically discovered by AST.
- No `tests/conftest.py` or root `conftest.py` is tracked.
- No `pytest.skip`, `pytest.xfail`, skip/xfail marker, or `unittest.skip` surfaced. One module-level `pytest.importorskip` exists at `tests/test_card_renderer.py:5-7`.
- Static AST scanning found no direct `requests.get/post`, `httpx.get/post`, or `urlopen` call in test bodies. This reduces but does not eliminate source-code network coupling.
- The largest test modules are `tests/test_espn_injury_scraper.py` (814 LOC), `tests/test_backfill_opponent_allowed.py` (742), `tests/test_backfill_team_ids.py` (733), `tests/test_run_sweep.py` (688), and `tests/test_train_pipeline.py` (676).

### Dashboard and CI

- Zero tracked dashboard `__tests__`, `*.test.*`, or `*.spec.*` files.
- `dashboard/package.json:5-10` defines `dev`, `build`, `start`, and `lint`; it has no `test`, `typecheck`, or E2E script.
- `dashboard/package.json:26-34` has no Jest, Vitest, React Testing Library, or Playwright dependency.
- One tracked workflow exists: `.github/workflows/ci.yml`.
- CI has three jobs: push-only Ruff auto-fix, Python lint/tests, and dashboard build (`.github/workflows/ci.yml:9-88`).

### Existing plans and audits

- The migration index requires characterization first, removal guards, focused tests, lane-wide regression, and scoped diff review (`.hermes/plans/god-class-migrations/README.md:14-23`).
- Audit 00 classifies Lanes 01-02 complete, Lane 03 core-complete, and Lanes 04-10 documentation-only; it explicitly says completed lanes still need their regression guards (`.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md:272-295`).
- Audit 01 identifies missing probability, calibration, artifact, sweep, orchestrator, linker, and DB-client contracts (`.hermes/audits/tech-debt/01-python-architecture.md:50-290`).
- Audit 04 identifies missing scheduler/ingestion status, overlap, timeout, CDN-only, retry, settlement, telemetry, environment, and schedule contracts (`.hermes/audits/tech-debt/04-scheduler-ingestion-operations.md:119-444`).
- Mechanical extraction found 103 unique test paths referenced by god-class plans; 44 exist and 59 do not. Missing future tests in documentation-only lanes are expected design backlog, not evidence that tests were deleted.

## Findings

### TV-01 — Push CI mutates the branch and can validate a different tree than the triggering commit

Severity: High
Confidence: High

Exact evidence:

- `.github/workflows/ci.yml:10-13` makes `lint-fix` push-only.
- `.github/workflows/ci.yml:23-33` installs an unpinned latest Ruff, executes `ruff check --fix src/ tests/`, and auto-commits matching source/test changes.
- `.github/workflows/ci.yml:35-44` makes `python-tests` depend on that job and checks out `ref: ${{ github.ref }}` rather than explicitly checking the triggering SHA.
- Pull requests do not run the auto-fix job, but the dependent Python job uses `if: always() && !cancelled()` (`:38-39`), creating different push and PR control flow.

Concrete failure mode:

A push containing fixable lint defects can cause CI to write an unreviewed follow-up commit directly to the branch. The dependent test job then resolves a branch ref after the mutating job, so evidence can describe the auto-fixed tip rather than the exact triggering commit. The original commit may receive a green workflow even though it was not itself clean, and source/test edits can land without the normal local diff review. Installing latest Ruff in that mutation job also makes the generated patch non-reproducible against the repository's pinned `ruff==0.9.2` (`requirements-dev.txt:10-14`, `pyproject.toml:19-27`).

Interaction with existing plans/findings:

Every god-class plan requires RED/GREEN evidence and scoped diff review. A CI job that rewrites source/tests obscures which tree produced that evidence and weakens the anti-regrowth controls completed in Lanes 01-03.

Safe evidence step:

On a disposable branch, introduce one Ruff-fixable formatting/import defect and record the workflow's triggering SHA, auto-fix SHA, and Python-test checkout SHA. Do not run this experiment on `main`; alternatively, inspect Actions checkout logs from a prior auto-fix run.

Done condition:

CI validates the immutable event SHA; lint is a non-mutating, version-pinned required check; any suggested fix is an artifact or local command, not an automatic source/test commit; and push/PR required-check semantics are identical or explicitly documented and tested.

### TV-02 — Python CI has a zero coverage gate, no type-check gate, and split configuration authority

Severity: High
Confidence: High

Exact evidence:

- `.github/workflows/ci.yml:56-60` runs Ruff and `pytest --no-header -q --cov-fail-under=0`; zero coverage cannot fail the job for uncovered production code.
- No workflow step runs Pyright despite it being pinned in `requirements-dev.txt:13-14` and `pyproject.toml:24-27`.
- `pyproject.toml:79-90` defines pytest options, including `asyncio_mode = "auto"`, while a higher-priority standalone `pytest.ini:1-29` duplicates discovery/options/markers and claims asyncio mode comes from pyproject (`pytest.ini:2-3`). Pytest selects one configuration file; these files are not a merged contract.
- Coverage omit/report policy is duplicated and divergent across `pyproject.toml:92-107`, `pytest.ini:41-68`, and `.coveragerc:1-26`. The pyproject omits processing archives/notebooks/docs/database; `.coveragerc` instead omits unrelated optional API/core/middleware paths.
- CI does not name a coverage source or config file; it relies on plugin/config discovery while setting only the threshold flag (`.github/workflows/ci.yml:59-60`).

Concrete failure mode:

A large untested production change can pass CI because the threshold is zero. Type errors are detected only incidentally by imports or the Next build, not by the pinned Python type checker. Developers can edit pyproject pytest/coverage settings believing they changed CI while `pytest.ini`/`.coveragerc` remain active, producing local/CI command drift and misleading coverage omissions.

Interaction with existing plans/findings:

Audit 01's PA-01 through PA-08 all ask for new contract tests. A zero floor does not prevent those owners from regrowing untested. Lane 04's typed config/result contracts and Lane 05's shared probability contracts also lack a type-check gate.

Safe evidence step:

Run only configuration introspection in a disposable environment: record pytest's selected `inifile`, coverage's selected config, and Pyright's current error count without DB/network. Separately generate a coverage report for the existing no-DB contract subset; use that measurement to choose a ratchet rather than guessing a global target.

Done condition:

One authoritative pytest configuration and one authoritative coverage configuration are documented and used locally/CI; CI runs pinned Ruff and Pyright; coverage has a nonzero ratchet or changed-lines/critical-module contract; and the workflow prints selected config/source so false configuration edits are visible.

### TV-03 — Dashboard verification is build-only despite unimplemented route/component test contracts

Severity: Critical for authenticated/payment/admin route changes; High overall
Confidence: High

Exact evidence:

- Mechanical inventory found zero dashboard test files.
- `dashboard/package.json:5-10` has no `test`, `typecheck`, or E2E script; `:26-34` has no test/browser tooling.
- CI installs and runs only `npm run build` for the dashboard (`.github/workflows/ci.yml:66-85`) with placeholder Supabase public credentials (`:86-88`). It does not run `npm run lint` even though that script exists.
- Lane 09 requires route shape, auth/rate-limit statuses, prompt parity, request validation, LLM isolation, repository behavior, and unchanged AskChat response behavior (`.hermes/plans/god-class-migrations/09-dashboard-ask-api-route-migration.md:330-339`, `:343-439`, `:602-625`). Its progress log remains documentation-only with no typecheck/lint/tests/API calls (`:644-661`).
- Lane 10 explicitly says to establish a harness first and requires endpoint-string/route inventory plus pure UI calculations (`.hermes/plans/god-class-migrations/10-dashboard-god-components-pages-migration.md:271-295`, `:299-369`). Its progress remains documentation-only (`:595-613`).

Concrete failure mode:

A route can still compile while changing authentication status, RLS user scoping, response fields, prompt content, Stripe/webhook handling, Kalshi approval/cancellation semantics, or client endpoint strings. Placeholder-env build success proves bundling, not behavior. There is no executable browser path for login → protected page, subscription/checkout, Ask, history, admin/trading controls, or error handling, so production-coupled regressions can be green until a real user or live integration exercises them.

Interaction with existing plans/findings:

This confirms the starting premise of god-class Lanes 09-10 and Audit 00's product/UI coverage gaps (`00-existing-inventory-reconciliation.md:383-396`). It does not authorize those migrations; the harness is the missing prerequisite. Existing Audit 00 also shows prior claims about chat persistence/history pagination drifted, which is precisely why executable route/client contracts are needed.

Safe evidence step:

Add no-network tests first in a future approved slice: pure helper tests, mocked route-handler contracts for auth/status/shape, and component tests with mocked Supabase/LLM/Stripe boundaries. Then design a separate preview/local E2E lane using disposable users and test-mode Stripe—never production credentials or live-money/admin actions.

Done condition:

Dashboard CI runs lint, explicit typecheck, unit/component tests, and a bounded mocked route-contract suite; critical authenticated/payment/admin paths have preview/local E2E coverage with safe test identities; RLS/auth response semantics and client route strings are asserted; and builds remain a separate packaging gate rather than the only test.

### TV-04 — Promotion-critical parity and anti-regrowth gates identified by existing audits are absent from CI

Severity: Critical
Confidence: High

Exact evidence:

- Daily inference positively tests a quantile fallback when samples are missing (`tests/test_daily_runner.py:242-273`).
- NBA backtesting instead uses empirical samples or a Gaussian `stats.norm.sf` fallback (`src/backtesting/backtest_harness.py:792-816`). There is no shared daily/backtest parity test; Audit 01 PA-02 records the divergent owners and required table-driven guard (`.hermes/audits/tech-debt/01-python-architecture.md:83-112`).
- Audit 01 PA-01 reports automatic calibration-offset writer/consumer activation and no forbidding characterization test (`01-python-architecture.md:52-81`).
- Audit 01 PA-03/PA-04 reports inconsistent NBA resolvers and writer-only MLB manifests without consumer rejection tests (`:114-173`).
- Audit 01 PA-06 reports NBA fast sweep/private harness duplication and no equivalence/inventory gate (`:205-233`).
- Audit 04 E-01 reports no MLB lines failure-propagation test (`.hermes/audits/tech-debt/04-scheduler-ingestion-operations.md:121-153`).
- Audit 04 E-04 reports no scheduler/job-level guard preserving Railway `--cdn-only` (`:222-253`), even though Lane 07 Phase 0 explicitly requires it (`.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:320-340`).
- The missing-test inventory confirms `tests/test_prediction_edge_calculator.py`, `tests/test_scheduler_inventory.py`, and the other planned Lane 05/07 contract files do not exist.

Concrete failure mode:

CI can remain green while backtest and inference compute different probabilities for the same prediction, global calibration offsets reactivate by artifact presence, incomplete/mixed artifacts are selected, optimized sweep policy drifts from the canonical path, an MLB ingestion child failure exits zero, or a scheduler refactor drops the Railway CDN-only invariant. These are false-green outcomes at promotion and production-operation boundaries, not merely low line coverage.

Interaction with existing plans/findings:

This finding consolidates—without duplicating—the evidence queue in Audit 01 PA-01/02/03/04/06 and Audit 04 E-01/E-04. It also updates the god-class Lane 04/05/07 characterization baseline. Completed MLB Lanes 01-02 should not be reopened; stronger artifact consumers must preserve their thin-owner architecture.

Safe evidence step:

Implement only deterministic no-DB RED tests in future approved slices, in this order: calibration-offset activation inventory; daily/backtest probability parity including missing samples; resolver/manifest temporary-directory fixtures; fast/canonical sweep golden parity; MLB lines mocked failure exit; static/mocked CDN-only command guard. Do not train, backtest, scrape, or call production.

Done condition:

Each invariant has a named test file and required CI gate; all promotion-capable paths use empirical CDF or fail closed; artifact identity is manifest-validated; optimized/canonical paths pass equivalence fixtures; ingestion failures propagate nonzero; and Railway CDN-only behavior has an anti-regrowth test.

### TV-05 — God-class plan validation commands contain stale and not-yet-real test paths

Severity: Medium
Confidence: High

Exact evidence:

- Mechanical extraction found 59 absent paths among 103 unique plan-referenced tests.
- Documentation-only Lane 04's Phase 0 command already names absent `tests/test_training_orchestrator_inventory.py` (`.hermes/plans/god-class-migrations/04-training-orchestrator-migration.md:303-326`); 10 of its 12 referenced test files are absent.
- Documentation-only Lane 05's baseline command names absent daily/MLB inventory tests (`.hermes/plans/god-class-migrations/05-daily-prediction-runner-migration.md:326-346`); 11 of its 12 referenced tests are absent.
- Documentation-only Lane 07 names seven test files and all seven are absent; Phase 0 is explicit at `.hermes/plans/god-class-migrations/07-scheduler-job-registry-migration.md:320-340`.
- The completed Lane 01 plan still validates `tests/test_mlb_sweep_runner.py` (`.hermes/plans/god-class-migrations/01-mlb-quote-clean-backtest-sweep-migration.md:655-669`), but that path is absent.
- Core-complete Lane 03 references absent `tests/test_nba_feature_requests.py` and `tests/test_nba_line_feature_sources.py`; the current inventory instead contains `tests/test_nba_feature_contracts.py`, `test_nba_feature_store_inventory.py`, `test_nba_feature_transforms.py`, `test_nba_line_sources.py`, and `test_nba_source_boundaries.py`.
- Lane 10's documented `npm run typecheck` / conditional `npm test` commands (`10-dashboard-god-components-pages-migration.md:289-295`) cannot run because those scripts do not exist in `dashboard/package.json:5-10`.

Concrete failure mode:

An implementer copying a plan's “baseline” can fail before reaching RED, substitute an ad hoc subset, or incorrectly report that a lane-wide command passed. Completed-lane closeout evidence becomes non-reproducible when test names were renamed or consolidated without updating the plan. Conversely, future test paths in documentation-only plans can be mistaken for existing safety coverage.

Interaction with existing plans/findings:

Audit 00 correctly distinguishes completed/core-complete and documentation-only lanes (`00-existing-inventory-reconciliation.md:272-295`). This finding does not relabel future files as missing implementation; it identifies command-contract drift and stale completed-lane evidence. Audit 04 already says Lane 07's count/control baseline is stale (`04-scheduler-ingestion-operations.md:29-42`).

Safe evidence step:

For each lane selected for implementation, run a file-existence preflight over its validation command before any source edit. For completed Lanes 01-03, map each stale path to the current successor test(s) and verify the replacement contract by targeted collection only; do not rerun broad suites merely to rename prose.

Done condition:

Every command labeled current/baseline references existing scripts and files; future paths are explicitly marked “expected RED/create”; completed lane docs map renamed tests to current anti-regrowth contracts; and dashboard commands correspond to real package scripts.

### TV-06 — Wall-clock rate-limit tests are unnecessarily timing-sensitive

Severity: Medium-Low
Confidence: High

Exact evidence:

- `tests/test_espn_injury_scraper.py:310-319` measures elapsed wall time and expects a sleep to exceed `min_request_interval - 0.01`.
- `tests/test_espn_injury_scraper.py:321-335` performs a real `time.sleep(...)`, then requires the next measured call to complete in under 50 ms.
- The test does not inject a clock/sleeper or mock `time.monotonic`/`time.sleep` in these cases.

Concrete failure mode:

A loaded shared CI runner, Windows timer granularity, process scheduling pause, or coverage overhead can make the “minimal delay” branch exceed 50 ms despite correct rate-limit logic. The suite pays real sleep time and can intermittently fail for host scheduling rather than behavior.

Interaction with existing plans/findings:

Audit 04 calls for deterministic retry/timeout/process contracts. Leaving wall-clock sleeps in neighboring scraper tests makes operational test failures harder to distinguish from real scheduler regressions.

Safe evidence step:

Run only these two tests repeatedly under a harmless local CPU load if reproduction is needed. Prefer code inspection first; no API or DB is involved.

Done condition:

Rate-limit policy accepts or internally wraps an injectable monotonic clock/sleeper; tests advance fake time and assert requested sleep duration/call order, with no real sleep or sub-50-ms wall-clock threshold.

### TV-07 — Shared workflow fixtures are duplicated locally, weakening parity tests

Severity: Medium-Low
Confidence: High for duplication; Medium for current defect risk

Exact evidence:

- No tracked `conftest.py` exists.
- `tests/test_backtest_harness.py:31-78`, `tests/test_daily_runner.py:12-43`, and `tests/test_train_pipeline.py:21-37` independently define `mock_engine`, `mock_feature_store`, and workflow owner fixtures.
- Backtest's mock pipeline sets concrete feature lists (`test_backtest_harness.py:48-54`), while daily runner's same-named fixture is a bare `MagicMock` (`test_daily_runner.py:26-29`).
- Backtest's mock predictor synthesizes random samples and prediction objects (`test_backtest_harness.py:56-66`); daily runner's same-named predictor is bare (`test_daily_runner.py:31-33`).
- Audit 01 PA-02 and PA-06 require the same prediction/line/sample fixture across daily/backtest and canonical/fast paths (`01-python-architecture.md:106-112`, `:227-233`).

Concrete failure mode:

Nominally similar tests can pass against different mock contracts and sample shapes. A feature key, sample-key tuple, clipping rule, or output-field change can be updated in one local fixture but not another, preventing the suite from proving actual cross-path parity.

Interaction with existing plans/findings:

Lane 05 explicitly requests strict edge/devig parity (`05-daily-prediction-runner-migration.md:350-375`). The issue is not that every fixture must be global; promotion-critical golden inputs need one versioned owner while test-specific mocks can remain local.

Safe evidence step:

Diff the minimal input/output schemas used by `test_backtest_harness.py`, `test_daily_runner.py`, and `test_run_sweep.py`; create a proposed pure fixture factory in a test-support module and run only the affected no-DB tests before adoption.

Done condition:

A shared deterministic golden fixture owns prediction rows, sample keys, lines, odds, expected probabilities/edges, and output schema for parity-critical tests; local fixtures either consume it or document intentional divergence; random samples are seeded or literal.

### TV-08 — Optional dependency behavior can disappear without a failing required check

Severity: Low
Confidence: High

Exact evidence:

- `tests/test_card_renderer.py:5-7` skips the entire 323-line module when Pillow is unavailable. CI currently installs Pillow through `requirements.txt:18-21`, so this is not an observed CI skip, but alternate pyproject-only installs do not include Pillow (`pyproject.toml:6-17`).
- `tests/test_analyze_minutes_bimodality.py:57-65` uses unseeded random data and asserts dip-test output only inside `if result is not None`; a regression returning `None` while the capability is expected passes that test.
- `tests/test_analyze_minutes_bimodality.py:67-80` separately tests the missing-dependency fallback.
- Neither `requirements.txt:1-21` nor `requirements-dev.txt:1-17` pins `diptest`, and CI has no optional-feature matrix.

Concrete failure mode:

A supported card-rendering install path can silently skip all renderer tests if dependency manifests drift, and dip-test integration can stop producing output while CI remains green because `None` is accepted in the positive test. The current single requirements-based CI path does not define whether these are required or optional capabilities.

Interaction with existing plans/findings:

This is not a god-class migration blocker. It is a verification-contract ambiguity: optional features need explicit install/test policy rather than conditional assertions that look like positive coverage.

Safe evidence step:

Decide capability status first. If required, add dependency/import smoke checks and deterministic positive tests. If optional, create a named optional-dependency CI job or mark the fallback test explicitly; do not silently broaden the main production dependency set.

Done condition:

Required capabilities cannot skip; optional capabilities have an explicit extras/install contract and at least one job that exercises them; positive tests fail if an installed integration unexpectedly returns the unavailable sentinel; random inputs are seeded.

## Rejected suspicions

1. **Rejected: the Python suite broadly calls live network APIs.** Static AST scanning found no direct requests/httpx/urlopen calls in test bodies, and inspected scraper tests patch session calls (`tests/test_live_odds_scraper.py:15-17`, `:54-56`, `:96-116`). This is not proof of a global network deny; that remains a coverage gap.
2. **Rejected: the current suite has widespread skip/xfail debt.** No skip/xfail markers surfaced. The only module skip is Pillow (`tests/test_card_renderer.py:5-7`), which the current CI requirements install. TV-08 covers the remaining manifest/optional-capability ambiguity.
3. **Rejected: every absent plan-referenced test is a regression.** Lanes 04-10 are documentation-only, so most absent names are future TDD files. TV-05 is limited to command clarity and stale completed/core-complete references.
4. **Rejected: unseeded random use is uniformly flaky.** Several unseeded samples feed type/shape assertions whose outcomes do not depend on exact values. Only the optional dip-test positive branch and parity-fixture duplication were promoted; random occurrences without a concrete threshold failure remain observations.
5. **Rejected: tests currently hit a production DB by direct URL.** Inspected DB tests use fakes/mocks or placeholder/in-memory URLs (`tests/test_db_client.py:42-71`, `tests/test_game_lines_scraper.py:51-54`, `tests/test_injury_database.py:14-31`). `tests/test_pipeline_resilience.py:13-21` explicitly disables the live DB dependency fallback for its unit class.
6. **Not promoted: use of `date.today()`/`datetime.now()` is automatically flaky.** Most inspected cases derive both fixture and production comparison from the same day and are not near a demonstrated boundary. `tests/test_mlb_daily_player_props_scraper.py:8-16` still deserves a future injected-clock contract, but no concrete intermittent failure was established.
7. **Not promoted: 47 mechanically detected tests without a literal `assert` are false greens.** Many use `unittest.mock.assert_called*`, expected no-crash semantics, or indirect assertions not recognized by the simple AST heuristic. They require targeted semantic review before classification.

## Coverage gaps

- No broad pytest collection or execution was run, so actual collected/pass/skip counts, runtime duration, warning volume, ordering sensitivity, and current CI parity remain unverified.
- No dashboard build/lint/typecheck/test command was run. The absence of scripts/files is static evidence; current build health was not disputed or re-tested.
- No GitHub Actions run logs or branch-protection settings were inspected. Whether required checks attach to the original versus auto-fix SHA needs the safe experiment in TV-01.
- No coverage report was generated. TV-02 concerns the explicit zero threshold/config ambiguity, not a claim about the current numeric percentage.
- No Pyright or ESLint run was performed, so current error counts are unknown.
- No network-deny fixture/plugin exists, but static inspection did not prove an accidental source-level call during test execution. Add a deny-by-default socket guard only after a focused compatibility inventory.
- CI clears only `DATABASE_URL`, `ODDS_API_KEY`, and `RAPIDAPI_KEY` (`.github/workflows/ci.yml:61-64`). There is no repository-wide test policy clearing all live-trading/provider/Discord/Stripe/Supabase variables; static review did not establish a current unsafe call, so this remains a hardening gap rather than a confirmed production-coupling defect.
- Fixture duplication was sampled around NBA training/daily/backtest parity. The 51 fixtures were not all semantically deduplicated.
- Existing Audit 04 runtime claims were not re-probed against Railway/DB/logs; this lane only audited whether the requested verification contracts exist.
- Browser accessibility, mobile layout, visual regression, and real preview RLS behavior remain outside static evidence.
- Dependency compatibility across `pyproject.toml` versus `requirements.txt` was not installed/tested. Notable version drift includes SQLAlchemy `2.0.37` vs `2.0.46` and python-dotenv `1.2.1` vs `1.0.1` (`pyproject.toml:12-16`, `requirements.txt:8-13`).

## Prioritized safe evidence queue

1. Make CI immutable and pin Ruff; verify checkout SHA behavior on a disposable branch.
2. Establish one pytest/coverage authority, add Pyright, and choose a measured coverage ratchet.
3. Add dashboard lint/typecheck scripts and a no-network unit/route test harness before any Lane 09/10 extraction.
4. Add the no-DB promotion/operations RED contracts in TV-04, preserving completed Lane 01/02 ownership.
5. Refresh validation commands for completed/core-complete plans and mark future test files explicitly.
6. Replace real clock/sleep assertions and establish shared deterministic parity fixtures.
7. Decide required versus optional dependency test policy.

This queue gathers evidence only. It does not authorize source/config/plan/register edits, DB access, deployment, production calls, training, or backtests.

## Validation record

- Report scope is limited to testing, CI, dashboard verification, and existing plan/audit contracts.
- Every promoted finding includes exact evidence, concrete failure mode, confidence, interaction, a safe evidence step, and a done condition.
- Rejected suspicions and unverified coverage gaps are recorded separately.
- No broad tests, DB/network operations, dashboard build, deployment, production action, training, or backtest were run.
- Only `.hermes/audits/tech-debt/02-testing-ci-verification.md` was written.
