# Implementation Spec: Complete one-command MLB batter-hits lifecycle

## Goal
Make one PowerShell-safe command start from the locked batter-hits candidate definition and orchestrate the complete report-only lifecycle:

1. train through the configured calibration cutoff;
2. resolve and verify the completed artifact identity;
3. run a quote-clean sweep on the separate independent window;
4. run full dropout, CLV, and +15m/+30m/+60m timing audits;
5. run CLV ranker diagnostics;
6. classify Confirm / Shelf / Exclude; and
7. write an explicit staking recommendation: `blocked`, `flat_paper`, or `capped_kelly_paper_eligible`.

The operator command must not require `artifact_dir`, `sweep_dir`, or a hand-computed `sweep_artifact_identity_sha256`.

## Allowed files
Implementation and tests may edit only these existing areas unless a directly required test fixture needs a narrowly justified addition:

- `src/models/mlb/lifecycle/runner.py`
- `src/models/mlb/lifecycle/decision.py`
- `src/models/mlb/lifecycle/config.py` only if required for a declarative staking policy; prefer no schema expansion
- `src/models/mlb/lifecycle/adapters.py` only if an existing command contract is insufficient
- `scripts/run_mlb_model_lifecycle.py`
- `tests/test_mlb_lifecycle_runner.py`
- `tests/test_mlb_lifecycle_decision.py`
- `tests/test_mlb_lifecycle_config.py` only if config behavior changes
- `tests/test_mlb_lifecycle_adapters.py` only if adapter behavior changes
- `configs/mlb/batter_hits/platoon_contact_end_to_end.yaml` (new)
- `docs/development_docs/mlb_model_lifecycle_usage_guide.md`
- `docs/development_docs/mlb_model_lifecycle_runner.md`

Do not edit the existing attached/resume presets except if a documentation-only comment is essential. They must continue to work as certification/resume presets.

## TDD requirement
Use vertical RED -> GREEN cycles. Add a failing behavioral test before each production behavior. Actually run each focused test and confirm the expected failure before implementation.

## Requirements

### 1. End-to-end preset
Add `configs/mlb/batter_hits/platoon_contact_end_to_end.yaml` with:

- profile `batter_hits`;
- locked candidate `no_prop_line` plus forced include families `platoon` and `contact_quality`;
- no `artifact_dir`;
- no `sweep_dir`;
- no `sweep_artifact_identity_sha256`;
- training seasons `[2024, 2025]`, calibration season `2026`, calibration end `2026-04-12`;
- independent evaluation window `2026-05-18` through `2026-06-21`;
- quote-clean dense CLV snapshots, `slate_or_tminus`, T-60, preferred-book routing;
- full audit, minimum 100 bets, 1000 bootstrap samples;
- finalist/report-only gates requiring positive ROI, CLV CI, ranker CI, monotonicity, independent-window evidence, and max drawdown 0.25;
- flat $100 sweep staking. The lifecycle may recommend capped-Kelly paper eligibility after evidence, but must not execute Kelly staking, deployment, production promotion, or live trading.

Use a truthful coverage note that is an operator attestation/reference, not a claim that the new run already passed.

### 2. Dynamic artifact-to-sweep handoff
The existing runner must pass the artifact directory resolved after training into the sweep command and all downstream audit identity checks. The generated sweep must be bound to that exact artifact identity without requiring a manual attached-sweep hash.

Preserve attached artifact/sweep behavior and fail-closed provenance checks.

### 3. Complete dry-run plan
A dry-run of the end-to-end preset from a clean output root must:

- invoke no subprocesses and perform no DB access;
- write isolated `*_dry_run` planning state;
- record planned training, sweep, full audit, ranker, decision, and staking-policy stages in manifests/commands/status;
- show the real generated artifact/sweep output roots without requiring them to exist;
- end with Shelf/live-blocked and staking `blocked` because no evidence ran.

If dry-run currently cannot represent downstream paths after a not-yet-created artifact, improve planning without weakening real-run output verification.

### 4. Fake subprocess end-to-end contract test
Add a deterministic tmp-path test using the real `LifecycleRunner` and an injected fake subprocess runner. Starting from a clean lifecycle root and the no-attach config, the fake must materialize the minimum valid outputs expected from each subprocess and prove one `runner.run()` call executes in order:

- training;
- artifact identity;
- independent quote-clean sweep;
- full audit including the complete dropout bundle and scored +15m/+30m/+60m timing evidence;
- ranker;
- decision;
- staking policy.

The test must prove the generated sweep command uses the artifact that training actually created. It must also prove no attached paths/hash were supplied by the YAML.

### 5. Explicit staking recommendation
Write durable report-only outputs:

- `staking_recommendation.json`
- `staking_recommendation.md`

The JSON must include at least recommendation, reasons, decision classification/posture, report-only marker, and explicit booleans showing no deployment/live/Kelly action was performed.

Conservative mapping:

- dry-run, Shelf, Exclude, discovery, missing/mismatched evidence, or any non-Confirm result -> `blocked`;
- Confirm independent validation with qualifying flat-paper evidence -> `flat_paper`;
- Confirm finalist certification with full verified audit/timing/CLV/ranker evidence -> `capped_kelly_paper_eligible`.

`capped_kelly_paper_eligible` is permission for a separately reviewed paper-only experiment, not an instruction and not live eligibility. Never emit a recommendation that enables production/live action.

Include this as a durable `staking_policy` lifecycle stage after `decision`, with input identity tied to the exact decision/evidence. Resume/force-stage behavior must remain fail closed.

### 6. CLI/operator docs
Update the usage guide so the primary batter-hits workflow is one copy-pasteable PowerShell command:

`.\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_end_to_end.yaml`

Document the dry-run command first. Clearly label the older independent/finalist presets as attach/resume certification workflows, not end-to-end. State that the real command is expensive and must be manually launched by Chase after dry-run review. Document staking output semantics and that no live/deploy/Kelly action occurs.

Update the runner reference consistently if it describes stages/output files.

## Non-goals

- Do not run real training, sweeps, audits, rankers, DB queries, deployments, or trading during implementation/validation.
- Do not change model math, empirical probability calculations, calibration, feature definitions, quote selection, or ranker algorithms.
- Do not promote/copy artifacts to production.
- Do not enable Kelly, live trading, or Kalshi.
- Do not change unrelated files or refactor the lifecycle broadly.
- Do not remove attached/resume support.

## GameFlow invariants

- Probabilities remain empirical `(samples > line).mean()`; never Gaussian CDF.
- Never globally recalibrate or conformal-shift Q10.
- Feature selection is not an ablation; retain exact platoon + contact_quality forced-family identity (2 families, 20 forced features under current registry).
- Preserve temporal separation: evaluation starts strictly after calibration cutoff.
- Keep quote-clean source/timing/routing consistent through sweep and audit.
- Lifecycle remains report-only and human-gated.
- No SQL/Supabase calls are needed for this implementation task.

## Validation

Run at minimum:

1. New focused RED/GREEN tests while implementing.
2. `./venv/Scripts/python.exe -m pytest tests/test_mlb_lifecycle_runner.py tests/test_mlb_lifecycle_decision.py tests/test_mlb_lifecycle_config.py tests/test_mlb_lifecycle_adapters.py -q`
3. `./venv/Scripts/python.exe -m ruff check src/models/mlb/lifecycle scripts/run_mlb_model_lifecycle.py tests/test_mlb_lifecycle_runner.py tests/test_mlb_lifecycle_decision.py tests/test_mlb_lifecycle_config.py tests/test_mlb_lifecycle_adapters.py`
4. Real CLI dry-run only (no expensive run): `./venv/Scripts/python.exe scripts/run_mlb_model_lifecycle.py --config configs/mlb/batter_hits/platoon_contact_end_to_end.yaml --dry-run`
5. Inspect dry-run `commands.json`, `stage_status.json`, `promotion_decision.json`, and `staking_recommendation.json`.
6. If focused validation passes, run `./venv/Scripts/python.exe -m pytest -q` unless runtime/resource constraints make that unreasonable; report any skipped or unrelated failure.
7. `git diff --check` on allowed files.

## Review criteria

- One command genuinely connects training output to independent sweep and full certification stages.
- No manual artifact/sweep/hash fields exist in the end-to-end preset.
- Fake subprocess test exercises every expensive-stage contract without real DB or expensive work.
- Dry-run is isolated and subprocess-free.
- Staking recommendation is explicit, conservative, paper-only, and identity-bound.
- Attached/resume behavior remains covered.
- Diff is limited to allowed files and contains no unrelated cleanup.
