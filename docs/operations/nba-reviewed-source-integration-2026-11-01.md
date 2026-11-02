# NBA reviewed-source integration — November 1, 2026

## Result and authority

Chase authorized completing slice 2's source-integration gate, excluding historical S1, then merging and pushing reviewed NBA work. Source is now integrated on main at `a44b09a4772cfa6b76e023a0105cbcf9c075ef39`.

This closes the pending source integration for coding slices 1–2 and 5. Slices 3–4 were already integrated and were preserved. It does not close historical-data, trained-model, platform-specific acceptance, or slices 6–8.

Commit chain:

- `9db47d2`: exact reviewed slices 1–2 implementation and previously approved fixture setups, six files.
- `8da6176`: exact four reviewed slice-5 source files only; inherited harness/simulator/validator copies excluded.
- `fee1d39`: neutral reconciliation of slices 1–2 with the already merged slice-3 CLI.
- `798f443`: reviewed slice-5 source integration into the combined branch.
- `a44b09a`: merge of the validated combined branch into main.

The earlier `3ad1203` sweep-titled commit was documentation-only. Git content, not its title, establishes source availability.

## Semantic reconciliation

Only `src/backtesting/run_backtest.py` and `tests/test_run_backtest.py` conflicted. Preserve both lanes:

- Keep slice-3 positive-finite flat validation/forwarding, actual effective sizing/stat/allowlist/predictor metadata, typed fixture collaborators and numerical owners.
- Keep slices-1/2 importable parser/main, DB/model/output-free planning, strict artifact execution with explicit honest legacy opt-in, actual accepted artifact identity/file digests, and no combined/global-offset consumption in canonical replay.
- Supersede stale metadata claiming slice 2 is absent with `verified_manifest` versus `legacy_unverified` inspection results. Both remain not decision-grade until separate historical/model acceptance.
- Reconcile only previously approved existing fixture setups. All nine existing replay test functions, decorators, fixture arguments, assertion/raises/mock-assert ASTs and action/assertion tails match both source branches; no new cases or weakened assertions.

Independent integration review: **SPEC PASS / QUALITY APPROVED**. Evidence: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-source-integration/independent-review-798f443/`.

## Actual destination-main verification

All execution imports were routed to the actual main checkout, not the original worktrees:

- **186 existing tests passed** across training, replay, NBA feature contracts/store inventory, simulator, harness and legacy sweep suites.
- **27 guarded entrypoint checks** and **five controlled real CLI executions** passed using the real predictor, harness, simulator, metrics and exports around controlled model/data adapters. Manifest/legacy identity, flat bankroll caps, copula/embedded calibration and diagnostic-offset nonconsumption passed.
- **551 sweep contract checks**, **seven sweep parity/failure groups with 24 guarded CLI calls**, and **27 nested-output/alias safety checks** passed.
- Scoped Ruff, compile/AST, staged allowlist and diff hygiene passed. Main source bytes match the independently reviewed integration; slice-5 files match `8da6176` exactly.

The first destination verifier attempt was blocked at import-time engine construction. Inspection established installed `python-dotenv` is 1.0.1, which ignores `PYTHON_DOTENV_DISABLED`; the mounted main checkout has environment files absent from isolated worktrees. The disposable verification harness was corrected to explicitly disable dotenv loading while retaining DB/network/model/fitting tripwires. Application source and dependencies were not changed. The initial blocked receipt is preserved separately; the corrected destination suite and operator receipts report zero external attempts. This is a verification-environment distinction, not a production runtime/configuration fix.

Receipts/logs: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-source-integration/main-verification/`. Controlled bundles are not trained/serialized-model or historical-profit evidence.

## Preservation and remaining gates

- Historical S1 remains deferred and unmerged. Its loader/harness changes were not copied; protected harness, simulator, Monte Carlo, quantile trainer and feature-source modules match pre-integration main.
- Active reconciliation work, the untracked slice-6 plan, old backups/stashes and unrelated dirty files were excluded. No broad staging, stash application, reset or branch deletion.
- No training, DB write, sync, production-model deserialization, manual deployment, historical repair or promotion was performed.
- Remaining work: implement/accept slices 6–8, verify sync/source readiness and approved historical policies, then bounded real feature/artifact/replay acceptance before owner-launched baseline training.

Publication is authorized to `origin/main`; completion must be verified against the exact remote ref after push. No force push or history rewrite is authorized. Preserve worktrees and excluded work after publication.
