# NBA paper promotion and runtime policy — 2026-11-12

## Delivered locally

`scripts/promote_model.py` now promotes the model and its BL/paper configuration together. It accepts explicit `--bl-tau`, `--bl-z-max`, `--bl-max-weight`, `--edge-threshold`, or `--sweep-config <exact cell config.json>`. With no supplied settings it preserves the currently loaded policy (prints all effective values); it does not choose an optimal sweep cell automatically.

Runtime policy is stored at `src/models/artifacts/production.policy.json`, alongside—not inside—the immutable `production/` artifact bundle. The sidecar is allowed by Git. The old `.source` marker is replaced by the sidecar's source-model field; `.source` is not an allowed artifact-inventory filename. Model inventory/digests remain unchanged. A deterministic evaluation ID binds model identity, settings and selection policy. The previous model/policy are retained in a printed rollback directory when replacing an installation.

Daily inference, edge refresh/reinference and paper placement read the same policy at process startup. Restart/redeploy consumers after changing it. A promotion is local filesystem preparation, not a deployment or job launch. Default runtime consumers read `production.policy.json`; alternate `--name` destinations are staged bundles, not an automatic runtime switch.

Current owner direction: baseline `no_injury_baseline/nba_run_20261108_122129`; 7% edge; BL tau .75 / z_max .5 / max_weight .75; PTS/REB/AST; best opportunity per player/game, then reject OVER without substitution; $10 flat stakes; 5,000 draws and seed 42. NBA legacy Q50/L5 recommendation filters are removed; the shared legacy function used by MLB remains unchanged. Saved copula is retained and global combined offsets are not loaded. Current Out-player, quote, pregame and immutable-entry safeguards remain. Missing/mismatched policy evidence and (once promoted) mismatched model identity fail closed at placement.

## Command

From the project root in PowerShell:

` .\venv\Scripts\python.exe scripts\promote_model.py no_injury_baseline/nba_run_20261108_122129 --bl-tau 0.75 --bl-z-max 0.5 --bl-max-weight 0.75 --edge-threshold 0.07 --dry-run `

Remove `--dry-run` only for the intended local promotion. Include both the model directory and `production.policy.json` in any separately authorized publication. Do not treat earlier grid profits as independent forward evidence.

## Verified

- Real reference artifact/cell CLI dry-run: exact .07/.75/.5/.75, reference identity `8e1c8b65b0e0dfc0c87f83d35341fb45ba4d185c428120c877b2366518ee151d`, no writes.
- Real trained bundle copied into disposable scratch: full promotion/readback passed. Controlled policy-install failure restored both previous model and sidecar. Runtime imports read nondefault persisted values (.6/.8/.65/.09), proving the loader is not using frozen BL constants.
- Populated actual daily/refresh functions: BL/selection parity, OVER rejection without alternate UNDER, threshold/odds bounds and removal of legacy flags passed.
- Existing UNDER-selection path passed with current policy metadata supplied in memory; missing/mismatched metadata failed closed. No test files edited or added.
- Scoped Ruff and diff checks passed. Guards recorded zero live DB/network/model-load/model-fit attempts.
- Untouched existing 50-test subset: 45 passed, five failed. Three failures were present before these changes (dated schedule freshness and incomplete mocked prediction input). Two now conflict with the new contract: OVER expected to pass; UNDER fixture omits required BL policy evidence. The repository suite is not fully green; test-file changes remain unapproved.

## Not done

No actual production artifact replacement, policy installation, commit, push, deployment, SQL, inference job or paper bet was performed. Candidate historical game-line timing remains unverified; these are exploratory forward-paper settings, not live-money promotion. Native schema/inference/placement/readback/settlement acceptance is still separate.
