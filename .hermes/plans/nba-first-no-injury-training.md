# Approved NBA first injury-free baseline launch fixes

Owner direction: stop adding seasonal sample gates. Implement injury exclusion and compatible incumbent parameters, verify offline, hand Chase full training command. Long training remains owner-launched. No live DB/model fit, deployment, commit/push. Preserve concurrent edits.

Allowlist: src/models/train_pipeline.py; new src/models/configs/nba_no_injury_baseline_hyperparams.json. If essential additional source is required, report exact scope before editing. Read relevant actual model feature selection/manifest/inference paths but don't redesign them. No new or modified unit tests.

Requirements:
1. Add explicit --exclude-injury-features flag for FULL ordinary training, default false for backwards compatibility. Thread into orchestrator and dry-run/run metadata. Reject incompatible partial/calibration-only/historical combinations if not supported; don't silently ignore.
2. Exclude complete existing injury-derived family from feature selection candidate sets before selector runs, then assert no injury features in selected lists passed to fitting. Use current names from contracts (team_out_*, opp_out_*, player_is_questionable/player_is_probable; inspect for any additional injury-derived members). Do not fill injuries with zero or drop games based on injury availability. No global feature list mutation. Saved selected features and model feature metadata must preserve exclusion across calibration/reload/inference (verify existing consumption). Unknown stats and other features unchanged. Explicitly reject incompatible force-include if relevant.
3. Copy incumbent best_hyperparams.json from src/models/artifacts/production into new baseline config containing exactly currently supported minutes/pts/reb/ast keys, preserving each value exactly. Do not edit production file or weaken shared validation to accept threes. Verify equality programmatically, reject if source structure differs from expectation. No tuning.
4. Effective baseline command --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --hyperparams-path src/models/configs/nba_no_injury_baseline_hyperparams.json --artifacts-dir src/models/artifacts/no_injury_baseline. Run --dry-run only, do not train. No strict attachment or new window/cold-start policy.
5. Validate scoped existing CLI/training tests under offline guard, Ruff/compile, and a small controlled probe through feature-selection candidate and actual artifact feature-consumption boundaries with fitting/model loads disabled. No test edits. Preserve empirical CDF/no global offsets. Do not reopen rolling sampling or existing game-line temporal caveat as launch gate; first run exploratory, not promotion certified.

Source baseline includes dirty requests/training_loader smoke edits owned by coordinator; don't modify those or unrelated scheduler/injury docs. User just approved narrow launch fixes. Deliver concise diff summary, verification and command; report actual failures honestly.

## Delivery verification

Implemented `--exclude-injury-features` in ordinary full training: candidate filtering before selection, assertions before fitting and finalization, persisted config/manifest flag, incompatible-mode rejection. New parameter JSON retains exactly incumbent minutes/pts/reb/ast values and omits threes; production artifact unchanged.

Coordinator reviewed scoped diff, independently reran 30 existing train-pipeline tests under offline guard (zero live DB/model attempts), Ruff, diff checks, exact parameter equality and full baseline `--dry-run`. All passed. No training launched, commit or push. No additional seasonal sampling gate.

Owner command from repository root: `.\venv\Scripts\python.exe -m src.models.train_pipeline --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --hyperparams-path src\models\configs\nba_no_injury_baseline_hyperparams.json --artifacts-dir src\models\artifacts\no_injury_baseline`
