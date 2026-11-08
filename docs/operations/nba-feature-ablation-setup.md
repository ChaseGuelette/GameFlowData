# NBA feature-family ablation setup

Status: owner-run discovery setup. This adds full-training feature controls; it does not launch training, replay, tuning, promotion, or a sweep.

## Fixed reference

- Reference artifact: `src/models/artifacts/no_injury_baseline/nba_run_20261108_122129`
- Fixed parameters: `src/models/configs/nba_no_injury_baseline_hyperparams.json`
- Fit seasons: `22023 22024`
- Calibration season: `22025`
- Selector tolerance: `0.005`
- Tuning: disabled
- Injury-derived inputs: excluded
- Reference manifest calibration cutoff: `null`. Do not invent a cutoff or use the partial-retrain parent field as lineage.

Compare the original artifact with the game-market-free control first. Then compare each additional-family exclusion (short form, opponent matchup, prop market) against that same game-market-free control, not independently against the original artifact. Each command writes a separate timestamped artifact below its own root. Run from the repository root in PowerShell. Remove `--dry-run` only when Chase authorizes the long owner-run fit.

```powershell
.\venv\Scripts\python.exe -m src.models.train_pipeline --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --exclude-feature-families game_market --hyperparams-path src/models/configs/nba_no_injury_baseline_hyperparams.json --feature-tolerance 0.005 --artifacts-dir src/models/artifacts/nba_feature_ablations/game_market_free --dry-run
```

```powershell
.\venv\Scripts\python.exe -m src.models.train_pipeline --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --exclude-feature-families game_market short_form --hyperparams-path src/models/configs/nba_no_injury_baseline_hyperparams.json --feature-tolerance 0.005 --artifacts-dir src/models/artifacts/nba_feature_ablations/game_market_short_form_free --dry-run
```

```powershell
.\venv\Scripts\python.exe -m src.models.train_pipeline --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --exclude-feature-families game_market opponent_matchup --hyperparams-path src/models/configs/nba_no_injury_baseline_hyperparams.json --feature-tolerance 0.005 --artifacts-dir src/models/artifacts/nba_feature_ablations/game_market_opponent_matchup_free --dry-run
```

```powershell
.\venv\Scripts\python.exe -m src.models.train_pipeline --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --exclude-feature-families game_market prop_market --hyperparams-path src/models/configs/nba_no_injury_baseline_hyperparams.json --feature-tolerance 0.005 --artifacts-dir src/models/artifacts/nba_feature_ablations/game_market_prop_market_free --dry-run
```

Forced-include example: `player_starter_prob` is appended to every minutes/PTS/REB/AST quantile list after selection and verified in the artifact evidence.

```powershell
.\venv\Scripts\python.exe -m src.models.train_pipeline --local --train-seasons 22023 22024 --cal-season 22025 --exclude-injury-features --exclude-feature-families game_market --include-features player_starter_prob --hyperparams-path src/models/configs/nba_no_injury_baseline_hyperparams.json --feature-tolerance 0.005 --artifacts-dir src/models/artifacts/nba_feature_ablations/game_market_free_forced_starter --dry-run
```

Dry-run validates requested names and conflicts but reports feature availability as unresolved because it does not load training data. A real fit fails before selection if any requested family member or exact feature is absent, nonnumeric, protected, or blocked by injury exclusion.

## Comparison posture

Use October 21-November 2, 2026 only as the already-consumed discovery window from the first sweep review. Preserve the same settings: $2,000 bankroll, $10 flat stake, 5,000 samples, seed 42, minimum average minutes 10, five fixed raw edges (`.03/.05/.07/.10/.12`), with raw `.05` primary. Retain one fixed BL comparison at edge `.05`, tau `.5`, z-max `.5`, max-weight `.5`; do not rerun and select from the full 95-cell grid for every family. These dates are not untouched or promotion-certified. Reserve later accepted `22026` observations for independent confirmation.

Excluding `prop_market` removes prop-line columns from model inputs only. Sportsbook lines and odds remain available to replay, edge calculation, and payoff evaluation.

The current lifecycle schema requires an explicit `training.calibration_end`, and its artifact inspection requires the saved artifact's actual non-null cutoff to match. The reference artifact records a null cutoff, so it is not lifecycle-ready and this setup does not invent a date to make it pass. Direct training above is the runnable path preserving the baseline recipe. Lifecycle config/argv propagation can be used for future runs that have a legitimate explicit cutoff; exact raw-plus-BL sweep-cell orchestration remains in the existing sweep tooling and was intentionally not changed here.

## Controls and evidence

Supported families and exact sizes are `game_market` (2), `prop_market` (4), `opponent_matchup` (14), `short_form` (12), `minutes_role` (8), and `schedule` (3). `opp_avg_pace_l5` is not in `opponent_matchup`.

New full-training options:

- `--include-feature-families`
- `--exclude-feature-families`
- `--include-features`
- `--exclude-features`

The existing `--force-features` remains partial-retraining-only. The new controls are rejected for partial retraining, calibration-only, and strict historical modes. Requested, resolved, candidate, required, excluded, injury-excluded, and effective per-quantile lists are stored in `run_config.json` and manifest training metadata; `selected_features.json` remains the actual model configuration. Artifact validation rejects absent, tampered, or inconsistent nondefault evidence.
