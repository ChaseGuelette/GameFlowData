# NBA feature store

Status: active source-backed reference. Reconciled 2026-08-24.

The stable facade is `src/models/feature_store.py`. It delegates mode-specific loading and source ownership to `src/models/features/nba/` while preserving the public `FeatureStore` API used by training, backtesting, and live inference.

## Source ownership

- `contracts.py`: stable feature lists and family validation.
- `training_loader.py`: historical training datasets.
- `inference_loader.py`: one player/game request.
- `date_batch_loader.py`: one-date batches.
- `date_range_loader.py`: inclusive date-range batches.
- `line_sources.py`: as-of game and prop lines.
- `injury_context.py`: team, opponent, and player injury context.
- `player_sources.py`, `team_sources.py`, `opponent_sources.py`, and `context_sources.py`: bounded source queries.
- `requests.py`: typed request objects passed to loaders.

New source SQL and transformations belong in these focused owners, not in the facade.

## Public API

```python
from datetime import date

from src.db.client import get_engine
from src.models.feature_store import FeatureStore

store = FeatureStore(get_engine())
training = store.get_training_dataset(["22023", "22024"])
slate = store.get_features_for_date(date(2026, 1, 15))
player = store.get_player_game_features(
    player_id=2544,
    game_id="0022500001",
    as_of_date=date(2026, 1, 15),
)
```

The facade exposes:

- `get_training_dataset(seasons)`
- `get_player_game_features(...)`
- `get_features_for_date(game_date)`
- `get_features_for_date_range(start_date, end_date, chunk_size=25)`

## Stable feature contracts

`src/models/features/nba/contracts.py` is the exact membership authority. At reconciliation time:

| Model contract | Features |
|---|---:|
| Minutes | 33 |
| Points rate | 31 |
| Rebounds rate | 30 |
| Assists rate | 30 |
| Threes rate | 32 |
| Unique across all five lists | 74 |

The facade re-exports these lists for compatibility. See [NBA feature catalog](nba_feature_catalog.md) for family-level interpretation; do not copy feature membership into another runtime constant.

## Temporal integrity

- Every training/inference route must use data available before the target game.
- Precomputed rolling-average rows are shift(1) snapshots. Selecting the row labeled with the target game date using `<= game_date` is safe because that row contains only prior-game information.
- Raw game and player-prop quotes require as-of and pre-commence filtering. “Latest row” without a decision-time boundary is not acceptable.
- Injury reports use report-date/as-of constraints.
- Feature additions must work in training, date, date-range, and single-player modes; compile-only validation is insufficient.

## Important semantics

- Missing features use the explicit defaults owned by the loaders/contracts. The facade no longer promises broad synthetic league-average filling.
- `player_is_questionable` and `player_is_probable` are minutes-contract features, not rate-contract features.
- REB/AST/3PT columns named `*_l3_l15_ratio` retain artifact-compatible names even though their implementation uses L3/L5. Points uses L3/L15.
- Prop-line values are model inputs only when the exact contract includes them; time-valid quote selection remains mandatory.
- `FeatureConfig` defaults to a ten-minute rate floor, three L5 games, and excluded seasons `22019`/`22020`.

## Validation

After feature-store changes, run the focused feature-store tests plus the full Python suite if a shared source/helper changed. Always exercise at least:

1. one training load;
2. one single-date load;
3. one date-range load;
4. one single-player inference load;
5. stable feature-contract validation.

Do not run DB-backed smoke paths without selecting the intended local or isolated database route.

## Related documentation

- [NBA feature catalog](nba_feature_catalog.md)
- [Populate average stats](populate_average_stats_documentation.md)
- [Model pipeline runbook](model_pipeline_runbook.md)
