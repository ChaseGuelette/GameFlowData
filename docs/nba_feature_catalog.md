# NBA feature-contract catalog

Status: active contract summary. Reconciled 2026-08-24.

The exact stable contracts live in `src/models/features/nba/contracts.py` and are re-exported by `src/models/feature_store.py` for compatibility.

## Architecture

The NBA model predicts minutes and per-minute rates separately, then combines their uncertainty through Monte Carlo simulation. Stable model contracts are:

| Contract | Target | Feature count |
|---|---|---:|
| Minutes | playing time | 33 |
| Points rate | points/minute | 31 |
| Rebounds rate | rebounds/minute | 30 |
| Assists rate | assists/minute | 30 |
| Threes rate | made threes/minute | 32 |
| Unique across all contracts | — | 74 |

Counts are a reconciliation snapshot. Source constants and the artifact's feature configuration are authoritative for a specific model.

## Coarse source families

`feature_family()` classifies the stable contract into:

- `player_rolling`
- `team_context`
- `opponent_context`
- `line_context`
- `game_context`
- `schedule_context`
- `injury_context`

`validate_feature_lists()` rejects duplicate or unowned contract features.

## Player and role context

Contracts draw from shift(1) player basic/advanced rolling tables, including recent/longer-term volume, variability, role trends, starter probability, and minutes floors. Artifact-compatible momentum names are preserved:

- points `player_pts_l3_l15_ratio` uses L3/L15;
- rebounds, assists, and threes retain `*_l3_l15_ratio` names but use L3/L5;
- minutes has explicit L3/L5 and L3/L15 role-change ratios.

Do not rename these compatibility columns without an artifact migration.

## Team, opponent, and game context

Sources include team/opponent pace and efficiency, defense versus position, home/away, sportsbook spread/total, and per-stat player prop lines.

All quote-derived features require an as-of decision boundary and pre-commence filtering. A latest-snapshot query without those constraints violates the temporal contract.

## Schedule and injury context

All contracts include rest and schedule density. Team/opponent absence aggregates and same-position opportunity signals appear where defined by the exact contract.

`player_is_questionable` and `player_is_probable` are minutes features only. They are not present in the rate contracts.

## Missing/default behavior

Defaults are owned by the focused NBA loaders. Do not assume the feature store always synthesizes league priors: the current facade explicitly favors stable loader defaults, often zero, for missing context.

Deprecated travel/opponent-rest compatibility fields may still be supplied as neutral values by source loaders but are not stable contract members unless listed in `contracts.py`.

## Temporal integrity

- Shift(1) rolling rows labeled with game date X contain only pre-X games.
- Position and injury context are selected as of the target date.
- Prop/game lines are selected as of the prediction decision and before game commencement.
- Training, date, date-range, and single-player modes must produce aligned names and semantics.

## Validation after feature changes

1. Run contract validation and focused loader tests.
2. Exercise training, date, date-range, and one-player inference paths.
3. Compare the exact dataframe columns with the artifact configuration.
4. Check non-default value distributions for any newly merged source.
5. Run the full Python suite when a shared contract/source helper changes.

## Related documentation

- [NBA feature store](feature_store_documentation.md)
- [NBA rolling-average population](populate_average_stats_documentation.md)
- [Model pipeline runbook](model_pipeline_runbook.md)
