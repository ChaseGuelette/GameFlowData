# MLB Pitcher K Phase 3A: Lineup / Contact Feature Expansion

Date: 2026-05-13
Status: completed experiment — do not promote

## 2026-05-14 reconciliation update

Phase 3A was implemented and evaluated, but it should not be promoted. The lineup/contact feature family improved or preserved aggregate plausibility while compressing the Phase 2 contrarian-under edge.

Primary diagnostic artifacts:

- `docs/analysis/mlb_phase3a_agreement_20260513/README.md`
- `docs/analysis/mlb_phase3a_agreement_20260513/final_causal_form.txt`

Superseding roadmap:

- `docs/development_docs/mlb_pitcher_k_phase3b_pitcher_extremes_roadmap.md`

Important reconciliation:

- Do not continue by adding more lineup/team-context features.
- Do not treat the small high-lineup-delta added-bet result as a design decision; it is only a hypothesis.
- Future work should first validate the Phase 2 baseline on non-overlap / quote-clean windows, then test small pitcher-side-extreme feature batches.

## Purpose

Phase 2 pitcher strikeout results showed real promise, especially under-only. The new context/workload feature set produced much stronger results than prior pitcher K attempts. The next proposed experiment is Phase 3A: add opposing projected lineup and batter contact-profile features, then retrain and backtest against the clean Phase 2 baselines.

This document records the exact implementation/validation sequence and the current blocker discovered before retraining.

## Phase 2 baselines to beat

Use 100+ bets as the minimum meaningful-volume threshold.

### Raw under-only baseline

Config:
- Stat: `pitcher_strikeouts`
- Direction: `under`
- Model: Phase 2 clean artifact `src/models/mlb/artifacts/mlb_run_20260513_111207`
- BL: none
- Edge: `0.05`
- Kelly: `0.125`

Result:
- 131 bets
- 60.3% hit rate
- +21.72% ROI
- +$9,596
- Sharpe 2.58
- MaxDD 9.2%

### BL under-only baseline

Config:
- Stat: `pitcher_strikeouts`
- Direction: `under`
- Model: Phase 2 clean artifact `src/models/mlb/artifacts/mlb_run_20260513_111207`
- Tau: `0.90`
- z_max: `0.25`
- max_weight: `0.80`
- Edge: `0.02`
- Kelly: `0.125`

Result:
- 110 bets
- 63.6% hit rate
- +34.68% ROI
- +$7,873
- Sharpe 4.00
- MaxDD 4.5%

## Feature sets in scope for Phase 3A

Add lineup/contact-profile features only.

### Repair / improve existing lineup feature

- `projected_lineup_k_pct`
  - Compute from actual projected/confirmed lineup batters.
  - Use batter rolling averages from `mlb_player_average_batting`.
  - Time-safe: use latest batter average row strictly before the game date.
  - Preferred batter rate: `avg_so_szn / avg_pa_szn` when `avg_pa_szn > 0`.
  - Fallback: `avg_so_l20 / avg_pa_l20`, then `avg_so_l10 / avg_pa_l10`, then neutral default `0.22`.
  - Use lineup-slot weights, not a simple unweighted average.

### New contact-profile features

- `projected_lineup_whiff_pct`
- `projected_lineup_chase_pct`
- `projected_lineup_contact_rate`

Use `mlb_player_average_statcast_batting`, time-safe latest row strictly before game date.

Fallbacks:
- whiff: season, then L10, then L5, then `0.22`
- chase: season, then L10, then L5, then `0.28`
- contact: `1.0 - whiff`

### New handedness-profile features

- `projected_lineup_same_hand_k_pct`
- `projected_lineup_opposite_hand_k_pct`
- `projected_lineup_hand_k_delta`

Definitions:
- same-hand means non-switch batter `bats == pitcher_throws`.
- opposite bucket includes opposite-side and switch hitters.
- if a bucket is empty, fall back to overall `projected_lineup_k_pct`.
- delta = same-hand K% minus opposite-hand K%.

### Top / middle / bottom lineup concentration features

- `projected_lineup_top3_k_pct`
- `projected_lineup_mid3_k_pct`
- `projected_lineup_bot3_k_pct`
- `projected_lineup_k_concentration`

Definitions:
- top: lineup positions 1–3
- middle: lineup positions 4–6
- bottom: lineup positions 7–9
- if a group is empty, fall back to overall `projected_lineup_k_pct`
- concentration = max(top3, mid3, bot3) - min(top3, mid3, bot3)

### Compatibility feature to preserve

- `pct_opp_lineup_same_hand`

Keep this feature available. It can be recomputed using the same lineup/handedness data.

## Features intentionally out of scope for now

Do not add these in Phase 3A:

- umpire features
- pitcher-specific umpire interactions
- hand-crafted pitch-mix interaction products
- battery history / catcher framing

Reason for deferring umpire features:
- `mlb_game_umpires` currently has only 4 rows, all from 2026-05-10.
- It does not currently provide called-strike tendency, zone expansion, or meaningful umpire K environment.
- Pitcher-specific umpire interactions would be too sparse even after assignment backfill.

Future umpire note:
- If umpire features become necessary, first backfill `mlb_game_umpires` over a much longer historical window and verify enough assignment history exists.
- Simple umpire assignment history might support an `umpire_k_rate_szn` feature later.
- Called-strike / zone features likely require a separate source beyond the current assignment scraper.

## Lineup slot weights

Use modest projected-PA-style weights by lineup position:

| Position | Weight |
|---:|---:|
| 1 | 1.12 |
| 2 | 1.09 |
| 3 | 1.06 |
| 4 | 1.03 |
| 5 | 1.00 |
| 6 | 0.97 |
| 7 | 0.94 |
| 8 | 0.91 |
| 9 | 0.88 |

Normalize by the sum of present weights.

## Data refresh already completed before implementation

The following data refresh steps were run before the Phase 3A implementation smoke check:

### Boxscore batting

Command:

```bash
venv/Scripts/python.exe -m src.scrapers.mlb.mlb_stats_scraper --season 2026 --boxscores-only
```

Result:
- completed successfully
- no missing games needed scraping

### Statcast batting/pitching

Initial command:

```bash
venv/Scripts/python.exe -m src.scrapers.mlb.mlb_statcast_scraper --backfill --start-date 2026-04-13 --end-date 2026-05-10
```

This hit the foreground timeout after completing through 2026-04-27. The remaining range was resumed as a tracked background job:

```bash
venv/Scripts/python.exe -m src.scrapers.mlb.mlb_statcast_scraper --backfill --start-date 2026-04-28 --end-date 2026-05-10
```

Result:
- completed successfully
- final chunk backfill totals: 3,587 batting rows and 1,484 pitching rows

### Regular batting averages

Command:

```bash
venv/Scripts/python.exe -m src.processing.mlb.mlb_populate_averages --table batting --season 2026
```

Result:
- completed successfully

### Statcast batting averages

Command:

```bash
venv/Scripts/python.exe -m src.processing.mlb.mlb_populate_statcast_averages --table batting --season 2026
```

Result:
- completed successfully
- inserted 12,191 Statcast batting average rows

### Sync affected remote tables to local

Command:

```bash
venv/Scripts/python.exe scripts/sync_local_db.py --tables mlb_player_game_stats_batting mlb_player_game_statcast_batting mlb_player_average_batting mlb_player_average_statcast_batting mlb_game_lineups
```

Result:
- completed successfully
- 24,093 rows synced incrementally

## Current implementation status

Phase 3A lineup/contact implementation has been applied to:

- `src/processing/mlb/mlb_matchup_features.py`
- `src/models/mlb/mlb_feature_store.py`

Validation after implementation:

```bash
venv/Scripts/python.exe -m py_compile src/processing/mlb/mlb_matchup_features.py src/models/mlb/mlb_feature_store.py
```

Result:
- passed

Runtime smoke check on local DB for 2026-05-10:

```python
from src.db.client import get_engine
from src.models.mlb.mlb_feature_store import MLBFeatureStore

cols = [
    "projected_lineup_k_pct",
    "projected_lineup_whiff_pct",
    "projected_lineup_chase_pct",
    "projected_lineup_contact_rate",
    "projected_lineup_same_hand_k_pct",
    "projected_lineup_opposite_hand_k_pct",
    "projected_lineup_hand_k_delta",
    "projected_lineup_top3_k_pct",
    "projected_lineup_mid3_k_pct",
    "projected_lineup_bot3_k_pct",
    "projected_lineup_k_concentration",
    "pct_opp_lineup_same_hand",
]

fs = MLBFeatureStore(get_engine(local=True))
df = fs.get_features_for_date("2026-05-10")
print("shape", df.shape)
print("missing", [c for c in cols if c not in df.columns])
print("single_valued", [c for c in cols if c in df.columns and df[c].nunique(dropna=True) <= 1])
print(df[cols].agg(["count", "nunique", "min", "max"]).T.to_string())
```

Result:
- shape: `(30, 95)`
- missing: `[]`
- single-valued: `[]`

Feature variation on 2026-05-10:

| Feature | Count | Unique | Min | Max |
|---|---:|---:|---:|---:|
| `projected_lineup_k_pct` | 30 | 22 | 0.163028 | 0.274716 |
| `projected_lineup_whiff_pct` | 30 | 22 | 0.144176 | 0.235036 |
| `projected_lineup_chase_pct` | 30 | 22 | 0.245699 | 0.375680 |
| `projected_lineup_contact_rate` | 30 | 22 | 0.764964 | 0.855824 |
| `projected_lineup_same_hand_k_pct` | 30 | 22 | 0.144350 | 0.276853 |
| `projected_lineup_opposite_hand_k_pct` | 30 | 22 | 0.155851 | 0.283019 |
| `projected_lineup_hand_k_delta` | 30 | 22 | -0.067127 | 0.111585 |
| `projected_lineup_top3_k_pct` | 30 | 22 | 0.117503 | 0.288685 |
| `projected_lineup_mid3_k_pct` | 30 | 22 | 0.125991 | 0.308178 |
| `projected_lineup_bot3_k_pct` | 30 | 22 | 0.167159 | 0.372162 |
| `projected_lineup_k_concentration` | 30 | 22 | 0.000000 | 0.178791 |
| `pct_opp_lineup_same_hand` | 30 | 8 | 0.111111 | 0.888889 |

## Important blocker discovered before retraining

Do not retrain yet.

A pre-retrain check showed that lineup table coverage currently exists only for 2026.

### Lineup table coverage query

Command:

```bash
venv/Scripts/python.exe - <<'PY'
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import os
load_dotenv(Path('.env'))
for name, url_key in [('REMOTE','DATABASE_URL'), ('LOCAL','LOCAL_DATABASE_URL')]:
    engine = create_engine(os.environ[url_key])
    print('\n==', name, '==')
    with engine.connect() as c:
        q = text('''
        SELECT EXTRACT(YEAR FROM game_date)::int AS season,
               COUNT(*) AS rows,
               COUNT(DISTINCT game_date) AS dates,
               COUNT(DISTINCT game_pk) AS games,
               MIN(game_date)::date AS min_date,
               MAX(game_date)::date AS max_date
        FROM mlb_game_lineups
        GROUP BY 1
        ORDER BY 1;
        ''')
        for row in c.execute(q):
            print(dict(row._mapping))
PY
```

Result:

REMOTE:
- season 2026 only
- 7,878 rows
- 29 dates
- 362 games
- min date 2026-04-15
- max date 2026-05-13

LOCAL:
- same as remote

### Actual training feature-path check

Command:

```bash
venv/Scripts/python.exe - <<'PY'
from src.db.client import get_engine
from src.models.mlb.mlb_feature_store import MLBFeatureStore
import pandas as pd

cols = [
    'projected_lineup_k_pct',
    'projected_lineup_whiff_pct',
    'projected_lineup_chase_pct',
    'projected_lineup_contact_rate',
    'projected_lineup_same_hand_k_pct',
    'projected_lineup_opposite_hand_k_pct',
    'projected_lineup_hand_k_delta',
    'projected_lineup_top3_k_pct',
    'projected_lineup_mid3_k_pct',
    'projected_lineup_bot3_k_pct',
    'projected_lineup_k_concentration',
    'pct_opp_lineup_same_hand',
]

def default_for(col):
    return {
        'projected_lineup_k_pct': 0.22,
        'projected_lineup_whiff_pct': 0.22,
        'projected_lineup_chase_pct': 0.28,
        'projected_lineup_contact_rate': 0.78,
        'projected_lineup_same_hand_k_pct': 0.22,
        'projected_lineup_opposite_hand_k_pct': 0.22,
        'projected_lineup_hand_k_delta': 0.0,
        'projected_lineup_top3_k_pct': 0.22,
        'projected_lineup_mid3_k_pct': 0.22,
        'projected_lineup_bot3_k_pct': 0.22,
        'projected_lineup_k_concentration': 0.0,
        'pct_opp_lineup_same_hand': 0.50,
    }[col]

fs = MLBFeatureStore(get_engine(local=True))
for season in [2024, 2025, 2026]:
    print('\n== season', season, '==')
    base = fs.get_training_dataset([season])
    enriched = fs.enrich_with_matchup_features(base)
    print('rows', len(enriched), 'date_range', enriched['game_date'].min(), enriched['game_date'].max())
    for c in cols:
        if c not in enriched.columns:
            print(c, 'MISSING')
            continue
        s = pd.to_numeric(enriched[c], errors='coerce')
        default = default_for(c)
        non_default = int((s.fillna(default).round(10) != default).sum())
        print(
            c,
            'nonnull', int(s.notna().sum()),
            'nunique', int(s.nunique(dropna=True)),
            'non_default', non_default,
            'min', float(s.min()) if s.notna().any() else None,
            'max', float(s.max()) if s.notna().any() else None,
        )
PY
```

Result summary:

2024:
- 4,850 rows
- every new lineup/contact feature has exactly 1 unique value
- every new lineup/contact feature has 0 non-default rows

2025:
- 4,850 rows
- every new lineup/contact feature has exactly 1 unique value
- every new lineup/contact feature has 0 non-default rows

2026:
- 1,190 rows
- new lineup/contact features have real variation
- most features have 662 unique values

Conclusion:
- Training on 2024–2025 right now would train on default-only lineup features.
- Calibration/backtest/inference on 2026 would then see real lineup features.
- This is train/serve distribution shift and would invalidate the experiment.

## Required next step before retraining

Backfill historical lineups for the training and calibration windows.

Required windows:

1. 2024 training season:
   - 2024-03-20 through 2024-09-30

2. 2025 training season:
   - 2025-03-18 through 2025-09-28

3. 2026 calibration/backtest gap:
   - 2026-03-25 through 2026-04-14

Existing `mlb_lineup_scraper` currently supports one date at a time, not a date range. Do not run a long ad-hoc shell loop. Add a small helper script instead.

## Proposed helper script

Add:

`scripts/mlb_backfill_lineups_range.py`

Expected behavior:
- arguments:
  - `--start-date YYYY-MM-DD`
  - `--end-date YYYY-MM-DD`
  - optional `--local`
  - optional `--dry-run`
  - optional `--sleep-seconds`, default modest delay
- loops over dates inclusively
- calls the existing lineup scraper logic for each date
- remote-first by default
- logs per-date success/failure counts
- does not invent new scraping logic beyond calling existing `MLBLineupScraper`

## Historical lineup backfill commands

After adding the helper script, run remote-first:

```bash
venv/Scripts/python.exe scripts/mlb_backfill_lineups_range.py --start-date 2024-03-20 --end-date 2024-09-30
```

```bash
venv/Scripts/python.exe scripts/mlb_backfill_lineups_range.py --start-date 2025-03-18 --end-date 2025-09-28
```

```bash
venv/Scripts/python.exe scripts/mlb_backfill_lineups_range.py --start-date 2026-03-25 --end-date 2026-04-14
```

Then sync lineups to local:

```bash
venv/Scripts/python.exe scripts/sync_local_db.py --tables mlb_game_lineups
```

## Post-backfill verification

### Verify lineup coverage by season

```bash
venv/Scripts/python.exe - <<'PY'
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import os
load_dotenv(Path('.env'))
engine = create_engine(os.environ['LOCAL_DATABASE_URL'])
with engine.connect() as c:
    q = text('''
    SELECT EXTRACT(YEAR FROM game_date)::int AS season,
           COUNT(*) AS rows,
           COUNT(DISTINCT game_date) AS dates,
           COUNT(DISTINCT game_pk) AS games,
           MIN(game_date)::date AS min_date,
           MAX(game_date)::date AS max_date
    FROM mlb_game_lineups
    GROUP BY 1
    ORDER BY 1;
    ''')
    for row in c.execute(q):
        print(dict(row._mapping))
PY
```

Expected:
- 2024 and 2025 should no longer be empty.
- 2026 should include 2026-03-25 through 2026-05-13 or later.

### Verify actual feature-path variation

```bash
venv/Scripts/python.exe - <<'PY'
from src.db.client import get_engine
from src.models.mlb.mlb_feature_store import MLBFeatureStore
import pandas as pd

cols = [
    'projected_lineup_k_pct',
    'projected_lineup_whiff_pct',
    'projected_lineup_chase_pct',
    'projected_lineup_contact_rate',
    'projected_lineup_same_hand_k_pct',
    'projected_lineup_opposite_hand_k_pct',
    'projected_lineup_hand_k_delta',
    'projected_lineup_top3_k_pct',
    'projected_lineup_mid3_k_pct',
    'projected_lineup_bot3_k_pct',
    'projected_lineup_k_concentration',
    'pct_opp_lineup_same_hand',
]

fs = MLBFeatureStore(get_engine(local=True))
for season in [2024, 2025, 2026]:
    print('\n== season', season, '==')
    df = fs.enrich_with_matchup_features(fs.get_training_dataset([season]))
    print('rows', len(df), 'date_range', df['game_date'].min(), df['game_date'].max())
    for c in cols:
        s = pd.to_numeric(df[c], errors='coerce')
        print(c, 'nonnull', int(s.notna().sum()), 'nunique', int(s.nunique(dropna=True)), 'min', float(s.min()), 'max', float(s.max()))
PY
```

Continue only if 2024 and 2025 lineup features have real variation, not 1 unique default value.

## Retrain command after historical lineup coverage is fixed

Do not run this until the post-backfill verification passes.

```bash
venv/Scripts/python.exe src/models/mlb/mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --n-simulations 10000 --output-dir src/models/mlb/artifacts
```

Notes:
- Keep calibration end fixed at 2026-04-12.
- Backtest starts 2026-04-13.
- This preserves clean no-overlap discipline.

## Inspect latest training artifact

After retraining:

```bash
venv/Scripts/python.exe - <<'PY'
from pathlib import Path
import json

runs = sorted(Path('src/models/mlb/artifacts').glob('mlb_run_*'), key=lambda p: p.stat().st_mtime)
run = runs[-1]
print('latest_run', run)

meta = json.loads((run / 'training_metadata.json').read_text())
print('feature_count', meta.get('feature_count'))
print('cal_end_date', meta.get('cal_end_date'))
print('train_rows', meta.get('train_rows'))
print('cal_rows', meta.get('cal_rows'))

manifest = json.loads((run / 'feature_manifest.json').read_text())
new_features = [
    'projected_lineup_whiff_pct',
    'projected_lineup_chase_pct',
    'projected_lineup_contact_rate',
    'projected_lineup_same_hand_k_pct',
    'projected_lineup_opposite_hand_k_pct',
    'projected_lineup_hand_k_delta',
    'projected_lineup_top3_k_pct',
    'projected_lineup_mid3_k_pct',
    'projected_lineup_bot3_k_pct',
    'projected_lineup_k_concentration',
]
selected = sorted(set(f for features in manifest.values() for f in features))
picked = [f for f in new_features if f in selected]
print('new_lineup_features_picked', len(picked), picked)
PY
```

If zero new lineup/contact features are picked, do not treat the retrain as a successful Phase 3A feature expansion.

## Backtest commands after retrain

Replace `<NEW_RUN_DIR>` with the latest artifact path printed above.

### Raw under-only

```bash
venv/Scripts/python.exe src/backtesting/mlb/run_mlb_sweep.py --local --start 2026-04-13 --end 2026-05-10 --stats pitcher_strikeouts --tau none --edge 0.02 0.05 0.08 0.10 0.12 0.15 --kelly 0.125 --z-max 0.25 --max-weight 0.50 --model-dir <NEW_RUN_DIR> --direction under --output-dir backtest_results/mlb_sweep_pitcher_k_phase3a_raw_under_20260413_20260510
```

### Focused BL under-only

```bash
venv/Scripts/python.exe src/backtesting/mlb/run_mlb_sweep.py --local --start 2026-04-13 --end 2026-05-10 --stats pitcher_strikeouts --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --kelly 0.125 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --model-dir <NEW_RUN_DIR> --direction under --output-dir backtest_results/mlb_sweep_pitcher_k_phase3a_bl_under_20260413_20260510
```

## Evaluation gates

Use only configs with 100+ bets for meaningful decisions.

Phase 3A is promising if it improves at least one of:
- ROI at 100+ bets
- Sharpe at 100+ bets
- Max drawdown at comparable volume
- profit at comparable volume

Phase 3A is not successful if:
- new lineup/contact features are not selected at all
- calibration fails
- under-only performance degrades materially versus Phase 2
- a result only looks good below 100 bets

## Current recommendation

Do not retrain yet.

First:
1. Add the historical lineup range backfill helper.
2. Backfill lineups for 2024, 2025, and the early 2026 gap.
3. Sync `mlb_game_lineups` to local.
4. Re-run the feature-path variation check.
5. Only then retrain and backtest.

If historical lineups cannot be backfilled, alternate options are:
- use Statcast/boxscore-derived actual batters as a training-time proxy for lineup features, or
- disable the new lineup/contact features for this retrain to avoid train/serve distribution shift.

Do not train on 2024–2025 default-only lineup features and evaluate on 2026 real lineup features.
