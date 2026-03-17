# Plan: Minimum-Minutes Filter for Rolling Average Calculations

**Created:** 2026-03-16
**Status:** TODO
**Priority:** Medium — model still performing well overall, but this is a known edge case

---

## Context

Leonard Miller's REB prediction for 3/16 exposed a structural weakness: a **1-minute game on 3/8** (likely an injury exit) is included in his L5 rolling averages, dragging `avg_reb_l5` from 8.0 to 6.4 and inflating `std_reb_l5` to 4.67. This produces a wildly wrong prediction (median 3.82 REB, 78% under probability) for a player who's been averaging 9.25 REB in his last 4 starter games. The market prices the Over at -130 (56.5% implied), a 35-point disagreement with the model.

**Root cause:** The rolling average pipeline treats all games equally regardless of minutes played. A 1-minute injury exit counts the same as a 36-minute start.

**Goal:** Filter out garbage-time/DNP-equivalent games (< 5 minutes) from performance rolling averages while preserving schedule features (rest_days, B2B, games_last_7d) computed from all games.

---

## Investigation Summary (Miller, 3/16)

### His Last 10 Games

| Date | MIN | REB | PTS | Role |
|------|-----|-----|-----|------|
| 3/13 | **36** | **8** | 14 | Starter |
| 3/12 | **31** | **9** | 15 | Starter |
| 3/10 | **38** | **11** | 17 | Starter |
| **3/8** | **1** | **0** | 0 | **DNP/Exit** |
| 3/5 | 24 | **9** | 8 | Starter |
| 3/3 | 21 | 3 | 8 | Rotation |
| 3/1 | 27 | 5 | 15 | Rotation |
| 2/26 | 23 | 3 | 11 | Rotation |
| 2/24 | 5 | 2 | 5 | Bench |
| 2/22 | 0 | 0 | 0 | DNP |

Before March: 0, 3, 0, 5, 7, 0, 3 minutes — deep end-of-bench player.

### What the Model Sees vs Reality

| Feature | Model Value | Without 1-min game | Impact |
|---------|-------------|---------------------|--------|
| avg_reb_l5 | **6.4** | **8.0** | -1.6 REB |
| avg_reb_l15 | 3.13 | ~3.5 | Slight improvement |
| avg_reb_szn | 2.19 | ~2.3 | Minimal |
| std_reb_l5 | **4.67** | **~3.3** | Variance over-inflated |
| min_floor_l5 | **1.0** | **~21** | Model thinks he could play 1 min again |
| avg_min_l5 | 23.0 | ~28.5 | Understating his current role |

### Model Prediction vs Market

| Metric | Model | Market |
|--------|-------|--------|
| P(Under 6.5) | **78.2%** | ~47.6% (Under -110) |
| P(Over 6.5) | 21.8% | **56.5%** (Over -130) |
| Disagreement | **~35 percentage points** | Market favors OVER |
| BL edge | +12.4% under | — |
| BL confidence | 0.745 | — |

### Injury Context

- **Bulls OUT (5):** Simons, Okoro, Ivey, Essengue, Collins (83 combined minutes)
- **Memphis OUT (6):** Also heavily depleted
- Miller is locked into 30+ minute starter role due to injuries
- 5 teammates out = more rebounding opportunities (fewer bodies competing for boards)

### Verdict on the Bet

**Do NOT take the under.** The model is confidently wrong here due to:
1. The 1-min outlier game contaminating L5 stats
2. Season averages (2.19 REB, 8.72 min) reflecting his old bench role, not his current starter role
3. The market (sharper than the model in regime-change scenarios) pricing Over -130
4. When Miller plays 20+ min, his last 4 rebound games: 8, 9, 11, 9 — all over 6.5

---

## Approach: NaN-Masking Before Rolling Calculations

Instead of removing rows (which would break schedule features), **mask stat values to NaN** for games where the player played < `MIN_MINUTES_FOR_STATS` (5) minutes. Pandas rolling automatically skips NaN values, so the averages/stds only reflect "real" games.

**Why this approach:**
- Schedule features (rest_days, games_last_7d, is_back_to_back) remain computed from ALL games — a player who played 1 minute yesterday is still physically on a back-to-back
- Performance features (avg_reb_l5, std_reb_l5, etc.) only reflect games with meaningful playing time
- No rows are deleted — `player_average_game_stats` table still has a row for every game
- Feature store needs zero changes (it just reads the pre-computed values)
- No model retraining needed — feature names stay identical, values are just cleaner for edge cases

---

## Files to Modify

### 1. `src/processing/populate_average_stats_incremental.py`

**Add constant** (after line 57):
```python
MIN_MINUTES_FOR_STATS = 5  # Exclude games < 5 min from performance rolling averages
```

**Modify `calculate_basic_rolling_for_player()`** (lines 243-308):

Before the rolling average loops, create a minutes mask based on shifted values (prior games):
```python
min_mask = player_df["min"].shift(1) >= MIN_MINUTES_FOR_STATS
```

Then in each rolling calculation, apply the mask:

- **Rolling averages** (lines 254-263): `shifted = player_df[stat].shift(1).where(min_mask)`
- **L3 averages** (lines 266-270): `shifted = player_df[stat].shift(1).where(min_mask)`
- **L5 standard deviations** (lines 272-277): `shifted = player_df[stat].shift(1).where(min_mask)`
- **Minutes floor** (lines 280-282): `shifted_min = player_df["min"].shift(1).where(min_mask)`
- **Games started** (lines 285-288): `shifted_starter = is_starter.shift(1).where(min_mask)`

**Leave untouched** (lines 290-306): rest_days, games_last_7d — these are schedule features computed from all games.

### 2. `src/processing/populate_average_stats.py`

**Add constant** (after line 40):
```python
MIN_MINUTES_FOR_STATS = 5
```

**Modify `calculate_player_basic_averages()`** (lines 208-243):

Create a grouped minutes mask:
```python
min_mask = df.groupby(group_cols)["min"].shift(1) >= MIN_MINUTES_FOR_STATS
```

Apply to each shifted stat before rolling:
```python
shifted = df.groupby(group_cols)[stat].shift(1).where(min_mask)
```

**Modify `calculate_b2_b3_b4_features()`** (lines 271-319):

Apply same mask to:
- L3 rolling averages (line 288)
- L5 standard deviations (line 293)
- Minutes floor (line 297)
- Games started (line 302)

**Leave untouched** (lines 306-316): rest_days, games_last_7d.

### 3. `tests/test_populate_average_stats.py`

Add test case: `test_low_minutes_games_excluded_from_rolling()` — create a player with a 1-minute game in L5, verify:
- avg_reb_l5 excludes the 1-min game's stats
- std_reb_l5 excludes the 1-min game's stats
- rest_days INCLUDES the 1-min game (schedule feature)
- games_last_7d INCLUDES the 1-min game

---

## Impact Analysis

**For most players (95%+):** Zero impact. Regular rotation players never have < 5 min games in their L5 window.

**For regime-change players like Miller:** Dramatic improvement:
- avg_reb_l5: 6.4 → ~8.0
- std_reb_l5: 4.67 → ~3.3
- min_floor_l5: 1.0 → ~21.0
- Predicted median likely shifts from 3.82 → ~6-7 range

**Risk:** Model was trained with unfiltered rolling averages. Changing input distribution could theoretically degrade predictions. However:
- XGBoost is tree-based and handles minor input shifts well
- The change only affects edge cases (sub-5-min games in L5 window)
- For those edge cases, current predictions are clearly wrong

---

## Execution Steps

1. Implement the NaN-masking changes in both scripts
2. Add the test case
3. Run `pytest tests/test_populate_average_stats.py` to verify
4. Run full backfill: `python -m src.processing.populate_average_stats --season 2025-26`
5. Run incremental for today: `python -m src.processing.populate_average_stats_incremental --date 2026-03-16`
6. Query Miller's updated rolling averages to verify the fix
7. Run a quick backtest on the last 30 days to compare ROI with old vs new averages
8. If ROI is neutral or better, deploy to Railway

---

## Verification

1. **Unit test:** New test case passes, existing tests still pass
2. **Spot check Miller:** `SELECT avg_reb_l5, std_reb_l5, min_floor_l5 FROM player_average_game_stats WHERE player_id = 1631159 AND game_date = '2026-03-13'` — verify avg_reb_l5 is ~8.0, not 6.4
3. **Backtest comparison:** Run `run_backtest.py` for last 30 days with old vs new averages, compare ROI
4. **Python tests:** `pytest tests/ -x` — all 693+ tests pass
5. **Ruff:** `ruff check src/processing/populate_average_stats*.py`
