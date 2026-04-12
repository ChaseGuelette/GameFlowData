# Plan: Batter H+R+RBI (HRR) Combined Model for Kalshi

## Overview

Build a `batter_hrr` model that predicts a batter's combined **hits + runs scored + RBIs**
in a single game, targeting Kalshi's H+R+RBI player prop markets.

This is the single highest-leverage MLB volume improvement available — potentially adding
80-120 batter candidates per day alongside the existing ~30-50 from hits and strikeouts.

---

## Why HRR Should Work When Runs Alone Didn't

This is the core question. Here's the analysis:

**Why standalone runs failed:**
- A batter averages ~0.8 runs/game (very low count, huge noise-to-signal ratio)
- Runs scored are passive — you need teammates to bat you in
- A player can go 2-for-3 with a double and score 0 runs (no one behind him hit)
- The Kalshi line for a runs-only market would be 0 or 1 — a coin flip with no real edge

**Why HRR is structurally different:**
- HRR averages ~1.8-2.8/game for a typical lineup batter
- **Hits are the dominant component** — they're predictable and our model is strong
- The three stats are positively correlated: a player who gets hits also tends to score
  (he got on base) and tend to drive in runs (he made contact, often when runners were on)
- At higher prop lines (2+, 3+), the prop is essentially asking "does this batter have
  a good offensive game?" — a much cleaner signal than any single component
- The combining effect reduces the noise from any single stat's randomness

**The structural argument:** You're not asking the model to predict runs in isolation.
You're asking it to predict "total offensive contribution," where hits carry ~50% of the
weight. The hit prediction is your edge — runs and RBIs come along for the ride.

**Evidence from the existing data:** The feature store (`mlb_batter_feature_store.py`)
already tracks:
- `batter_avg_r_l5`, `batter_avg_rbi_l5`, `batter_avg_h_l5` — rolling averages for all three
- `batter_std_r_l5`, `batter_std_rbi_l5` — variance for both
- `lineup_position` — the most important HRR feature (leadoff hitters score runs, cleanup hitters drive in runs)
- `park_runs_factor` — already computed

**No new feature engineering needed.** The feature set for HRR is essentially
`BATTER_RBIS_FEATURES` + `park_hits_factor` + the prop line for HRR once we have markets.

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Model doesn't outperform Kalshi's implied price | Medium | Backtest over 2024-2025 seasons before deploying. Require positive ROI in backtest. |
| Runs component adds too much noise | Medium | The combination dampens noise — monitor ECE by component in backtest |
| Kalshi H+R+RBI markets have low liquidity (vol < 20) | Medium | **Verify liquidity before building model.** No point training if markets don't exist. |
| Prop line from sportsbooks unavailable for BL blending | High (expected) | Use Kalshi mid-price as market prior — same fallback already in edge calculator |
| Correlation between hits/runs/RBIs causes model over-confidence | Low | NegBin naturally handles overdispersion; validate calibration before live |

**Biggest pre-condition: Verify the Kalshi H+R+RBI markets exist and have vol > 20 before
starting model work.** This is step 0.

---

## Step 0: Verify Kalshi Markets Exist (Before Any Code)

Run against the live API (requires Kalshi credentials):

```bash
python -m src.scrapers.kalshi.kalshi_market_scraper --sport mlb --dry-run
```

Look for any series that includes hits+runs+RBIs. Common Kalshi ticker patterns:
- `KXMLBHRR` — likely ticker for hits+runs+RBIs
- `KXMLBHRRBI` — alternative

If markets exist with vol > 20 consistently, proceed. If they don't exist or are illiquid,
**stop here** — the model isn't worth building without a market to bet into.

Also check: what is the typical line? If Kalshi sets lines at 1 only (binary near-certain),
that's low-value. Lines at 2+ with real uncertainty are the target.

---

## Implementation Steps

### Step 1: Add `batter_hrr` to the model config

**File: `src/models/mlb/mlb_stat_config.py`**

Add to `MLB_STATS`:
```python
"batter_hrr": {"model_type": "negbin", "edge_threshold": 0.10},
```

Notes:
- NegBin is correct — HRR is an overdispersed count (same as RBIs)
- Edge threshold 0.10 (10%) rather than 0.12 because this is a noisier stat
  and the market is less efficient (Kalshi-only, no sportsbook consensus)
- No `BL_CONFIG` entry needed initially — it will use `DEFAULT_BL_CONFIG`
  (tau=0.5, z_max=1.0, max_weight=0.50), which is the conservative fallback

### Step 2: Add feature list and target column to the feature store

**File: `src/models/mlb/mlb_batter_feature_store.py`**

Add to `BATTER_STAT_MARKET_KEY`:
```python
"hrr": "batter_hrr",
```

Add to `BATTER_STAT_TARGET`:
```python
"hrr": None,  # computed as h + r + rbi, not a direct column
```

Add feature list after `BATTER_RUNS_FEATURES`:
```python
BATTER_HRR_FEATURES: list[str] = BATTER_BASE_FEATURES + [
    "park_hits_factor", "park_runs_factor", "prop_line_batter_hrr",
]
```

Add to `BATTER_FEATURE_MAP`:
```python
"hrr": BATTER_HRR_FEATURES,
```

The `prop_line_batter_hrr` feature won't exist for historical training data (no sportsbook
offered it), so the feature will be NaN for all training rows. This is fine — the model
will simply not use it during training. Once Kalshi data is available, we can revisit.

### Step 3: Add target computation in feature store's training data query

**File: `src/models/mlb/mlb_batter_feature_store.py`** (wherever `get_training_data` is)

When building the training DataFrame for `hrr`, compute:
```python
df["target"] = df["h"] + df["r"] + df["rbi"]
```

This is done the same way as other stats: query `mlb_player_game_stats_batting` and build
the target from the raw columns. The batting stats table already has all three columns.

### Step 4: Add to MLB paper trader resolution mapping

**File: `src/paper_trading/mlb_paper_trader.py`**

Add to `MLB_STAT_RESOLUTION`:
```python
"batter_hrr": ("mlb_player_game_stats_batting", "h_plus_r_plus_rbi"),
```

Wait — the table doesn't have a `h_plus_r_plus_rbi` column. This needs special handling.
The resolution logic in `mlb_paper_trader.py` will need a multi-column sum for HRR,
similar to how the NBA paper trader handles `pra` (pts + reb + ast).

Two options:
1. Add a computed column to `mlb_player_game_stats_batting` via migration
2. Handle it in the resolution code as a special case

**Recommended: option 2 (special case in code).** Add to `_fetch_actuals` in the paper
trader: if `stat_type == "batter_hrr"`, query `h + r + rbi` directly.

### Step 5: Add Kalshi series to utils

**File: `src/scrapers/kalshi/kalshi_utils.py`**

Once you know the real Kalshi series ticker (from Step 0), add:

```python
# KALSHI_STAT_MAP
"HRR": "batter_hrr",  # (if that's Kalshi's ticker key)

# KALSHI_PROP_SERIES["mlb"]
"KXMLBHRR": "batter_hrr",  # replace KXMLBHRR with the real series ticker
```

### Step 6: Add to Kalshi paper trader supported stats

**File: `src/paper_trading/kalshi_paper_trader.py`**

```python
SUPPORTED_STATS: dict[str, set[str]] = {
    ...
    "mlb": {"pitcher_strikeouts", "pitcher_outs", "batter_hits", "batter_rbis", "batter_hrr"},
}
```

---

## Training the Model

The HRR model trains using the exact same pipeline as `batter_rbis`.

```bash
# Train the batter_hrr model
python src/models/mlb/train_pipeline.py --stats batter_hrr --model-type negbin
```

(Adjust command to match your actual train CLI; the point is `batter_hrr` slots into
the existing MLBModelSuite discovery and `from_directory` logic automatically.)

**Training data:** Use full 2022-2025 season data. HRR doesn't have a structural break —
the relationship between hitting quality and H+R+RBI has been consistent.

**Expected model quality:**
- Target mean: ~1.8-2.2 H+R+RBI per game for typical lineup batters
- Variance: higher than hits alone (expected with combo stat)
- ECE target: < 0.06 (same threshold as all other stats)

---

## Backtesting Before Deploying

**Do not deploy until backtesting confirms ROI > 0 at 10% edge threshold.**

Run the MLB backtest harness:

```bash
python src/backtesting/mlb/mlb_backtest_harness.py --stat batter_hrr --seasons 2024 2025
```

You'll need to add `"batter_hrr"` to the backtest harness's resolution map first:
```python
# mlb_backtest_harness.py
"batter_hrr": ("mlb_player_game_stats_batting", None),  # special case: h + r + rbi
```

Key things to look for in backtest:
1. **ROI > 0% at 10% edge threshold** — minimum bar to deploy
2. **Z-score > 2 (LIKELY EDGE)** — statistical significance
3. **ECE < 0.06** — calibration is good (not just lucky)
4. **No blowup in specific lineup positions** — check if leadoff (pos 1-2) vs.
   run-producing (pos 3-5) slots have very different calibration

---

## Monitoring After Deployment

Add `batter_hrr` to the Discord calibration report's by-stat breakdown. The `compute_kalshi_analysis()` function in `src/paper_trading/kalshi_analysis.py` already groups by `stat_type` automatically — no changes needed.

Watch for:
- **Win rate vs break-even over first 30 bets** — HRR should settle near the overall model rate
- **Prop line distribution** — if Kalshi only offers line=1 (near-certainty), the market
  is not priceable and the model won't find edges worth taking

---

## Summary of Files Changed

| File | Change |
|------|--------|
| `src/models/mlb/mlb_stat_config.py` | Add `batter_hrr` NegBin config |
| `src/models/mlb/mlb_batter_feature_store.py` | Add `hrr` target, feature list, feature map entry |
| `src/paper_trading/mlb_paper_trader.py` | Add HRR resolution (h+r+rbi special case) |
| `src/paper_trading/kalshi_paper_trader.py` | Add `batter_hrr` to `SUPPORTED_STATS["mlb"]` |
| `src/scrapers/kalshi/kalshi_utils.py` | Add HRR Kalshi series (after Step 0 confirms ticker) |
| `src/backtesting/mlb/mlb_backtest_harness.py` | Add HRR to resolution map |

**No database migrations needed.** All required columns (`h`, `r`, `rbi`) already exist
in `mlb_player_game_stats_batting`.

---

## Go/No-Go Gates

```
Step 0  → Verify Kalshi HRR markets exist with vol > 20  ← STOP if no
Step 3  → Train model, check ECE < 0.06                  ← STOP if no
Step 4  → Backtest ROI > 0%, Z > 1.5 on 2024-2025        ← STOP if no
Step 5  → Deploy to paper trader, monitor 30 bets         ← STOP if win rate < break-even
Step 6  → Live trading gate (same as other stats)
```

---

## What This Will NOT Solve

- It will not restore the 327-bet opening-week volume. That was a market inefficiency
  that no longer exists.
- It will not get above ~80 bets/day without also lowering the edge threshold below 15%
  (a separate decision with its own risk profile).
- If Kalshi's HRR markets have thin liquidity (vol < 20), spread > 15, or only offer
  line=1, the model can't bet into them regardless of edge.
