---
description: Run a full calibration health check — diagnostic drift, betting ROI, and comparison to training baseline
---

# Calibration Health Check

Run a comprehensive calibration diagnostic against recent production data, pull paper trading performance for the same window, compare against the training baseline, and summarize findings with a recommended action.

## Step 1: Determine Date Window

Calculate the date window: **last 14 days** ending yesterday (today's games may be incomplete).

```
END_DATE   = yesterday (YYYY-MM-DD)
START_DATE = END_DATE minus 13 days
```

## Step 2: Run Calibration Diagnostic

Run the per-stat calibration diagnostic against production DB data and save JSON output:

```bash
python -m src.diagnostics.calibration_per_stat --db --start {START_DATE} --end {END_DATE} --output calibration_check_{TODAY}.json
```

Where `{TODAY}` is today's date in `YYYYMMDD` format (no dashes).

Display the console output to the user as it runs.

## Step 3: Pull Paper Trading Performance

Query Supabase for paper trading results in the same date window. Use the `mcp__supabase__execute_sql` tool:

**Daily P&L summary** (columns: `bets_won`, `bets_lost`, `bets_push`, `total_pnl`, `cumulative_pnl`, `roi_pct`):
```sql
SELECT game_date, total_bets, bets_won, bets_lost, bets_push,
       round(total_pnl::numeric, 2) as daily_pnl,
       round(cumulative_pnl::numeric, 2) as cumulative_pnl,
       round(roi_pct::numeric, 2) as roi_pct
FROM paper_trading_daily_log
WHERE game_date BETWEEN '{START_DATE}' AND '{END_DATE}'
ORDER BY game_date;
```

**Bet-level breakdown by stat and direction** (column: `status` not `result`, values: `'won'`, `'lost'`, `'push'`):
```sql
SELECT stat_type,
       bet_direction,
       COUNT(*) as total_bets,
       SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,
       ROUND(100.0 * SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
       ROUND(SUM(pnl)::numeric, 2) as total_pnl,
       ROUND(100.0 * SUM(pnl) / SUM(ABS(stake))::numeric, 1) as roi_pct
FROM paper_bets
WHERE game_date BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND status IN ('won', 'lost', 'push')
GROUP BY stat_type, bet_direction
ORDER BY stat_type, bet_direction;
```

**Overall summary:**
```sql
SELECT COUNT(*) as total_bets,
       SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,
       ROUND(100.0 * SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
       ROUND(SUM(pnl)::numeric, 2) as total_pnl,
       ROUND(SUM(ABS(stake))::numeric, 2) as total_wagered,
       ROUND(100.0 * SUM(pnl) / SUM(ABS(stake))::numeric, 1) as roi_pct
FROM paper_bets
WHERE game_date BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND status IN ('won', 'lost', 'push');
```

## Step 4: Load Training Baseline

Read the production model's training-time calibration report:

```
src/models/artifacts/production/calibration_report.json
```

Also read the production model config to get model age and training details:

```
src/models/artifacts/production/run_config.json
```

And read the `.source` file to get the run ID:

```
src/models/artifacts/production/.source
```

## Step 5: Check Prior History

Read the calibration log from memory for context on prior checks and known structural issues:

```
memory/calibration_log.md
```

(Located in the auto-memory directory at `C:\Users\Chase\.claude\projects\C--Users-Chase-Projects-GameFlowData\memory\calibration_log.md`)

If this file doesn't exist yet, note that and still proceed. Key known issues to keep in mind:
- **AST Q10 combined gap (+7-10%) is STRUCTURAL** — ~18% of games have 0 assists, creating a floor on Q10 coverage
- **Better calibration numbers do NOT equal better edges** — always cross-reference with betting ROI
- **Combined conformal recalibration improved metrics but HURT betting ROI** — don't recommend deploying recalibration offsets unless ROI is also suffering

## Step 6: Present Comparison Summary

Format the results as a clear comparison report with these sections:

### A. Model Info
- Production run ID, model age in days, training seasons

### B. Quantile Coverage Drift Table
Show training baseline vs current production for each stat and quantile:

```
QUANTILE COVERAGE: Training → Production (Drift)
─────────────────────────────────────────────────────
         Q10           Q25           Q50           Q75           Q90
PTS   0.100→0.144  0.250→0.263  0.500→0.512  0.750→0.761  0.900→0.908
      (+4.4pp)      (+1.3pp)      (+1.2pp)      (+1.1pp)      (+0.8pp)
...
```

Flag any quantile where drift exceeds 5 percentage points with a warning marker.

### C. Bias Summary
Show mean bias % for each stat. Flag if >5%.

### D. Betting ROI Summary
Show the stat/direction breakdown table from Step 3, plus overall ROI and total P&L.

### E. Diagnosis & Recommended Action

Based on the combined evidence, provide ONE of these recommendations:

1. **HOLD** — Drift is present but betting ROI is healthy. No action needed.
2. **MONITOR** — Drift is increasing or ROI is declining. Check again in a few days.
3. **RECALIBRATE** — Both drift AND ROI degradation suggest recalibration is needed. But ALWAYS validate with backtests first (never deploy calibration changes based on metrics alone).
4. **RETRAIN** — Fundamental model degradation. But note: full retrains have historically hurt performance, so proceed with extreme caution.

Include specific reasoning referencing both the calibration metrics AND the betting performance.

## Step 7: Update Calibration Log

After presenting results, ask the user if they want to update the calibration log in memory. If yes, append a dated entry to `memory/calibration_log.md` with:
- Date, model run ID, model age
- Key drift numbers (Q10 coverage gaps for PTS/REB/AST)
- Betting ROI summary
- Recommendation made

## Cleanup

Delete the `calibration_check_{TODAY}.json` file after the analysis is complete (it was only needed for structured parsing during the check).
