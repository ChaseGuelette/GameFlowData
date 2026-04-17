---
description: Run a full calibration health check — diagnostic drift, betting ROI, and comparison to training baseline
---

# Calibration Health Check

Run a comprehensive calibration diagnostic against recent production data, pull paper trading performance for the same window, compare against the training baseline, and summarize findings with a recommended action.

## Architecture Note — Token Efficiency

This command delegates data-gathering steps to **haiku subagents** to keep large SQL results and file contents out of the main Opus context. Only the summarized results come back for interpretation.

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

## Step 3: Pull Paper Trading Performance (DELEGATE TO SUBAGENT)

Use the **Task tool** with `subagent_type: "general-purpose"` and `model: "haiku"` to run the following three SQL queries via `mcp__supabase__execute_sql`. The subagent should return a concise summary of the results, not raw rows.

**Prompt for the subagent:**

> Run these 3 SQL queries using `mcp__supabase__execute_sql` and return a concise summary of the results.
>
> Query 1 — Daily P&L summary:
> ```sql
> SELECT game_date, total_bets, bets_won, bets_lost, bets_push,
>        round(total_pnl::numeric, 2) as daily_pnl,
>        round(cumulative_pnl::numeric, 2) as cumulative_pnl,
>        round(roi_pct::numeric, 2) as roi_pct
> FROM paper_trading_daily_log
> WHERE game_date BETWEEN '{START_DATE}' AND '{END_DATE}'
> ORDER BY game_date;
> ```
>
> Query 2 — Bet-level breakdown by stat and direction:
> ```sql
> SELECT stat_type, bet_direction,
>        COUNT(*) as total_bets,
>        SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,
>        ROUND(100.0 * SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
>        ROUND(SUM(pnl)::numeric, 2) as total_pnl,
>        ROUND(100.0 * SUM(pnl) / SUM(ABS(stake))::numeric, 1) as roi_pct
> FROM paper_bets
> WHERE game_date BETWEEN '{START_DATE}' AND '{END_DATE}'
>   AND status IN ('won', 'lost', 'push')
> GROUP BY stat_type, bet_direction
> ORDER BY stat_type, bet_direction;
> ```
>
> Query 3 — Overall summary:
> ```sql
> SELECT COUNT(*) as total_bets,
>        SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) as wins,
>        ROUND(100.0 * SUM(CASE WHEN status = 'won' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
>        ROUND(SUM(pnl)::numeric, 2) as total_pnl,
>        ROUND(SUM(ABS(stake))::numeric, 2) as total_wagered,
>        ROUND(100.0 * SUM(pnl) / SUM(ABS(stake))::numeric, 1) as roi_pct
> FROM paper_bets
> WHERE game_date BETWEEN '{START_DATE}' AND '{END_DATE}'
>   AND status IN ('won', 'lost', 'push');
> ```
>
> Format your response as:
> - **Overall**: X bets, Y% win, $Z PnL, W% ROI
> - **By stat/direction**: table with stat, direction, bets, win%, PnL, ROI
> - **Daily trend**: any notable patterns (streaks, acceleration/deceleration)

## Step 4: Load Training Baseline (DELEGATE TO SUBAGENT)

Use the **Task tool** with `subagent_type: "general-purpose"` and `model: "haiku"` to read these files and return a summary:

**Prompt for the subagent:**

> Read these 3 files and return a concise summary:
> 1. `src/models/artifacts/production/calibration_report.json` — report the per-stat, per-quantile coverage values
> 2. `src/models/artifacts/production/run_config.json` — report the run ID, training date, and key hyperparameters
> 3. `src/models/artifacts/production/.source` — report the run ID string
>
> Also read `C:\Users\Chase\.claude\projects\C--Users-Chase-Projects-GameFlowData\memory\calibration_log.md` if it exists — summarize the last 2 entries.

**Note:** Steps 3 and 4 can run in **parallel** as Task tool calls since they're independent.

## Step 5: Present Comparison Summary

Using the summarized results from Steps 2-4, format the results as a clear comparison report:

### A. Model Info
- Production run ID, model age in days, training seasons

### B. Quantile Coverage Drift Table
Show training baseline vs current production for each stat and quantile. Flag any quantile where drift exceeds 5 percentage points.

### C. Bias Summary
Show mean bias % for each stat. Flag if >4%.

### D. Betting ROI Summary
Show the stat/direction breakdown table from Step 3, plus overall ROI and total P&L.

### E. Diagnosis & Recommended Action

Based on the combined evidence, provide ONE of these recommendations:

1. **HOLD** — Drift is present but betting ROI is healthy. No action needed.
2. **MONITOR** — Drift is increasing or ROI is declining. Check again in a few days.
3. **RECALIBRATE** — Both drift AND ROI degradation suggest recalibration is needed. But ALWAYS validate with backtests first.
4. **RETRAIN** — Fundamental model degradation. Proceed with extreme caution.

## Step 6: Update Calibration Log

After presenting results, ask the user if they want to update the calibration log in memory. If yes, append a dated entry to `C:\Users\Chase\.claude\projects\C--Users-Chase-Projects-GameFlowData\memory\calibration_log.md`.

## Cleanup

Delete the `calibration_check_{TODAY}.json` file after the analysis is complete.
