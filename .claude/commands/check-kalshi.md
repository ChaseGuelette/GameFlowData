---
description: Run a full Kalshi bot health check — go-live readiness, cross-sectional consistency, weekly comparison, overflow analysis, and scale-up verdict
---

# Kalshi Bot Health Check

Run the comprehensive Kalshi paper trading analysis and produce an interpreted go-live / scale-up report.

## Step 1: Determine Date Window

Use today's date and a 30-day lookback unless the user specified a different range:

```
END_DATE   = yesterday  (today's bets may not be resolved yet)
START_DATE = END_DATE minus 29 days  (30-day window)
```

Default split date for before/after NO-only analysis: **2026-04-11**
(Update this if a new deployment happens.)

## Step 2: Run the Analysis Script

Run the full analysis with:

```bash
python scripts/analyze_kalshi_paper_bets.py \
  --date-start {START_DATE} \
  --date-end {END_DATE} \
  --split-date 2026-04-11
```

Show the full console output to the user.

Also run a short 7-day window for recent trend:

```bash
python scripts/analyze_kalshi_paper_bets.py --days 7 --no-split
```

Show this output too, labeled as "LAST 7 DAYS".

## Step 3: Query Live Bankroll from DB

Pull current bankroll and cumulative P&L from the daily log:

```sql
SELECT game_date,
       total_bets,
       bets_won,
       bets_lost,
       round(total_pnl::numeric, 2)       AS daily_pnl,
       round(cumulative_pnl::numeric, 2)  AS cumulative_pnl,
       round(bankroll_after::numeric, 2)  AS bankroll
FROM kalshi_paper_trading_daily_log
ORDER BY game_date DESC
LIMIT 14;
```

Use `mcp__supabase__execute_sql` to run this query.

## Step 4: Check Live Trader Circuit Breaker Status

Query for any active circuit breaker trips:

```sql
SELECT *
FROM kalshi_live_trading_log
WHERE created_at >= NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC
LIMIT 20;
```

If the table doesn't exist (live trading not yet enabled), note that and skip.

Also check if live trading is currently enabled:

```sql
SELECT setting_value, updated_at
FROM kalshi_settings
WHERE setting_key = 'live_trading_enabled';
```

If this table doesn't exist, note that live trading is not yet enabled.

## Step 5: Interpret and Summarize

After running the script and queries, provide a structured interpretation:

### A. Current Status

State clearly:
- Is the bot **GO LIVE**, **MONITOR**, or **NOT READY**? (from the script's verdict section)
- Current bankroll and cumulative P&L
- Days of data in the analysis window
- Whether live trading is currently enabled

### B. Cross-Sectional Consistency

Report whether all stat types are profitable. This is the **strongest evidence of real edge** because random luck rarely produces positive returns across 4-5 uncorrelated sports/stat combinations simultaneously.

- List each stat: win%, P&L, ROI
- Flag any stat that has gone negative since the last check

### C. Weekly Trend

Compare last 2 weeks:
- Is ROI trending up, flat, or down?
- Is win rate stable?
- Are we above or below the 8% ROI threshold each week?

### D. Z-Score Summary

Report both:
- **Real bets Z-score**: what actually would execute with live money
- **Combined Z-score**: real + overflow (the true underlying edge signal)

Explain the difference: real-only Z is limited by the $80/day exposure cap, not the edge itself.

### E. Overflow Analysis

State:
- How much edge is being lost to the exposure cap (% of total P&L)
- At current $300 live bankroll with 90% exposure (~$270/day), how much more would be captured vs the paper cap

### F. Scale-Up Recommendation

Based on startup playbook thresholds:
- **2 weeks at ROI > 8%** → increase to $500
- **4 weeks at ROI > 8%** → increase to $1,000

State whether we've hit either milestone.

### G. Action Items

List 1-3 specific next steps. Examples:
- "All checks pass — fund the Kalshi account and set `KALSHI_LIVE_TRADING_ENABLED=true`"
- "ROI is healthy but week-over-week is decelerating — monitor for 3 more days before going live"
- "batter_rbis went negative — investigate whether it's noise (< 15 bets) or structural"

## Step 6: Update Memory (Optional)

Ask the user if they want to record this check in the calibration/monitoring log at:

```
C:\Users\Chase\.claude\projects\C--Users-Chase-Projects-GameFlowData\memory\MEMORY.md
```

If yes, update the "Latest Check" entry in the MEMORY.md Kalshi section with:
- Date, verdict, window
- Overall: bets, win%, PnL, ROI
- Cross-sectional: all stats profitable Y/N, any failing stats
- Z-scores (real and combined)
- Next check date

## Reference: Key Thresholds

| Metric | Threshold | Note |
|--------|-----------|------|
| ROI (go-live) | > 8% | Real bets, after NO-only deployment |
| Z-score (real) | > 3 | Strong edge vs break-even |
| Z-score (combined) | > 5 | Extremely significant |
| All stats profitable | Yes | Cross-sectional consistency |
| Bankroll growth | $100 → $731 | Paper baseline (first 13 days) |
| Scale-up 1 | 2 weeks at ROI > 8% | → $500 bankroll |
| Scale-up 2 | 4 weeks at ROI > 8% | → $1,000 bankroll |

## Important Invariants

- **NO-only mode** is permanent until explicitly reversed — `KALSHI_ALLOW_YES_BETS=false`
- **Real Z-score below 3** is expected and NOT a problem — it's caused by the $80 exposure cap limiting sample size, not a weak edge
- **Overflow Z-score = 5.11** is the true signal — the same algorithm, just more volume
- Do NOT recommend re-enabling YES bets unless there is a specific data-driven case
