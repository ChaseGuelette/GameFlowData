# Kalshi Live Trading — Startup Playbook

> **Status:** POST-INCIDENT — Trade approval flow required. NBA trading disabled.
> **Decision date:** Apr 10, 2026 (go-live), Apr 20, 2026 (post-mortem overhaul)
> **Analysis script:** `python scripts/analyze_kalshi_paper_bets.py`
>
> **Apr 19 Incident**: First live day placed 21 NBA bets ($233) in 16 seconds using a broken model (17-46% edges). 8-fix overhaul deployed Apr 20. See [[handoff-016]].

---

## Why We're Ready

### Statistical evidence — 1,871 resolved paper bets

| Side | Bets | P&L | Consistency |
|------|------|-----|-------------|
| YES | 872 | **-$409** | Below break-even in every cost bucket under 41c |
| **NO** | **999** | **+$3,305** | **Above break-even in every cost bucket AND every stat type** |

At 999 NO bets, the standard error on win rate is ~1.5%. A 60%+ win rate is **40+ standard deviations above break-even** — this is not variance.

The critical signal is cross-sectional consistency: profitable across all cost buckets AND all stat types simultaneously. This is the hardest pattern to fake through luck.

### What drives the edge

- Model's Q10 quantile structurally undershoots → UNDER wins more often than market implies
- ~18% of NBA games produce 0 assists → asymmetric floor on NO coverage
- The Q10 "miscalibration" **is** the edge — correcting it removes profitability (confirmed 4×)

---

## Pre-Flight Checklist

Complete in order before enabling `KALSHI_LIVE_TRADING_ENABLED=true`:

- [ ] Deploy NO-only + stat whitelist + bankroll-proportional exposure changes (done Apr 10)
- [ ] Test locally: `python src/orchestration/kalshi_refresh_job.py --skip-live --sport nba`
  - BET POOL log shows counts at all tiers (>0%, >3%, >5%, >10%, >15%)
  - Only NO bets selected (no YES in Discord alerts)
  - No `batter_hits_runs_rbis` / `batter_total_bases` / `batter_home_runs` bets
  - Exposure cap = bankroll × 60% (e.g., $394 × 60% = $236)
- [ ] Run paper on Railway for **2-3 days** in NO-only mode
  - Discord alerts show `[NO-ONLY]` badge
  - Bet counts align with historical (5-20 NBA bets/day)
- [ ] Run `python scripts/analyze_kalshi_paper_bets.py --sport nba` and confirm Z-score > 3σ
- [ ] Verify Kalshi API credentials set in Railway env (`KALSHI_API_KEY`, `KALSHI_API_SECRET`)
- [ ] Fund Kalshi account with $300+

---

## Initial Configuration

### Recommended starting bankroll: $300

| Bankroll | Daily cap (60%) | vs. $80 floor | Daily loss limit | Drawdown halt at |
|----------|----------------|----------------|-----------------|------------------|
| $100 | $60 | **below floor** → always $80 | $10 | $70 |
| $200 | $120 | above floor | $20 | $140 |
| **$300** | **$180** | **comfortable headroom** | **$30** | **$210** |
| $500 | $300 | aggressive for first run | $50 | $350 |

Below $133, the 60% cap is always overridden by the $80 floor anyway. $300 is the sweet spot for meaningful volume without over-exposing on an unproven live execution.

### Railway environment variables

```bash
# Gate
KALSHI_LIVE_TRADING_ENABLED=true

# Per-sport gates (added post-incident Apr 20)
NBA_TRADING_ENABLED=false                 # Disabled until NBA model validated
MLB_TRADING_ENABLED=true

# Sizing
KALSHI_LIVE_STARTING_BANKROLL=300
KALSHI_LIVE_KELLY_FRACTION=0.125          # 1/8 Kelly — conservative
KALSHI_LIVE_MIN_EDGE=0.15                 # 15% fee-adjusted edge minimum
KALSHI_LIVE_MAX_CONTRACTS=50

# Dynamic exposure cap (API balance × 70%)
KALSHI_DAILY_EXPOSURE_PCT=0.70            # 70% of live Kalshi balance
KALSHI_MIN_DAILY_EXPOSURE=20              # floor (prevents trading on micro-balance)
KALSHI_MAX_DAILY_EXPOSURE=1000            # safety ceiling

# Edge sanity (added post-incident Apr 20)
KALSHI_LIVE_MAX_EDGE=0.40                 # Reject edges > 40% as model garbage

# Circuit breakers
KALSHI_LIVE_DRAWDOWN_LIMIT=0.30           # halt at $210 balance
KALSHI_LIVE_DAILY_LOSS_LIMIT=30.0         # $30/day (10% of $300)
KALSHI_LIVE_CONSEC_LOSS_LIMIT=5           # pause after 5 straight losses

# Mode (NO-only is default — can omit)
KALSHI_ALLOW_YES_BETS=false
```

### Post-incident additions (Apr 20, 2026)

1. **Trade approval flow**: All trades go to `kalshi_trade_queue` for human approval on dashboard. Trades expire after 30 minutes.
2. **Resolution decoupled**: `resolve_settled()` + `reconcile_fills()` run ALWAYS, even with `KALSHI_LIVE_TRADING_ENABLED=false`.
3. **Morning resolution job**: 9:15 AM ET daily via `--resolve-only` flag.
4. **Edge sanity cap**: Edges > `KALSHI_LIVE_MAX_EDGE` (40%) are rejected as model garbage.
5. **Per-sport gates**: `NBA_TRADING_ENABLED=false` blocks NBA. `MLB_TRADING_ENABLED=true` allows MLB.
6. **Shared exposure cap**: All sports share one $200/day pool. MLB fires first at :00, NBA at :02.

---

## Circuit Breaker Reference

| Breaker | Threshold | Action | Reset |
|---------|-----------|--------|-------|
| Drawdown (HWM) | Portfolio < HWM × 0.70 | **Permanent halt** — sets `is_halted=true` in DB. HWM ratchets up on new portfolio highs, never down. | Add `KALSHI_LIVE_FORCE_RESUME=true` to env, then remove |
| Daily loss | > $30 in one day | Pause until next calendar day (auto) | Automatic at midnight |
| Consecutive losses | 5 in a row | Pause for review | Clears after next winning trade |

---

## Scaling Plan

Scale up only after positive ROI over a full measurement window:

| Milestone | Action | Update these vars |
|-----------|--------|-------------------|
| 2 weeks live, ROI > 8% | Add $200 → **$500 total** | `STARTING_BANKROLL=500`, `DAILY_LOSS_LIMIT=50` |
| 4 weeks live, ROI > 8% | Add $500 → **$1,000 total** | `STARTING_BANKROLL=1000`, `DAILY_LOSS_LIMIT=100` |
| ROI drops below 8% (14-day) | Run analysis script, check for drift | — |
| Model age > 3 weeks | Check recalibration triggers | See [[NBA-Model]] |

Always update `KALSHI_LIVE_STARTING_BANKROLL` and `KALSHI_LIVE_DAILY_LOSS_LIMIT` together (loss limit = ~10% of bankroll).

> **Ceiling note:** Once bankroll crosses ~$1,000, Kelly sizing wants more contracts than some markets can fill. Effective edge expression compresses. This is a good problem — diversify into more markets rather than sizing bigger.

---

## Ongoing Monitoring

### Daily
- Discord `#kalshi`: `[NO-ONLY]` badge on every trade alert
- Railway logs: BET POOL line showing tier counts (signals how many edges are available)
- Any circuit breaker fire = investigate before next session

### Weekly
```bash
python scripts/analyze_kalshi_paper_bets.py --days 14
```
- Z-score on NO side should stay > 3σ
- ROI should stay above 8% (recalibration trigger threshold)

### Recalibration triggers
These are from MEMORY.md — do not retrain unless:
- ROI drops below **8%** over rolling 14-day window
- Any stat ECE exceeds **0.06**
- Model age exceeds **3 weeks**

---

## Failure Mode Guide

| Symptom | Likely cause | Check |
|---------|-------------|-------|
| No trades placed | Edge threshold too high, or no open markets | BET POOL log — if `>0%: 0`, no markets scraped |
| YES bets appearing in Discord | `KALSHI_ALLOW_YES_BETS` not set to false | Railway env vars |
| Unsupported MLB stat traded | Bug in whitelist filter | `SUPPORTED_STATS` in `kalshi_paper_trader.py:64` |
| Exposure always exactly $80 | Bankroll below $133 (floor dominates) | Fund account more or lower `KALSHI_MIN_DAILY_EXPOSURE` |
| Circuit breaker fires repeatedly | API errors, not actual losses | Check Kalshi API status; inspect `kalshi_live_orders` table |
| Edge degrades over time | Model drift | Run calibration check; don't retrain unless ROI < 8% |

---

## Key Invariants (NEVER VIOLATE)

1. **Never correct the Q10 "miscalibration"** — it IS the edge
2. **Never deploy global recalibration offsets** — 4× confirmed to hurt ROI
3. **Always run NO-only** — YES side is structurally negative (confirmed on 872 bets)
4. **Never trade unsupported MLB stats** (`batter_hits_runs_rbis`, `batter_total_bases`, `batter_home_runs` have no trained models)
5. **Always validate with backtests before model changes** — better calibration numbers ≠ better ROI
