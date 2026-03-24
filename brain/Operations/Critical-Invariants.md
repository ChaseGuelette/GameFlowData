# Critical Invariants

> Part of [[Operations]]

These rules must NEVER be violated regardless of what domain you're working in:

1. **NEVER deploy global conformal recalibration offsets** — 4x confirmed to hurt ROI. The model's Q10 "miscalibration" IS the edge. See [[Calibration-Guide]].

2. **NEVER put advanced stats scraping on Railway** — stats.nba.com blocks datacenter IPs. Always local only, never with a proxy.

3. **Railway daily_stats_job uses CDN only** — `--cdn-only` flag. No stats.nba.com calls from Railway.

4. **NEVER run non-concurrent CREATE INDEX on `raw_player_props_combined`** (67M+ rows). Supabase `apply_migration` runs in a transaction so CONCURRENTLY won't work. Use Supabase dashboard SQL editor.

5. **Empirical CDF for probabilities** — Always `(samples > line).mean()`, never `scipy.stats.norm.cdf()`. Gaussian CDF produces phantom edges.

6. **Quantile monotonicity enforced** — Q10 <= Q25 <= Q50 <= Q75 <= Q90 via isotonic regression.

7. **MIN_MINUTES_FOR_STATS = 8** — Games < 8 minutes excluded from rolling stat averages.

8. **Combo samples NEVER stored to DB** — Always derived on-the-fly from base stat samples.

9. **Python backend uses `postgres` role** (bypasses RLS). Dashboard uses `authenticated` role (8s statement_timeout).

10. **Temporal integrity** — Feature generation uses ONLY data where `game_date < target_game_date`. Pre-computed rolling averages use `shift(1)`.

11. **Recalibration triggers** — ROI < 8% over 14d, ECE > 0.06, model age > 3 weeks.

12. **Full retrains are risky** — Always validate with backtests. Lock hyperparams from production.

#invariants #operations #critical
