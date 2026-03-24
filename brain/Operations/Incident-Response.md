# Incident Response

> Part of [[Operations]]

## Priority 1: No Predictions Today
1. Check Discord `#alerts` — was inference job skipped?
2. Check Railway logs: `src/orchestration/scheduler.py`
3. If daily stats failed, manually trigger: `python src/orchestration/daily_stats_job.py`
4. Then trigger inference: `python src/orchestration/inference_job.py`
5. Inference without fresh stats is OK — L5/L15 averages are still 80-93% fresh

## Priority 2: Model Performance Degraded
1. Run calibration check: `python -m src.diagnostics.calibration_per_stat --db --start <14d_ago> --end <today>`
2. Check against recalibration triggers (ROI < 8%, ECE > 0.06, age > 3 weeks)
3. If triggered, consider targeted single-stat single-quantile adjustment
4. NEVER deploy global offsets (4x confirmed to hurt ROI)
5. Full retrains are last resort — lock hyperparams from production

## Priority 3: Database Performance Issues
1. Check `pg_locks` and `pg_stat_activity` first
2. `raw_player_props_combined` at 67M+ rows is the usual suspect
3. Ensure all queries have `snapshot_time` cutoffs
4. For urgent index creation: use Supabase dashboard SQL editor (not migrations)
5. Long-term: archive old prop data

## Priority 4: Railway Process Crashed
1. Railway auto-restarts on crash
2. Check Railway deployment logs
3. Verify APScheduler resumed correctly
4. Jobs missed during downtime won't auto-catch-up (except daily stats retry at 11:30)

#incident #operations #response
