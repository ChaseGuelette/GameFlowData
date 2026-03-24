# Daily Runbook

> Part of [[Operations]]

## Morning Checks (after 11:30 AM ET)
1. Check Discord `#alerts` for daily stats job success/failure
2. If failed, check Railway logs for the 11 AM and 11:30 AM retry
3. Verify `player_average_game_stats` has been updated with last night's games

## Midday Checks (after 12:30 PM ET)
1. Check Discord `#predictions` for today's picks
2. Verify inference job completed successfully
3. Spot-check a few predictions on the dashboard

## Evening Monitoring
- 5-minute props scrape and edge refresh runs silently (Discord only on failure)
- If unusual silence from `#alerts`, check Railway process is still running
- Edge refresh drift detection may trigger selective re-inference (~5-20 players)

## Weekly Maintenance
- Monitor model age against 3-week trigger
- Check overall ROI against 8% threshold (14-day window)
- Review any calibration drift warnings
- Check `raw_player_props_combined` row count growth

## What to Do If...
- **Daily stats failed twice**: Check NBA CDN availability, database connectivity
- **Inference shows stale warning**: Rolling averages weren't updated. Predictions still usable (L5 has 4/5 overlap).
- **Edge refresh hanging**: Check if `raw_player_props_combined` queries are timing out. Ensure snapshot_time cutoffs are in place.
- **Advanced scraper failed locally**: Check if PC was awake at 9 AM ET. Re-run `scripts/run_advanced_scraper.bat` manually.

#runbook #operations #daily
