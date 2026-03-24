# Maintenance Tasks

> Part of [[Operations]]

## Regular
| Task | Frequency | Details |
|------|-----------|---------|
| Monitor model ROI | Weekly | Check 14-day ROI against 8% threshold |
| Check calibration | Weekly | ECE < 0.06, quantile gaps < 3% |
| Review paper trading P&L | Daily | Discord #performance channel |
| Check Railway process health | Daily | Discord #alerts for job completions |

## Periodic
| Task | Frequency | Details |
|------|-----------|---------|
| Archive old prop data | Monthly | `raw_player_props_combined` grows ~2-3M rows/month |
| Clean old model backups | After validation | `production_old_*` directories |
| Drop unused indexes | As identified | `idx_props_dfs_latest` still exists |
| Tune drift threshold | Monthly | If re-inference triggers >30 players/cycle, increase from 1.0 |
| Advanced stats backfill | After gaps | Run `scripts/run_advanced_scraper.bat` manually |

## Database Cleanup Needed
- `idx_props_dfs_latest` — unused index, should be dropped
- `idx_props_dfs_commence` / `idx_props_sb_commence` — may be invalid from failed creation
- Old model backups: `production_old_20260210/`, `production_old_20260323/`

## Future Infrastructure
- CI/CD pipeline (currently manual git push deploys)
- Redis for rate limiting (currently in-memory)
- Pagination for history/performance pages
- Discord bot as Railway second service

#maintenance #operations #cleanup
