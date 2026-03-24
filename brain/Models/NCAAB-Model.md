# NCAAB Model

> Part of [[Models]]

## Status: Code Complete, Blocked on Data

### What's Built (Session 63)
- Complete pipeline: 3 database migrations (009-011), 3 scrapers, 4 processing modules, feature store (~30 features), XGBoost spread + total models, time-travel backtester, 2 orchestration scripts, 34 tests passing
- Game-level only (no player props for college — regulatory)
- Features are team differentials (home - away)
- Barttorvik for adjusted efficiency (free KenPom alternative)
- LATERAL JOIN for point-in-time ratings
- Neutral site handling for March Madness (363 D1 teams)

### Blockers
| Blocker | Details |
|---------|---------|
| Migrations 009-011 | Not applied to Supabase |
| No historical data | Nothing backfilled |
| Missing dependency | `cbbpy` not in `requirements.txt` |
| Railway cron jobs | Removed (Session 65) — were failing |

### Key Differences from NBA/MLB
- Game-level, not player-level — each row is one game
- No minutes decomposition — targets are `home_margin` and `total_score` directly
- 363 D1 teams — much larger namespace than NBA (30) or MLB (30)
- Team alias dictionaries are the biggest manual effort

### Key Files
| File | Purpose |
|------|---------|
| `src/models/ncaab_feature_store.py` | ~30 game-level matchup features |
| `src/models/ncaab_trainer.py` | XGBoost spread + total models |
| `src/models/ncaab_backtest.py` | Time-travel backtester |
| `src/scrapers/ncaab/` | CBBpy, Barttorvik, Odds API scrapers |
| `src/processing/ncaab/` | Config, linker, averages, Barttorvik linker |

#ncaab #model #blocked
