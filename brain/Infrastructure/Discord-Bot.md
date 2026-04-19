# Discord Bot

> Part of [[Infrastructure]]

## Status
REST API alerts are working (no bot process needed). Slash commands require a running bot process (local only, Phase 1).

## Automated Alerts (No Bot Process Needed)
| Channel | Trigger | Content |
|---------|---------|---------|
| `#predictions` | After inference job | Daily picks with edges (NBA + MLB) |
| `#alerts` | After any job | Success/failure with duration and metrics |
| `#performance` | After NBA bet resolution | NBA P&L summary (bankroll, daily/cumulative PnL) |
| `#mlb-performance` (or `#performance`) | After MLB bet resolution | MLB P&L summary (Session 15). Channel: `DISCORD_MLB_CHANNEL_PERFORMANCE` → fallback `DISCORD_CHANNEL_PERFORMANCE` |
| `#kalshi` (or `#predictions`) | After Kalshi refresh | Top 5 Kalshi edges (violet embed, fee-adjusted >=5%) |

## Kalshi Alert Price Filter (Session 34)
- `src/discord_bot/alerts.py` now filters Kalshi markets before selecting top-5 by edge
- Filter: `yes_price < 5 or yes_price > 95` — removes in-play/near-settled 1¢ markets that have trivial edge but no real opportunity
- Applied before the top-5 sort so stale in-play markets never appear in Discord alerts

## Slash Commands (Require Running Bot)
| Command | Purpose |
|---------|---------|
| `/picks` | Today's predictions |
| `/player` | Player-specific lookup |
| `/bankroll` | Current bankroll status |
| `/performance` | Performance metrics |
| `/toppicks` | Top edges for today |

## Code Location
- `src/discord_bot/bot.py` — Bot process
- `src/discord_bot/alerts.py` — REST webhook alerts
- `src/discord_bot/services/` — Data services
- `src/discord_bot/formatters/` — Message formatting

## Silent Alert System
5-minute cron jobs (props scrape, edge refresh) use `silent_on_success=True` — only alert on failure to avoid spam. Full scrape and inference jobs alert on both success and failure.

## Future: Phase 2
Deploy as Railway second service for persistent slash commands.

#discord #infrastructure #alerts
