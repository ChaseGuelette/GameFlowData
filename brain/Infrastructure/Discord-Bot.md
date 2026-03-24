# Discord Bot

> Part of [[Infrastructure]]

## Status
REST API alerts are working (no bot process needed). Slash commands require a running bot process (local only, Phase 1).

## Automated Alerts (No Bot Process Needed)
| Channel | Trigger | Content |
|---------|---------|---------|
| `#predictions` | After inference job | Daily picks with edges |
| `#alerts` | After any job | Success/failure with duration and metrics |
| `#performance` | Daily | P&L summary |

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
