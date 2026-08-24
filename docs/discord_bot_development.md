# Discord integration

GameFlowData retains one Discord package for NBA/MLB prediction alerts, scheduler status, paper-trading performance, calibration notices, and optional slash-command access. Kalshi and arbitrage alert builders were removed in the repository reduction.

## Current package

```text
src/discord_bot/
  alerts.py
  bot.py
  run_bot.py
  formatters/embeds.py
  services/predictions.py
  services/paper_trading.py
```

Slash commands are registered inside `GameFlowBot._register_commands()` in `bot.py`; there are no separate command implementation files.

Current commands:

- `/picks` — today's predictions, optionally filtered by NBA stat and minimum edge.
- `/player` — today's predictions for a fuzzy-matched player.
- `/bankroll` — paper-trading bankroll summary.
- `/performance` — resolved-bet performance over a requested lookback.
- `/toppicks` — top five high-edge picks for alert testing.

The slash-command bot describes NBA props. Scheduled alerts support both NBA and MLB and can use MLB-specific channel overrides.

## Alert transport

`src/discord_bot/alerts.py` exposes synchronous wrappers used by jobs and async implementations for:

- prediction alerts;
- job success/failure alerts;
- paper-trading P&L summaries;
- calibration alerts.

Alert failure is non-fatal to model inference and scheduler completion, but it must be logged. High-frequency jobs normally suppress success messages and alert only on failure.

## Configuration

Keep tokens and channel IDs in the runtime environment, never in Git:

- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_PREDICTIONS`
- `DISCORD_CHANNEL_ALERTS`
- `DISCORD_CHANNEL_PERFORMANCE`
- `DISCORD_MLB_CHANNEL_PREDICTIONS` (optional override)
- `DISCORD_MLB_CHANNEL_PERFORMANCE` (optional override)

`DISCORD_ENABLED` can disable alert sending where supported. Missing channel configuration should produce a warning and a skipped alert, not a fabricated success.

## Run the slash-command bot

This is optional and separate from scheduler-triggered alerts:

```powershell
Set-Location 'C:\Users\Chase\Projects\GameFlowData'
.\venv\Scripts\python.exe -m src.discord_bot.run_bot
```

Running it requires valid Discord credentials and performs an external connection, so use it only when intentionally testing the bot.

## Safe verification

Local verification that does not send messages:

```powershell
.\venv\Scripts\python.exe -m compileall -q src\discord_bot
.\venv\Scripts\python.exe -m pytest tests -q
```

For a real alert test, use a test channel or the supported infrastructure test path, then verify the exact message in Discord. Do not claim delivery from a successful HTTP call alone.

## Development rules

1. Keep provider-neutral NBA/MLB alert transport in `alerts.py`.
2. Do not restore retired exchange/arbitrage channels or renderers without an explicit product-scope decision.
3. Redact tokens and secret-bearing provider responses from logs.
4. Keep Discord optional: prediction and scheduler jobs must report alert failure without converting core job success into failure unless policy explicitly changes.
5. Update this document and focused tests whenever commands, channel routing, or alert types change.
