# GameFlowData Discord Bot

> **Status: ✅ IMPLEMENTED** (2026-02-15, Session 32)
>
> This document was originally a development plan. The Discord bot is now fully implemented.
> See `ARCHITECTURE.md` Section 11 for current architecture documentation.

## Overview

Interactive Discord bot that sends daily prediction alerts and responds to slash commands for querying predictions, player stats, and paper trading performance.

---

## Prerequisites (Manual Setup Required)

### Discord Developer Portal Setup (~5 minutes)

1. **Create Discord Server**
   - Open Discord → Click "+" → "Create My Own" → Name it (e.g., "GameFlow Alerts")
   - Create channels: `#predictions`, `#alerts`, `#performance`

2. **Create Bot Application**
   - Go to https://discord.com/developers/applications
   - Click "New Application" → Name: "GameFlow Bot"
   - Go to "Bot" tab → Click "Add Bot"
   - Copy the **Bot Token** (keep secret!)
   - Enable "Message Content Intent" under Privileged Gateway Intents

3. **Generate Invite URL**
   - Go to "OAuth2" → "URL Generator"
   - Scopes: `bot`, `applications.commands`
   - Bot Permissions: `Send Messages`, `Read Message History`, `Embed Links`, `Use Slash Commands`
   - Copy URL → Open in browser → Select server → Authorize

4. **Get Channel IDs**
   - Discord Settings → Advanced → Enable "Developer Mode"
   - Right-click each channel → "Copy Channel ID"

5. **Add to Environment**
   ```bash
   # Add to .env
   DISCORD_BOT_TOKEN=your_bot_token_here
   DISCORD_CHANNEL_PREDICTIONS=channel_id_here
   DISCORD_CHANNEL_ALERTS=channel_id_here
   ```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DISCORD BOT ARCHITECTURE                 │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────┐ │
│  │ Discord.py   │────▶│  Bot Core    │────▶│  Supabase   │ │
│  │ (Gateway)    │     │  (Commands)  │     │  (Database) │ │
│  └──────────────┘     └──────────────┘     └─────────────┘ │
│         │                    │                              │
│         ▼                    ▼                              │
│  ┌──────────────┐     ┌──────────────┐                     │
│  │ Slash Cmds   │     │ Formatters   │                     │
│  │ /picks       │     │ (Embeds)     │                     │
│  │ /player      │     └──────────────┘                     │
│  │ /bankroll    │                                          │
│  │ /performance │                                          │
│  └──────────────┘                                          │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                   SCHEDULED TASKS                     │  │
│  │  • Post-inference alert (triggered by inference_job) │  │
│  │  • Daily performance summary (optional cron)         │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
src/discord_bot/
├── __init__.py
├── bot.py              # Main bot class, command registration
├── commands/
│   ├── __init__.py
│   ├── picks.py        # /picks command - today's predictions
│   ├── player.py       # /player <name> - player-specific predictions
│   ├── bankroll.py     # /bankroll - paper trading balance
│   └── performance.py  # /performance - ROI, win rate stats
├── services/
│   ├── __init__.py
│   ├── predictions.py  # Query daily_predictions table
│   ├── paper_trading.py # Query paper_trading tables
│   └── players.py      # Player lookup and fuzzy matching
├── formatters/
│   ├── __init__.py
│   ├── embeds.py       # Discord embed builders
│   └── tables.py       # ASCII table formatters
├── alerts.py           # Alert sending functions
└── run_bot.py          # Entry point / main()

# Integration point
src/orchestration/inference_job.py  # Add alert trigger after predictions
```

---

## Dependencies

```txt
# Add to requirements.txt
discord.py>=2.3.0
python-dotenv>=1.0.0  # Already have this
```

---

## Implementation Plan

### Phase 1: Bot Foundation (Core Setup)

#### 1.1 Create Bot Entry Point
**File:** `src/discord_bot/run_bot.py`

```python
import os
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Bot connected as {bot.user}")
    await bot.tree.sync()  # Sync slash commands

if __name__ == "__main__":
    bot.run(os.getenv("DISCORD_BOT_TOKEN"))
```

#### 1.2 Create Prediction Service
**File:** `src/discord_bot/services/predictions.py`

- `get_todays_picks(min_edge: float = 0.05) -> list[dict]`
- `get_picks_for_date(date: date, min_edge: float = 0.05) -> list[dict]`
- `get_player_predictions(player_name: str, date: date) -> list[dict]`
- `get_top_picks(n: int = 5) -> list[dict]`

#### 1.3 Create Embed Formatter
**File:** `src/discord_bot/formatters/embeds.py`

```python
def create_picks_embed(picks: list[dict], date: date) -> discord.Embed:
    embed = discord.Embed(
        title=f"🏀 Top Picks for {date.strftime('%b %d')}",
        color=discord.Color.green()
    )
    for pick in picks[:10]:
        edge = pick['over_edge'] if pick['over_edge'] > pick['under_edge'] else pick['under_edge']
        side = "Over" if pick['over_edge'] > pick['under_edge'] else "Under"
        embed.add_field(
            name=f"{pick['player_name']} - {pick['stat'].upper()}",
            value=f"{side} {pick['line']} | Edge: {edge:.1%}",
            inline=False
        )
    return embed
```

### Phase 2: Slash Commands

#### 2.1 /picks Command
**File:** `src/discord_bot/commands/picks.py`

```python
@bot.tree.command(name="picks", description="Get today's top predictions")
@app_commands.describe(
    stat="Filter by stat type (pts/reb/ast/all)",
    min_edge="Minimum edge threshold (default: 5%)"
)
async def picks(
    interaction: discord.Interaction,
    stat: str = "all",
    min_edge: float = 0.05
):
    await interaction.response.defer()
    picks = await get_todays_picks(stat=stat, min_edge=min_edge)
    embed = create_picks_embed(picks)
    await interaction.followup.send(embed=embed)
```

#### 2.2 /player Command
**File:** `src/discord_bot/commands/player.py`

```python
@bot.tree.command(name="player", description="Get predictions for a specific player")
@app_commands.describe(player_name="Player name (fuzzy match supported)")
async def player(interaction: discord.Interaction, player_name: str):
    await interaction.response.defer()
    predictions = await get_player_predictions(player_name)
    if not predictions:
        await interaction.followup.send(f"No predictions found for '{player_name}'")
        return
    embed = create_player_embed(predictions)
    await interaction.followup.send(embed=embed)
```

#### 2.3 /bankroll Command
**File:** `src/discord_bot/commands/bankroll.py`

```python
@bot.tree.command(name="bankroll", description="Show paper trading balance")
async def bankroll(interaction: discord.Interaction):
    summary = await get_bankroll_summary()
    embed = discord.Embed(title="💰 Paper Trading Bankroll", color=discord.Color.gold())
    embed.add_field(name="Balance", value=f"${summary['balance']:,.2f}", inline=True)
    embed.add_field(name="Today P&L", value=f"${summary['daily_pnl']:+,.2f}", inline=True)
    embed.add_field(name="Total P&L", value=f"${summary['total_pnl']:+,.2f}", inline=True)
    await interaction.response.send_message(embed=embed)
```

#### 2.4 /performance Command
**File:** `src/discord_bot/commands/performance.py`

```python
@bot.tree.command(name="performance", description="Show model performance stats")
async def performance(interaction: discord.Interaction):
    stats = await get_performance_stats()
    embed = discord.Embed(title="📊 Model Performance", color=discord.Color.blue())
    embed.add_field(name="Win Rate", value=f"{stats['win_rate']:.1%}", inline=True)
    embed.add_field(name="ROI", value=f"{stats['roi']:+.1%}", inline=True)
    embed.add_field(name="Total Bets", value=str(stats['total_bets']), inline=True)
    embed.add_field(name="Best Stat", value=stats['best_stat'].upper(), inline=True)
    await interaction.response.send_message(embed=embed)
```

### Phase 3: Automated Alerts

#### 3.1 Alert Function
**File:** `src/discord_bot/alerts.py`

```python
import aiohttp
import os

async def send_predictions_alert(predictions_df, channel_id: str = None):
    """Send top picks to Discord after inference job completes."""
    channel_id = channel_id or os.getenv("DISCORD_CHANNEL_PREDICTIONS")
    bot_token = os.getenv("DISCORD_BOT_TOKEN")

    # Format top 5 picks
    top_picks = predictions_df.nlargest(5, 'over_edge')

    embed = {
        "title": "🏀 Daily Predictions Ready!",
        "color": 0x00ff00,
        "fields": []
    }

    for _, pick in top_picks.iterrows():
        embed["fields"].append({
            "name": f"{pick['player_name']} - {pick['stat'].upper()}",
            "value": f"Over {pick['line']} | Edge: {pick['over_edge']:.1%}",
            "inline": False
        })

    # Send via Discord API (doesn't require bot to be running)
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {bot_token}"},
            json={"embeds": [embed]}
        )
```

#### 3.2 Integration with Inference Job
**File:** `src/orchestration/inference_job.py` (modification)

```python
# Add after predictions generated successfully
from src.discord_bot.alerts import send_predictions_alert
import asyncio

# After: logger.info(f"Generated {len(preds)} predictions")
if not args.dry_run and os.getenv("DISCORD_BOT_TOKEN"):
    try:
        asyncio.run(send_predictions_alert(preds))
        logger.info("Sent Discord alert")
    except Exception as e:
        logger.warning(f"Discord alert failed: {e}")
```

### Phase 4: Bot Hosting

#### 4.1 Windows Task Scheduler (Simple)
Create a scheduled task to run `run_bot.py` at system startup.

```batch
:: start_discord_bot.bat
cd C:\Users\Chase\Projects\GameFlowData
call .venv\Scripts\activate
python src/discord_bot/run_bot.py
```

#### 4.2 Alternative: Run as Windows Service
Use `pywin32` or `NSSM` to run the bot as a Windows service for better reliability.

---

## Database Queries

### Get Today's Predictions
```sql
SELECT
    p.player_name,
    dp.stat,
    dp.line,
    dp.over_edge,
    dp.under_edge,
    dp.pred_q50,
    t.abbreviation as team
FROM daily_predictions dp
JOIN players p ON dp.player_id = p.player_id
JOIN teams t ON dp.team_id = t.team_id
WHERE dp.prediction_date = CURRENT_DATE
  AND (dp.over_edge > 0.05 OR dp.under_edge > 0.05)
ORDER BY GREATEST(dp.over_edge, dp.under_edge) DESC
LIMIT 10;
```

### Get Bankroll Summary
```sql
SELECT
    current_bankroll as balance,
    daily_pnl,
    current_bankroll - 1000 as total_pnl  -- Assuming $1000 start
FROM paper_trading_daily_log
ORDER BY game_date DESC
LIMIT 1;
```

### Get Performance Stats
```sql
SELECT
    COUNT(*) as total_bets,
    SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END)::float / COUNT(*) as win_rate,
    SUM(pnl) / SUM(stake) as roi
FROM paper_trading_bets
WHERE resolved_at IS NOT NULL;
```

---

## Environment Variables

```bash
# Add to .env
DISCORD_BOT_TOKEN=your_bot_token_here
DISCORD_CHANNEL_PREDICTIONS=123456789012345678
DISCORD_CHANNEL_ALERTS=123456789012345679
DISCORD_ENABLED=true  # Set to false to disable alerts
```

---

## Testing Plan

### Manual Testing
1. Run bot locally: `python src/discord_bot/run_bot.py`
2. Test each slash command in Discord:
   - `/picks` - Should show today's predictions
   - `/picks stat:pts min_edge:0.07` - Should filter
   - `/player LeBron` - Should fuzzy match and show predictions
   - `/bankroll` - Should show paper trading balance
   - `/performance` - Should show win rate and ROI

### Integration Testing
1. Run inference job with `--dry-run` first
2. Run inference job normally → Verify Discord alert appears
3. Verify alert formatting looks correct

### Error Cases
- No predictions for today → Should show "No predictions available"
- Player not found → Should suggest similar names
- Database connection error → Should show friendly error message

---

## Implementation Order

| Step | Task | Est. Time |
|------|------|-----------|
| 1 | User: Complete Discord setup (server, bot, tokens) | 5 min |
| 2 | Create bot foundation (`run_bot.py`, basic structure) | 30 min |
| 3 | Implement prediction service + `/picks` command | 45 min |
| 4 | Implement `/player` command with fuzzy matching | 30 min |
| 5 | Implement `/bankroll` and `/performance` commands | 30 min |
| 6 | Implement alert function + inference job integration | 30 min |
| 7 | Test all commands and alerts | 20 min |
| 8 | Set up Windows Task Scheduler for bot hosting | 15 min |
| **Total** | | **~3.5 hours** |

---

## Future Enhancements (V2)

- **Line movement alerts** - Notify when lines move significantly
- **Game start reminders** - Alert 30 min before games with active picks
- **Bet slip tracking** - `/bet add <player> <stat> <side>` to track personal bets
- **Leaderboard** - Track prediction accuracy over time
- **Charts** - Generate and send performance charts as images

---

## Files to Create

| File | Purpose |
|------|---------|
| `src/discord_bot/__init__.py` | Package init |
| `src/discord_bot/run_bot.py` | Entry point |
| `src/discord_bot/bot.py` | Bot class and command registration |
| `src/discord_bot/commands/__init__.py` | Commands package |
| `src/discord_bot/commands/picks.py` | /picks command |
| `src/discord_bot/commands/player.py` | /player command |
| `src/discord_bot/commands/bankroll.py` | /bankroll command |
| `src/discord_bot/commands/performance.py` | /performance command |
| `src/discord_bot/services/__init__.py` | Services package |
| `src/discord_bot/services/predictions.py` | Prediction queries |
| `src/discord_bot/services/paper_trading.py` | Paper trading queries |
| `src/discord_bot/formatters/__init__.py` | Formatters package |
| `src/discord_bot/formatters/embeds.py` | Discord embed builders |
| `src/discord_bot/alerts.py` | Alert sending functions |

## Files to Modify

| File | Change |
|------|--------|
| `src/orchestration/inference_job.py` | Add Discord alert trigger |
| `.env` | Add Discord tokens and channel IDs |
| `requirements.txt` | Add discord.py dependency |

---

## Verification

1. Bot connects and shows "Bot connected as GameFlow Bot#1234"
2. `/picks` returns formatted embed with predictions
3. `/player Curry` finds Stephen Curry's predictions
4. `/bankroll` shows current paper trading balance
5. Running inference job sends alert to #predictions channel
6. Bot continues running after system restart (Task Scheduler)

---

## Implementation Notes (2026-02-15)

### Changes from Original Plan

**Simplified Architecture:**
- Commands are inline in `bot.py` instead of separate files per command (simpler, less boilerplate)
- No separate `players.py` service — player lookup is part of `predictions.py`
- No `tables.py` formatter — embeds handle all formatting

**Additional Commands:**
- Added `/toppicks` command for quick top 5 view

**Database Schema Differences:**
- `paper_bets` table uses `status` not `result`, values are `'won'`/`'lost'` not `'win'`/`'loss'`
- `paper_bets` uses `stat_type`, `bet_direction`, `odds_at_bet` not `stat`, `side`, `odds`
- `paper_trading_daily_log` uses `bankroll_after` not `current_bankroll`
- `teams` table only has `team_name`, not `abbreviation` — use `feat_opp_abbrev` from predictions

**Files Actually Created:**
| File | Purpose |
|------|---------|
| `src/discord_bot/__init__.py` | Package init |
| `src/discord_bot/commands/__init__.py` | Commands package init |
| `src/discord_bot/services/__init__.py` | Services package init |
| `src/discord_bot/formatters/__init__.py` | Formatters package init |
| `src/discord_bot/run_bot.py` | Entry point with graceful shutdown |
| `src/discord_bot/bot.py` | Bot class with all slash commands (250 lines) |
| `src/discord_bot/services/predictions.py` | Prediction queries (225 lines) |
| `src/discord_bot/services/paper_trading.py` | Paper trading queries (225 lines) |
| `src/discord_bot/formatters/embeds.py` | Discord embed builders (280 lines) |
| `src/discord_bot/alerts.py` | REST API alert sender (195 lines) |
| `scripts/run_discord_bot.bat` | Windows Task Scheduler script |

**Hosting:**
- `scripts/run_discord_bot.bat` uses `venv\` not `.venv\`
- Sets `PYTHONPATH` for module imports
- Railway-ready architecture with graceful shutdown handlers
