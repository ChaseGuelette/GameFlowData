# AI Q&A Chat

> Part of [[Product]]

## Overview
Multi-turn conversational AI in the AnalysisModal. Users ask natural language questions about any player prop and get LLM-powered answers grounded in actual data. Conversations persist per player/stat/game_date so users can close and reopen the modal without losing context.

## Architecture
- **Model**: Claude Haiku 4.5 (~$0.003/question)
- **Endpoints**: `/api/ask` (POST, server-side, auth-gated), `/api/ask/history` (GET/DELETE)
- **Rate limit**: 20 questions/24hr per user (in-memory — needs Redis for multi-instance)
- **Context window**: Max 5 messages sent to Claude per turn
- **max_tokens**: 2048
- **Persistence**: `chat_conversations` + `chat_messages` tables (Supabase, RLS-protected)

## Data Enrichment
Each question triggers parallel data queries organized in batches:

**Batch 1** (5 parallel):
1. Extended 25-game log (with game_id for advanced stats join)
2. Rolling averages (L3/L5/L15/SZN)
3. Player injury status
4. Player position
5. Matchup history vs opponent

**Batch 2** (parallel with team_id lookup):
6. Advanced stats per game (USG%, OffRtg, NRtg, Pace from `player_game_advanced_stats`)

**Batch 3** (after team_id resolved):
7. Teammate injuries (enriched with position + L15 averages)
8. Positional depth chart (same-position teammates, L5 averages, sorted by minutes)
9. Injury timeline (45-day teammate status transitions)

**Batch 4** (after opponent team_id resolved):
10. Opponent defense by position
11. Opponent injuries (enriched)

### System Prompt Sections
- Game log (two-tier: detailed #1-10 with USG/OffRtg/NRtg, condensed #11-25)
- Matchup history vs opponent
- Rolling averages
- Game context (home/away, spread, total, pace, def rating)
- Minutes/usage context
- Model quantile predictions
- Injury status + teammate injuries
- Teammate injury timeline (status changes over 45 days)
- Positional depth chart (same-position rotation with stats)
- Opponent defense + opponent injuries
- Sportsbook lines
- Model insights

## Chat Persistence
- **Scoping**: One conversation per `(user_id, player_id, stat, game_date)`
- **Tables**: `chat_conversations` (unique key lookup), `chat_messages` (cascade delete)
- **Flow**: POST /api/ask upserts conversation + inserts messages after Claude responds. GET /api/ask/history loads previous messages on component mount. DELETE clears conversation.
- **RLS**: Users can only access their own conversations and messages.

## Component: AskChat
- Collapsible "Ask AI about this pick" section
- Loads previous conversation on mount (shows message count when collapsed)
- Suggested question chips for quick start
- Scrollable message history (max-h-80)
- Markdown rendering for assistant messages (react-markdown + prose-invert)
- "Clear chat" button (deletes conversation server-side)
- 500-char input limit
- Remaining questions counter
- AbortController for cleanup on modal close

## Environment Variable
`ANTHROPIC_API_KEY` must be set on Vercel for this to work.

## MLB Support (Session 35)
Ask AI now works for MLB players. Detection: `prediction.stat.startsWith('batter_') || prediction.stat.startsWith('pitcher_')` routes to the MLB branch before the NBA path.

**MLB Data Enrichment** (2 parallel rounds):

Round 1 (parallel):
- Game log: `mlb_player_game_stats_batting` (15 games) or `mlb_player_game_stats_pitching` (10 starts)
- Rolling averages: `mlb_player_average_batting` or `mlb_player_average_pitching`
- Player info: `mlb_players` (position, bats/throws)
- Game schedule: `mlb_game_schedule` (venue, probable pitchers, home/away)

Round 2 (parallel, after game_id + team_id resolved):
- Park factors: `mlb_park_factors` (runs/HR/hits/K multipliers)
- Opposing pitcher rolling avgs: `mlb_player_average_pitching` (ERA, WHIP, K/9, BB/9, H allowed)
- Opposing pitcher last 5 starts: `mlb_player_game_stats_pitching`
- Opposing pitcher name + handedness: `mlb_players`

**MLB System Prompt Sections**: Game log, rolling averages, game context (venue), park factors, opposing pitcher (for batters only), model prediction (binary or quantile), sportsbook lines.

**Binary model framing**: When `isBinary` is true (q10=q25=q50=0, q90≤1), shows `P(stat ≥ 1)` and `P(No stat)` instead of quantile bars.

**Feature flag**: `askChat: true` in MLB sport config (flipped in Session 35).

## Known Issues
- In-memory rate limiting won't work multi-instance (needs Redis)
- `players` table only has ~2K rows — may miss very new/obscure players for depth chart (NBA only)
- MLB `mlb_game_lineups` table is currently empty (0 rows) — lineup-based filtering in daily runner falls back gracefully, but Ask AI cannot reference lineup order beyond what's in game_stats

#ai #product #feature
