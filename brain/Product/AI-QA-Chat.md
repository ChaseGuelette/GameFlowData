# AI Q&A Chat

> Part of [[Product]]

## Overview
Multi-turn conversational AI in the AnalysisModal. Users ask natural language questions about any player prop and get LLM-powered answers grounded in actual data.

## Architecture
- **Model**: Claude Haiku 4.5 (~$0.003/question)
- **Endpoint**: `/api/ask` (server-side, auth-gated)
- **Rate limit**: 20 questions/24hr per user (in-memory — needs Redis for multi-instance)
- **Context window**: Max 5 messages in conversation

## Data Enrichment
Each question triggers 5 parallel data queries:
1. Extended 10-game log
2. Rolling averages (L3/L5/L15/SZN)
3. Player injury status
4. Teammate injuries
5. Opponent defense by position

These are injected into a structured system prompt so the AI's answers are grounded in real data, not hallucinated.

## Component: AskChat
- Collapsible "Ask AI about this pick" section
- Suggested question chips for quick start
- Scrollable message history
- 500-char input limit
- Remaining questions counter
- AbortController for cleanup on modal close

## Environment Variable
`ANTHROPIC_API_KEY` must be set on Vercel for this to work.

## Known Issues
- In-memory rate limiting won't work multi-instance (needs Redis)
- Chat history not persisted across modal close
- Consider persisting per player/stat to Supabase for returning users

#ai #product #feature
