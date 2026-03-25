import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import Anthropic from '@anthropic-ai/sdk'
import { type ChatMessage } from '@/types/chat'
import { type Prediction, type PlayerGameStats, type BookmakerLine, STAT_LABELS } from '@/types/predictions'
import { type Insight } from '@/lib/insights'

// --- Rate limiting ---
const RATE_LIMIT = 20
const WINDOW_MS = 24 * 60 * 60 * 1000

interface RateBucket {
  timestamps: number[]
}

const rateLimitMap = new Map<string, RateBucket>()

function checkRateLimit(userId: string): { allowed: boolean; remaining: number } {
  const now = Date.now()
  const bucket = rateLimitMap.get(userId) ?? { timestamps: [] }

  // Prune timestamps outside the rolling window
  bucket.timestamps = bucket.timestamps.filter(t => now - t < WINDOW_MS)

  if (bucket.timestamps.length >= RATE_LIMIT) {
    rateLimitMap.set(userId, bucket)
    return { allowed: false, remaining: 0 }
  }

  bucket.timestamps.push(now)
  rateLimitMap.set(userId, bucket)
  return { allowed: true, remaining: RATE_LIMIT - bucket.timestamps.length }
}

// --- Combo stat helpers ---
const COMBO_COMPONENTS: Record<string, string[]> = {
  pra: ['pts', 'reb', 'ast'],
  pr: ['pts', 'reb'],
  pa: ['pts', 'ast'],
  ra: ['reb', 'ast'],
}

function isComboStat(stat: string): boolean {
  return stat in COMBO_COMPONENTS
}

// --- Request body types ---
interface AskRequestBody {
  question: string
  conversationHistory: ChatMessage[]
  playerContext: {
    prediction: Prediction
    history: PlayerGameStats[]
    insights: Insight[]
    bookmakerLines: BookmakerLine[]
    isOverBet: boolean
    edge: number
    probability: number
  }
}

// --- Helpers ---
function formatShortDate(dateStr: string): string {
  const parts = dateStr.split('-')
  if (parts.length === 3) return `${parseInt(parts[1])}/${parseInt(parts[2])}`
  return dateStr
}

const POSITION_GROUP_TO_DEFENSE: Record<string, string> = {
  'Guard': 'G',
  'Forward': 'W',
  'Big': 'B',
}

// --- Build system prompt ---
function buildSystemPrompt(
  prediction: Prediction,
  gameLog: Array<Record<string, unknown>>,
  rollingAvgs: Record<string, unknown> | null,
  playerInjury: Record<string, unknown> | null,
  teammateInjuries: Array<Record<string, unknown>>,
  oppDefense: Record<string, unknown> | null,
  oppInjuries: Array<Record<string, unknown>>,
  vsOpponentLog: Array<Record<string, unknown>>,
  insights: Insight[],
  bookmakerLines: BookmakerLine[],
  isOverBet: boolean,
  edge: number,
  probability: number,
  playerPosition: string | null,
): string {
  const stat = prediction.stat
  const statLabel = STAT_LABELS[stat] || stat.toUpperCase()
  const direction = isOverBet ? 'Over' : 'Under'
  const combo = isComboStat(stat)
  const components = COMBO_COMPONENTS[stat]

  // Game log — numbered rows with compact format
  let gameLogSection = 'No recent game data available.'
  if (gameLog.length > 0) {
    const rows = gameLog.map((g: Record<string, unknown>, i: number) => {
      const num = `#${i + 1}`
      const shortDate = formatShortDate(String(g.game_date || ''))

      // Compact matchup: "vs HOU" or "@ BOS" from "MIA vs. HOU"
      const matchup = String(g.matchup || '')
      const matchParts = matchup.split(/\s+(vs\.?|@)\s+/)
      const oppStr = matchParts.length >= 3
        ? `${matchParts[1].replace('.', '')} ${matchParts[2]}`
        : matchup

      const oreb = g.oreb ?? 0
      const base = `${num.padEnd(4)}${shortDate.padEnd(6)}${oppStr.padEnd(8)}| ${String(g.min ?? '-').padEnd(3)} MIN | ${String(g.pts ?? '-')} PTS | ${g.reb ?? '-'} REB (${oreb} OREB) | ${g.ast ?? '-'} AST`

      if (combo) {
        const total = (components || []).reduce((s: number, c: string) => s + (Number(g[c]) || 0), 0)
        return `${base} | ${statLabel}=${total} | ${g.wl ?? '-'} | ${g.started ? 'Started' : 'Bench'}`
      }
      return `${base} | ${g.stl ?? '-'} STL | ${g.blk ?? '-'} BLK | ${g.fg3m ?? '-'} 3PM | ${g.tov ?? '-'} TOV | ${g.fga ?? '-'} FGA | ${g.fta ?? '-'} FTA | ${g.wl ?? '-'} | ${g.started ? 'Started' : 'Bench'}`
    })

    gameLogSection = `LAST ${gameLog.length} GAMES (most recent first):\n${rows.join('\n')}`
  }

  // Rolling averages
  let avgSection = ''
  if (rollingAvgs) {
    const statsToShow = combo ? (components || []) : [stat === '3pm' ? 'fg3m' : stat]
    const lines: string[] = []
    for (const s of statsToShow) {
      const l5 = rollingAvgs[`avg_${s}_l5`]
      const l15 = rollingAvgs[`avg_${s}_l15`]
      const szn = rollingAvgs[`avg_${s}_szn`]
      lines.push(`  ${s.toUpperCase()}: L5=${l5 ?? '?'} | L15=${l15 ?? '?'} | SZN=${szn ?? '?'}`)
    }
    const minL5 = rollingAvgs['avg_min_l5']
    const minL15 = rollingAvgs['avg_min_l15']
    const minSzn = rollingAvgs['avg_min_szn']
    lines.push(`  MIN: L5=${minL5 ?? '?'} | L15=${minL15 ?? '?'} | SZN=${minSzn ?? '?'}`)

    const restDays = rollingAvgs['rest_days']
    const gamesL7 = rollingAvgs['games_last_7d']
    if (restDays != null) lines.push(`  Rest days: ${restDays}`)
    if (gamesL7 != null) lines.push(`  Games in last 7 days: ${gamesL7}`)

    avgSection = `ROLLING AVERAGES:\n${lines.join('\n')}`
  }

  // Injury context — with report dates and enriched teammate data
  let injurySection = ''
  if (playerInjury) {
    const reportStr = playerInjury.report_date ? ` (reported ${formatShortDate(String(playerInjury.report_date))})` : ''
    injurySection += `PLAYER INJURY STATUS: ${playerInjury.status} - ${playerInjury.reason || 'No reason listed'}${playerInjury.injury_detail ? ` (${playerInjury.injury_detail})` : ''}${reportStr}\n`
  }
  if (teammateInjuries.length > 0) {
    injurySection += 'TEAMMATE INJURIES:\n'
    for (const inj of teammateInjuries) {
      const pos = inj._position ? ` (${inj._position})` : ''
      const stats = inj._avg_stats ? ` — ${inj._avg_stats}` : ''
      const reportStr = inj.report_date ? ` [reported ${formatShortDate(String(inj.report_date))}]` : ''
      injurySection += `  ${inj.player}${pos} - ${inj.status}: ${inj.reason || 'Unknown'}${inj.injury_detail ? ` (${inj.injury_detail})` : ''}${stats}${reportStr}\n`
    }
  }

  // Opponent defense — expanded with per100, off_rtg, season, and all relevant stats
  let defenseSection = ''
  if (oppDefense) {
    const posGroup = oppDefense.position_group || 'position'
    const defLines: string[] = []
    const allStats = combo ? (components || []) : [stat === '3pm' ? 'threes' : stat]

    for (const s of allStats) {
      const l5 = oppDefense[`${s}_allowed_l5`]
      const l15 = oppDefense[`${s}_allowed_l15`]
      const szn = oppDefense[`${s}_allowed_szn`]
      const per100L5 = oppDefense[`${s}_per100_allowed_l5`]
      const per100L15 = oppDefense[`${s}_per100_allowed_l15`]
      defLines.push(`  ${s.toUpperCase()} allowed: L5=${l5 ?? '?'} | L15=${l15 ?? '?'} | SZN=${szn ?? '?'} (per100: L5=${per100L5 ?? '?'}, L15=${per100L15 ?? '?'})`)
    }

    // Always include some context stats regardless of the target stat
    const contextStats = ['pts', 'reb', 'ast', 'threes'].filter(s => !allStats.includes(s))
    for (const s of contextStats) {
      const l5 = oppDefense[`${s}_allowed_l5`]
      if (l5 != null) {
        const l15 = oppDefense[`${s}_allowed_l15`]
        defLines.push(`  ${s.toUpperCase()} allowed: L5=${l5} | L15=${l15 ?? '?'}`)
      }
    }

    const offRtgL5 = oppDefense['off_rtg_allowed_l5']
    const offRtgL15 = oppDefense['off_rtg_allowed_l15']
    if (offRtgL5 != null) {
      defLines.push(`  Offensive Rating allowed: L5=${offRtgL5} | L15=${offRtgL15 ?? '?'} (lower = better defense)`)
    }

    defenseSection = `OPPONENT DEFENSE (${prediction.opponent_abbrev || '???'}) vs ${posGroup}:\n${defLines.join('\n')}`
  }

  // Opponent injuries — who's out/questionable on the opposing team
  let oppInjurySection = ''
  if (oppInjuries.length > 0) {
    oppInjurySection = `OPPONENT INJURIES (${prediction.opponent_abbrev || '???'}):\n`
    for (const inj of oppInjuries) {
      const pos = inj._position ? ` (${inj._position})` : ''
      const stats = inj._avg_stats ? ` — ${inj._avg_stats}` : ''
      const reportStr = inj.report_date ? ` [reported ${formatShortDate(String(inj.report_date))}]` : ''
      oppInjurySection += `  ${inj.player}${pos} - ${inj.status}: ${inj.reason || 'Unknown'}${stats}${reportStr}\n`
    }
  }

  // Matchup history vs this specific opponent
  let vsOpponentSection = ''
  if (vsOpponentLog.length > 0) {
    const rows = vsOpponentLog.map((g: Record<string, unknown>, i: number) => {
      const num = `#${i + 1}`
      const shortDate = formatShortDate(String(g.game_date || ''))
      const oreb = g.oreb ?? 0

      let base = `${num.padEnd(4)}${shortDate.padEnd(6)}| ${String(g.min ?? '-').padEnd(3)} MIN | ${g.pts ?? '-'} PTS | ${g.reb ?? '-'} REB (${oreb} OREB) | ${g.ast ?? '-'} AST`
      if (combo) {
        const total = (components || []).reduce((s: number, c: string) => s + (Number(g[c]) || 0), 0)
        base += ` | ${statLabel}=${total}`
      } else {
        base += ` | ${g.stl ?? '-'} STL | ${g.blk ?? '-'} BLK | ${g.fg3m ?? '-'} 3PM`
      }
      base += ` | ${g.wl ?? '-'} | ${g.started ? 'Started' : 'Bench'}`
      return base
    })
    vsOpponentSection = `MATCHUP HISTORY vs ${prediction.opponent_abbrev || '???'} (this season, most recent first):\n${rows.join('\n')}`
  }

  // Sportsbook lines
  let linesSection = ''
  if (bookmakerLines.length > 0) {
    const lineStrs = bookmakerLines.slice(0, 6).map(l =>
      `  ${l.bookmaker}: Line ${l.line} | Over ${l.over_odds >= 0 ? '+' : ''}${l.over_odds} | Under ${l.under_odds >= 0 ? '+' : ''}${l.under_odds}`
    )
    linesSection = `SPORTSBOOK LINES:\n${lineStrs.join('\n')}`
  }

  // Model insights
  let insightSection = ''
  if (insights.length > 0) {
    const insightStrs = insights.map(i => `  [${i.sentiment}] ${i.text}`)
    insightSection = `MODEL INSIGHTS:\n${insightStrs.join('\n')}`
  }

  // Game context from model features
  const pred = prediction as unknown as Record<string, unknown>

  const gameContext: string[] = []
  // Home/away
  if (pred.feat_is_home != null) gameContext.push(`Location: ${pred.feat_is_home ? 'HOME' : 'AWAY'}`)
  // Spread & total
  if (pred.feat_line_spread != null) {
    const spread = Number(pred.feat_line_spread)
    gameContext.push(`Vegas Spread: ${spread > 0 ? '+' : ''}${spread.toFixed(1)} (${spread < 0 ? 'favored' : 'underdog'})`)
  }
  if (pred.feat_line_total != null) gameContext.push(`Vegas Total: ${Number(pred.feat_line_total).toFixed(1)}`)
  // Pace
  if (pred.feat_team_avg_pace_l5 != null && pred.feat_opp_avg_pace_l5 != null) {
    const teamPace = Number(pred.feat_team_avg_pace_l5).toFixed(1)
    const oppPace = Number(pred.feat_opp_avg_pace_l5).toFixed(1)
    const expectedPace = (Number(pred.feat_team_avg_pace_l5) * Number(pred.feat_opp_avg_pace_l5) / 99.5).toFixed(1)
    gameContext.push(`Pace: Team L5=${teamPace} | Opp L5=${oppPace} | Expected=${expectedPace} (league avg ~99.5)`)
  }
  // Opponent defense rating
  if (pred.feat_opp_avg_def_rtg_l5 != null) {
    const defRtg = Number(pred.feat_opp_avg_def_rtg_l5).toFixed(1)
    gameContext.push(`Opp Defensive Rating L5: ${defRtg} (league avg ~112, lower = better defense)`)
  }

  const gameSection = gameContext.length > 0
    ? `GAME CONTEXT:\n  ${gameContext.join('\n  ')}`
    : ''

  // Minutes/usage context from model features
  const minutesContext: string[] = []
  if (pred.feat_player_avg_min_l3 != null) minutesContext.push(`Minutes avg L3: ${Number(pred.feat_player_avg_min_l3).toFixed(1)}`)
  if (pred.feat_player_min_floor_l5 != null) minutesContext.push(`Minutes floor L5: ${Number(pred.feat_player_min_floor_l5).toFixed(1)} (worst of last 5)`)
  if (pred.feat_player_min_std_l5 != null) minutesContext.push(`Minutes std dev L5: ${Number(pred.feat_player_min_std_l5).toFixed(1)} (${Number(pred.feat_player_min_std_l5) < 3 ? 'stable' : Number(pred.feat_player_min_std_l5) < 7 ? 'moderate variance' : 'high variance'})`)
  if (pred.feat_player_starter_prob != null) minutesContext.push(`Starter probability: ${(Number(pred.feat_player_starter_prob) * 100).toFixed(0)}%`)
  if (pred.feat_player_avg_usg_pct_l5 != null) minutesContext.push(`Usage rate L5: ${(Number(pred.feat_player_avg_usg_pct_l5) * 100).toFixed(1)}% (league avg ~20%)`)
  if (pred.feat_rest_days != null) minutesContext.push(`Rest days: ${pred.feat_rest_days}`)
  if (pred.feat_is_back_to_back != null && Number(pred.feat_is_back_to_back) === 1) minutesContext.push(`Back-to-back: YES`)
  if (pred.feat_games_last_7d != null) minutesContext.push(`Games in last 7 days: ${pred.feat_games_last_7d}`)

  const minutesSection = minutesContext.length > 0
    ? `MINUTES/USAGE CONTEXT:\n  ${minutesContext.join('\n  ')}`
    : ''

  const quantileSection = `MODEL QUANTILE PREDICTIONS:
  Q10=${prediction.q10} | Q25=${prediction.q25} | Q50=${prediction.q50} | Q75=${prediction.q75} | Q90=${prediction.q90}
  Prop Line: ${prediction.prop_line}
  Model Direction: ${direction} (probability ${(probability * 100).toFixed(1)}%, edge ${(edge * 100).toFixed(1)}%)`

  const positionStr = playerPosition ? `\nPOSITION: ${playerPosition}` : ''

  return `You are a sharp NBA analytics assistant for the GameFlowData platform. The user is analyzing a player prop bet and has questions.

PLAYER: ${prediction.player_name || 'Unknown'} (${prediction.team_abbrev || '???'})${positionStr}
OPPONENT: ${prediction.opponent_abbrev || '???'}
STAT: ${statLabel}
BET: ${direction} ${prediction.prop_line}

${gameLogSection}

${vsOpponentSection}

${avgSection}

${gameSection}

${minutesSection}

${quantileSection}

${injurySection}
${oppInjurySection}
${defenseSection}

${linesSection}

${insightSection}

RULES:
- Only reference data provided above. Do not make up stats or games.
- Cite specific games, dates, and numbers when relevant.
- Keep responses to 2-4 short paragraphs.
- Be direct and analytical. Avoid generic hedging like "it depends on many factors."
- When discussing trends, reference the rolling averages and game log.
- When asked about minutes or playing time, reference the MINUTES/USAGE CONTEXT section with starter probability, minutes floor, usage rate, and recent averages.
- When asked about game script, pace, or blowout risk, reference the GAME CONTEXT section. A large negative spread means the team is heavily favored. High pace + high total = more possessions = more stat opportunities.
- When asked about matchup history vs a specific team, reference the MATCHUP HISTORY section. If no matchup history exists, say so and use the opponent defense stats instead.
- When asked about opponent defense, reference both OPPONENT DEFENSE and OPPONENT INJURIES sections. Missing key players affects defensive quality.
- For combo stats (${Object.keys(COMBO_COMPONENTS).join(', ')}), break down which component stat is driving the total.
- If the user asks about something not covered by the data, say so honestly.`
}

export async function POST(request: NextRequest) {
  // Auth
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  // Rate limit
  const { allowed, remaining } = checkRateLimit(user.id)
  if (!allowed) {
    return NextResponse.json(
      { error: 'Rate limit exceeded. Try again tomorrow.', remaining: 0 },
      { status: 429 }
    )
  }

  // Parse body
  let body: AskRequestBody
  try {
    body = await request.json()
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 })
  }

  const { question, conversationHistory, playerContext } = body
  if (!question || question.length > 500) {
    return NextResponse.json({ error: 'Question is required (max 500 chars)' }, { status: 400 })
  }

  const { prediction, insights, bookmakerLines, isOverBet, edge, probability } = playerContext

  const oppAbbrev = prediction.opponent_abbrev || prediction.feat_opp_team_abbrev

  // --- Data enrichment: 5 parallel queries ---
  const [gameLogRes, rollingAvgRes, playerInjuryRes, playerPosRes, vsOpponentRes] = await Promise.all([
    // 1. Extended game log (last 10) — includes oreb, dreb, tov, fga, fta
    supabase
      .from('player_game_stats')
      .select('game_date, pts, reb, ast, fg3m, min, matchup, wl, started, stl, blk, oreb, dreb, tov, fga, fta')
      .eq('player_id', prediction.player_id)
      .gt('min', 0)
      .order('game_date', { ascending: false })
      .limit(10),

    // 2. Rolling averages (latest row for this player)
    supabase
      .from('player_average_game_stats')
      .select('*')
      .eq('player_id', prediction.player_id)
      .order('game_date', { ascending: false })
      .limit(1),

    // 3. Player's own injury status (with report_date)
    supabase
      .from('rapidapi_injuries')
      .select('status, reason, injury_detail, report_date')
      .eq('player_id', prediction.player_id)
      .order('report_date', { ascending: false })
      .limit(1),

    // 4. Player position
    supabase
      .from('players')
      .select('primary_position, position_group')
      .eq('player_id', prediction.player_id)
      .limit(1),

    // 5. Matchup history vs this specific opponent (this season, up to 5 games)
    oppAbbrev
      ? supabase
          .from('player_game_stats')
          .select('game_date, pts, reb, ast, fg3m, min, matchup, wl, started, stl, blk, oreb, dreb, tov, fga, fta')
          .eq('player_id', prediction.player_id)
          .gt('min', 0)
          .or(`matchup.ilike.%vs. ${oppAbbrev},matchup.ilike.%@ ${oppAbbrev}`)
          .order('game_date', { ascending: false })
          .limit(5)
      : Promise.resolve({ data: [], error: null }),
  ])

  // Player position from query #4
  const playerPosition = playerPosRes.data?.[0]?.primary_position || null
  const playerPositionGroup = playerPosRes.data?.[0]?.position_group || null

  // Process teammate injuries — look up team_id, then enrich with position + avg stats
  let teammateInjuries: Array<Record<string, unknown>> = []
  const teamLookup = await supabase
    .from('player_game_stats')
    .select('team_id')
    .eq('player_id', prediction.player_id)
    .order('game_date', { ascending: false })
    .limit(1)

  const playerTeamId = teamLookup.data?.[0]?.team_id

  if (playerTeamId) {
    const injRes = await supabase
      .from('rapidapi_injuries')
      .select('player, player_id, status, reason, injury_detail, report_date')
      .eq('nba_team_id', playerTeamId)
      .neq('player_id', prediction.player_id)
      .in('status', ['Out', 'Questionable'])
      .order('report_date', { ascending: false })
      .limit(15)

    if (injRes.data) {
      // Deduplicate by player name (keep latest)
      const seen = new Set<string>()
      teammateInjuries = injRes.data.filter(row => {
        if (seen.has(row.player)) return false
        seen.add(row.player)
        return true
      })

      // Enrich injured teammates with position + recent averages
      const injuredPlayerIds = teammateInjuries
        .map(t => t.player_id as number)
        .filter(Boolean)

      if (injuredPlayerIds.length > 0) {
        const [posRes, avgRes] = await Promise.all([
          supabase
            .from('players')
            .select('player_id, primary_position')
            .in('player_id', injuredPlayerIds),
          supabase
            .from('player_average_game_stats')
            .select('player_id, avg_min_l15, avg_pts_l15, avg_reb_l15, avg_ast_l15')
            .in('player_id', injuredPlayerIds)
            .order('game_date', { ascending: false }),
        ])

        // Build lookup maps
        const posMap = new Map<number, string>()
        for (const p of posRes.data || []) {
          posMap.set(p.player_id, p.primary_position)
        }

        // For averages, keep only the most recent row per player
        const avgMap = new Map<number, Record<string, unknown>>()
        for (const a of avgRes.data || []) {
          if (!avgMap.has(a.player_id)) {
            avgMap.set(a.player_id, a)
          }
        }

        // Enrich each teammate injury
        for (const inj of teammateInjuries) {
          const pid = inj.player_id as number
          if (posMap.has(pid)) {
            inj._position = posMap.get(pid)
          }
          const avg = avgMap.get(pid)
          if (avg) {
            const min = Number(avg.avg_min_l15)
            const pts = Number(avg.avg_pts_l15)
            const reb = Number(avg.avg_reb_l15)
            const ast = Number(avg.avg_ast_l15)
            if (!isNaN(min)) {
              inj._avg_stats = `avg ${min.toFixed(1)} min, ${pts.toFixed(1)} pts, ${reb.toFixed(1)} reb, ${ast.toFixed(1)} ast`
            }
          }
        }
      }
    }
  }

  // Process opponent defense + opponent injuries — look up opponent team_id
  let oppDefense: Record<string, unknown> | null = null
  let oppInjuries: Array<Record<string, unknown>> = []
  if (oppAbbrev) {
    // Map player position_group (Guard/Forward/Big) to defense group (G/W/B)
    const defPosGroup = playerPositionGroup
      ? POSITION_GROUP_TO_DEFENSE[playerPositionGroup] || 'G'
      : 'G'

    // Look up opponent team_id from team_game_stats
    const oppTeamRes = await supabase
      .from('team_game_stats')
      .select('team_id')
      .eq('team_abbreviation', oppAbbrev)
      .limit(1)

    const oppTeamId = oppTeamRes.data?.[0]?.team_id
    if (oppTeamId) {
      // Parallel: opponent defense + opponent injuries
      const [oppDefRes, oppInjRes] = await Promise.all([
        supabase
          .from('team_allowed_by_position')
          .select('*')
          .eq('team_id', oppTeamId)
          .eq('position_group', defPosGroup)
          .order('game_date', { ascending: false })
          .limit(1),
        supabase
          .from('rapidapi_injuries')
          .select('player, player_id, status, reason, injury_detail, report_date')
          .eq('nba_team_id', oppTeamId)
          .in('status', ['Out', 'Questionable'])
          .order('report_date', { ascending: false })
          .limit(15),
      ])

      if (oppDefRes.data?.[0]) {
        oppDefense = oppDefRes.data[0] as Record<string, unknown>
      }

      if (oppInjRes.data) {
        // Deduplicate by player name (keep latest)
        const seen = new Set<string>()
        const deduped = oppInjRes.data.filter(row => {
          if (seen.has(row.player)) return false
          seen.add(row.player)
          return true
        })

        // Cast to Record for dynamic property enrichment
        const oppInjList = deduped as Array<Record<string, unknown>>

        // Enrich opponent injuries with position + averages
        const oppInjPlayerIds = oppInjList.map(t => t.player_id as number).filter(Boolean)
        if (oppInjPlayerIds.length > 0) {
          const [oppPosRes, oppAvgRes] = await Promise.all([
            supabase
              .from('players')
              .select('player_id, primary_position')
              .in('player_id', oppInjPlayerIds),
            supabase
              .from('player_average_game_stats')
              .select('player_id, avg_min_l15, avg_pts_l15, avg_reb_l15, avg_ast_l15')
              .in('player_id', oppInjPlayerIds)
              .order('game_date', { ascending: false }),
          ])

          const oppPosMap = new Map<number, string>()
          for (const p of oppPosRes.data || []) {
            oppPosMap.set(p.player_id, p.primary_position)
          }
          const oppAvgMap = new Map<number, Record<string, unknown>>()
          for (const a of oppAvgRes.data || []) {
            if (!oppAvgMap.has(a.player_id)) oppAvgMap.set(a.player_id, a)
          }

          for (const inj of oppInjList) {
            const pid = inj.player_id as number
            if (oppPosMap.has(pid)) inj._position = oppPosMap.get(pid)
            const avg = oppAvgMap.get(pid)
            if (avg) {
              const min = Number(avg.avg_min_l15)
              const pts = Number(avg.avg_pts_l15)
              const reb = Number(avg.avg_reb_l15)
              const ast = Number(avg.avg_ast_l15)
              if (!isNaN(min)) {
                inj._avg_stats = `avg ${min.toFixed(1)} min, ${pts.toFixed(1)} pts, ${reb.toFixed(1)} reb, ${ast.toFixed(1)} ast`
              }
            }
          }
        }

        oppInjuries = oppInjList
      }
    }
  }

  const gameLog = (gameLogRes.data || []) as Array<Record<string, unknown>>
  const rollingAvgs = (rollingAvgRes.data?.[0] || null) as Record<string, unknown> | null
  const playerInjury = (playerInjuryRes.data?.[0] || null) as Record<string, unknown> | null
  const vsOpponentLog = (vsOpponentRes.data || []) as Array<Record<string, unknown>>

  // Build system prompt
  const systemPrompt = buildSystemPrompt(
    prediction,
    gameLog,
    rollingAvgs,
    playerInjury,
    teammateInjuries,
    oppDefense,
    oppInjuries,
    vsOpponentLog,
    insights,
    bookmakerLines,
    isOverBet,
    edge,
    probability,
    playerPosition,
  )

  // Build messages (keep last 5 from conversation history)
  const trimmedHistory = conversationHistory.slice(-5)
  const messages: Array<{ role: 'user' | 'assistant'; content: string }> = [
    ...trimmedHistory.map(m => ({ role: m.role, content: m.content })),
    { role: 'user' as const, content: question },
  ]

  // Call Claude
  try {
    const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })

    const response = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1024,
      system: systemPrompt,
      messages,
    })

    const answer = response.content
      .filter(block => block.type === 'text')
      .map(block => block.text)
      .join('')

    return NextResponse.json({ answer, remaining })
  } catch (err) {
    console.error('Anthropic API error:', err)
    return NextResponse.json(
      { error: 'Failed to generate response. Please try again.', remaining },
      { status: 500 }
    )
  }
}
