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
  advancedStatsMap: Map<string, Record<string, unknown>>,
  depthChart: Array<Record<string, unknown>>,
  playerPositionGroup: string | null,
  injuryTimeline: Array<{ player: string; transitions: string[] }>,
): string {
  const stat = prediction.stat
  const statLabel = STAT_LABELS[stat] || stat.toUpperCase()
  const direction = isOverBet ? 'Over' : 'Under'
  const combo = isComboStat(stat)
  const components = COMBO_COMPONENTS[stat]

  // Game log — two-tier format: detailed (1-10), condensed (11-25), with advanced stats
  let gameLogSection = 'No recent game data available.'
  if (gameLog.length > 0) {
    const rows = gameLog.map((g: Record<string, unknown>, i: number) => {
      const num = `#${i + 1}`
      const shortDate = formatShortDate(String(g.game_date || ''))
      const gameId = String(g.game_id || '')
      const adv = advancedStatsMap.get(gameId)
      const usg = adv?.usage_percentage != null ? `USG ${Number(adv.usage_percentage).toFixed(1)}%` : null
      const offRtg = adv?.offensive_rating != null ? `OffRtg ${Number(adv.offensive_rating).toFixed(0)}` : null
      const nRtg = adv?.net_rating != null ? `NRtg ${Number(adv.net_rating) >= 0 ? '+' : ''}${Number(adv.net_rating).toFixed(0)}` : null

      // Compact matchup: "vs HOU" or "@ BOS" from "MIA vs. HOU"
      const matchup = String(g.matchup || '')
      const matchParts = matchup.split(/\s+(vs\.?|@)\s+/)
      const oppStr = matchParts.length >= 3
        ? `${matchParts[1].replace('.', '')} ${matchParts[2]}`
        : matchup

      // Condensed format for games 11-25
      if (i >= 10) {
        const comboTotal = combo ? ` | ${statLabel}=${(components || []).reduce((s: number, c: string) => s + (Number(g[c]) || 0), 0)}` : ''
        const advStr = usg ? ` | ${usg}` : ''
        return `${num.padEnd(4)}${shortDate.padEnd(6)}${oppStr.padEnd(8)}| ${String(g.min ?? '-').padEnd(3)} MIN | ${g.pts ?? '-'}/${g.reb ?? '-'}/${g.ast ?? '-'} PTS/REB/AST${comboTotal}${advStr} | ${g.wl ?? '-'}`
      }

      // Detailed format for games 1-10
      const oreb = g.oreb ?? 0
      const advParts = [usg, offRtg, nRtg].filter(Boolean).join(' | ')
      const advStr = advParts ? ` | ${advParts}` : ''
      const base = `${num.padEnd(4)}${shortDate.padEnd(6)}${oppStr.padEnd(8)}| ${String(g.min ?? '-').padEnd(3)} MIN | ${String(g.pts ?? '-')} PTS | ${g.reb ?? '-'} REB (${oreb} OREB) | ${g.ast ?? '-'} AST`

      if (combo) {
        const total = (components || []).reduce((s: number, c: string) => s + (Number(g[c]) || 0), 0)
        return `${base} | ${statLabel}=${total}${advStr} | ${g.wl ?? '-'} | ${g.started ? 'Started' : 'Bench'}`
      }
      return `${base} | ${g.stl ?? '-'} STL | ${g.blk ?? '-'} BLK | ${g.fg3m ?? '-'} 3PM | ${g.tov ?? '-'} TOV | ${g.fga ?? '-'} FGA | ${g.fta ?? '-'} FTA${advStr} | ${g.wl ?? '-'} | ${g.started ? 'Started' : 'Bench'}`
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

  // Positional depth chart
  let depthChartSection = ''
  if (depthChart.length > 0) {
    const posLabel = playerPositionGroup || 'Teammates'
    const teamAbbrev = prediction.team_abbrev || '???'
    const lines = depthChart.map((p: Record<string, unknown>) => {
      const isTarget = p.player_id === prediction.player_id
      const prefix = isTarget ? '  >>> THIS PLAYER: ' : '  '
      const pos = p.position ? ` (${p.position})` : ''
      const min = Number(p.avg_min).toFixed(1)
      const pts = Number(p.avg_pts).toFixed(1)
      const reb = Number(p.avg_reb).toFixed(1)
      const ast = Number(p.avg_ast).toFixed(1)
      const role = p.is_starter ? 'Starter' : 'Bench'
      const injNote = p.injury_status ? ` [${p.injury_status}${p.injury_reason ? ` - ${p.injury_reason}` : ''}]` : ''
      return `${prefix}${p.player_name}${pos} — L5: ${min} min, ${pts} pts, ${reb} reb, ${ast} ast | ${role}${injNote}`
    })
    depthChartSection = `POSITIONAL DEPTH CHART (${posLabel} — ${teamAbbrev}):\n${lines.join('\n')}`
  }

  // Injury timeline
  let injuryTimelineSection = ''
  if (injuryTimeline.length > 0) {
    const lines = injuryTimeline.map(t => `  ${t.player}: ${t.transitions.join(' → ')}`)
    injuryTimelineSection = `TEAMMATE INJURY TIMELINE (last 45 days — status changes):\n${lines.join('\n')}`
  }

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
${injuryTimelineSection}

${depthChartSection}

${defenseSection}

${linesSection}

${insightSection}

RULES:
- Only reference data provided above. Do not make up stats or games.
- Cite specific games, dates, and numbers when relevant.
- Keep responses to 2-4 short paragraphs.
- Be direct and analytical. Avoid generic hedging like "it depends on many factors."
- You have comprehensive data. Before saying "I don't have" information, carefully re-read ALL sections above. Opponent defense IS in OPPONENT DEFENSE. Game history IS in the game log. Injury data IS in TEAMMATE INJURIES and TEAMMATE INJURY TIMELINE.
- Use markdown: **bold** for emphasis, bullet points for lists. Keep responses analytical and specific.
- When discussing trends, reference the rolling averages and game log.
- When asked about minutes or playing time, reference the MINUTES/USAGE CONTEXT section with starter probability, minutes floor, usage rate, and recent averages.
- When asked about game script, pace, or blowout risk, reference the GAME CONTEXT section. A large negative spread means the team is heavily favored. High pace + high total = more possessions = more stat opportunities.
- When asked about matchup history vs a specific team, reference the MATCHUP HISTORY section. If no matchup history exists, say so and use the opponent defense stats instead.
- When asked about opponent defense, reference both OPPONENT DEFENSE and OPPONENT INJURIES sections. Missing key players affects defensive quality.
- When asked about role, rotation, or playing time, reference the POSITIONAL DEPTH CHART showing all same-position teammates with their recent stats.
- When asked about production changes or why stats shifted, cross-reference the TEAMMATE INJURY TIMELINE dates with game log entries to identify roster-driven performance shifts.
- For combo stats (${Object.keys(COMBO_COMPONENTS).join(', ')}), break down which component stat is driving the total.
- If the user asks about something not covered by the data, say so honestly.`
}

// --- MLB stat labels ---
const MLB_STAT_LABELS: Record<string, string> = {
  pitcher_strikeouts: 'Strikeouts',
  batter_hits: 'Hits',
  batter_total_bases: 'Total Bases',
  batter_home_runs: 'Home Runs',
  batter_rbis: 'RBI',
  batter_runs_scored: 'Runs',
}

// --- MLB system prompt ---
function buildMlbSystemPrompt(
  prediction: Prediction,
  isPitcher: boolean,
  gameLog: Array<Record<string, unknown>>,
  rollingAvgs: Record<string, unknown> | null,
  playerInfo: Record<string, unknown> | null,
  gameInfo: Record<string, unknown> | null,
  parkFactors: Record<string, unknown> | null,
  oppPitcherAvgs: Record<string, unknown> | null,
  oppPitcherLog: Array<Record<string, unknown>>,
  oppPitcherInfo: Record<string, unknown> | null,
  insights: Insight[],
  bookmakerLines: BookmakerLine[],
  isOverBet: boolean,
  edge: number,
  probability: number,
): string {
  const statLabel = MLB_STAT_LABELS[prediction.stat] || prediction.stat
  const direction = isOverBet ? 'Over' : 'Under'

  // Game log
  let gameLogSection = 'No recent game data available.'
  if (gameLog.length > 0) {
    if (isPitcher) {
      const rows = gameLog.map((g, i) => {
        const date = formatShortDate(String(g.game_date || ''))
        const role = g.is_starter ? 'SP' : 'RP'
        return `#${String(i + 1).padEnd(3)}${date.padEnd(6)}| ${String(g.ip ?? '-').padEnd(5)} IP | ${g.so ?? '-'} K | ${g.h_allowed ?? '-'} H | ${g.er ?? '-'} ER | ${g.bb ?? '-'} BB | ${g.hr_allowed ?? '-'} HR | ${g.pitches_thrown ?? '-'} P | ${role}`
      })
      gameLogSection = `LAST ${gameLog.length} APPEARANCES (most recent first):\n${rows.join('\n')}`
    } else {
      const rows = gameLog.map((g, i) => {
        const date = formatShortDate(String(g.game_date || ''))
        const spot = g.lineup_position ? ` Lineup #${g.lineup_position}` : ''
        return `#${String(i + 1).padEnd(3)}${date.padEnd(6)}| ${g.ab ?? '-'} AB | ${g.h ?? '-'} H | ${g.doubles ?? '-'} 2B | ${g.hr ?? '-'} HR | ${g.rbi ?? '-'} RBI | ${g.r ?? '-'} R | ${g.tb ?? '-'} TB | ${g.bb ?? '-'} BB | ${g.so ?? '-'} K | AVG ${Number(g.avg ?? 0).toFixed(3)}${spot}`
      })
      gameLogSection = `LAST ${gameLog.length} GAMES (most recent first):\n${rows.join('\n')}`
    }
  }

  // Rolling averages
  let avgSection = ''
  if (rollingAvgs) {
    if (isPitcher) {
      avgSection = `ROLLING AVERAGES (PITCHER):
  SO: L3=${rollingAvgs.avg_so_l3 ?? '?'} | L5=${rollingAvgs.avg_so_l5 ?? '?'} | SZN=${rollingAvgs.avg_so_szn ?? '?'} (std dev L3: ${rollingAvgs.std_so_l3 ?? '?'})
  IP: L3=${rollingAvgs.avg_ip_l3 ?? '?'} | L5=${rollingAvgs.avg_ip_l5 ?? '?'} | SZN=${rollingAvgs.avg_ip_szn ?? '?'}
  ER: L3=${rollingAvgs.avg_er_l3 ?? '?'} | L5=${rollingAvgs.avg_er_l5 ?? '?'}
  ERA L5: ${rollingAvgs.avg_era_l5 ?? '?'} | WHIP L5: ${rollingAvgs.avg_whip_l5 ?? '?'} | K/9 L5: ${rollingAvgs.avg_k_per_9_l5 ?? '?'} | BB/9 L5: ${rollingAvgs.avg_bb_per_9_l5 ?? '?'}
  Pitches L5: ${rollingAvgs.avg_pitches_thrown_l5 ?? '?'} | Days rest: ${rollingAvgs.days_rest ?? '?'} | Pitch count last start: ${rollingAvgs.pitch_count_last_start ?? '?'}`
    } else {
      avgSection = `ROLLING AVERAGES (BATTER):
  H: L5=${rollingAvgs.avg_h_l5 ?? '?'} | L10=${rollingAvgs.avg_h_l10 ?? '?'} | SZN=${rollingAvgs.avg_h_szn ?? '?'}
  HR: L5=${rollingAvgs.avg_hr_l5 ?? '?'} | L10=${rollingAvgs.avg_hr_l10 ?? '?'} | SZN=${rollingAvgs.avg_hr_szn ?? '?'}
  TB: L5=${rollingAvgs.avg_tb_l5 ?? '?'} | L10=${rollingAvgs.avg_tb_l10 ?? '?'} | SZN=${rollingAvgs.avg_tb_szn ?? '?'}
  RBI: L5=${rollingAvgs.avg_rbi_l5 ?? '?'} | L10=${rollingAvgs.avg_rbi_l10 ?? '?'} | SZN=${rollingAvgs.avg_rbi_szn ?? '?'}
  R: L5=${rollingAvgs.avg_r_l5 ?? '?'} | L10=${rollingAvgs.avg_r_l10 ?? '?'} | SZN=${rollingAvgs.avg_r_szn ?? '?'}
  AB L5: ${rollingAvgs.avg_ab_l5 ?? '?'} | L10: ${rollingAvgs.avg_ab_l10 ?? '?'} | BB L5: ${rollingAvgs.avg_bb_l5 ?? '?'} | SO L5: ${rollingAvgs.avg_so_l5 ?? '?'}
  AVG L10: ${rollingAvgs.avg_batting_avg_l10 ?? '?'} | OBP: ${rollingAvgs.avg_obp_l10 ?? '?'} | SLG: ${rollingAvgs.avg_slg_l10 ?? '?'} | OPS: ${rollingAvgs.avg_ops_l10 ?? '?'}
  Rest days: ${rollingAvgs.rest_days ?? '?'} | Games last 7 days: ${rollingAvgs.games_last_7d ?? '?'}`
    }
  }

  // Park factors
  let parkSection = ''
  if (parkFactors) {
    const rf = Number(parkFactors.runs_factor ?? 1).toFixed(3)
    const hrf = Number(parkFactors.hr_factor ?? 1).toFixed(3)
    const hf = Number(parkFactors.hits_factor ?? 1).toFixed(3)
    const sof = Number(parkFactors.so_factor ?? 1).toFixed(3)
    parkSection = `PARK FACTORS (${parkFactors.venue_name || 'Stadium'}, 1.000 = league avg, >1.000 = inflates stat):
  Runs: ${rf} | HR: ${hrf} | Hits: ${hf} | K: ${sof}`
  }

  // Game context
  let gameSection = ''
  if (gameInfo) {
    gameSection = `GAME CONTEXT:
  Venue: ${gameInfo.venue_name || 'Unknown'}
  Matchup: ${prediction.team_abbrev || '???'} vs ${prediction.opponent_abbrev || '???'}`
  }

  // Opposing pitcher (for batters only)
  let oppPitcherSection = ''
  if (!isPitcher && (oppPitcherAvgs || oppPitcherLog.length > 0)) {
    const name = oppPitcherInfo?.player_name as string || 'Probable Pitcher'
    const throws = oppPitcherInfo?.throws as string || '?'
    const lines = [`OPPOSING PITCHER: ${name} (Throws: ${throws})`]
    if (oppPitcherAvgs) {
      lines.push(
        `  ERA L5: ${oppPitcherAvgs.avg_era_l5 ?? '?'} | WHIP L5: ${oppPitcherAvgs.avg_whip_l5 ?? '?'} | K/9 L5: ${oppPitcherAvgs.avg_k_per_9_l5 ?? '?'} | BB/9 L5: ${oppPitcherAvgs.avg_bb_per_9_l5 ?? '?'}`,
        `  SO avg: L3=${oppPitcherAvgs.avg_so_l3 ?? '?'} | L5=${oppPitcherAvgs.avg_so_l5 ?? '?'} | IP L5: ${oppPitcherAvgs.avg_ip_l5 ?? '?'} | H allowed L5: ${oppPitcherAvgs.avg_h_allowed_l5 ?? '?'}`,
        `  Days rest: ${oppPitcherAvgs.days_rest ?? '?'}`,
      )
    }
    if (oppPitcherLog.length > 0) {
      const startRows = oppPitcherLog.map((g, i) =>
        `    #${i + 1} ${formatShortDate(String(g.game_date || ''))}: ${g.ip ?? '-'} IP | ${g.so ?? '-'} K | ${g.h_allowed ?? '-'} H | ${g.er ?? '-'} ER | ${g.bb ?? '-'} BB`
      )
      lines.push(`  Last ${oppPitcherLog.length} starts:\n${startRows.join('\n')}`)
    }
    oppPitcherSection = lines.join('\n')
  }

  // Sportsbook lines
  let linesSection = ''
  if (bookmakerLines.length > 0) {
    const lineStrs = bookmakerLines.slice(0, 6).map(l =>
      `  ${l.bookmaker}: Line ${l.line} | Over ${l.over_odds >= 0 ? '+' : ''}${l.over_odds} | Under ${l.under_odds >= 0 ? '+' : ''}${l.under_odds}`
    )
    linesSection = `SPORTSBOOK LINES:\n${lineStrs.join('\n')}`
  }

  // Model prediction
  const isBinary = prediction.q10 === 0 && prediction.q25 === 0 && prediction.q50 === 0 && prediction.q90 <= 1
  const quantileSection = isBinary
    ? `MODEL PREDICTION (Binary — predicts whether player records any ${statLabel.toLowerCase()}):
  P(${statLabel} ≥ 1): ${(probability * 100).toFixed(1)}% | P(No ${statLabel}): ${((1 - probability) * 100).toFixed(1)}%
  Bet: ${direction} ${prediction.prop_line} | Edge: ${(edge * 100).toFixed(1)}%`
    : `MODEL QUANTILE PREDICTIONS:
  Q10=${prediction.q10} | Q25=${prediction.q25} | Q50=${prediction.q50} | Q75=${prediction.q75} | Q90=${prediction.q90}
  Prop Line: ${prediction.prop_line} | Direction: ${direction} | Probability: ${(probability * 100).toFixed(1)}% | Edge: ${(edge * 100).toFixed(1)}%`

  // Insights (usually empty for MLB since feat_ fields are NBA-specific)
  let insightSection = ''
  if (insights.length > 0) {
    insightSection = `MODEL INSIGHTS:\n${insights.map(i => `  [${i.sentiment}] ${i.text}`).join('\n')}`
  }

  const playerPos = playerInfo ? ` | Position: ${playerInfo.primary_position || '?'} | ${isPitcher ? `Throws: ${playerInfo.throws || '?'}` : `Bats: ${playerInfo.bats || '?'}`}` : ''

  const sections = [gameLogSection, avgSection, gameSection, parkSection, oppPitcherSection, quantileSection, linesSection, insightSection].filter(Boolean)

  return `You are a sharp MLB analytics assistant for the GameFlowData platform. The user is analyzing a player prop bet and has questions.

PLAYER: ${prediction.player_name || 'Unknown'} (${prediction.team_abbrev || '???'})${playerPos}
OPPONENT: ${prediction.opponent_abbrev || '???'}
STAT: ${statLabel}
BET: ${direction} ${prediction.prop_line}

${sections.join('\n\n')}

RULES:
- Only reference data provided above. Do not make up stats or games.
- Cite specific games, dates, and numbers when relevant.
- Keep responses to 2-4 short paragraphs.
- Be direct and analytical. Avoid generic hedging like "it depends on many factors."
- Use markdown: **bold** for emphasis, bullet points for lists.
- ${isPitcher
    ? 'When discussing K trends, cite the SO averages and K/9 rate. Mention days rest, pitch count last start, and whether the range (std dev) is consistent or volatile.'
    : 'When discussing hit probability, reference both the rolling averages and the OPPOSING PITCHER section. Pitcher handedness, K/9, and ERA are crucial context. For binary stats like HR, note recent HR rate vs historical.'
  }
- Park factors above 1.000 inflate that stat vs league average; below 1.000 suppresses it.
- ${!isPitcher ? 'The opposing pitcher section is critical — a high-K/9 pitcher suppresses hit probability. A low-ERA pitcher is harder to score off.' : 'For pitcher strikeout props, K/9 rate and IP (how long they stay in) are the primary drivers.'}
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

  // --- MLB branch ---
  const isMlb = prediction.stat.startsWith('batter_') || prediction.stat.startsWith('pitcher_')

  if (isMlb) {
    const isPitcher = prediction.stat.startsWith('pitcher_')

    // Round 1: parallel base queries
    const [gameLogRes, rollingAvgRes, playerInfoRes, gameInfoRes] = await Promise.all([
      isPitcher
        ? supabase
            .from('mlb_player_game_stats_pitching')
            .select('game_date, ip, so, h_allowed, er, bb, hr_allowed, pitches_thrown, is_starter, era')
            .eq('player_id', prediction.player_id)
            .eq('did_not_play', false)
            .order('game_date', { ascending: false })
            .limit(10)
        : supabase
            .from('mlb_player_game_stats_batting')
            .select('game_date, lineup_position, is_starter, pa, ab, h, doubles, hr, rbi, r, bb, so, tb, avg')
            .eq('player_id', prediction.player_id)
            .eq('did_not_play', false)
            .order('game_date', { ascending: false })
            .limit(15),
      isPitcher
        ? supabase
            .from('mlb_player_average_pitching')
            .select('avg_so_l3, avg_so_l5, avg_so_szn, avg_ip_l3, avg_ip_l5, avg_ip_szn, avg_er_l3, avg_er_l5, avg_era_l5, avg_whip_l5, avg_k_per_9_l5, avg_bb_per_9_l5, avg_pitches_thrown_l5, std_so_l3, days_rest, pitch_count_last_start')
            .eq('player_id', prediction.player_id)
            .order('game_date', { ascending: false })
            .limit(1)
        : supabase
            .from('mlb_player_average_batting')
            .select('avg_h_l5, avg_h_l10, avg_h_szn, avg_hr_l5, avg_hr_l10, avg_hr_szn, avg_tb_l5, avg_tb_l10, avg_tb_szn, avg_rbi_l5, avg_rbi_l10, avg_rbi_szn, avg_r_l5, avg_r_l10, avg_r_szn, avg_ab_l5, avg_ab_l10, avg_bb_l5, avg_so_l5, avg_batting_avg_l10, avg_obp_l10, avg_slg_l10, avg_ops_l10, rest_days, games_last_7d')
            .eq('player_id', prediction.player_id)
            .order('game_date', { ascending: false })
            .limit(1),
      supabase
        .from('mlb_players')
        .select('player_name, primary_position, bats, throws')
        .eq('player_id', prediction.player_id)
        .limit(1),
      prediction.game_id
        ? supabase
            .from('mlb_game_schedule')
            .select('home_team_id, away_team_id, venue_id, venue_name, probable_pitcher_home_id, probable_pitcher_away_id')
            .eq('game_id', prediction.game_id)
            .limit(1)
        : Promise.resolve({ data: [], error: null }),
    ])

    const mlbGameLog = (gameLogRes.data || []) as Array<Record<string, unknown>>
    const mlbRollingAvgs = (rollingAvgRes.data?.[0] || null) as Record<string, unknown> | null
    const mlbPlayerInfo = (playerInfoRes.data?.[0] || null) as Record<string, unknown> | null
    const mlbGameInfo = (gameInfoRes.data?.[0] || null) as Record<string, unknown> | null

    // Round 2: park factors + player team_id + opp pitcher (parallel)
    let parkFactors: Record<string, unknown> | null = null
    let oppPitcherAvgs: Record<string, unknown> | null = null
    let oppPitcherLog: Array<Record<string, unknown>> = []
    let oppPitcherInfo: Record<string, unknown> | null = null

    if (mlbGameInfo) {
      const venueId = mlbGameInfo.venue_id as number | null

      // Get player team_id to determine home/away
      const teamStatTable = isPitcher ? 'mlb_player_game_stats_pitching' : 'mlb_player_game_stats_batting'
      const playerTeamRes = await supabase
        .from(teamStatTable)
        .select('team_id')
        .eq('player_id', prediction.player_id)
        .order('game_date', { ascending: false })
        .limit(1)

      const playerTeamId = playerTeamRes.data?.[0]?.team_id as number | null

      // Determine opposing pitcher id
      let oppPitcherId: number | null = null
      if (playerTeamId && !isPitcher) {
        const isHome = playerTeamId === (mlbGameInfo.home_team_id as number)
        const homeId = mlbGameInfo.probable_pitcher_home_id as number | null
        const awayId = mlbGameInfo.probable_pitcher_away_id as number | null
        oppPitcherId = isHome ? awayId : homeId
      }

      const [pfRes, oppAvgRes, oppLogRes, oppInfoRes] = await Promise.all([
        venueId
          ? supabase
              .from('mlb_park_factors')
              .select('runs_factor, hr_factor, hits_factor, so_factor, venue_name')
              .eq('venue_id', venueId)
              .order('season', { ascending: false })
              .limit(1)
          : Promise.resolve({ data: null, error: null }),
        oppPitcherId
          ? supabase
              .from('mlb_player_average_pitching')
              .select('avg_so_l3, avg_so_l5, avg_ip_l5, avg_era_l5, avg_whip_l5, avg_k_per_9_l5, avg_bb_per_9_l5, avg_h_allowed_l5, days_rest')
              .eq('player_id', oppPitcherId)
              .order('game_date', { ascending: false })
              .limit(1)
          : Promise.resolve({ data: null, error: null }),
        oppPitcherId
          ? supabase
              .from('mlb_player_game_stats_pitching')
              .select('game_date, ip, so, h_allowed, er, bb, pitches_thrown')
              .eq('player_id', oppPitcherId)
              .eq('did_not_play', false)
              .order('game_date', { ascending: false })
              .limit(5)
          : Promise.resolve({ data: null, error: null }),
        oppPitcherId
          ? supabase
              .from('mlb_players')
              .select('player_name, throws')
              .eq('player_id', oppPitcherId)
              .limit(1)
          : Promise.resolve({ data: null, error: null }),
      ])

      parkFactors = (pfRes.data as Array<Record<string, unknown>> | null)?.[0] ?? null
      if (oppPitcherId) {
        oppPitcherAvgs = (oppAvgRes.data as Array<Record<string, unknown>> | null)?.[0] ?? null
        oppPitcherLog = (oppLogRes.data as Array<Record<string, unknown>> | null) ?? []
        oppPitcherInfo = (oppInfoRes.data as Array<Record<string, unknown>> | null)?.[0] ?? null
      }
    }

    // Build MLB system prompt
    const mlbSystemPrompt = buildMlbSystemPrompt(
      prediction,
      isPitcher,
      mlbGameLog,
      mlbRollingAvgs,
      mlbPlayerInfo,
      mlbGameInfo,
      parkFactors,
      oppPitcherAvgs,
      oppPitcherLog,
      oppPitcherInfo,
      insights,
      bookmakerLines,
      isOverBet,
      edge,
      probability,
    )

    // Call Claude
    const mlbMessages: Array<{ role: 'user' | 'assistant'; content: string }> = [
      ...conversationHistory.slice(-5).map(m => ({ role: m.role, content: m.content })),
      { role: 'user' as const, content: question },
    ]

    try {
      const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
      const response = await anthropic.messages.create({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 2048,
        system: mlbSystemPrompt,
        messages: mlbMessages,
      })

      const answer = response.content.filter(b => b.type === 'text').map(b => b.text).join('')

      const gameDate = prediction.prediction_date || new Date().toISOString().split('T')[0]
      const persistPromise = (async () => {
        try {
          const { data: convData } = await supabase
            .from('chat_conversations')
            .upsert({
              user_id: user.id,
              player_id: prediction.player_id,
              player_name: prediction.player_name || 'Unknown',
              stat: prediction.stat,
              game_date: gameDate,
              updated_at: new Date().toISOString(),
            }, { onConflict: 'user_id,player_id,stat,game_date' })
            .select('id')
            .single()

          if (convData?.id) {
            await supabase.from('chat_messages').insert([
              { conversation_id: convData.id, role: 'user', content: question.trim() },
              { conversation_id: convData.id, role: 'assistant', content: answer },
            ])
          }
          return convData?.id ?? null
        } catch (e) {
          console.error('MLB chat persistence error:', e)
          return null
        }
      })()

      const conversationId = await Promise.race([
        persistPromise,
        new Promise<null>(resolve => setTimeout(() => resolve(null), 2000)),
      ])

      return NextResponse.json({ answer, remaining, conversation_id: conversationId })
    } catch (err) {
      console.error('MLB Anthropic API error:', err)
      return NextResponse.json({ error: 'Failed to generate response. Please try again.', remaining }, { status: 500 })
    }
  }

  // --- NBA branch (unchanged) ---
  const oppAbbrev = prediction.opponent_abbrev || prediction.feat_opp_team_abbrev

  // --- Data enrichment: 5 parallel queries ---
  const [gameLogRes, rollingAvgRes, playerInjuryRes, playerPosRes, vsOpponentRes] = await Promise.all([
    // 1. Extended game log (last 25) — includes oreb, dreb, tov, fga, fta, game_id
    supabase
      .from('player_game_stats')
      .select('game_id, game_date, pts, reb, ast, fg3m, min, matchup, wl, started, stl, blk, oreb, dreb, tov, fga, fta')
      .eq('player_id', prediction.player_id)
      .gt('min', 0)
      .order('game_date', { ascending: false })
      .limit(25),

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

  // Extract game IDs for advanced stats lookup
  const gameLog = (gameLogRes.data || []) as Array<Record<string, unknown>>
  const gameIds = gameLog.map(g => String(g.game_id)).filter(Boolean)

  // Advanced stats query — parallel with team lookup
  const [advancedStatsRes, teamLookup] = await Promise.all([
    gameIds.length > 0
      ? supabase
          .from('player_game_advanced_stats')
          .select('game_id, usage_percentage, offensive_rating, net_rating, pace')
          .eq('player_id', prediction.player_id)
          .in('game_id', gameIds)
      : Promise.resolve({ data: [], error: null }),
    supabase
      .from('player_game_stats')
      .select('team_id')
      .eq('player_id', prediction.player_id)
      .order('game_date', { ascending: false })
      .limit(1),
  ])

  // Build advanced stats map
  const advancedStatsMap = new Map<string, Record<string, unknown>>()
  for (const row of advancedStatsRes.data || []) {
    advancedStatsMap.set(row.game_id, row as Record<string, unknown>)
  }

  // Process teammate injuries — look up team_id, then enrich with position + avg stats
  let teammateInjuries: Array<Record<string, unknown>> = []
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

  const rollingAvgs = (rollingAvgRes.data?.[0] || null) as Record<string, unknown> | null
  const playerInjury = (playerInjuryRes.data?.[0] || null) as Record<string, unknown> | null
  const vsOpponentLog = (vsOpponentRes.data || []) as Array<Record<string, unknown>>

  // --- Depth chart + injury timeline (parallel, both need playerTeamId) ---
  let depthChart: Array<Record<string, unknown>> = []
  const injuryTimeline: Array<{ player: string; transitions: string[] }> = []

  if (playerTeamId) {
    const fortyFiveDaysAgo = new Date()
    fortyFiveDaysAgo.setDate(fortyFiveDaysAgo.getDate() - 45)
    const fortyFiveDaysAgoStr = fortyFiveDaysAgo.toISOString().split('T')[0]

    const [depthChartRes, injTimelineRes] = await Promise.all([
      // Depth chart: recent teammates on same team (last ~15 games worth)
      supabase
        .from('player_game_stats')
        .select('player_id, min, pts, reb, ast, fg3m, started, game_date')
        .eq('team_id', playerTeamId)
        .gt('min', 0)
        .order('game_date', { ascending: false })
        .limit(200),

      // Injury timeline: all status reports for teammates over 45 days
      supabase
        .from('rapidapi_injuries')
        .select('player, player_id, status, reason, report_date')
        .eq('nba_team_id', playerTeamId)
        .neq('player_id', prediction.player_id)
        .gte('report_date', fortyFiveDaysAgoStr)
        .order('report_date', { ascending: true })
        .order('player', { ascending: true }),
    ])

    // --- Process depth chart ---
    if (depthChartRes.data && depthChartRes.data.length > 0) {
      // Group by player_id, compute L5 averages
      const playerStats = new Map<number, { games: Array<Record<string, unknown>> }>()
      for (const row of depthChartRes.data) {
        const pid = row.player_id as number
        if (!playerStats.has(pid)) playerStats.set(pid, { games: [] })
        const entry = playerStats.get(pid)!
        if (entry.games.length < 5) entry.games.push(row as Record<string, unknown>)
      }

      // Get positions for all teammates (reuse posMap pattern)
      const allTeammateIds = Array.from(playerStats.keys())
      const [teamPosRes, teamInjRes] = await Promise.all([
        supabase
          .from('players')
          .select('player_id, player_name, primary_position, position_group')
          .in('player_id', allTeammateIds),
        // Current injury status for depth chart annotations
        supabase
          .from('rapidapi_injuries')
          .select('player_id, status, reason')
          .eq('nba_team_id', playerTeamId)
          .in('status', ['Out', 'Questionable', 'Doubtful'])
          .order('report_date', { ascending: false })
          .limit(30),
      ])

      const teamPosMap = new Map<number, { name: string; position: string; group: string }>()
      for (const p of teamPosRes.data || []) {
        teamPosMap.set(p.player_id, {
          name: p.player_name,
          position: p.primary_position,
          group: p.position_group,
        })
      }

      // Deduplicated injury status map
      const teamInjMap = new Map<number, { status: string; reason: string }>()
      for (const inj of teamInjRes.data || []) {
        if (!teamInjMap.has(inj.player_id)) {
          teamInjMap.set(inj.player_id, { status: inj.status, reason: inj.reason })
        }
      }

      // Filter to same position group as target player, compute averages
      const targetGroup = playerPositionGroup || 'Guard'
      const depthEntries: Array<Record<string, unknown>> = []

      for (const [pid, { games }] of playerStats) {
        const info = teamPosMap.get(pid)
        if (!info) continue
        // Include target player + same position group teammates
        if (info.group !== targetGroup && pid !== prediction.player_id) continue
        if (games.length < 2) continue // skip players with barely any games

        const avg = (field: string) => games.reduce((s, g) => s + (Number(g[field]) || 0), 0) / games.length
        const injStatus = teamInjMap.get(pid)

        depthEntries.push({
          player_id: pid,
          player_name: info.name,
          position: info.position,
          avg_min: avg('min'),
          avg_pts: avg('pts'),
          avg_reb: avg('reb'),
          avg_ast: avg('ast'),
          is_starter: games[0]?.started === true,
          injury_status: injStatus?.status || null,
          injury_reason: injStatus?.reason || null,
        })
      }

      // Sort by avg minutes descending (starters first)
      depthEntries.sort((a, b) => Number(b.avg_min) - Number(a.avg_min))
      depthChart = depthEntries
    }

    // --- Process injury timeline ---
    if (injTimelineRes.data && injTimelineRes.data.length > 0) {
      // Group by player, detect status transitions
      const playerReports = new Map<string, Array<{ status: string; reason: string; date: string }>>()
      for (const row of injTimelineRes.data) {
        const name = row.player as string
        if (!playerReports.has(name)) playerReports.set(name, [])
        playerReports.get(name)!.push({
          status: row.status as string,
          reason: (row.reason || '') as string,
          date: row.report_date as string,
        })
      }

      for (const [player, reports] of playerReports) {
        // Keep only rows where status changed from previous
        const transitions: string[] = []
        let lastStatus = 'Available'
        for (const r of reports) {
          if (r.status !== lastStatus) {
            if (r.status === 'Available') {
              transitions.push(`Available ${formatShortDate(r.date)}`)
            } else {
              transitions.push(`${r.status} ${formatShortDate(r.date)}${r.reason ? ` (${r.reason})` : ''}`)
            }
            lastStatus = r.status
          }
        }
        // Only include players with meaningful transitions
        if (transitions.length > 0) {
          // Prepend "Available" if first transition is going out
          if (!transitions[0].startsWith('Available')) {
            transitions.unshift('Available')
          }
          // Append current status if still out
          if (lastStatus !== 'Available') {
            transitions.push(`still ${lastStatus}`)
          }
          injuryTimeline.push({ player, transitions })
        }
      }
    }
  }

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
    advancedStatsMap,
    depthChart,
    playerPositionGroup,
    injuryTimeline,
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
      max_tokens: 2048,
      system: systemPrompt,
      messages,
    })

    const answer = response.content
      .filter(block => block.type === 'text')
      .map(block => block.text)
      .join('')

    // Persist conversation (fire-and-forget — don't block the response)
    const gameDate = prediction.prediction_date || new Date().toISOString().split('T')[0]
    const persistPromise = (async () => {
      try {
        // Upsert conversation
        const { data: convData } = await supabase
          .from('chat_conversations')
          .upsert({
            user_id: user.id,
            player_id: prediction.player_id,
            player_name: prediction.player_name || 'Unknown',
            stat: prediction.stat,
            game_date: gameDate,
            updated_at: new Date().toISOString(),
          }, { onConflict: 'user_id,player_id,stat,game_date' })
          .select('id')
          .single()

        if (convData?.id) {
          // Insert both messages
          await supabase.from('chat_messages').insert([
            { conversation_id: convData.id, role: 'user', content: question.trim() },
            { conversation_id: convData.id, role: 'assistant', content: answer },
          ])
        }

        return convData?.id ?? null
      } catch (e) {
        console.error('Chat persistence error:', e)
        return null
      }
    })()

    // Wait briefly for persistence to complete so we can return conversation_id
    const conversationId = await Promise.race([
      persistPromise,
      new Promise<null>(resolve => setTimeout(() => resolve(null), 2000)),
    ])

    return NextResponse.json({ answer, remaining, conversation_id: conversationId })
  } catch (err) {
    console.error('Anthropic API error:', err)
    return NextResponse.json(
      { error: 'Failed to generate response. Please try again.', remaining },
      { status: 500 }
    )
  }
}
