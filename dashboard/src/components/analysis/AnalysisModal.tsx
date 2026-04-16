'use client'

import { useEffect, useMemo, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge, EdgeBadge } from '@/components/shared/Badge'
import { Last5Chart } from './Last5Chart'
import { QuantileSummary } from './QuantileSummary'
import { type Prediction, type PlayerGameStats, type BookmakerLine, type BetContext, STAT_LABELS } from '@/types/predictions'
import { formatProb } from '@/lib/utils'
import { generateInsights } from '@/lib/insights'
import { getAllowedBookmakers, DFS_BOOKMAKERS } from '@/lib/sportsbook-availability'
import { estimateUnderProb, americanToImpliedProb, formatBookmaker } from '@/lib/dfs-utils'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'
import { buildBetContext } from '@/lib/buildBetContext'
import { AskChat } from './AskChat'
import { useSport } from '@/contexts/SportContext'

export interface TakeBetData {
  book: string
  odds: number
  line: number
  stake: number
  modelProb: number
  edge: number
  direction: 'over' | 'under'
  betContext?: BetContext
  userConfidence?: number | null
}

interface AnalysisModalProps {
  prediction: Prediction
  onClose: () => void
  onTakeBet?: (prediction: Prediction, data: TakeBetData) => void
}

// Map stat type to column name in player_game_stats
const STAT_COLUMN_MAP: Record<string, keyof PlayerGameStats | null> = {
  pts: 'pts',
  reb: 'reb',
  ast: 'ast',
  pra: null,  // combo — computed from components
  pr: null,
  pa: null,
  ra: null,
}

// Combo stat component definitions (for L5 chart summing)
const COMBO_COMPONENTS: Record<string, (keyof PlayerGameStats)[]> = {
  pra: ['pts', 'reb', 'ast'],
  pr: ['pts', 'reb'],
  pa: ['pts', 'ast'],
  ra: ['reb', 'ast'],
}

// Map stat type to market_key in raw_player_props_combined (NBA) / mlb_raw_player_props (MLB)
const STAT_TO_MARKET: Record<string, string> = {
  pts: 'player_points',
  reb: 'player_rebounds',
  ast: 'player_assists',
  stl: 'player_steals',
  blk: 'player_blocks',
  '3pm': 'player_threes',
  pra: 'player_points_rebounds_assists',
  pr: 'player_points_rebounds',
  pa: 'player_points_assists',
  ra: 'player_rebounds_assists',
  // MLB — market_key = stat type (identity mapping)
  pitcher_strikeouts: 'pitcher_strikeouts',
  batter_hits: 'batter_hits',
  batter_total_bases: 'batter_total_bases',
  batter_home_runs: 'batter_home_runs',
  batter_rbis: 'batter_rbis',
  batter_runs_scored: 'batter_runs_scored',
}

// MLB stat → table + column for game history
const MLB_STAT_HISTORY: Record<string, { table: string; column: string }> = {
  pitcher_strikeouts: { table: 'mlb_player_game_stats_pitching', column: 'so' },
  batter_hits: { table: 'mlb_player_game_stats_batting', column: 'h' },
  batter_total_bases: { table: 'mlb_player_game_stats_batting', column: 'tb' },
  batter_home_runs: { table: 'mlb_player_game_stats_batting', column: 'hr' },
  batter_rbis: { table: 'mlb_player_game_stats_batting', column: 'rbi' },
  batter_runs_scored: { table: 'mlb_player_game_stats_batting', column: 'r' },
}

// Format odds for display
const formatOdds = (odds: number): string => {
  return odds >= 0 ? `+${odds}` : `${odds}`
}

// Calculate Kelly stake as fraction of bankroll
const calculateKelly = (modelProb: number, odds: number, kellyFraction: number): number => {
  if (odds === 0 || modelProb <= 0 || modelProb >= 1) return 0

  // Convert to decimal odds (b = net fractional odds)
  const b = odds > 0 ? odds / 100 : 100 / Math.abs(odds)

  // Kelly formula: f = (p * (b + 1) - 1) / b
  const f = (modelProb * (b + 1) - 1) / b

  if (f <= 0) return 0

  // Apply Kelly fraction and cap at 25% max
  return Math.min(f * kellyFraction, 0.25)
}

// Kelly fraction options
const KELLY_OPTIONS = [
  { value: 0.125, label: '1/8 Kelly (Conservative)' },
  { value: 0.25, label: '1/4 Kelly (Recommended)' },
  { value: 0.5, label: '1/2 Kelly (Aggressive)' },
  { value: 1.0, label: 'Full Kelly (Max Risk)' },
]

export function AnalysisModal({ prediction, onClose, onTakeBet }: AnalysisModalProps) {
  const { config } = useSport()
  const [history, setHistory] = useState<PlayerGameStats[]>([])
  const [mlbHistoryValues, setMlbHistoryValues] = useState<number[]>([])
  const [mlbRawRows, setMlbRawRows] = useState<Record<string, unknown>[]>([])
  const [bookmakerLines, setBookmakerLines] = useState<BookmakerLine[]>([])
  const [loading, setLoading] = useState(true)
  const [linesLoading, setLinesLoading] = useState(true)

  // Cross-device synced preferences
  const { prefs, updatePref } = useUserPreferences()
  const userState = prefs.userState
  const bankroll = prefs.bankroll
  const kellyFraction = prefs.kellyFraction
  const useCustomKelly = prefs.useCustomKelly

  // Local input state for controlled text fields
  const [bankrollInput, setBankrollInput] = useState<string>(bankroll.toString())
  const [customKellyInput, setCustomKellyInput] = useState<string>(kellyFraction.toString())

  // Sync input fields when prefs load from DB
  useEffect(() => {
    setBankrollInput(prefs.bankroll.toString())
    setCustomKellyInput(prefs.kellyFraction.toString())
  }, [prefs.bankroll, prefs.kellyFraction])

  const handleBankrollChange = (value: string) => {
    setBankrollInput(value)
    const num = parseFloat(value) || 0
    updatePref('bankroll', num)
  }

  const handleKellyChange = (value: string) => {
    const num = parseFloat(value)
    setCustomKellyInput(num.toString())
    updatePref('kellyFraction', num)
  }

  const handleCustomKellyChange = (value: string) => {
    setCustomKellyInput(value)
    const num = parseFloat(value)
    if (!isNaN(num) && num >= 0 && num <= 1) {
      updatePref('kellyFraction', num)
    }
  }

  const handleKellyToggle = (useCustom: boolean) => {
    updatePref('useCustomKelly', useCustom)
  }

  useEffect(() => {
    async function fetchHistory() {
      const supabase = createClient()

      if (config.sport === 'mlb') {
        const mlbStat = MLB_STAT_HISTORY[prediction.stat]
        if (!mlbStat) { setLoading(false); return }

        const isPitching = mlbStat.table === 'mlb_player_game_stats_pitching'
        const col = mlbStat.column

        // Query the right table — use select('*') to avoid template string type issues
        const query = isPitching
          ? supabase.from('mlb_player_game_stats_pitching').select('game_date, so')
              .eq('player_id', prediction.player_id).eq('did_not_play', false)
              .order('game_date', { ascending: false }).limit(5)
          : supabase.from('mlb_player_game_stats_batting').select('game_date, h, ab, tb, hr, rbi, r')
              .eq('player_id', prediction.player_id).eq('did_not_play', false)
              .order('game_date', { ascending: false }).limit(5)

        const { data, error } = await query

        if (!error && data) {
          const reversed = [...data].reverse()
          setHistory(reversed.map((row) => ({ game_date: (row as { game_date: string }).game_date } as PlayerGameStats)))
          setMlbHistoryValues(reversed.map((row) => Number((row as Record<string, unknown>)[col]) || 0))
          setMlbRawRows(reversed as Record<string, unknown>[])
        }
        setLoading(false)
        return
      }

      // NBA
      const { data, error } = await supabase
        .from('player_game_stats')
        .select('game_date, pts, reb, ast, fg3m, min')
        .eq('player_id', prediction.player_id)
        .gt('min', 0)
        .order('game_date', { ascending: false })
        .limit(5)

      if (!error && data) {
        setHistory(data.reverse()) // Chronological order for chart
      }
      setLoading(false)
    }

    async function fetchBookmakerLines() {
      const supabase = createClient()
      const marketKey = STAT_TO_MARKET[prediction.stat]

      // Skip if no game_id
      if (!prediction.game_id) {
        setLinesLoading(false)
        return
      }

      // Pick the right props table for the sport
      const propsTable = config.sport === 'mlb' ? 'mlb_raw_player_props' : 'raw_player_props_combined'

      // Get bookmaker lines for this player/stat/game
      let query = supabase
        .from(propsTable)
        .select('bookmaker, line, outcome_label, odds_american, snapshot_time')
        .eq('player_id', prediction.player_id)
        .eq('game_id', prediction.game_id)
        .eq('market_key', marketKey)
        .not('bookmaker', 'in', `(${DFS_BOOKMAKERS.join(',')})`)
        .order('bookmaker')

      // MLB props may have unlinked rows — filter to linked only
      if (config.sport === 'mlb') {
        query = query.not('player_id', 'is', null)
      }

      const { data, error } = await query

      if (error) {
        console.error('Error fetching bookmaker lines:', error)
      }

      if (!error && data) {
        console.log('Bookmaker lines fetched:', data.length, 'rows')
        // Deduplicate: keep only the latest snapshot per bookmaker
        // Sort by snapshot_time DESC so newest rows come first
        const sorted = [...data].sort((a, b) =>
          (b.snapshot_time || '').localeCompare(a.snapshot_time || '')
        )

        const lineMap = new Map<string, BookmakerLine>()
        // Track each bookmaker+line's latest snapshot_time for staleness filtering
        const snapshotMap = new Map<string, string>()

        for (const row of sorted) {
          // Key on bookmaker+line so DraftKings 0.5 and DraftKings 2.5 are independent entries
          const key = `${row.bookmaker}:${row.line}`
          if (!lineMap.has(key)) {
            lineMap.set(key, {
              bookmaker: row.bookmaker,
              line: parseFloat(row.line),
              over_odds: 0,
              under_odds: 0,
            })
            snapshotMap.set(key, row.snapshot_time || '')
          }
          const entry = lineMap.get(key)!
          // Only set odds if not already set (first encountered = newest snapshot)
          if (row.outcome_label === 'Over' && entry.over_odds === 0) {
            entry.over_odds = row.odds_american
            entry.line = parseFloat(row.line)
          } else if (row.outcome_label === 'Under' && entry.under_odds === 0) {
            entry.under_odds = row.odds_american
          }
        }

        // Drop ghost lines: if a bookmaker's latest snapshot is >2 hours
        // behind the newest snapshot in the dataset, the line was likely pulled
        const newestSnapshot = sorted.length > 0 ? sorted[0].snapshot_time || '' : ''
        const staleCutoffMs = 2 * 60 * 60 * 1000 // 2 hours

        // Filter to complete lines that aren't stale
        const allLines = Array.from(lineMap.values())
          .filter(l => {
            if (l.over_odds === 0 || l.under_odds === 0) return false
            const bookmakerSnapshot = snapshotMap.get(`${l.bookmaker}:${l.line}`) || ''
            if (!newestSnapshot || !bookmakerSnapshot) return true // no timestamp data, keep it
            const age = new Date(newestSnapshot).getTime() - new Date(bookmakerSnapshot).getTime()
            return age <= staleCutoffMs
          })

        // Note: We'll sort by line value in the render based on bet direction
        // For now, just pass all lines - sorting happens in display
        setBookmakerLines(allLines)
      }
      setLinesLoading(false)
    }

    fetchHistory()
    fetchBookmakerLines()
  }, [prediction.player_id, prediction.game_id, prediction.stat, prediction.prop_line])

  // Determine bet direction (with NaN safety)
  const overEdge = Number.isFinite(prediction.over_edge) ? prediction.over_edge : 0
  const underEdge = Number.isFinite(prediction.under_edge) ? prediction.under_edge : 0
  const isOverBet = overEdge > underEdge
  const edge = isOverBet ? overEdge : underEdge
  const direction = isOverBet ? 'Over' : 'Under'
  const probability = isOverBet
    ? (Number.isFinite(prediction.model_prob_over) ? prediction.model_prob_over : 0)
    : (Number.isFinite(prediction.model_prob_under) ? prediction.model_prob_under : 0)
  const marketProb = isOverBet
    ? (Number.isFinite(prediction.implied_prob_over) ? prediction.implied_prob_over : 0)
    : (Number.isFinite(prediction.implied_prob_under) ? prediction.implied_prob_under : 0)

  // Get stat values for chart
  const statColumn = STAT_COLUMN_MAP[prediction.stat] ?? null
  const comboComponents = COMBO_COMPONENTS[prediction.stat]
  const statLabel = STAT_LABELS[prediction.stat]
  const isPitcherStat = config.sport === 'mlb' && prediction.stat.startsWith('pitcher_')
  const mlbStatCol = MLB_STAT_HISTORY[prediction.stat]?.column
  // Binary model: quantiles are all 0/1 (Bernoulli output), show probability instead of bars
  const isBinaryDistribution = prediction.q10 === 0 && prediction.q25 === 0 &&
    prediction.q50 === 0 && prediction.q90 <= 1

  // Calculate L5 average for the relevant stat (sum components for combos)
  const l5Avg = history.length > 0
    ? config.sport === 'mlb' && mlbHistoryValues.length > 0
      ? mlbHistoryValues.reduce((s, v) => s + v, 0) / mlbHistoryValues.length
      : comboComponents
        ? history.reduce((sum, g) =>
            sum + comboComponents.reduce((s, col) => s + (Number(g[col]) || 0), 0), 0
          ) / history.length
        : statColumn
          ? history.reduce((sum, g) => sum + (Number(g[statColumn]) || 0), 0) / history.length
          : null
    : null

  // Process bookmaker lines: compute edge per line, sort by best edge
  const processedLines = useMemo(() => {
    if (bookmakerLines.length === 0) return []
    const allowed = getAllowedBookmakers(userState)
    return [...bookmakerLines]
      .filter((line) => !allowed || allowed.includes(line.bookmaker))
      .map((line) => {
        const underProb = estimateUnderProb(
          line.line,
          prediction.q10,
          prediction.q25,
          prediction.q50,
          prediction.q75,
          prediction.q90
        )
        const overProb = 1 - underProb
        const relevantOdds = isOverBet ? line.over_odds : line.under_odds
        const modelProb = isOverBet ? overProb : underProb
        const impliedProb = americanToImpliedProb(relevantOdds)
        const lineEdge = modelProb - impliedProb
        return { ...line, modelProb, impliedProb, lineEdge, relevantOdds }
      })
      .sort((a, b) => b.lineEdge - a.lineEdge)
  }, [bookmakerLines, prediction.q10, prediction.q25, prediction.q50, prediction.q75, prediction.q90, isOverBet, userState])

  // Insights (memoized for reuse in both Model Context and AskChat)
  const insights = useMemo(() => generateInsights(prediction), [prediction])

  // Take Bet state
  const [customStake, setCustomStake] = useState<string>('')
  const [betPlaced, setBetPlaced] = useState(false)
  const [userConfidence, setUserConfidence] = useState<number | null>(null)

  // Selected line index for bet sizing (defaults to best edge = index 0)
  const [selectedLineIndex, setSelectedLineIndex] = useState<number>(0)

  // Reset selection when processedLines changes (new player, state filter, etc.)
  useEffect(() => {
    setSelectedLineIndex(0)
  }, [processedLines])

  // The line used for bet sizing: user-selected or auto-best
  const selectedLine = processedLines.length > 0 && processedLines[selectedLineIndex]?.lineEdge > 0
    ? processedLines[selectedLineIndex]
    : null

  // Lift sizing computation to useMemo for reuse in Take Bet
  const sizingData = useMemo(() => {
    const sizingOdds = selectedLine
      ? selectedLine.relevantOdds
      : (isOverBet ? prediction.best_over_odds : prediction.best_under_odds) || -110
    const sizingModelProb = selectedLine
      ? selectedLine.modelProb
      : probability
    const sizingBookmaker = selectedLine ? selectedLine.bookmaker : null
    const sizingLineVal = selectedLine ? selectedLine.line : prediction.prop_line

    const kellyPct = calculateKelly(sizingModelProb, sizingOdds, kellyFraction)
    const recommendedBet = bankroll * kellyPct

    return { sizingOdds, sizingModelProb, sizingBookmaker, sizingLineVal, kellyPct, recommendedBet }
  }, [selectedLine, isOverBet, prediction.best_over_odds, prediction.best_under_odds, prediction.prop_line, probability, kellyFraction, bankroll])

  // Sync customStake from recommended bet when sizing changes
  useEffect(() => {
    if (!betPlaced) {
      setCustomStake(sizingData.recommendedBet > 0 ? sizingData.recommendedBet.toFixed(2) : '')
    }
  }, [sizingData.recommendedBet, betPlaced])

  // Reset betPlaced and confidence when prediction changes
  useEffect(() => {
    setBetPlaced(false)
    setUserConfidence(null)
  }, [prediction.id])

  // Handle escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-2 sm:p-4">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/70" onClick={onClose} />

      {/* Modal */}
      <div className="relative bg-slate-800 rounded-lg border border-slate-700 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 text-slate-400 hover:text-slate-200 text-2xl"
        >
          &times;
        </button>

        {/* Header */}
        <div className="p-4 sm:p-6 border-b border-slate-700">
          <div className="flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-4">
            <PlayerAvatar
              playerId={prediction.player_id}
              playerName={prediction.player_name || 'Unknown'}
              size="lg"
              className="hidden sm:block"
            />
            <div className="flex-1">
              <h2 className="text-xl sm:text-2xl font-bold text-slate-50">
                {prediction.player_name || `Player ${prediction.player_id}`}
              </h2>
              <p className="text-slate-400">
                {prediction.team_abbrev || '???'} vs {prediction.opponent_abbrev || '???'}
              </p>
            </div>
            <div className="text-left sm:text-right">
              <Badge stat={prediction.stat} className="mb-1" />
              <div className="text-xl font-bold text-slate-50">
                {direction} {prediction.prop_line}
              </div>
            </div>
          </div>
        </div>

        {/* Last 5 Games */}
        <div className="p-4 sm:p-6 border-b border-slate-700">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-50">Last 5 Games</h3>
            {l5Avg !== null && (
              <div className="text-sm text-slate-400">
                L5 Avg: <span className="text-slate-200 font-medium">{l5Avg.toFixed(1)} {statLabel.toLowerCase()}</span>
              </div>
            )}
          </div>
          {loading ? (
            <div className="h-48 flex items-center justify-center text-slate-400">
              Loading...
            </div>
          ) : history.length > 0 ? (
            <>
              <Last5Chart
                games={history}
                stat={statColumn}
                line={prediction.prop_line}
                values={
                  config.sport === 'mlb' && mlbHistoryValues.length > 0
                    ? mlbHistoryValues
                    : comboComponents
                      ? history.map(g => comboComponents.reduce((s, col) => s + (Number(g[col]) || 0), 0))
                      : undefined
                }
              />
              {/* Stats table as fallback/supplement */}
              <div className="mt-4 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-slate-400 border-b border-slate-700">
                      <th className="text-left py-2 px-1.5 sm:px-2">Date</th>
                      {config.sport === 'mlb' ? (
                        isPitcherStat ? (
                          <th className="text-center py-2 px-1.5 sm:px-2">K</th>
                        ) : (
                          <>
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-500">AB</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'h' ? 'text-slate-200' : ''}`}>H</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'tb' ? 'text-slate-200' : ''}`}>TB</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'hr' ? 'text-slate-200' : ''}`}>HR</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'rbi' ? 'text-slate-200' : ''}`}>RBI</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'r' ? 'text-slate-200' : ''}`}>R</th>
                          </>
                        )
                      ) : (
                        <>
                          <th className="text-center py-2 px-1.5 sm:px-2">MIN</th>
                          <th className="text-center py-2 px-1.5 sm:px-2">PTS</th>
                          <th className="text-center py-2 px-1.5 sm:px-2">REB</th>
                          <th className="text-center py-2 px-1.5 sm:px-2">AST</th>
                          {comboComponents && (
                            <th className="text-center py-2 px-1.5 sm:px-2">{statLabel}</th>
                          )}
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((game, i) => {
                      if (config.sport === 'mlb') {
                        const rawRow = mlbRawRows[i] || {}
                        return (
                          <tr key={i} className="border-b border-slate-700/50">
                            <td className="py-2 px-1.5 sm:px-2 text-slate-300">{game.game_date}</td>
                            {isPitcherStat ? (
                              <td className="text-center py-2 px-1.5 sm:px-2 text-slate-50 font-medium">
                                {mlbHistoryValues[i] ?? '—'}
                              </td>
                            ) : (
                              <>
                                <td className="text-center py-2 px-1.5 sm:px-2 text-slate-500">
                                  {String(rawRow['ab'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'h' ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['h'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'tb' ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['tb'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'hr' ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['hr'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'rbi' ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['rbi'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'r' ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['r'] ?? '—')}
                                </td>
                              </>
                            )}
                          </tr>
                        )
                      }
                      const isComboComponent = (col: string) =>
                        comboComponents?.includes(col as keyof PlayerGameStats) ?? false
                      const highlightBase = prediction.stat === 'pts' || isComboComponent('pts')
                      const highlightReb = prediction.stat === 'reb' || isComboComponent('reb')
                      const highlightAst = prediction.stat === 'ast' || isComboComponent('ast')
                      return (
                      <tr key={i} className="border-b border-slate-700/50">
                        <td className="py-2 px-1.5 sm:px-2 text-slate-300">{game.game_date}</td>
                        <td className="text-center py-2 px-1.5 sm:px-2 text-slate-400">{game.min}</td>
                        <td className={`text-center py-2 px-1.5 sm:px-2 ${highlightBase ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                          {game.pts}
                        </td>
                        <td className={`text-center py-2 px-1.5 sm:px-2 ${highlightReb ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                          {game.reb}
                        </td>
                        <td className={`text-center py-2 px-1.5 sm:px-2 ${highlightAst ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                          {game.ast}
                        </td>
                        {comboComponents && (
                          <td className="text-center py-2 px-1.5 sm:px-2 text-amber-400 font-semibold">
                            {comboComponents.reduce((s, col) => s + (Number(game[col]) || 0), 0)}
                          </td>
                        )}
                      </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <div className="h-48 flex items-center justify-center text-slate-400">
              No game history available
            </div>
          )}
        </div>

        {/* Model Context / Insights */}
        {insights.length > 0 && (
          <div className="p-4 sm:p-6 border-b border-slate-700">
            <h3 className="text-lg font-semibold text-slate-50 mb-3">Model Context</h3>
            <div className="space-y-2">
              {insights.map((insight, i) => (
                <div
                  key={i}
                  className={`flex items-center gap-2 text-sm ${
                    insight.sentiment === 'positive'
                      ? 'text-green-400'
                      : insight.sentiment === 'negative'
                        ? 'text-red-400'
                        : 'text-slate-300'
                  }`}
                >
                  <span className="w-4 text-center">
                    {insight.sentiment === 'positive' ? '✓' : insight.sentiment === 'negative' ? '⚠' : '•'}
                  </span>
                  <span>{insight.text}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* AI Q&A */}
        {config.features.askChat && (
          <AskChat
            prediction={prediction}
            history={history}
            insights={insights}
            bookmakerLines={bookmakerLines}
            isOverBet={isOverBet}
            edge={edge}
            probability={probability}
          />
        )}

        {/* Sportsbook Lines */}
        <div className="p-4 sm:p-6 border-b border-slate-700">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-50">
              Sportsbook Lines {userState && <span className="text-xs text-slate-500">({userState} only)</span>}
            </h3>
            <div className="text-sm text-slate-400">
              Betting: <span className={isOverBet ? 'text-green-400' : 'text-red-400'}>{direction} {prediction.prop_line}</span>
            </div>
          </div>
          {linesLoading ? (
            <div className="text-slate-400 text-sm">Loading lines...</div>
          ) : processedLines.length > 0 ? (
            <div className="space-y-2">
              {(() => {
                // Get top 10 lines by edge for display
                const displayedLines = processedLines.slice(0, 10)

                // Find the line that's easiest to hit for this bet direction
                // (among displayed lines only)
                // Under: highest line is easiest | Over: lowest line is easiest
                const easiestLineValue = isOverBet
                  ? Math.min(...displayedLines.map(l => l.line))
                  : Math.max(...displayedLines.map(l => l.line))

                return displayedLines.map((line, i) => {
                    const hasPositiveEdge = line.lineEdge > 0
                    const isBestEdge = i === 0 && hasPositiveEdge
                    const isEasiestLine = line.line === easiestLineValue
                    const isSelected = i === selectedLineIndex

                    return (
                    <button
                      key={i}
                      type="button"
                      onClick={() => setSelectedLineIndex(i)}
                      className={`w-full flex items-center justify-between rounded px-3 py-2 text-left transition-colors ${
                        isSelected
                          ? 'bg-green-900/40 border border-green-500 ring-1 ring-green-500/30'
                          : hasPositiveEdge
                            ? 'bg-green-900/20 border border-green-700/40 hover:border-green-600/60'
                            : 'bg-slate-700/30 border border-transparent hover:border-slate-600'
                      }`}
                    >
                      <div className="flex items-center gap-3">
                        {/* Line value - prominent */}
                        <span className={`text-lg font-bold ${hasPositiveEdge ? 'text-green-400' : 'text-slate-300'}`}>
                          {line.line}
                        </span>
                        <span className="text-slate-400 text-sm">{formatBookmaker(line.bookmaker)}</span>
                        {isBestEdge && (
                          <span className="text-xs bg-green-600/40 text-green-300 px-1.5 py-0.5 rounded font-medium">
                            BEST EDGE
                          </span>
                        )}
                        {isEasiestLine && (
                          <span className="text-xs bg-blue-600/40 text-blue-300 px-1.5 py-0.5 rounded font-medium">
                            EASIEST
                          </span>
                        )}
                        {isSelected && (
                          <span className="text-xs bg-green-500/30 text-green-300 px-1.5 py-0.5 rounded font-medium">
                            SIZING
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-3 text-sm">
                        <span className={hasPositiveEdge ? 'text-green-400 font-medium' : 'text-slate-400'}>
                          {line.lineEdge >= 0 ? '+' : ''}{(line.lineEdge * 100).toFixed(1)}%
                        </span>
                        <span className={isOverBet ? 'text-green-400' : 'text-slate-500'}>
                          O {formatOdds(line.over_odds)}
                        </span>
                        <span className="text-slate-600">/</span>
                        <span className={!isOverBet ? 'text-green-400' : 'text-slate-500'}>
                          U {formatOdds(line.under_odds)}
                        </span>
                      </div>
                    </button>
                  )
                })
              })()}
            </div>
          ) : (
            <div className="text-slate-400 text-sm">
              {bookmakerLines.length > 0 && userState
                ? `No lines from ${userState}-licensed books`
                : 'No lines available'}
            </div>
          )}
          {userState && (
            <p className="text-xs text-slate-500 mt-3">
              Lines are filtered to {userState}-licensed sportsbooks. Sharper books or larger edges may be available in other states.
            </p>
          )}
        </div>

        {/* Bet Sizing Calculator */}
        <div className="p-4 sm:p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Bet Sizing</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
            <div>
              <label className="block text-sm text-slate-400 mb-1">Bankroll ($)</label>
              <input
                type="text"
                inputMode="decimal"
                value={bankrollInput}
                onChange={(e) => handleBankrollChange(e.target.value)}
                onFocus={(e) => e.target.select()}
                className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
              />
            </div>
            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="text-sm text-slate-400">Kelly Fraction</label>
                <button
                  onClick={() => handleKellyToggle(!useCustomKelly)}
                  className="text-xs text-blue-400 hover:text-blue-300"
                >
                  {useCustomKelly ? 'Use Presets' : 'Custom'}
                </button>
              </div>
              {useCustomKelly ? (
                <input
                  type="text"
                  inputMode="decimal"
                  value={customKellyInput}
                  onChange={(e) => handleCustomKellyChange(e.target.value)}
                  onFocus={(e) => e.target.select()}
                  placeholder="0.25"
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                />
              ) : (
                <select
                  value={kellyFraction}
                  onChange={(e) => handleKellyChange(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-slate-200 focus:outline-none focus:border-blue-500"
                >
                  {KELLY_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </select>
              )}
            </div>
          </div>

          {/* Recommended bet size — uses selected sportsbook line when available */}
          <div className="bg-slate-700/50 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-slate-400 text-sm">Recommended Bet</span>
              <span className="text-slate-400 text-sm">
                {(sizingData.kellyPct * 100).toFixed(2)}% of bankroll
              </span>
            </div>
            <div className="text-2xl font-bold text-green-400">
              ${sizingData.recommendedBet.toFixed(2)}
            </div>
            <div className="text-xs text-slate-500 mt-2">
              Based on {(sizingData.sizingModelProb * 100).toFixed(1)}% model prob at {formatOdds(sizingData.sizingOdds)} odds
              {sizingData.sizingBookmaker && ` (${formatBookmaker(sizingData.sizingBookmaker)} ${direction} ${sizingData.sizingLineVal})`}
            </div>
          </div>
        </div>

        {/* Quantile Summary */}
        <div className="p-4 sm:p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Model Prediction Distribution</h3>
          {isBinaryDistribution && Number.isFinite(prediction.pred_mean) ? (
            <div className="space-y-3">
              <p className="text-xs text-slate-500">
                Binary model — predicts whether the player records any {statLabel.toLowerCase()} (0 or 1+).
              </p>
              <div className="grid grid-cols-2 gap-3">
                <div className="bg-slate-700/50 rounded-lg p-3 text-center">
                  <div className="text-xs text-slate-400 mb-1">P({statLabel} ≥ 1)</div>
                  <div className="text-2xl font-bold text-green-400">
                    {(prediction.pred_mean * 100).toFixed(1)}%
                  </div>
                </div>
                <div className="bg-slate-700/50 rounded-lg p-3 text-center">
                  <div className="text-xs text-slate-400 mb-1">P(No {statLabel})</div>
                  <div className="text-2xl font-bold text-red-400">
                    {((1 - prediction.pred_mean) * 100).toFixed(1)}%
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <QuantileSummary
              q10={prediction.q10}
              q25={prediction.q25}
              q50={prediction.q50}
              q75={prediction.q75}
              q90={prediction.q90}
              line={prediction.prop_line}
            />
          )}
        </div>

        {/* Edge Summary — updates to reflect selected sportsbook line when available */}
        <div className="p-4 sm:p-6">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-slate-400 text-sm">Model Probability</div>
              <div className="text-xl font-semibold text-slate-50">
                {formatProb(selectedLine ? selectedLine.modelProb : probability)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-sm">Market Implied</div>
              <div className="text-xl font-semibold text-slate-50">
                {formatProb(selectedLine ? selectedLine.impliedProb : marketProb)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-sm">Edge</div>
              <EdgeBadge edge={selectedLine ? selectedLine.lineEdge : edge} className="text-lg" />
            </div>
          </div>
          {selectedLine && (
            <div className="text-xs text-slate-500 mt-2 text-center">
              Based on {formatBookmaker(selectedLine.bookmaker)} {direction} {selectedLine.line} @ {formatOdds(selectedLine.relevantOdds)}
            </div>
          )}
        </div>

        {/* Footer: Close + Confidence + Take Bet */}
        <div className="p-4 sm:p-6 pt-0 flex flex-col sm:flex-row items-stretch sm:items-center gap-2 sm:gap-3">
          <button
            onClick={onClose}
            className="py-3 px-4 bg-slate-700 hover:bg-slate-600 text-slate-200 rounded-md font-medium transition-colors"
          >
            Close
          </button>
          {onTakeBet && selectedLine && (
            <div className="flex-1 flex items-center gap-2 justify-end">
              {/* Confidence Stars */}
              <div className="flex items-center gap-0.5 mr-1">
                {[1, 2, 3, 4, 5].map((star) => (
                  <button
                    key={star}
                    type="button"
                    onClick={() => setUserConfidence(prev => prev === star ? null : star)}
                    disabled={betPlaced}
                    className="p-0.5 transition-colors disabled:cursor-default"
                    title={`${star} star${star > 1 ? 's' : ''}`}
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      viewBox="0 0 20 20"
                      fill="currentColor"
                      className={`w-5 h-5 ${
                        userConfidence && star <= userConfidence
                          ? 'text-yellow-400'
                          : 'text-slate-600'
                      }`}
                    >
                      <path fillRule="evenodd" d="M10.868 2.884c-.321-.772-1.415-.772-1.736 0l-1.83 4.401-4.753.381c-.833.067-1.171 1.107-.536 1.651l3.62 3.102-1.106 4.637c-.194.813.691 1.456 1.405 1.02L10 15.591l4.069 2.485c.713.436 1.598-.207 1.404-1.02l-1.106-4.637 3.62-3.102c.635-.544.297-1.584-.536-1.65l-4.752-.382-1.831-4.401Z" clipRule="evenodd" />
                    </svg>
                  </button>
                ))}
              </div>
              <span className="text-slate-400 text-sm">$</span>
              <input
                type="text"
                inputMode="decimal"
                value={customStake}
                onChange={(e) => setCustomStake(e.target.value)}
                onFocus={(e) => e.target.select()}
                className="w-24 bg-slate-700 border border-slate-600 rounded px-3 py-2 text-slate-200 text-sm focus:outline-none focus:border-green-500"
                placeholder="Stake"
                disabled={betPlaced}
              />
              <button
                onClick={() => {
                  const stake = parseFloat(customStake) || 0
                  if (stake <= 0) return

                  const l5Games = history.length > 0
                    ? history.map(g => ({
                        date: g.game_date,
                        value: comboComponents
                          ? comboComponents.reduce((s, col) => s + (Number(g[col]) || 0), 0)
                          : statColumn ? (Number(g[statColumn]) || 0) : 0,
                      }))
                    : null

                  const betContext = buildBetContext(prediction, {
                    l5Avg: l5Avg,
                    l5Games: l5Games,
                    kelly: sizingData.kellyPct > 0 ? {
                      fraction: kellyFraction,
                      recommended_stake: sizingData.recommendedBet,
                      bankroll_pct: sizingData.kellyPct,
                    } : null,
                    sportsbookLines: bookmakerLines.length > 0 ? bookmakerLines : null,
                    source: 'analysis_modal',
                  })

                  onTakeBet(prediction, {
                    book: selectedLine.bookmaker,
                    odds: selectedLine.relevantOdds,
                    line: selectedLine.line,
                    stake,
                    modelProb: selectedLine.modelProb,
                    edge: selectedLine.lineEdge,
                    direction: isOverBet ? 'over' : 'under',
                    betContext,
                    userConfidence,
                  })
                  setBetPlaced(true)
                  // Auto-close modal after brief delay so user sees confirmation
                  setTimeout(() => onClose(), 1500)
                }}
                disabled={betPlaced || !customStake || parseFloat(customStake) <= 0}
                className={`py-2 px-4 rounded-md font-medium text-sm transition-colors ${
                  betPlaced
                    ? 'bg-green-700 text-green-200 cursor-default'
                    : 'bg-green-600 hover:bg-green-500 text-white disabled:bg-slate-700 disabled:text-slate-500'
                }`}
              >
                {betPlaced ? 'Bet Taken!' : 'Take Bet'}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
