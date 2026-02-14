'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Badge, EdgeBadge } from '@/components/shared/Badge'
import { Last5Chart } from './Last5Chart'
import { QuantileSummary } from './QuantileSummary'
import { type Prediction, type PlayerGameStats, type StatType, type BookmakerLine, STAT_LABELS } from '@/types/predictions'
import { formatProb } from '@/lib/utils'

interface AnalysisModalProps {
  prediction: Prediction
  onClose: () => void
}

// Map stat type to column name in player_game_stats
const STAT_COLUMN_MAP: Record<StatType, keyof PlayerGameStats> = {
  pts: 'pts',
  reb: 'reb',
  ast: 'ast',
}

// Map stat type to market_key in raw_player_props_combined
const STAT_TO_MARKET: Record<StatType, string> = {
  pts: 'player_points',
  reb: 'player_rebounds',
  ast: 'player_assists',
}

// Format bookmaker name for display
const formatBookmaker = (name: string): string => {
  const names: Record<string, string> = {
    'draftkings': 'DraftKings',
    'fanduel': 'FanDuel',
    'betmgm': 'BetMGM',
    'caesars': 'Caesars',
    'pointsbet': 'PointsBet',
    'bet365': 'Bet365',
    'unibet': 'Unibet',
    'williamhill': 'William Hill',
    'williamhill_us': 'William Hill',
    'fliff': 'Fliff',
    'hardrockbet': 'Hard Rock',
    'betrivers': 'BetRivers',
    'espnbet': 'ESPN Bet',
    'fanatics': 'Fanatics',
    'novig': 'Novig',
    'prophetx': 'ProphetX',
    'pinnacle': 'Pinnacle',
    'bovada': 'Bovada',
  }
  return names[name.toLowerCase()] || name.charAt(0).toUpperCase() + name.slice(1)
}

// Format odds for display
const formatOdds = (odds: number): string => {
  return odds >= 0 ? `+${odds}` : `${odds}`
}

// Convert American odds to implied probability
const oddsToImpliedProb = (odds: number): number => {
  if (odds > 0) {
    return 100 / (odds + 100)
  } else {
    return Math.abs(odds) / (Math.abs(odds) + 100)
  }
}

// Estimate probability of Under X using quantile interpolation
// P(stat < line) = probability the Under hits
// Higher line = easier Under = higher probability
// Lower line = harder Under = lower probability
const estimateUnderProb = (
  line: number,
  q10: number,
  q25: number,
  q50: number,
  q75: number,
  q90: number
): number => {
  // Quantile points: (value, cumulative probability that stat < value)
  const points = [
    { val: q10, prob: 0.10 },
    { val: q25, prob: 0.25 },
    { val: q50, prob: 0.50 },
    { val: q75, prob: 0.75 },
    { val: q90, prob: 0.90 },
  ]

  // Extrapolate below q10 (Under is hard to hit at low lines)
  if (line <= q10) {
    // Linear extrapolation toward 0, but cap at 0.01
    const slope = (points[1].prob - points[0].prob) / (points[1].val - points[0].val)
    const extrapolated = points[0].prob + slope * (line - points[0].val)
    return Math.max(0.01, Math.min(0.10, extrapolated))
  }

  // Extrapolate above q90 (Under is easy to hit at high lines)
  if (line >= q90) {
    // Linear extrapolation toward 1, but cap at 0.99
    // Use the slope from q75 to q90 for extrapolation
    const slope = (points[4].prob - points[3].prob) / (points[4].val - points[3].val)
    const extrapolated = points[4].prob + slope * (line - points[4].val)
    return Math.max(0.90, Math.min(0.99, extrapolated))
  }

  // Find the two quantiles that bracket the line and interpolate
  for (let i = 0; i < points.length - 1; i++) {
    if (line >= points[i].val && line <= points[i + 1].val) {
      const range = points[i + 1].val - points[i].val
      if (range === 0) return points[i].prob
      const fraction = (line - points[i].val) / range
      return points[i].prob + fraction * (points[i + 1].prob - points[i].prob)
    }
  }

  return 0.5 // Fallback
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

export function AnalysisModal({ prediction, onClose }: AnalysisModalProps) {
  const [history, setHistory] = useState<PlayerGameStats[]>([])
  const [bookmakerLines, setBookmakerLines] = useState<BookmakerLine[]>([])
  const [loading, setLoading] = useState(true)
  const [linesLoading, setLinesLoading] = useState(true)

  // Bankroll and Kelly settings (persisted to localStorage) - use lazy initialization
  const [bankroll, setBankroll] = useState<number>(() => {
    if (typeof window === 'undefined') return 1000
    const saved = localStorage.getItem('betting_bankroll')
    return saved ? parseFloat(saved) : 1000
  })
  const [bankrollInput, setBankrollInput] = useState<string>(() => {
    if (typeof window === 'undefined') return '1000'
    const saved = localStorage.getItem('betting_bankroll')
    return saved || '1000'
  })
  const [kellyFraction, setKellyFraction] = useState<number>(() => {
    if (typeof window === 'undefined') return 0.25
    const saved = localStorage.getItem('betting_kelly_fraction')
    return saved ? parseFloat(saved) : 0.25
  })
  const [useCustomKelly, setUseCustomKelly] = useState<boolean>(() => {
    if (typeof window === 'undefined') return false
    return localStorage.getItem('betting_use_custom_kelly') === 'true'
  })
  const [customKellyInput, setCustomKellyInput] = useState<string>(() => {
    if (typeof window === 'undefined') return '0.25'
    const saved = localStorage.getItem('betting_kelly_fraction')
    return saved || '0.25'
  })

  // Save bankroll to localStorage when changed
  const handleBankrollChange = (value: string) => {
    setBankrollInput(value)
    const num = parseFloat(value) || 0
    setBankroll(num)
    localStorage.setItem('betting_bankroll', num.toString())
  }

  // Save Kelly fraction to localStorage when changed (preset)
  const handleKellyChange = (value: string) => {
    const num = parseFloat(value)
    setKellyFraction(num)
    setCustomKellyInput(num.toString())
    localStorage.setItem('betting_kelly_fraction', num.toString())
  }

  // Save custom Kelly fraction
  const handleCustomKellyChange = (value: string) => {
    setCustomKellyInput(value)
    const num = parseFloat(value)
    if (!isNaN(num) && num >= 0 && num <= 1) {
      setKellyFraction(num)
      localStorage.setItem('betting_kelly_fraction', num.toString())
    }
  }

  // Toggle custom Kelly mode
  const handleKellyToggle = (useCustom: boolean) => {
    setUseCustomKelly(useCustom)
    localStorage.setItem('betting_use_custom_kelly', useCustom.toString())
  }

  useEffect(() => {
    async function fetchHistory() {
      const supabase = createClient()
      const { data, error } = await supabase
        .from('player_game_stats')
        .select('game_date, pts, reb, ast, fg3m, min')
        .eq('player_id', prediction.player_id)
        .order('game_date', { ascending: false })
        .limit(5)

      if (!error && data) {
        console.log('Last 5 games data:', data)
        setHistory(data.reverse()) // Chronological order for chart
      } else {
        console.error('Error fetching history:', error)
      }
      setLoading(false)
    }

    async function fetchBookmakerLines() {
      const supabase = createClient()
      const marketKey = STAT_TO_MARKET[prediction.stat]

      // Skip if no game_id
      if (!prediction.game_id) {
        console.log('No game_id available for bookmaker lookup')
        setLinesLoading(false)
        return
      }

      console.log('Fetching bookmaker lines:', {
        player_id: prediction.player_id,
        game_id: prediction.game_id,
        market_key: marketKey,
      })

      // Get bookmaker lines for this player/stat/game
      // Both tables use the same NBA game_id format (e.g., "0022500771")
      const { data, error } = await supabase
        .from('raw_player_props_combined')
        .select('bookmaker, line, outcome_label, odds_american')
        .eq('player_id', prediction.player_id)
        .eq('game_id', prediction.game_id)
        .eq('market_key', marketKey)
        .order('bookmaker')

      if (error) {
        console.error('Error fetching bookmaker lines:', error)
      }

      if (!error && data) {
        console.log('Bookmaker lines fetched:', data.length, 'rows')
        // Group by bookmaker and line, get over/under odds
        const lineMap = new Map<string, BookmakerLine>()

        for (const row of data) {
          const key = `${row.bookmaker}-${row.line}`
          if (!lineMap.has(key)) {
            lineMap.set(key, {
              bookmaker: row.bookmaker,
              line: parseFloat(row.line),
              over_odds: 0,
              under_odds: 0,
            })
          }
          const entry = lineMap.get(key)!
          if (row.outcome_label === 'Over') {
            entry.over_odds = row.odds_american
          } else if (row.outcome_label === 'Under') {
            entry.under_odds = row.odds_american
          }
        }

        // Filter to complete lines only
        const allLines = Array.from(lineMap.values())
          .filter(l => l.over_odds !== 0 && l.under_odds !== 0)

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
  const statColumn = STAT_COLUMN_MAP[prediction.stat]
  const statLabel = STAT_LABELS[prediction.stat]

  // Calculate L5 average for the relevant stat
  const l5Avg = history.length > 0
    ? history.reduce((sum, g) => sum + (Number(g[statColumn]) || 0), 0) / history.length
    : null

  // Handle escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
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
        <div className="p-6 border-b border-slate-700">
          <div className="flex items-center space-x-4">
            <PlayerAvatar
              playerId={prediction.player_id}
              playerName={prediction.player_name || 'Unknown'}
              size="lg"
            />
            <div className="flex-1">
              <h2 className="text-2xl font-bold text-slate-50">
                {prediction.player_name || `Player ${prediction.player_id}`}
              </h2>
              <p className="text-slate-400">
                {prediction.team_abbrev || '???'} vs {prediction.opponent_abbrev || '???'}
              </p>
            </div>
            <div className="text-right">
              <Badge stat={prediction.stat} className="mb-1" />
              <div className="text-xl font-bold text-slate-50">
                {direction} {prediction.prop_line}
              </div>
            </div>
          </div>
        </div>

        {/* Last 5 Games */}
        <div className="p-6 border-b border-slate-700">
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
              />
              {/* Stats table as fallback/supplement */}
              <div className="mt-4 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-slate-400 border-b border-slate-700">
                      <th className="text-left py-2 px-2">Date</th>
                      <th className="text-center py-2 px-2">MIN</th>
                      <th className="text-center py-2 px-2">PTS</th>
                      <th className="text-center py-2 px-2">REB</th>
                      <th className="text-center py-2 px-2">AST</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((game, i) => (
                      <tr key={i} className="border-b border-slate-700/50">
                        <td className="py-2 px-2 text-slate-300">{game.game_date}</td>
                        <td className="text-center py-2 px-2 text-slate-400">{game.min}</td>
                        <td className={`text-center py-2 px-2 ${prediction.stat === 'pts' ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                          {game.pts}
                        </td>
                        <td className={`text-center py-2 px-2 ${prediction.stat === 'reb' ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                          {game.reb}
                        </td>
                        <td className={`text-center py-2 px-2 ${prediction.stat === 'ast' ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                          {game.ast}
                        </td>
                      </tr>
                    ))}
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

        {/* Sportsbook Lines */}
        <div className="p-6 border-b border-slate-700">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-slate-50">Sportsbook Lines</h3>
            <div className="text-sm text-slate-400">
              Betting: <span className={isOverBet ? 'text-green-400' : 'text-red-400'}>{direction} {prediction.prop_line}</span>
            </div>
          </div>
          {linesLoading ? (
            <div className="text-slate-400 text-sm">Loading lines...</div>
          ) : bookmakerLines.length > 0 ? (
            <div className="space-y-2">
              {/* Calculate edge for each line and sort by best edge */}
              {(() => {
                // Process all lines with edge calculations
                const processedLines = [...bookmakerLines]
                  .map((line) => {
                    // Estimate model probability at this line using quantiles
                    const underProb = estimateUnderProb(
                      line.line,
                      prediction.q10,
                      prediction.q25,
                      prediction.q50,
                      prediction.q75,
                      prediction.q90
                    )
                    const overProb = 1 - underProb

                    // Get relevant odds and calculate edge
                    const relevantOdds = isOverBet ? line.over_odds : line.under_odds
                    const modelProb = isOverBet ? overProb : underProb
                    const impliedProb = oddsToImpliedProb(relevantOdds)
                    const lineEdge = modelProb - impliedProb

                    return { ...line, modelProb, impliedProb, lineEdge, relevantOdds }
                  })
                  .sort((a, b) => b.lineEdge - a.lineEdge) // Best edge first

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

                    return (
                    <div
                      key={i}
                      className={`flex items-center justify-between rounded px-3 py-2 ${
                        isBestEdge
                          ? 'bg-green-900/40 border border-green-600/60'
                          : hasPositiveEdge
                            ? 'bg-green-900/20 border border-green-700/40'
                            : 'bg-slate-700/30'
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
                    </div>
                  )
                })
              })()}
            </div>
          ) : (
            <div className="text-slate-400 text-sm">No lines available</div>
          )}
        </div>

        {/* Bet Sizing Calculator */}
        <div className="p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Bet Sizing</h3>
          <div className="grid grid-cols-2 gap-4 mb-4">
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

          {/* Recommended bet size */}
          {(() => {
            const bestOdds = isOverBet ? prediction.best_over_odds : prediction.best_under_odds
            const kellyPct = calculateKelly(probability, bestOdds || -110, kellyFraction)
            const recommendedBet = bankroll * kellyPct

            return (
              <div className="bg-slate-700/50 rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-slate-400 text-sm">Recommended Bet</span>
                  <span className="text-slate-400 text-sm">
                    {(kellyPct * 100).toFixed(2)}% of bankroll
                  </span>
                </div>
                <div className="text-2xl font-bold text-green-400">
                  ${recommendedBet.toFixed(2)}
                </div>
                <div className="text-xs text-slate-500 mt-2">
                  Based on {(probability * 100).toFixed(1)}% model probability at {formatOdds(bestOdds || -110)} odds
                </div>
              </div>
            )
          })()}
        </div>

        {/* Quantile Summary */}
        <div className="p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Model Prediction Distribution</h3>
          <QuantileSummary
            q10={prediction.q10}
            q25={prediction.q25}
            q50={prediction.q50}
            q75={prediction.q75}
            q90={prediction.q90}
            line={prediction.prop_line}
          />
        </div>

        {/* Edge Summary */}
        <div className="p-6">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-slate-400 text-sm">Model Probability</div>
              <div className="text-xl font-semibold text-slate-50">
                {formatProb(probability)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-sm">Market Implied</div>
              <div className="text-xl font-semibold text-slate-50">
                {formatProb(marketProb)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-sm">Edge</div>
              <EdgeBadge edge={edge} className="text-lg" />
            </div>
          </div>
        </div>

        {/* Close button */}
        <div className="p-6 pt-0">
          <button
            onClick={onClose}
            className="w-full py-3 px-4 bg-slate-700 hover:bg-slate-600 text-slate-200 rounded-md font-medium transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  )
}
