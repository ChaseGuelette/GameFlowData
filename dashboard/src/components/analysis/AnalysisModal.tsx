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

export function AnalysisModal({ prediction, onClose }: AnalysisModalProps) {
  const [history, setHistory] = useState<PlayerGameStats[]>([])
  const [bookmakerLines, setBookmakerLines] = useState<BookmakerLine[]>([])
  const [loading, setLoading] = useState(true)
  const [linesLoading, setLinesLoading] = useState(true)

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

        // Filter to complete lines, prioritize matching prop_line, sort by bookmaker
        const allLines = Array.from(lineMap.values())
          .filter(l => l.over_odds !== 0 && l.under_odds !== 0)

        // Prioritize lines matching the prop_line
        const matchingLines = allLines.filter(l => l.line === prediction.prop_line)
        const otherLines = allLines.filter(l => l.line !== prediction.prop_line)

        // Show matching lines first, then other lines (limited to 8 total)
        const lines = [...matchingLines, ...otherLines]
          .slice(0, 8)
          .sort((a, b) => a.bookmaker.localeCompare(b.bookmaker))

        setBookmakerLines(lines)
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
          <h3 className="text-lg font-semibold text-slate-50 mb-4">Sportsbook Lines</h3>
          {linesLoading ? (
            <div className="text-slate-400 text-sm">Loading lines...</div>
          ) : bookmakerLines.length > 0 ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {bookmakerLines.map((line, i) => (
                <div
                  key={i}
                  className="flex items-center justify-between bg-slate-700/50 rounded px-3 py-2"
                >
                  <div>
                    <span className="text-slate-200 font-medium">{formatBookmaker(line.bookmaker)}</span>
                    <span className="text-slate-400 ml-2 text-sm">{line.line}</span>
                  </div>
                  <div className="flex gap-3 text-sm">
                    <span className={`${isOverBet ? 'text-green-400 font-medium' : 'text-slate-400'}`}>
                      O {formatOdds(line.over_odds)}
                    </span>
                    <span className={`${!isOverBet ? 'text-green-400 font-medium' : 'text-slate-400'}`}>
                      U {formatOdds(line.under_odds)}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-slate-400 text-sm">No lines available</div>
          )}
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
