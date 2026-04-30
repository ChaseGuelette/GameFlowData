'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { PlayerAvatar } from '@/components/shared/PlayerAvatar'
import { Last5Chart } from '@/components/analysis/Last5Chart'
import type { KalshiLiveOrder, KalshiPaperBet, BotTab } from '@/types/bot-tracker'
import { KALSHI_STAT_LABELS } from '@/types/bot-tracker'
import type { PlayerGameStats } from '@/types/predictions'
import { NBA_CONFIG } from '@/lib/sport-config'
import type { Sport } from '@/lib/sport-config'

interface BetAnalysisModalProps {
  order: KalshiLiveOrder | KalshiPaperBet
  tab: BotTab
  onClose: () => void
}

const MLB_STAT_HISTORY: Record<string, { table: string; column: string }> = {
  pitcher_strikeouts: { table: 'mlb_player_game_stats_pitching', column: 'so' },
  batter_hits: { table: 'mlb_player_game_stats_batting', column: 'h' },
  batter_total_bases: { table: 'mlb_player_game_stats_batting', column: 'tb' },
  batter_home_runs: { table: 'mlb_player_game_stats_batting', column: 'hr' },
  batter_rbis: { table: 'mlb_player_game_stats_batting', column: 'rbi' },
  batter_runs_scored: { table: 'mlb_player_game_stats_batting', column: 'r' },
  batter_hrr: { table: 'mlb_player_game_stats_batting', column: 'h' },
}

export function BetAnalysisModal({ order, tab, onClose }: BetAnalysisModalProps) {
  const [history, setHistory] = useState<PlayerGameStats[]>([])
  const [mlbHistoryValues, setMlbHistoryValues] = useState<number[]>([])
  const [mlbRawRows, setMlbRawRows] = useState<Record<string, unknown>[]>([])
  const [loading, setLoading] = useState(true)
  const [teamAbbrev, setTeamAbbrev] = useState<string | null>(null)
  const [matchupText, setMatchupText] = useState<string | null>(null)

  const isMlb = order.sport === 'mlb' || order.stat_type.startsWith('batter_') || order.stat_type.startsWith('pitcher_')
  const isPitcherStat = order.stat_type.startsWith('pitcher_')
  const isHrrCombo = order.stat_type === 'batter_hrr'
  const mlbStatCol = MLB_STAT_HISTORY[order.stat_type]?.column
  const isYesBet = order.side === 'yes'
  const statLabel = KALSHI_STAT_LABELS[order.stat_type] ?? order.stat_type

  useEffect(() => {
    async function fetchHistory() {
      if (!order.player_id) {
        setLoading(false)
        return
      }

      const supabase = createClient()

      if (isMlb) {
        const mlbStat = MLB_STAT_HISTORY[order.stat_type]
        if (!mlbStat) { setLoading(false); return }

        const isPitching = mlbStat.table === 'mlb_player_game_stats_pitching'
        const col = mlbStat.column

        const query = isPitching
          ? supabase.from('mlb_player_game_stats_pitching').select('game_date, so')
              .eq('player_id', order.player_id).eq('did_not_play', false)
              .order('game_date', { ascending: false }).limit(5)
          : supabase.from('mlb_player_game_stats_batting').select('game_date, h, ab, tb, hr, rbi, r')
              .eq('player_id', order.player_id).eq('did_not_play', false)
              .order('game_date', { ascending: false }).limit(5)

        const { data, error } = await query

        if (!error && data) {
          const reversed = [...data].reverse()
          setHistory(reversed.map((row) => ({ game_date: (row as { game_date: string }).game_date } as PlayerGameStats)))
          setMlbHistoryValues(reversed.map((row) => {
            if (order.stat_type === 'batter_hrr') {
              return (Number((row as Record<string, unknown>)['h']) || 0) +
                     (Number((row as Record<string, unknown>)['r']) || 0) +
                     (Number((row as Record<string, unknown>)['rbi']) || 0)
            }
            return Number((row as Record<string, unknown>)[col]) || 0
          }))
          setMlbRawRows(reversed as Record<string, unknown>[])
        }
        setLoading(false)
        return
      }

      const { data, error } = await supabase
        .from('player_game_stats')
        .select('game_date, pts, reb, ast, fg3m, stl, blk, tov, min, team_id, matchup')
        .eq('player_id', order.player_id)
        .gt('min', 0)
        .order('game_date', { ascending: false })
        .limit(5)

      if (!error && data) {
        const reversed = data.reverse()
        setHistory(reversed)
        // Extract team info from most recent game
        const latest = reversed[reversed.length - 1]
        if (latest?.team_id) {
          setTeamAbbrev(NBA_CONFIG.teams[latest.team_id] ?? null)
        }
        if (latest?.matchup) {
          setMatchupText(latest.matchup)
        }
      }
      setLoading(false)
    }

    fetchHistory()
  }, [order.player_id, order.stat_type, isMlb])

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [onClose])

  // NBA stat column mapping — single stats map directly, combos are computed from components
  const NBA_STAT_COLUMN: Record<string, keyof PlayerGameStats | null> = {
    pts: 'pts', reb: 'reb', ast: 'ast',
    stl: 'stl', blk: 'blk', fg3m: 'fg3m', tov: 'tov',
    pra: null, pr: null, pa: null, ra: null, sb: null,
  }
  const NBA_COMBO_COMPONENTS: Record<string, (keyof PlayerGameStats)[]> = {
    pra: ['pts', 'reb', 'ast'],
    pr: ['pts', 'reb'],
    pa: ['pts', 'ast'],
    ra: ['reb', 'ast'],
    sb: ['stl', 'blk'],
  }

  const nbaStatColumn = isMlb ? null : (NBA_STAT_COLUMN[order.stat_type] ?? null)
  const nbaComboComponents = isMlb ? undefined : NBA_COMBO_COMPONENTS[order.stat_type]

  const l5Avg = history.length > 0
    ? isMlb && mlbHistoryValues.length > 0
      ? mlbHistoryValues.reduce((s, v) => s + v, 0) / mlbHistoryValues.length
      : nbaComboComponents
        ? history.reduce((sum, g) => sum + nbaComboComponents.reduce((s, col) => s + (Number(g[col]) || 0), 0), 0) / history.length
        : nbaStatColumn
          ? history.reduce((sum, g) => sum + (Number(g[nbaStatColumn]) || 0), 0) / history.length
          : null
    : null

  const statusColors: Record<string, string> = {
    won: 'bg-green-500/20 text-green-400',
    lost: 'bg-red-500/20 text-red-400',
    pending: 'bg-yellow-500/20 text-yellow-400',
    cancelled: 'bg-slate-500/20 text-slate-400',
    filled: 'bg-blue-500/20 text-blue-400',
  }
  const status = order.status ?? 'pending'
  const pnlValue = order.pnl != null ? Number(order.pnl) : null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-2 sm:p-4">
      <div className="absolute inset-0 bg-black/70" onClick={onClose} />
      <div className="relative bg-slate-800 rounded-lg border border-slate-700 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <button
          onClick={onClose}
          className="absolute top-4 right-4 text-slate-400 hover:text-slate-200 text-2xl"
        >
          &times;
        </button>

        <div className="p-4 sm:p-6 border-b border-slate-700">
          <div className="flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-4">
            {order.player_id && (
              <PlayerAvatar
                playerId={order.player_id}
                playerName={order.player_name || 'Unknown'}
                size="lg"
                className="hidden sm:block"
                sportOverride={order.sport as Sport}
              />
            )}
            <div className="flex-1">
              <h2 className="text-xl font-bold text-slate-50">
                {order.player_name ?? 'Unknown'}
              </h2>
              <div className="flex items-center gap-2 mt-0.5">
                <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                  order.sport === 'nba'
                    ? 'bg-orange-500/20 text-orange-400'
                    : 'bg-blue-500/20 text-blue-400'
                }`}>
                  {order.sport.toUpperCase()}
                </span>
                {teamAbbrev && (
                  <span className="text-sm text-slate-400">{teamAbbrev}</span>
                )}
                {matchupText && (
                  <span className="text-xs text-slate-500">{matchupText}</span>
                )}
              </div>
            </div>
            <div className="text-left sm:text-right">
              <div className="text-lg font-bold text-slate-50">
                {statLabel} — <span className={isYesBet ? 'text-green-400' : 'text-red-400'}>{order.side.toUpperCase()}</span> {Number(order.line)}
              </div>
            </div>
          </div>
        </div>

        <div className="p-4 sm:p-6 border-b border-slate-700">
          <h3 className="text-lg font-semibold text-slate-50 mb-3">Bet Details</h3>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="bg-slate-700/50 rounded-lg p-3 text-center">
              <div className="text-xs text-slate-400 mb-1">Edge</div>
              <div className={`text-lg font-bold ${
                order.fee_adjusted_edge != null && Number(order.fee_adjusted_edge) > 0
                  ? 'text-green-400'
                  : order.fee_adjusted_edge != null && Number(order.fee_adjusted_edge) < 0
                    ? 'text-red-400'
                    : 'text-slate-300'
              }`}>
                {order.fee_adjusted_edge != null
                  ? `${(Number(order.fee_adjusted_edge) * 100).toFixed(1)}%`
                  : '—'}
              </div>
            </div>
            <div className="bg-slate-700/50 rounded-lg p-3 text-center">
              <div className="text-xs text-slate-400 mb-1">Model Prob</div>
              <div className="text-lg font-bold text-slate-50">
                {order.model_prob != null
                  ? `${(Number(order.model_prob) * 100).toFixed(1)}%`
                  : '—'}
              </div>
            </div>
            <div className="bg-slate-700/50 rounded-lg p-3 text-center">
              <div className="text-xs text-slate-400 mb-1">Kalshi Implied</div>
              <div className="text-lg font-bold text-slate-50">
                {order.kalshi_implied != null
                  ? `${(Number(order.kalshi_implied) * 100).toFixed(1)}%`
                  : '—'}
              </div>
            </div>
            <div className="bg-slate-700/50 rounded-lg p-3 text-center">
              <div className="text-xs text-slate-400 mb-1">Result</div>
              <div className="flex flex-col items-center gap-1">
                <span className={`px-2 py-0.5 rounded text-xs font-medium ${statusColors[status] ?? statusColors.pending}`}>
                  {status}
                </span>
                {pnlValue != null && (
                  <span className={`text-lg font-bold ${
                    pnlValue > 0 ? 'text-green-400' : pnlValue < 0 ? 'text-red-400' : 'text-slate-400'
                  }`}>
                    {pnlValue >= 0 ? '+' : ''}${Math.abs(pnlValue).toFixed(2)}
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>

        <div className="p-4 sm:p-6">
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
                stat={nbaStatColumn}
                line={Number(order.line)}
                values={
                  isMlb && mlbHistoryValues.length > 0
                    ? mlbHistoryValues
                    : nbaComboComponents
                      ? history.map(g => nbaComboComponents.reduce((s, col) => s + (Number(g[col]) || 0), 0))
                      : undefined
                }
              />
              <div className="mt-4 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-slate-400 border-b border-slate-700">
                      <th className="text-left py-2 px-1.5 sm:px-2">Date</th>
                      {isMlb ? (
                        isPitcherStat ? (
                          <th className="text-center py-2 px-1.5 sm:px-2">K</th>
                        ) : (
                          <>
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-500">AB</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'h' || isHrrCombo ? 'text-slate-200' : ''}`}>H</th>
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-500">TB</th>
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-500">HR</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'rbi' || isHrrCombo ? 'text-slate-200' : ''}`}>RBI</th>
                            <th className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'r' || isHrrCombo ? 'text-slate-200' : ''}`}>R</th>
                            {isHrrCombo && <th className="text-center py-2 px-1.5 sm:px-2 text-amber-400 font-semibold">H+R+RBI</th>}
                          </>
                        )
                      ) : (
                        <>
                          <th className="text-center py-2 px-1.5 sm:px-2">MIN</th>
                          <th className={`text-center py-2 px-1.5 sm:px-2 ${nbaStatColumn === 'pts' || nbaComboComponents?.includes('pts') ? 'text-slate-200' : ''}`}>PTS</th>
                          <th className={`text-center py-2 px-1.5 sm:px-2 ${nbaStatColumn === 'reb' || nbaComboComponents?.includes('reb') ? 'text-slate-200' : ''}`}>REB</th>
                          <th className={`text-center py-2 px-1.5 sm:px-2 ${nbaStatColumn === 'ast' || nbaComboComponents?.includes('ast') ? 'text-slate-200' : ''}`}>AST</th>
                          {(nbaStatColumn === 'stl' || nbaComboComponents?.includes('stl')) && (
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-200">STL</th>
                          )}
                          {(nbaStatColumn === 'blk' || nbaComboComponents?.includes('blk')) && (
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-200">BLK</th>
                          )}
                          {nbaStatColumn === 'fg3m' && (
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-200">3PM</th>
                          )}
                          {nbaStatColumn === 'tov' && (
                            <th className="text-center py-2 px-1.5 sm:px-2 text-slate-200">TOV</th>
                          )}
                          {nbaComboComponents && (
                            <th className="text-center py-2 px-1.5 sm:px-2 text-amber-400 font-semibold">{statLabel}</th>
                          )}
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((game, i) => {
                      if (isMlb) {
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
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'h' || isHrrCombo ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['h'] ?? '—')}
                                </td>
                                <td className="text-center py-2 px-1.5 sm:px-2 text-slate-400">
                                  {String(rawRow['tb'] ?? '—')}
                                </td>
                                <td className="text-center py-2 px-1.5 sm:px-2 text-slate-400">
                                  {String(rawRow['hr'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'rbi' || isHrrCombo ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['rbi'] ?? '—')}
                                </td>
                                <td className={`text-center py-2 px-1.5 sm:px-2 ${mlbStatCol === 'r' || isHrrCombo ? 'text-slate-50 font-semibold' : 'text-slate-400'}`}>
                                  {String(rawRow['r'] ?? '—')}
                                </td>
                                {isHrrCombo && (
                                  <td className="text-center py-2 px-1.5 sm:px-2 text-amber-400 font-bold">
                                    {(Number(rawRow['h']) || 0) + (Number(rawRow['r']) || 0) + (Number(rawRow['rbi']) || 0)}
                                  </td>
                                )}
                              </>
                            )}
                          </tr>
                        )
                      }
                      const isHighlighted = (col: string) =>
                        nbaStatColumn === col || nbaComboComponents?.includes(col as keyof PlayerGameStats)
                      return (
                        <tr key={i} className="border-b border-slate-700/50">
                          <td className="py-2 px-1.5 sm:px-2 text-slate-300">{game.game_date}</td>
                          <td className="text-center py-2 px-1.5 sm:px-2 text-slate-400">{game.min}</td>
                          <td className={`text-center py-2 px-1.5 sm:px-2 ${isHighlighted('pts') ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                            {game.pts}
                          </td>
                          <td className={`text-center py-2 px-1.5 sm:px-2 ${isHighlighted('reb') ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                            {game.reb}
                          </td>
                          <td className={`text-center py-2 px-1.5 sm:px-2 ${isHighlighted('ast') ? 'text-slate-50 font-medium' : 'text-slate-400'}`}>
                            {game.ast}
                          </td>
                          {(nbaStatColumn === 'stl' || nbaComboComponents?.includes('stl')) && (
                            <td className="text-center py-2 px-1.5 sm:px-2 text-slate-50 font-medium">{game.stl ?? '—'}</td>
                          )}
                          {(nbaStatColumn === 'blk' || nbaComboComponents?.includes('blk')) && (
                            <td className="text-center py-2 px-1.5 sm:px-2 text-slate-50 font-medium">{game.blk ?? '—'}</td>
                          )}
                          {nbaStatColumn === 'fg3m' && (
                            <td className="text-center py-2 px-1.5 sm:px-2 text-slate-50 font-medium">{game.fg3m}</td>
                          )}
                          {nbaStatColumn === 'tov' && (
                            <td className="text-center py-2 px-1.5 sm:px-2 text-slate-50 font-medium">{game.tov ?? '—'}</td>
                          )}
                          {nbaComboComponents && (
                            <td className="text-center py-2 px-1.5 sm:px-2 text-amber-400 font-bold">
                              {nbaComboComponents.reduce((s, col) => s + (Number(game[col]) || 0), 0)}
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

        <div className="p-4 sm:p-6 pt-0">
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
