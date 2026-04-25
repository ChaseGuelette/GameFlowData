'use client'

import { useState, useEffect, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useTradeQueue, useTradeApproval } from '@/lib/hooks/useTradeQueue'
import { KALSHI_STAT_LABELS } from '@/types/bot-tracker'
import type { KalshiTradeQueueItem } from '@/types/bot-tracker'
import { createClient } from '@/lib/supabase/client'
import { AnalysisModal } from '@/components/analysis/AnalysisModal'
import type { Prediction } from '@/types/predictions'
import { NBA_CONFIG } from '@/lib/sport-config'

function TimeRemaining({ expiresAt }: { expiresAt: string }) {
  const [remaining, setRemaining] = useState('')

  useEffect(() => {
    function update() {
      const diff = new Date(expiresAt).getTime() - Date.now()
      if (diff <= 0) {
        setRemaining('Expired')
        return
      }
      const mins = Math.floor(diff / 60000)
      const secs = Math.floor((diff % 60000) / 1000)
      setRemaining(`${mins}:${secs.toString().padStart(2, '0')}`)
    }
    update()
    const interval = setInterval(update, 1000)
    return () => clearInterval(interval)
  }, [expiresAt])

  const isLow = remaining !== 'Expired' && parseInt(remaining) < 5
  return (
    <span className={`text-xs font-mono ${remaining === 'Expired' ? 'text-red-400' : isLow ? 'text-yellow-400' : 'text-slate-400'}`}>
      {remaining}
    </span>
  )
}

function TradeRow({
  trade,
  selected,
  onToggle,
  onAnalyze,
}: {
  trade: KalshiTradeQueueItem
  selected: boolean
  onToggle: (id: number) => void
  onAnalyze: (trade: KalshiTradeQueueItem) => void
}) {
  return (
    <tr className="hover:bg-slate-700/30">
      <td className="px-3 py-2">
        <input
          type="checkbox"
          checked={selected}
          onChange={() => onToggle(trade.id)}
          className="accent-blue-500"
        />
      </td>
      <td className="px-3 py-2">
        <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
          trade.sport === 'nba' ? 'bg-orange-500/20 text-orange-400' : 'bg-blue-500/20 text-blue-400'
        }`}>
          {trade.sport.toUpperCase()}
        </span>
      </td>
      <td className="px-3 py-2">
        <button
          onClick={() => onAnalyze(trade)}
          className="text-slate-200 font-medium hover:text-blue-400 hover:underline transition-colors text-left"
        >
          {trade.player_name ?? '—'}
        </button>
      </td>
      <td className="px-3 py-2 text-slate-300">{KALSHI_STAT_LABELS[trade.stat_type] ?? trade.stat_type}</td>
      <td className="px-3 py-2 text-slate-300">
        <div>{Number(trade.line)}</div>
        {trade.sportsbook_consensus_line != null && (
          <div className="flex items-center gap-1">
            <span className="text-[10px] text-slate-500">SB: {Number(trade.sportsbook_consensus_line)}</span>
            {Math.ceil(Number(trade.sportsbook_consensus_line)) !== Number(trade.line) && (
              <span className="text-[10px] text-yellow-400" title="Kalshi line does not match sportsbook consensus">&#9888;</span>
            )}
          </div>
        )}
      </td>
      <td className="px-3 py-2">
        <span className={`text-xs font-medium ${trade.side === 'yes' ? 'text-green-400' : 'text-red-400'}`}>
          {trade.side.toUpperCase()}
        </span>
      </td>
      <td className="px-3 py-2 text-slate-300">{trade.contracts}</td>
      <td className="px-3 py-2 text-slate-300">${Number(trade.expected_cost).toFixed(2)}</td>
      <td className="px-3 py-2 text-slate-300">
        {trade.fee_adjusted_edge != null ? `${(Number(trade.fee_adjusted_edge) * 100).toFixed(1)}%` : '—'}
      </td>
      <td className="px-3 py-2">
        <TimeRemaining expiresAt={trade.expires_at} />
      </td>
      <td className="px-3 py-2">
        <a
          href={`https://kalshi.com/markets/${trade.ticker.split('-')[0].toLowerCase()}/${trade.ticker.toLowerCase()}`}
          target="_blank"
          rel="noopener noreferrer"
          className="text-slate-400 hover:text-blue-400 transition-colors"
          title="View on Kalshi"
        >
          ↗
        </a>
      </td>
    </tr>
  )
}

export function TradeApprovalPanel() {
  const { data: trades = [], isLoading } = useTradeQueue()
  const { approve, reject, approveAll, retry } = useTradeApproval()
  const queryClient = useQueryClient()
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [sportFilter, setSportFilter] = useState<string>('all')
  const [selectedPrediction, setSelectedPrediction] = useState<Prediction | null>(null)
  const [predLoading, setPredLoading] = useState(false)

  const pendingTrades = useMemo(() => trades.filter(t => t.status === 'pending_approval'), [trades])
  const failedTrades = useMemo(() => trades.filter(t => t.status === 'failed'), [trades])

  async function handleAnalyze(trade: KalshiTradeQueueItem) {
    if (!trade.player_id) return
    setPredLoading(true)
    const supabase = createClient()
    const table = trade.sport === 'mlb' ? 'mlb_daily_predictions' : 'daily_predictions'
    const { data, error } = await supabase
      .from(table)
      .select('*')
      .eq('prediction_date', trade.game_date)
      .eq('player_id', trade.player_id)
      .eq('stat', trade.stat_type)
      .single()

    if (error || !data) {
      setPredLoading(false)
      return
    }

    // Resolve team abbreviation from team_id
    let teamAbbrev: string | undefined
    if (trade.sport === 'nba' && data.team_id) {
      teamAbbrev = NBA_CONFIG.teams[data.team_id as number] ?? undefined
    } else if (trade.sport === 'mlb' && data.team_id) {
      const { data: teamRow } = await supabase
        .from('mlb_teams')
        .select('team_abbreviation')
        .eq('team_id', data.team_id)
        .single()
      teamAbbrev = teamRow?.team_abbreviation ?? undefined
    }

    setPredLoading(false)
    setSelectedPrediction({
      ...data,
      prop_line: data.line,
      model_prob_over: data.over_prob,
      model_prob_under: data.under_prob,
      implied_prob_over: data.implied_over,
      implied_prob_under: data.implied_under,
      q10: data.pred_q10,
      q25: data.pred_q25,
      q50: data.pred_q50,
      q75: data.pred_q75,
      q90: data.pred_q90,
      team_abbrev: teamAbbrev,
      opponent_abbrev: (data.feat_opp_abbrev as string | undefined) ?? undefined,
    } as Prediction)
  }

  // Prune stale selections — trades that disappeared (expired/approved) get dropped.
  // No useEffect needed; computed inline on each render.
  const tradeIdSet = useMemo(() => new Set(pendingTrades.map(t => t.id)), [pendingTrades])
  const validSelected = useMemo(
    () => new Set([...selected].filter(id => tradeIdSet.has(id))),
    [selected, tradeIdSet]
  )
  const sports = useMemo(
    () => Array.from(new Set(pendingTrades.map((t) => t.sport))).sort(),
    [pendingTrades]
  )
  const filteredTrades = useMemo(
    () => sportFilter === 'all' ? pendingTrades : pendingTrades.filter((t) => t.sport === sportFilter),
    [pendingTrades, sportFilter]
  )

  if (isLoading || (pendingTrades.length === 0 && failedTrades.length === 0)) return null

  const totalExposure = pendingTrades.reduce((sum, t) => sum + Number(t.expected_cost), 0)

  const allSelected = validSelected.size === filteredTrades.length && filteredTrades.length > 0

  const toggleOne = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const toggleAll = () => {
    if (allSelected) setSelected(new Set())
    else setSelected(new Set(filteredTrades.map((t) => t.id)))
  }

  const handleApprove = () => {
    if (validSelected.size === pendingTrades.length) {
      approveAll.mutate()
    } else {
      approve.mutate(Array.from(validSelected))
    }
  }

  const handleReject = () => {
    reject.mutate(Array.from(validSelected))
  }

  const isPending = approve.isPending || reject.isPending || approveAll.isPending || retry.isPending

  return (
    <>
    <div className="rounded-lg border-2 border-yellow-500/50 bg-slate-800 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-slate-700 bg-yellow-500/5">
        <div>
          <h3 className="text-sm font-semibold text-yellow-400">
            Pending Trade Approval
          </h3>
          <p className="text-xs text-slate-400 mt-0.5">
            {pendingTrades.length} trade{pendingTrades.length !== 1 ? 's' : ''} pending
            {' | '}Total exposure: ${totalExposure.toFixed(2)}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {sports.length > 1 && (
            <select
              value={sportFilter}
              onChange={(e) => setSportFilter(e.target.value)}
              className="bg-slate-900 border border-slate-600 rounded px-2 py-1 text-xs text-slate-300"
            >
              <option value="all">All Sports</option>
              {sports.map((s) => (
                <option key={s} value={s}>{s.toUpperCase()}</option>
              ))}
            </select>
          )}
          <button
            onClick={handleReject}
            disabled={validSelected.size === 0 || isPending}
            className="px-3 py-1.5 text-xs font-medium rounded bg-red-500/20 text-red-400 hover:bg-red-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            Reject ({validSelected.size})
          </button>
          <button
            onClick={handleApprove}
            disabled={validSelected.size === 0 || isPending}
            className="px-3 py-1.5 text-xs font-medium rounded bg-green-500/20 text-green-400 hover:bg-green-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {validSelected.size === pendingTrades.length ? 'Approve All' : `Approve (${validSelected.size})`}
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-slate-400 bg-slate-900/50">
            <tr>
              <th className="px-3 py-2">
                <input
                  type="checkbox"
                  checked={allSelected}
                  onChange={toggleAll}
                  className="accent-blue-500"
                />
              </th>
              <th className="px-3 py-2">Sport</th>
              <th className="px-3 py-2">Player</th>
              <th className="px-3 py-2">Stat</th>
              <th className="px-3 py-2">Line</th>
              <th className="px-3 py-2">Side</th>
              <th className="px-3 py-2">Contracts</th>
              <th className="px-3 py-2">Cost</th>
              <th className="px-3 py-2">Edge</th>
              <th className="px-3 py-2">Expires</th>
              <th className="px-3 py-2">Link</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {filteredTrades.map((trade) => (
              <TradeRow
                key={trade.id}
                trade={trade}
                selected={validSelected.has(trade.id)}
                onToggle={toggleOne}
                onAnalyze={handleAnalyze}
              />
            ))}
          </tbody>
        </table>
      </div>

      {/* Status messages */}
      {(approve.isSuccess || approveAll.isSuccess) && (
        <div className="px-4 py-2 bg-green-500/10 text-green-400 text-xs">
          Trades approved and queued for execution.
        </div>
      )}
      {reject.isSuccess && (
        <div className="px-4 py-2 bg-red-500/10 text-red-400 text-xs">
          Trades rejected.
        </div>
      )}
      {(approve.isError || reject.isError || approveAll.isError || retry.isError) && (
        <div className="px-4 py-2 bg-red-500/10 text-red-400 text-xs">
          Error: {(approve.error ?? reject.error ?? approveAll.error ?? retry.error)?.message}
        </div>
      )}
      {predLoading && (
        <div className="px-4 py-2 bg-slate-700/50 text-slate-400 text-xs">
          Loading player analysis...
        </div>
      )}
      {selectedPrediction && (
        <AnalysisModal
          prediction={selectedPrediction}
          onClose={() => setSelectedPrediction(null)}
        />
      )}
    </div>

    {failedTrades.length > 0 && (
      <div className="mt-4 rounded-lg border-2 border-red-500/50 bg-slate-800 overflow-hidden">
        <div className="flex items-center justify-between p-4 border-b border-slate-700 bg-red-500/5">
          <div>
            <h3 className="text-sm font-semibold text-red-400">
              Failed Orders (last 24h)
            </h3>
            <p className="text-xs text-slate-400 mt-0.5">
              {failedTrades.length} failed order{failedTrades.length !== 1 ? 's' : ''}
            </p>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead className="text-xs text-slate-400 bg-slate-900/50">
              <tr>
                <th className="px-3 py-2">Player</th>
                <th className="px-3 py-2">Stat</th>
                <th className="px-3 py-2">Line</th>
                <th className="px-3 py-2">Side</th>
                <th className="px-3 py-2">Cost</th>
                <th className="px-3 py-2">Edge</th>
                <th className="px-3 py-2">Placed</th>
                <th className="px-3 py-2">Ticker</th>
                <th className="px-3 py-2">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {failedTrades.map((trade) => (
                <tr key={trade.id} className="hover:bg-slate-700/30 bg-red-500/5">
                  <td className="px-3 py-2">
                    <button
                      onClick={() => handleAnalyze(trade)}
                      className="text-slate-200 font-medium hover:text-blue-400 hover:underline transition-colors text-left"
                    >
                      {trade.player_name ?? '—'}
                    </button>
                  </td>
                  <td className="px-3 py-2 text-slate-300">{KALSHI_STAT_LABELS[trade.stat_type] ?? trade.stat_type}</td>
                  <td className="px-3 py-2 text-slate-300">{Number(trade.line)}</td>
                  <td className="px-3 py-2">
                    <span className={`text-xs font-medium ${trade.side === 'yes' ? 'text-green-400' : 'text-red-400'}`}>
                      {trade.side.toUpperCase()}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-slate-300">${Number(trade.expected_cost).toFixed(2)}</td>
                  <td className="px-3 py-2 text-slate-300">
                    {trade.fee_adjusted_edge != null ? `${(Number(trade.fee_adjusted_edge) * 100).toFixed(1)}%` : '—'}
                  </td>
                  <td className="px-3 py-2 text-slate-400 text-xs whitespace-nowrap">
                    {(() => {
                      const d = new Date(trade.proposed_at)
                      const diffMs = Date.now() - d.getTime()
                      const diffMins = Math.floor(diffMs / 60000)
                      if (diffMins < 60) return `${diffMins}m ago`
                      const diffHrs = Math.floor(diffMins / 60)
                      const remMins = diffMins % 60
                      return remMins > 0 ? `${diffHrs}h ${remMins}m ago` : `${diffHrs}h ago`
                    })()}
                  </td>
                  <td className="px-3 py-2 text-slate-400 text-xs font-mono">{trade.ticker}</td>
                  <td className="px-3 py-2">
                    <button
                      onClick={() => {
                        retry.mutate([trade.id], {
                          onSuccess: () => {
                            queryClient.invalidateQueries({ queryKey: ['trade-queue'] })
                          },
                        })
                      }}
                      disabled={retry.isPending}
                      className="px-2 py-1 text-xs font-medium rounded bg-yellow-500/20 text-yellow-400 hover:bg-yellow-500/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                    >
                      Retry
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {retry.isSuccess && (
          <div className="px-4 py-2 bg-green-500/10 text-green-400 text-xs">
            Trade re-queued for execution.
          </div>
        )}
      </div>
    )}
    </>
  )
}
