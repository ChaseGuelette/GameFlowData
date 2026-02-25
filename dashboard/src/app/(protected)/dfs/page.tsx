'use client'

import { useEffect, useState, useMemo, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { DfsFilters } from '@/components/dfs/DfsFilters'
import { DfsTable, type DfsRow } from '@/components/dfs/DfsTable'
import { type Prediction, type StatType } from '@/types/predictions'
import { type DfsLine, type DfsComparison, type DfsPlatformLine, MARKET_TO_STAT, DFS_SLIP_TYPES } from '@/types/dfs'
import { TEAM_ABBREV } from '@/lib/constants'
import { getToday, formatDate } from '@/lib/utils'
import { estimateOverProb, estimateUnderProb, calcAllSlipEvs } from '@/lib/dfs-utils'

export default function DfsPage() {
  const [predictions, setPredictions] = useState<Prediction[]>([])
  const [dfsLines, setDfsLines] = useState<DfsLine[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedDate, setSelectedDate] = useState<string>(getToday())
  const [availableDates, setAvailableDates] = useState<string[]>([])

  // Filters
  const [platformFilter, setPlatformFilter] = useState<string>('all')
  const [slipType, setSlipType] = useState<string>('pp_6_flex')
  const [statFilter, setStatFilter] = useState<'all' | StatType>('all')
  const [evOnly, setEvOnly] = useState(true)

  // Fetch available dates
  useEffect(() => {
    async function fetchDates() {
      const supabase = createClient()
      const { data } = await supabase
        .rpc('get_prediction_dates', { days_back: 30 })

      if (data) {
        const dates = data.map((d: { prediction_date: string }) => d.prediction_date)
        setAvailableDates(dates)
        const today = getToday()
        if (dates.length > 0 && !dates.includes(today)) {
          setSelectedDate(dates[0])
        }
      }
    }
    fetchDates()
  }, [])

  // Fetch predictions and DFS lines when date changes
  const fetchData = useCallback(async (date: string) => {
    setLoading(true)
    const supabase = createClient()

    // Fetch predictions and DFS lines in parallel
    const [predictionsRes, dfsRes] = await Promise.all([
      supabase
        .from('daily_predictions')
        .select('*')
        .eq('prediction_date', date)
        .not('line', 'is', null)
        .limit(3000),
      supabase
        .rpc('get_dfs_lines', { target_date: date }),
    ])

    if (!predictionsRes.error && predictionsRes.data) {
      const mapped = predictionsRes.data
        .filter(p => Number.isFinite(p.over_edge) || Number.isFinite(p.under_edge))
        .map(p => ({
          ...p,
          prop_line: p.line,
          model_prob_over: p.over_prob,
          model_prob_under: p.under_prob,
          implied_prob_over: p.implied_over,
          implied_prob_under: p.implied_under,
          q10: p.pred_q10,
          q25: p.pred_q25,
          q50: p.pred_q50,
          q75: p.pred_q75,
          q90: p.pred_q90,
          team_abbrev: TEAM_ABBREV[p.team_id] || 'UNK',
          opponent_abbrev: TEAM_ABBREV[p.opponent_id] || 'UNK',
        }))
      setPredictions(mapped)
    } else {
      setPredictions([])
    }

    if (!dfsRes.error && dfsRes.data) {
      setDfsLines(dfsRes.data)
    } else {
      setDfsLines([])
    }

    setLoading(false)
  }, [])

  useEffect(() => {
    if (selectedDate) fetchData(selectedDate)
  }, [selectedDate, fetchData])

  // Join predictions with DFS lines and compute comparisons
  const comparisons = useMemo<DfsComparison[]>(() => {
    if (predictions.length === 0 || dfsLines.length === 0) return []

    // Index predictions by (player_id, game_id, stat)
    const predMap = new Map<string, Prediction>()
    for (const p of predictions) {
      predMap.set(`${p.player_id}-${p.game_id}-${p.stat}`, p)
    }

    // Group DFS lines by (player_id, game_id, market_key)
    const dfsGrouped = new Map<string, DfsLine[]>()
    for (const dl of dfsLines) {
      const stat = MARKET_TO_STAT[dl.market_key]
      if (!stat) continue
      const key = `${dl.player_id}-${dl.game_id}-${stat}`
      if (!dfsGrouped.has(key)) dfsGrouped.set(key, [])
      dfsGrouped.get(key)!.push(dl)
    }

    const results: DfsComparison[] = []

    for (const [key, lines] of dfsGrouped) {
      const pred = predMap.get(key)
      if (!pred) continue

      const hasQuantiles = pred.q10 != null && pred.q25 != null && pred.q50 != null && pred.q75 != null && pred.q90 != null

      const platformLines: DfsPlatformLine[] = lines.map(dl => {
        const dfsLine = Number(dl.line)
        const lineDiff = dfsLine - pred.prop_line

        let overProb: number
        let underProb: number

        if (hasQuantiles) {
          overProb = estimateOverProb(dfsLine, pred.q10, pred.q25, pred.q50, pred.q75, pred.q90)
          underProb = estimateUnderProb(dfsLine, pred.q10, pred.q25, pred.q50, pred.q75, pred.q90)
        } else {
          // Fallback: use model probabilities (only valid when DFS line matches sharp line)
          overProb = pred.model_prob_over
          underProb = pred.model_prob_under
        }

        const bestDirection = overProb >= underProb ? 'over' as const : 'under' as const
        const bestProb = Math.max(overProb, underProb)
        const evBySlip = calcAllSlipEvs(bestProb)

        return {
          bookmaker: dl.bookmaker,
          line: dfsLine,
          line_diff: lineDiff,
          model_prob_over: overProb,
          model_prob_under: underProb,
          best_direction: bestDirection,
          best_prob: bestProb,
          ev_by_slip: evBySlip,
        }
      })

      results.push({
        player_id: pred.player_id,
        player_name: pred.player_name || `Player ${pred.player_id}`,
        game_id: pred.game_id,
        stat: pred.stat,
        team_abbrev: pred.team_abbrev || 'UNK',
        opponent_abbrev: pred.opponent_abbrev || 'UNK',
        game_time: pred.game_time,
        sharp_line: pred.prop_line,
        q10: pred.q10,
        q25: pred.q25,
        q50: pred.q50,
        q75: pred.q75,
        q90: pred.q90,
        model_prob_over: pred.model_prob_over,
        model_prob_under: pred.model_prob_under,
        dfs_lines: platformLines,
      })
    }

    return results
  }, [predictions, dfsLines])

  // Flatten comparisons into table rows and apply filters
  const filteredRows = useMemo<DfsRow[]>(() => {
    const rows: DfsRow[] = []
    const breakEven = DFS_SLIP_TYPES[slipType]?.breakEven ?? 0.55

    for (const comp of comparisons) {
      // Stat filter
      if (statFilter !== 'all' && comp.stat !== statFilter) continue

      for (const pl of comp.dfs_lines) {
        // Platform filter
        if (platformFilter !== 'all' && pl.bookmaker !== platformFilter) continue

        const edge = pl.ev_by_slip[slipType] ?? 0

        // EV filter
        if (evOnly && edge <= 0) continue

        rows.push({ comparison: comp, platform: pl })
      }
    }

    // Default sort by edge descending
    rows.sort((a, b) => {
      const aEdge = a.platform.ev_by_slip[slipType] ?? 0
      const bEdge = b.platform.ev_by_slip[slipType] ?? 0
      return bEdge - aEdge
    })

    return rows
  }, [comparisons, platformFilter, statFilter, slipType, evOnly])

  // Summary stats
  const summaryStats = useMemo(() => {
    if (filteredRows.length === 0) return { count: 0, avgEdge: 0, bestPick: null as DfsRow | null }

    const edges = filteredRows.map(r => r.platform.ev_by_slip[slipType] ?? 0)
    const avgEdge = edges.reduce((a, b) => a + b, 0) / edges.length

    return {
      count: filteredRows.length,
      avgEdge,
      bestPick: filteredRows[0],
    }
  }, [filteredRows, slipType])

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-50">DFS Edge Finder</h1>
          <p className="text-slate-400">
            {formatDate(selectedDate)} {!loading && (
              <span>
                {' '}&bull; {summaryStats.count} {evOnly ? '+EV' : 'total'} picks
              </span>
            )}
          </p>
        </div>
        <select
          value={selectedDate}
          onChange={(e) => setSelectedDate(e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
        >
          {availableDates.map((date) => (
            <option key={date} value={date}>
              {date === getToday() ? `${formatDate(date)} (Today)` : formatDate(date)}
            </option>
          ))}
        </select>
      </div>

      {/* Filters */}
      <div className="mb-6">
        <DfsFilters
          platformFilter={platformFilter}
          onPlatformChange={setPlatformFilter}
          slipType={slipType}
          onSlipTypeChange={setSlipType}
          statFilter={statFilter}
          onStatChange={setStatFilter}
          evOnly={evOnly}
          onEvOnlyChange={setEvOnly}
        />
      </div>

      {/* Summary KPI cards */}
      {!loading && summaryStats.count > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
          <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
            <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">+EV Picks</div>
            <div className="text-2xl font-bold text-green-400">{summaryStats.count}</div>
          </div>
          <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
            <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">Avg Edge</div>
            <div className="text-2xl font-bold text-green-400">
              +{(summaryStats.avgEdge * 100).toFixed(1)}%
            </div>
          </div>
          {summaryStats.bestPick && (
            <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
              <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">Best Edge</div>
              <div className="text-2xl font-bold text-green-400">
                +{((summaryStats.bestPick.platform.ev_by_slip[slipType] ?? 0) * 100).toFixed(1)}%
              </div>
              <div className="text-xs text-slate-500 mt-1">
                {summaryStats.bestPick.comparison.player_name} {summaryStats.bestPick.comparison.stat.toUpperCase()}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-16">
          <div className="flex flex-col items-center gap-3">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
            <div className="text-slate-400">Loading DFS lines...</div>
          </div>
        </div>
      ) : (
        <div className="bg-slate-800/50 rounded-lg border border-slate-700">
          <DfsTable rows={filteredRows} slipType={slipType} />
        </div>
      )}
    </main>
  )
}
