'use client'

import { useEffect, useState, useMemo } from 'react'
import { createClient } from '@/lib/supabase/client'
import { DfsFilters } from '@/components/dfs/DfsFilters'
import { DfsTable, type DfsRow, type ModelDfsRow, type MarketDfsRow, type CombinedDfsRow } from '@/components/dfs/DfsTable'
import { SlipBuilderPanel } from '@/components/dfs/SlipBuilderPanel'
import { type Prediction, type StatType } from '@/types/predictions'
import {
  type DfsLine, type DfsComparison, type DfsPlatformLine,
  type SportsbookLine, type MarketEdgePlatformLine, type CombinedEdgePlatformLine,
  type EdgeMode, MARKET_TO_STAT, DFS_SLIP_TYPES,
} from '@/types/dfs'
import { getToday, formatDate } from '@/lib/utils'
import { estimateOverProb, estimateUnderProb, calcAllSlipEvs, devig, computeVig } from '@/lib/dfs-utils'
import { useSport } from '@/contexts/SportContext'
import { useSlipBuilder } from '@/lib/hooks/useSlipBuilder'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'
import { useDfsLines } from '@/lib/hooks/useDfsLines'

// Normalize game_id to 10-digit zero-padded format to match RPC LPAD output
const padGameId = (id: string) => id.padStart(10, '0')

export default function DfsPage() {
  const { config } = useSport()

  if (config.sport !== 'nba') {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="text-center">
          <h2 className="text-xl font-semibold text-slate-200 mb-2">DFS Edge Finder</h2>
          <p className="text-slate-400">DFS analysis is currently available for NBA only.</p>
        </div>
      </div>
    )
  }

  const [selectedDate, setSelectedDate] = useState<string>(getToday())
  const [availableDates, setAvailableDates] = useState<string[]>([])

  // Filters
  const [edgeMode, setEdgeMode] = useState<EdgeMode>('model')
  const [platformFilter, setPlatformFilter] = useState<string>('all')
  const [slipType, setSlipType] = useState<string>('pp_2_power')
  const [statFilter, setStatFilter] = useState<'all' | StatType>('all')
  const [evOnly, setEvOnly] = useState(true)
  const [showLive, setShowLive] = useState(false)

  // Slip builder
  const slip = useSlipBuilder(edgeMode)
  const { prefs } = useUserPreferences()

  // React Query hook — replaces manual fetchData + useEffect
  const { data: dfsData, isLoading: loading } = useDfsLines(selectedDate)
  const predictions = dfsData?.predictions ?? []
  const dfsLines = dfsData?.dfsLines ?? []
  const sportsbookLines = dfsData?.sportsbookLines ?? []

  // Fetch available dates
  useEffect(() => {
    async function fetchDates() {
      const supabase = createClient()
      const { data } = await supabase
        .rpc('get_prediction_dates', { days_back: 30 })

      if (data) {
        const dates = data.map((d: { prediction_date: string }) => d.prediction_date)
        const today = getToday()
        if (!dates.includes(today)) {
          dates.unshift(today)
        }
        setAvailableDates(dates)
      }
    }
    fetchDates()
  }, [])

  // Join predictions with DFS lines and compute model comparisons
  const comparisons = useMemo<DfsComparison[]>(() => {
    if (predictions.length === 0 || dfsLines.length === 0) return []

    const predMap = new Map<string, Prediction>()
    for (const p of predictions) {
      predMap.set(`${p.player_id}-${padGameId(p.game_id)}-${p.stat}`, p)
    }

    const dfsGrouped = new Map<string, DfsLine[]>()
    for (const dl of dfsLines) {
      const stat = MARKET_TO_STAT[dl.market_key]
      if (!stat) continue
      const key = `${dl.player_id}-${padGameId(dl.game_id)}-${stat}`
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

  // Index sportsbook lines by {player_id}-{game_id}-{stat}
  const sbIndex = useMemo(() => {
    const idx = new Map<string, SportsbookLine[]>()
    for (const sb of sportsbookLines) {
      const stat = MARKET_TO_STAT[sb.market_key]
      if (!stat) continue
      const key = `${sb.player_id}-${padGameId(sb.game_id)}-${stat}`
      if (!idx.has(key)) idx.set(key, [])
      idx.get(key)!.push(sb)
    }
    return idx
  }, [sportsbookLines])

  // Compute market edge data for each DFS line
  const marketComparisons = useMemo(() => {
    if (dfsLines.length === 0) return new Map<string, { comp: DfsComparison; platforms: MarketEdgePlatformLine[] }>()

    const predMap = new Map<string, Prediction>()
    for (const p of predictions) {
      predMap.set(`${p.player_id}-${padGameId(p.game_id)}-${p.stat}`, p)
    }

    const dfsGrouped = new Map<string, DfsLine[]>()
    for (const dl of dfsLines) {
      const stat = MARKET_TO_STAT[dl.market_key]
      if (!stat) continue
      const key = `${dl.player_id}-${padGameId(dl.game_id)}-${stat}`
      if (!dfsGrouped.has(key)) dfsGrouped.set(key, [])
      dfsGrouped.get(key)!.push(dl)
    }

    const results = new Map<string, { comp: DfsComparison; platforms: MarketEdgePlatformLine[] }>()

    for (const [key, dfsLinesForPlayer] of dfsGrouped) {
      const pred = predMap.get(key)
      const firstLine = dfsLinesForPlayer[0]
      const stat = MARKET_TO_STAT[firstLine.market_key] as StatType

      const sbLines = sbIndex.get(key) || []

      const platforms: MarketEdgePlatformLine[] = dfsLinesForPlayer.map(dl => {
        const dfsLine = Number(dl.line)

        const matchingBooks = sbLines.filter(sb => Number(sb.line) === dfsLine && sb.over_odds !== null && sb.under_odds !== null)

        let marketProbOver: number | null = null
        let marketProbUnder: number | null = null
        if (matchingBooks.length > 0) {
          let sumOver = 0
          let sumUnder = 0
          for (const mb of matchingBooks) {
            const [devOver, devUnder] = devig(mb.over_odds!, mb.under_odds!)
            sumOver += devOver
            sumUnder += devUnder
          }
          marketProbOver = sumOver / matchingBooks.length
          marketProbUnder = sumUnder / matchingBooks.length
        }

        let sharpBook: string | null = null
        let sharpLine: number | null = null
        let lowestVig = Infinity
        for (const sb of sbLines) {
          if (sb.over_odds === null || sb.under_odds === null) continue
          const vig = computeVig(sb.over_odds, sb.under_odds)
          if (vig < lowestVig) {
            lowestVig = vig
            sharpBook = sb.bookmaker
            sharpLine = Number(sb.line)
          }
        }

        const lineDiff = sharpLine !== null ? dfsLine - sharpLine : null

        let bestDirection: 'over' | 'under' = 'over'
        let bestMarketProb: number | null = null
        if (marketProbOver !== null && marketProbUnder !== null) {
          bestDirection = marketProbOver >= marketProbUnder ? 'over' : 'under'
          bestMarketProb = Math.max(marketProbOver, marketProbUnder)
        } else if (sharpLine !== null) {
          bestDirection = dfsLine > sharpLine ? 'under' : 'over'
        }

        const evBySlip: Record<string, number> = {}
        if (bestMarketProb !== null) {
          for (const [slipKey, slipDef] of Object.entries(DFS_SLIP_TYPES)) {
            evBySlip[slipKey] = bestMarketProb - slipDef.breakEven
          }
        }

        return {
          bookmaker: dl.bookmaker,
          line: dfsLine,
          best_direction: bestDirection,
          market_prob: bestMarketProb,
          market_books_count: matchingBooks.length,
          sharp_book: sharpBook,
          sharp_line: sharpLine,
          line_diff: lineDiff,
          ev_by_slip: evBySlip,
        }
      })

      const comp: DfsComparison = {
        player_id: pred?.player_id ?? firstLine.player_id,
        player_name: pred?.player_name || firstLine.player_name || `Player ${firstLine.player_id}`,
        game_id: pred?.game_id ?? firstLine.game_id,
        stat: pred?.stat ?? stat,
        team_abbrev: pred?.team_abbrev || 'UNK',
        opponent_abbrev: pred?.opponent_abbrev || 'UNK',
        game_time: pred?.game_time || firstLine.game_time,
        sharp_line: pred?.prop_line ?? 0,
        q10: pred?.q10 ?? 0,
        q25: pred?.q25 ?? 0,
        q50: pred?.q50 ?? 0,
        q75: pred?.q75 ?? 0,
        q90: pred?.q90 ?? 0,
        model_prob_over: pred?.model_prob_over ?? 0,
        model_prob_under: pred?.model_prob_under ?? 0,
        dfs_lines: [],
      }

      results.set(key, { comp, platforms })
    }

    return results
  }, [dfsLines, predictions, sbIndex])

  // Flatten comparisons into table rows and apply filters
  const filteredRows = useMemo<DfsRow[]>(() => {
    const breakEven = DFS_SLIP_TYPES[slipType]?.breakEven ?? 0.55

    const now = new Date()

    if (edgeMode === 'model') {
      const rows: ModelDfsRow[] = []
      for (const comp of comparisons) {
        if (!showLive && comp.game_time && new Date(comp.game_time) <= now) continue
        if (statFilter !== 'all' && comp.stat !== statFilter) continue
        for (const pl of comp.dfs_lines) {
          if (platformFilter !== 'all' && pl.bookmaker !== platformFilter) continue
          const edge = pl.ev_by_slip[slipType] ?? 0
          if (evOnly && edge <= 0) continue
          rows.push({ comparison: comp, platform: pl })
        }
      }
      rows.sort((a, b) => (b.platform.ev_by_slip[slipType] ?? 0) - (a.platform.ev_by_slip[slipType] ?? 0))
      return rows
    }

    if (edgeMode === 'market') {
      const rows: MarketDfsRow[] = []
      for (const [, { comp, platforms }] of marketComparisons) {
        if (!showLive && comp.game_time && new Date(comp.game_time) <= now) continue
        if (statFilter !== 'all' && comp.stat !== statFilter) continue
        for (const pl of platforms) {
          if (platformFilter !== 'all' && pl.bookmaker !== platformFilter) continue
          const edge = pl.ev_by_slip[slipType] ?? 0
          if (evOnly && (pl.market_prob === null || edge <= 0)) continue
          rows.push({ comparison: comp, platform: pl })
        }
      }
      rows.sort((a, b) => {
        const aHas = a.platform.market_prob !== null ? 1 : 0
        const bHas = b.platform.market_prob !== null ? 1 : 0
        if (aHas !== bHas) return bHas - aHas
        return (b.platform.ev_by_slip[slipType] ?? 0) - (a.platform.ev_by_slip[slipType] ?? 0)
      })
      return rows
    }

    // Combined mode
    const rows: CombinedDfsRow[] = []

    for (const comp of comparisons) {
      if (!showLive && comp.game_time && new Date(comp.game_time) <= now) continue
      if (statFilter !== 'all' && comp.stat !== statFilter) continue

      for (const modelPl of comp.dfs_lines) {
        if (platformFilter !== 'all' && modelPl.bookmaker !== platformFilter) continue

        const key = `${comp.player_id}-${padGameId(comp.game_id)}-${comp.stat}`
        const marketData = marketComparisons.get(key)
        if (!marketData) continue

        const marketPl = marketData.platforms.find(mp => mp.bookmaker === modelPl.bookmaker && mp.line === modelPl.line)
        if (!marketPl || marketPl.market_prob === null) continue

        if (modelPl.best_direction !== marketPl.best_direction) continue

        const modelEdge = modelPl.best_prob - breakEven
        const marketEdge = marketPl.market_prob - breakEven

        if (modelEdge <= 0 || marketEdge <= 0) continue

        const combinedEvBySlip: Record<string, number> = {}
        const modelEvBySlip = modelPl.ev_by_slip
        const marketEvBySlip = marketPl.ev_by_slip
        for (const [slipKey, slipDef] of Object.entries(DFS_SLIP_TYPES)) {
          const mEdge = modelPl.best_prob - slipDef.breakEven
          const mkEdge = (marketPl.market_prob ?? 0) - slipDef.breakEven
          combinedEvBySlip[slipKey] = Math.min(mEdge, mkEdge)
        }

        const combinedPl: CombinedEdgePlatformLine = {
          bookmaker: modelPl.bookmaker,
          line: modelPl.line,
          best_direction: modelPl.best_direction,
          model_prob: modelPl.best_prob,
          model_ev_by_slip: modelEvBySlip,
          market_prob: marketPl.market_prob,
          market_books_count: marketPl.market_books_count,
          market_ev_by_slip: marketEvBySlip,
          sharp_book: marketPl.sharp_book,
          sharp_line: marketPl.sharp_line,
          line_diff: marketPl.line_diff,
          ev_by_slip: combinedEvBySlip,
        }

        const edge = combinedEvBySlip[slipType] ?? 0
        if (evOnly && edge <= 0) continue

        rows.push({ comparison: comp, platform: combinedPl })
      }
    }

    rows.sort((a, b) => (b.platform.ev_by_slip[slipType] ?? 0) - (a.platform.ev_by_slip[slipType] ?? 0))
    return rows
  }, [comparisons, marketComparisons, platformFilter, statFilter, slipType, evOnly, edgeMode, showLive])

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

  // Detect when Pre-Game filter is hiding all results
  const allGamesStarted = useMemo(() => {
    if (showLive || loading) return false
    const now = new Date()
    const hasData = edgeMode === 'model'
      ? comparisons.length > 0
      : marketComparisons.size > 0
    if (!hasData) return false
    if (edgeMode === 'model') {
      return comparisons.every(c => c.game_time && new Date(c.game_time) <= now)
    }
    return [...marketComparisons.values()].every(({ comp }) => comp.game_time && new Date(comp.game_time) <= now)
  }, [showLive, loading, edgeMode, comparisons, marketComparisons])

  const modeSubtitles: Record<EdgeMode, string> = {
    model: 'Model probability vs DFS break-even',
    market: 'Sharp sportsbook consensus vs DFS line',
    combined: 'Both model & market agree',
  }

  return (
    <main className="flex-1 max-w-[90rem] mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
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
          edgeMode={edgeMode}
          onEdgeModeChange={setEdgeMode}
          platformFilter={platformFilter}
          onPlatformChange={setPlatformFilter}
          slipType={slipType}
          onSlipTypeChange={setSlipType}
          statFilter={statFilter}
          onStatChange={setStatFilter}
          evOnly={evOnly}
          onEvOnlyChange={setEvOnly}
          showLive={showLive}
          onShowLiveChange={setShowLive}
        />
      </div>

      {/* Summary KPI cards */}
      {!loading && summaryStats.count > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
          <div className="bg-slate-800 rounded-lg border border-slate-700 p-4">
            <div className="text-xs text-slate-400 uppercase tracking-wider mb-1">+EV Picks</div>
            <div className="text-2xl font-bold text-green-400">{summaryStats.count}</div>
            <div className="text-xs text-slate-500 mt-1">{modeSubtitles[edgeMode]}</div>
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

      {/* Content + Slip Builder */}
      <div className="flex gap-6">
        <div className="flex-1 min-w-0">
          {loading ? (
            <div className="flex items-center justify-center py-16">
              <div className="flex flex-col items-center gap-3">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
                <div className="text-slate-400">Loading DFS lines...</div>
              </div>
            </div>
          ) : allGamesStarted ? (
            <div className="bg-slate-800/50 rounded-lg border border-slate-700">
              <div className="flex items-center justify-center py-16">
                <div className="text-center">
                  <p className="text-slate-400 text-lg">All games have started</p>
                  <p className="text-slate-500 text-sm mt-2">
                    Click <button onClick={() => setShowLive(true)} className="text-orange-400 hover:text-orange-300 font-medium">+ Live</button> to view in-progress and completed game lines
                  </p>
                </div>
              </div>
            </div>
          ) : (
            <div className="bg-slate-800/50 rounded-lg border border-slate-700">
              <DfsTable
                rows={filteredRows}
                slipType={slipType}
                edgeMode={edgeMode}
                selectedLegKeys={slip.selectedLegKeys}
                onToggleLeg={slip.toggleLeg}
              />
            </div>
          )}
        </div>

        <SlipBuilderPanel
          legs={slip.legs}
          slipType={slip.slipType}
          onSlipTypeChange={slip.setSlipType}
          edgeMode={edgeMode}
          bankroll={prefs.bankroll}
          kellyFraction={prefs.kellyFraction}
          validation={slip.validation}
          placing={slip.placing}
          error={slip.error}
          onRemoveLeg={slip.removeLeg}
          onClearAll={slip.clearAll}
          onPlace={() => slip.placeEntry(prefs.bankroll, prefs.kellyFraction, selectedDate)}
        />
      </div>
    </main>
  )
}
