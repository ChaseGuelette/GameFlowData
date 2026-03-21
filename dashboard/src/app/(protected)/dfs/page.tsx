'use client'

import { useEffect, useState, useMemo, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { DfsFilters } from '@/components/dfs/DfsFilters'
import { DfsTable, type DfsRow, type ModelDfsRow, type MarketDfsRow, type CombinedDfsRow } from '@/components/dfs/DfsTable'
import { type Prediction, type StatType } from '@/types/predictions'
import {
  type DfsLine, type DfsComparison, type DfsPlatformLine,
  type SportsbookLine, type MarketEdgePlatformLine, type CombinedEdgePlatformLine,
  type EdgeMode, MARKET_TO_STAT, DFS_SLIP_TYPES,
} from '@/types/dfs'
import { TEAM_ABBREV } from '@/lib/constants'
import { getToday, formatDate } from '@/lib/utils'
import { estimateOverProb, estimateUnderProb, calcAllSlipEvs, devig, computeVig } from '@/lib/dfs-utils'

// Normalize game_id to 10-digit zero-padded format to match RPC LPAD output
const padGameId = (id: string) => id.padStart(10, '0')

export default function DfsPage() {
  const [predictions, setPredictions] = useState<Prediction[]>([])
  const [dfsLines, setDfsLines] = useState<DfsLine[]>([])
  const [sportsbookLines, setSportsbookLines] = useState<SportsbookLine[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedDate, setSelectedDate] = useState<string>(getToday())
  const [availableDates, setAvailableDates] = useState<string[]>([])

  // Filters
  const [edgeMode, setEdgeMode] = useState<EdgeMode>('model')
  const [platformFilter, setPlatformFilter] = useState<string>('all')
  const [slipType, setSlipType] = useState<string>('pp_6_flex')
  const [statFilter, setStatFilter] = useState<'all' | StatType>('all')
  const [evOnly, setEvOnly] = useState(true)
  const [showLive, setShowLive] = useState(false)

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

  // Fetch predictions, DFS lines, and sportsbook lines when date changes
  const fetchData = useCallback(async (date: string) => {
    setLoading(true)
    const supabase = createClient()

    // Step 1: Fetch predictions and DFS lines in parallel
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

    let dfsData: DfsLine[] = []
    if (!dfsRes.error && dfsRes.data) {
      dfsData = dfsRes.data
      setDfsLines(dfsData)
    } else {
      // eslint-disable-next-line no-console
      if (dfsRes.error) console.error('get_dfs_lines RPC error:', dfsRes.error)
      setDfsLines([])
    }

    // Step 2: Fetch sportsbook lines per-game in parallel (avoids timeout on large table)
    const gameIds = [...new Set(dfsData.map(dl => dl.game_id))]
    if (gameIds.length > 0) {
      // Batch game_ids into groups of 3 to balance parallelism vs query size
      const batchSize = 3
      const batches: string[][] = []
      for (let i = 0; i < gameIds.length; i += batchSize) {
        batches.push(gameIds.slice(i, i + batchSize))
      }
      const sbResults = await Promise.all(
        batches.map(batch =>
          supabase.rpc('get_sportsbook_lines_by_games', { p_game_ids: batch })
        )
      )
      const allSbLines: SportsbookLine[] = []
      for (const res of sbResults) {
        if (!res.error && res.data) {
          allSbLines.push(...res.data)
        }
      }
      setSportsbookLines(allSbLines)
    } else {
      setSportsbookLines([])
    }

    setLoading(false)
  }, [])

  useEffect(() => {
    if (selectedDate) fetchData(selectedDate)
  }, [selectedDate, fetchData])

  // Join predictions with DFS lines and compute model comparisons (unchanged logic)
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

    // Build prediction lookup (may be empty before inference)
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

      // Get sportsbook lines for this player/stat
      const sbLines = sbIndex.get(key) || []

      const platforms: MarketEdgePlatformLine[] = dfsLinesForPlayer.map(dl => {
        const dfsLine = Number(dl.line)

        // Find sportsbooks offering the exact same line value
        const matchingBooks = sbLines.filter(sb => Number(sb.line) === dfsLine && sb.over_odds !== null && sb.under_odds !== null)

        // Compute devigged consensus from matching books
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

        // Find the sharpest sportsbook (lowest vig) regardless of line value
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

        // Determine best direction from market probs
        let bestDirection: 'over' | 'under' = 'over'
        let bestMarketProb: number | null = null
        if (marketProbOver !== null && marketProbUnder !== null) {
          bestDirection = marketProbOver >= marketProbUnder ? 'over' : 'under'
          bestMarketProb = Math.max(marketProbOver, marketProbUnder)
        } else if (sharpLine !== null) {
          // No exact match — use sharp line direction heuristic
          bestDirection = dfsLine > sharpLine ? 'under' : 'over'
        }

        // Edge = market_prob - break_even per slip type
        const evBySlip: Record<string, number> = {}
        if (bestMarketProb !== null) {
          for (const [slipKey, slip] of Object.entries(DFS_SLIP_TYPES)) {
            evBySlip[slipKey] = bestMarketProb - slip.breakEven
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

  // Flatten comparisons into table rows and apply filters — branch on edgeMode
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
      // Sort: rows with market_prob first by edge desc, then rows without market_prob
      rows.sort((a, b) => {
        const aHas = a.platform.market_prob !== null ? 1 : 0
        const bHas = b.platform.market_prob !== null ? 1 : 0
        if (aHas !== bHas) return bHas - aHas
        return (b.platform.ev_by_slip[slipType] ?? 0) - (a.platform.ev_by_slip[slipType] ?? 0)
      })
      return rows
    }

    // Combined mode — only rows where BOTH model AND market have positive edge and agree on direction
    const rows: CombinedDfsRow[] = []

    for (const comp of comparisons) {
      if (!showLive && comp.game_time && new Date(comp.game_time) <= now) continue
      if (statFilter !== 'all' && comp.stat !== statFilter) continue

      for (const modelPl of comp.dfs_lines) {
        if (platformFilter !== 'all' && modelPl.bookmaker !== platformFilter) continue

        // Find matching market data
        const key = `${comp.player_id}-${padGameId(comp.game_id)}-${comp.stat}`
        const marketData = marketComparisons.get(key)
        if (!marketData) continue

        const marketPl = marketData.platforms.find(mp => mp.bookmaker === modelPl.bookmaker && mp.line === modelPl.line)
        if (!marketPl || marketPl.market_prob === null) continue

        // Direction must agree
        if (modelPl.best_direction !== marketPl.best_direction) continue

        const modelEdge = modelPl.best_prob - breakEven
        const marketEdge = marketPl.market_prob - breakEven

        // Both must be positive
        if (modelEdge <= 0 || marketEdge <= 0) continue

        // Combined edge = min(model, market) per slip type — conservative
        const combinedEvBySlip: Record<string, number> = {}
        const modelEvBySlip = modelPl.ev_by_slip
        const marketEvBySlip = marketPl.ev_by_slip
        for (const [slipKey, slip] of Object.entries(DFS_SLIP_TYPES)) {
          const mEdge = modelPl.best_prob - slip.breakEven
          const mkEdge = (marketPl.market_prob ?? 0) - slip.breakEven
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

  // Detect when Pre-Game filter is hiding all results (all games have started)
  const allGamesStarted = useMemo(() => {
    if (showLive || loading) return false
    const now = new Date()
    const hasData = edgeMode === 'model'
      ? comparisons.length > 0
      : marketComparisons.size > 0
    if (!hasData) return false
    // Check if every comparison has a past game_time
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

      {/* Content */}
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
          <DfsTable rows={filteredRows} slipType={slipType} edgeMode={edgeMode} />
        </div>
      )}
    </main>
  )
}
