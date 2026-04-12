'use client'

import { useEffect, useState, useMemo, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { cn } from '@/lib/utils'
import { MLB_CONFIG } from '@/lib/sport-config'
import { CategoryTabs } from '@/components/stats/CategoryTabs'
import { WindowToggle } from '@/components/stats/WindowToggle'
import { HeatmapTable, HeatmapLegend } from '@/components/stats/HeatmapTable'
import {
  mlbBatterBoxColumns,
  mlbBatterRatesColumns,
  mlbBatterConsistencyColumns,
  mlbPitcherBoxColumns,
  mlbPitcherRatesColumns,
  mlbPitcherConsistencyColumns,
} from '@/lib/stats/columns'
import type { StatRow, SortState, WindowSuffix } from '@/types/stats'

// ─── Types ──────────────────────────────────────────────────────────
type MLBMainTab = 'batters' | 'pitchers'
type BatterCategory = 'box' | 'rates' | 'consistency'
type PitcherCategory = 'box' | 'rates' | 'consistency'

// ─── Window options ──────────────────────────────────────────────────
const BATTER_WINDOWS: { value: WindowSuffix; label: string }[] = [
  { value: 'l5', label: 'L5' },
  { value: 'l10', label: 'L10' },
  { value: 'l20', label: 'L20' },
  { value: 'szn', label: 'SZN' },
]

const PITCHER_WINDOWS: { value: WindowSuffix; label: string }[] = [
  { value: 'l3', label: 'L3' },
  { value: 'l5', label: 'L5' },
  { value: 'szn', label: 'SZN' },
]

// ─── Category options ────────────────────────────────────────────────
const batterCategories: { value: BatterCategory; label: string }[] = [
  { value: 'box', label: 'Box Score' },
  { value: 'rates', label: 'Rates' },
  { value: 'consistency', label: 'Consistency' },
]

const pitcherCategories: { value: PitcherCategory; label: string }[] = [
  { value: 'box', label: 'Box Score' },
  { value: 'rates', label: 'Rates' },
  { value: 'consistency', label: 'Consistency' },
]

const teamOptions = Object.values(MLB_CONFIG.teams).sort()

// ─── Component ──────────────────────────────────────────────────────
export function MLBStatsPage() {
  const supabase = createClient()

  // Tab state
  const [mainTab, setMainTab] = useState<MLBMainTab>('batters')
  const [batterCategory, setBatterCategory] = useState<BatterCategory>('box')
  const [pitcherCategory, setPitcherCategory] = useState<PitcherCategory>('box')

  // Window state
  const [batterWindow, setBatterWindow] = useState<WindowSuffix>('l5')
  const [pitcherWindow, setPitcherWindow] = useState<WindowSuffix>('l5')

  // Filter state
  const [teamFilter, setTeamFilter] = useState('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [minCount, setMinCount] = useState(3)

  // Sort state
  const [batterSort, setBatterSort] = useState<SortState>({ column: 'h', direction: 'desc' })
  const [pitcherSort, setPitcherSort] = useState<SortState>({ column: 'so', direction: 'desc' })

  // Data state
  const [batterData, setBatterData] = useState<StatRow[]>([])
  const [pitcherData, setPitcherData] = useState<StatRow[]>([])
  const [loading, setLoading] = useState(true)

  // ─── Fetch ──────────────────────────────────────────────────────
  useEffect(() => {
    async function fetchAll() {
      setLoading(true)
      const [battersRes, pitchersRes] = await Promise.all([
        supabase.from('mlb_batters_latest').select('*'),
        supabase.from('mlb_pitchers_latest').select('*'),
      ])
      if (battersRes.data) {
        setBatterData(
          battersRes.data.map((r: Record<string, unknown>) => ({
            ...r,
            id: r.player_id as number,
            name: r.player_name as string,
            teamAbbrev: r.team_abbrev as string,
          })) as StatRow[]
        )
      }
      if (pitchersRes.data) {
        setPitcherData(
          pitchersRes.data.map((r: Record<string, unknown>) => ({
            ...r,
            id: r.player_id as number,
            name: r.player_name as string,
            teamAbbrev: r.team_abbrev as string,
          })) as StatRow[]
        )
      }
      setLoading(false)
    }
    fetchAll()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // ─── Filtered rows ──────────────────────────────────────────────
  const filteredBatters = useMemo(() => {
    return batterData.filter((row) => {
      if (teamFilter !== 'all' && row.teamAbbrev !== teamFilter) return false
      if (searchQuery && !row.name.toLowerCase().includes(searchQuery.toLowerCase())) return false
      const gp = row[`games_${batterWindow}`]
      if (gp != null && typeof gp === 'number' && gp < minCount) return false
      return true
    })
  }, [batterData, teamFilter, searchQuery, minCount, batterWindow])

  const filteredPitchers = useMemo(() => {
    return pitcherData.filter((row) => {
      if (teamFilter !== 'all' && row.teamAbbrev !== teamFilter) return false
      if (searchQuery && !row.name.toLowerCase().includes(searchQuery.toLowerCase())) return false
      const starts = row[`starts_${pitcherWindow}`]
      if (starts != null && typeof starts === 'number' && starts < minCount) return false
      return true
    })
  }, [pitcherData, teamFilter, searchQuery, minCount, pitcherWindow])

  // ─── Active columns / window / sort ─────────────────────────────
  const activeColumns = useMemo(() => {
    if (mainTab === 'batters') {
      if (batterCategory === 'box') return mlbBatterBoxColumns
      if (batterCategory === 'rates') return mlbBatterRatesColumns
      return mlbBatterConsistencyColumns
    }
    if (pitcherCategory === 'box') return mlbPitcherBoxColumns
    if (pitcherCategory === 'rates') return mlbPitcherRatesColumns
    return mlbPitcherConsistencyColumns
  }, [mainTab, batterCategory, pitcherCategory])

  const activeWindow = mainTab === 'batters' ? batterWindow : pitcherWindow
  const activeRows = mainTab === 'batters' ? filteredBatters : filteredPitchers
  const activeSort = mainTab === 'batters' ? batterSort : pitcherSort

  const handleSortChange = useCallback(
    (s: SortState) => {
      if (mainTab === 'batters') setBatterSort(s)
      else setPitcherSort(s)
    },
    [mainTab]
  )

  const showWindowToggle =
    (mainTab === 'batters' && batterCategory === 'box') ||
    (mainTab === 'pitchers' && pitcherCategory === 'box')

  const minLabel = mainTab === 'batters' ? 'Min GP' : 'Min GS'
  const countLabel = mainTab === 'batters' ? 'batters' : 'pitchers'

  return (
    <main className="flex-1 max-w-[1400px] mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-white">Data Vault</h1>
        <p className="text-sm text-slate-400 mt-1">
          Rolling averages for MLB batters and pitchers
        </p>
      </div>

      {/* Main tabs */}
      <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg w-fit mb-4">
        {(['batters', 'pitchers'] as MLBMainTab[]).map((tab) => (
          <button
            key={tab}
            onClick={() => setMainTab(tab)}
            className={cn(
              'px-4 py-2 text-sm font-medium rounded-md transition-colors capitalize',
              mainTab === tab
                ? 'bg-blue-600 text-white'
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
            )}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* Controls row */}
      <div className="flex flex-col sm:flex-row sm:flex-wrap sm:items-center gap-2 sm:gap-3 mb-4">
        {showWindowToggle && (
          <WindowToggle
            active={activeWindow}
            onChange={mainTab === 'batters' ? setBatterWindow : setPitcherWindow}
            options={mainTab === 'batters' ? BATTER_WINDOWS : PITCHER_WINDOWS}
          />
        )}
        {mainTab === 'batters' && (
          <CategoryTabs
            categories={batterCategories}
            active={batterCategory}
            onChange={setBatterCategory}
          />
        )}
        {mainTab === 'pitchers' && (
          <CategoryTabs
            categories={pitcherCategories}
            active={pitcherCategory}
            onChange={setPitcherCategory}
          />
        )}
      </div>

      {/* Filters row */}
      <div className="flex flex-col sm:flex-row sm:flex-wrap sm:items-center gap-2 sm:gap-3 mb-4">
        <input
          type="text"
          placeholder="Search player..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="bg-slate-800 text-slate-200 text-sm rounded-lg px-3 py-1.5 border border-slate-700 focus:outline-none focus:border-blue-500 w-full sm:w-48"
        />
        <select
          value={teamFilter}
          onChange={(e) => setTeamFilter(e.target.value)}
          className="bg-slate-800 text-slate-200 text-sm rounded-lg px-3 py-1.5 border border-slate-700 focus:outline-none focus:border-blue-500"
        >
          <option value="all">All Teams</option>
          {teamOptions.map((abbrev) => (
            <option key={abbrev} value={abbrev}>
              {abbrev}
            </option>
          ))}
        </select>
        <label className="flex items-center gap-2 text-xs text-slate-400">
          {minLabel}
          <input
            type="number"
            min={0}
            max={162}
            value={minCount}
            onChange={(e) => setMinCount(parseInt(e.target.value) || 0)}
            className="bg-slate-800 text-slate-200 text-sm rounded-lg px-2 py-1.5 border border-slate-700 focus:outline-none focus:border-blue-500 w-14"
          />
        </label>
        <span className="text-xs text-slate-500">
          {activeRows.length} {countLabel}
        </span>
      </div>

      {/* Heatmap legend */}
      <div className="mb-3">
        <HeatmapLegend />
      </div>

      {/* Loading / table */}
      {loading ? (
        <div className="flex items-center justify-center py-24">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
        </div>
      ) : (
        <HeatmapTable
          rows={activeRows}
          columns={activeColumns}
          window={activeWindow}
          sort={activeSort}
          onSort={handleSortChange}
          nameLabel={mainTab === 'batters' ? 'Batter' : 'Pitcher'}
          showTeam={true}
          showPosition={false}
        />
      )}
    </main>
  )
}
