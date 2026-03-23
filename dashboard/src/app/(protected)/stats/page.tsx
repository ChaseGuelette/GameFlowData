'use client'

import { useEffect, useState, useMemo, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { TEAM_ABBREV } from '@/lib/constants'
import { StatTabs } from '@/components/stats/StatTabs'
import { CategoryTabs } from '@/components/stats/CategoryTabs'
import { WindowToggle } from '@/components/stats/WindowToggle'
import { OffDefToggle } from '@/components/stats/OffDefToggle'
import { PositionFilter, type PositionOption } from '@/components/stats/PositionFilter'
import { HeatmapTable, HeatmapLegend } from '@/components/stats/HeatmapTable'
import {
  playerColumnMap,
  teamColumnMap,
  defenseColumnMap,
  playTypeColumnMap,
} from '@/lib/stats/columns'
import { pivotPlayTypes } from '@/lib/stats/pivotPlayTypes'
import type {
  StatsMainTab,
  PlayerCategory,
  TeamCategory,
  DefenseCategory,
  PlayTypeCategory,
  PlayTypeGrouping,
  WindowSuffix,
  StatRow,
  SortState,
} from '@/types/stats'

// ─── Category Options ───────────────────────────────────────────────
const playerCategories: { value: PlayerCategory; label: string }[] = [
  { value: 'box', label: 'Box Score' },
  { value: 'shooting', label: 'Shooting' },
  { value: 'advanced', label: 'Advanced' },
  { value: 'consistency', label: 'Consistency' },
]

const teamCategories: { value: TeamCategory; label: string }[] = [
  { value: 'offense', label: 'Offense' },
  { value: 'defense', label: 'Defense' },
  { value: 'overall', label: 'Overall' },
]

const defenseCategories: { value: DefenseCategory; label: string }[] = [
  { value: 'totals', label: 'Totals' },
  { value: 'per100', label: 'Per 100 Poss' },
]

const playTypeCategories: { value: PlayTypeCategory; label: string }[] = [
  { value: 'frequency', label: 'Frequency' },
  { value: 'efficiency', label: 'Efficiency' },
]

// ─── Build team abbreviation options ────────────────────────────────
const teamOptions = Object.entries(TEAM_ABBREV)
  .map(([, abbrev]) => abbrev)
  .sort()

export default function StatsPage() {
  const supabase = createClient()

  // ─── Tab state ──────────────────────────────────────────────────
  const [mainTab, setMainTab] = useState<StatsMainTab>('players')
  const [playerCategory, setPlayerCategory] = useState<PlayerCategory>('box')
  const [teamCategory, setTeamCategory] = useState<TeamCategory>('offense')
  const [defenseCategory, setDefenseCategory] = useState<DefenseCategory>('totals')
  const [playTypeCategory, setPlayTypeCategory] = useState<PlayTypeCategory>('frequency')
  const [playTypeGrouping, setPlayTypeGrouping] = useState<PlayTypeGrouping>('Offensive')

  // ─── Window state (per tab) ─────────────────────────────────────
  const [playerWindow, setPlayerWindow] = useState<WindowSuffix>('l5')
  const [teamWindow, setTeamWindow] = useState<WindowSuffix>('l5')
  const [defenseWindow, setDefenseWindow] = useState<WindowSuffix>('l5')

  // ─── Filter state ───────────────────────────────────────────────
  const [positionFilter, setPositionFilter] = useState<PositionOption>('all')
  const [teamFilter, setTeamFilter] = useState<string>('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [minGP, setMinGP] = useState(3)
  const [defensePosition, setDefensePosition] = useState<PositionOption>('G')

  // ─── Sort state (per tab) ───────────────────────────────────────
  const [playerSort, setPlayerSort] = useState<SortState>({ column: 'pts', direction: 'desc' })
  const [teamSort, setTeamSort] = useState<SortState>({ column: 'pts', direction: 'desc' })
  const [defenseSort, setDefenseSort] = useState<SortState>({ column: 'pts', direction: 'desc' })
  const [playTypeSort, setPlayTypeSort] = useState<SortState>({ column: 'isolation_poss_pct', direction: 'desc' })

  // ─── Data state ─────────────────────────────────────────────────
  const [playerData, setPlayerData] = useState<StatRow[]>([])
  const [teamData, setTeamData] = useState<StatRow[]>([])
  const [defenseData, setDefenseData] = useState<StatRow[]>([])
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [playTypeRawData, setPlayTypeRawData] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  // ─── Data fetching ──────────────────────────────────────────────
  useEffect(() => {
    async function fetchAll() {
      setLoading(true)

      const [playerRes, teamRes, defenseRes, playTypeRes] = await Promise.all([
        supabase.from('player_stats_latest').select('*'),
        supabase.from('team_stats_latest').select('*'),
        supabase.from('defense_by_position_latest').select('*'),
        supabase.from('team_play_types').select('team_id,team_name,team_abbreviation,play_type,type_grouping,poss_pct,ppp,percentile'),
      ])

      if (playerRes.data) {
        setPlayerData(
          playerRes.data.map((r: Record<string, unknown>) => ({
            ...r,
            id: r.player_id as number,
            name: r.player_name as string,
            teamAbbrev: TEAM_ABBREV[r.team_id as number] || String(r.team_id),
            position: (r.position_group as string) || '—',
          })) as StatRow[]
        )
      }

      if (teamRes.data) {
        setTeamData(
          teamRes.data.map((r: Record<string, unknown>) => ({
            ...r,
            id: r.team_id as number,
            name: r.team_name as string,
            teamAbbrev: TEAM_ABBREV[r.team_id as number] || String(r.team_id),
          })) as StatRow[]
        )
      }

      if (defenseRes.data) {
        setDefenseData(
          defenseRes.data.map((r: Record<string, unknown>) => ({
            ...r,
            id: `${r.team_id}-${r.position_group}`,
            name: r.team_name as string,
            teamAbbrev: TEAM_ABBREV[r.team_id as number] || String(r.team_id),
            position: r.position_group as string,
          })) as StatRow[]
        )
      }

      if (playTypeRes.data) {
        setPlayTypeRawData(playTypeRes.data)
      }

      setLoading(false)
    }

    fetchAll()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // ─── Filtered player rows ──────────────────────────────────────
  const filteredPlayers = useMemo(() => {
    const gpCol = `games_${playerWindow}`
    return playerData.filter((row) => {
      if (positionFilter !== 'all' && row.position !== positionFilter) return false
      if (teamFilter !== 'all' && row.teamAbbrev !== teamFilter) return false
      if (searchQuery && !row.name.toLowerCase().includes(searchQuery.toLowerCase())) return false
      const gp = row[gpCol]
      if (gp != null && typeof gp === 'number' && gp < minGP) return false
      return true
    })
  }, [playerData, positionFilter, teamFilter, searchQuery, minGP, playerWindow])

  // ─── Filtered defense rows ─────────────────────────────────────
  const filteredDefense = useMemo(() => {
    if (defensePosition === 'all') return defenseData
    return defenseData.filter((row) => row.position === defensePosition)
  }, [defenseData, defensePosition])

  // ─── Pivoted play type rows ──────────────────────────────────────
  const playTypeData = useMemo(
    () => pivotPlayTypes(playTypeRawData, playTypeGrouping),
    [playTypeRawData, playTypeGrouping]
  )

  // ─── Current columns/window/sort based on active tab ───────────
  const activeColumns = useMemo(() => {
    if (mainTab === 'players') return playerColumnMap[playerCategory]
    if (mainTab === 'teams') return teamColumnMap[teamCategory]
    if (mainTab === 'playTypes') return playTypeColumnMap[playTypeCategory]
    return defenseColumnMap[defenseCategory]
  }, [mainTab, playerCategory, teamCategory, defenseCategory, playTypeCategory])

  const activeWindow = mainTab === 'players' ? playerWindow : mainTab === 'teams' ? teamWindow : defenseWindow
  const activeSort = mainTab === 'players' ? playerSort : mainTab === 'teams' ? teamSort : mainTab === 'playTypes' ? playTypeSort : defenseSort

  const handleWindowChange = useCallback(
    (w: WindowSuffix) => {
      if (mainTab === 'players') setPlayerWindow(w)
      else if (mainTab === 'teams') setTeamWindow(w)
      else setDefenseWindow(w)
    },
    [mainTab]
  )

  const handleSortChange = useCallback(
    (s: SortState) => {
      if (mainTab === 'players') setPlayerSort(s)
      else if (mainTab === 'teams') setTeamSort(s)
      else if (mainTab === 'playTypes') setPlayTypeSort(s)
      else setDefenseSort(s)
    },
    [mainTab]
  )

  const activeRows = mainTab === 'players' ? filteredPlayers : mainTab === 'teams' ? teamData : mainTab === 'playTypes' ? playTypeData : filteredDefense

  return (
    <main className="flex-1 max-w-[1400px] mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-white">Data Vault</h1>
        <p className="text-sm text-slate-400 mt-1">
          Rolling averages across players, teams, and defensive matchups
        </p>
      </div>

      {/* Main tabs */}
      <div className="mb-4">
        <StatTabs active={mainTab} onChange={setMainTab} />
      </div>

      {/* Controls row */}
      <div className="flex flex-col sm:flex-row sm:flex-wrap sm:items-center gap-2 sm:gap-3 mb-4">
        {/* Window toggle (hidden for Consistency and Play Types which are windowless) */}
        {!(mainTab === 'players' && playerCategory === 'consistency') && mainTab !== 'playTypes' && (
          <WindowToggle active={activeWindow} onChange={handleWindowChange} />
        )}

        {/* Category tabs */}
        {mainTab === 'players' && (
          <CategoryTabs categories={playerCategories} active={playerCategory} onChange={setPlayerCategory} />
        )}
        {mainTab === 'teams' && (
          <CategoryTabs categories={teamCategories} active={teamCategory} onChange={setTeamCategory} />
        )}
        {mainTab === 'defense' && (
          <CategoryTabs categories={defenseCategories} active={defenseCategory} onChange={setDefenseCategory} />
        )}
        {mainTab === 'playTypes' && (
          <>
            <OffDefToggle active={playTypeGrouping} onChange={setPlayTypeGrouping} />
            <CategoryTabs categories={playTypeCategories} active={playTypeCategory} onChange={setPlayTypeCategory} />
          </>
        )}

        {/* Position filter (Players tab) */}
        {mainTab === 'players' && (
          <PositionFilter active={positionFilter} onChange={setPositionFilter} />
        )}

        {/* Defense position selector */}
        {mainTab === 'defense' && (
          <PositionFilter active={defensePosition} onChange={setDefensePosition} />
        )}
      </div>

      {/* Player-specific filters row */}
      {mainTab === 'players' && (
        <div className="flex flex-col sm:flex-row sm:flex-wrap sm:items-center gap-2 sm:gap-3 mb-4">
          {/* Search */}
          <input
            type="text"
            placeholder="Search player..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="bg-slate-800 text-slate-200 text-sm rounded-lg px-3 py-1.5 border border-slate-700 focus:outline-none focus:border-blue-500 w-full sm:w-48"
          />

          {/* Team dropdown */}
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

          {/* Min GP */}
          <label className="flex items-center gap-2 text-xs text-slate-400">
            Min GP
            <input
              type="number"
              min={0}
              max={82}
              value={minGP}
              onChange={(e) => setMinGP(parseInt(e.target.value) || 0)}
              className="bg-slate-800 text-slate-200 text-sm rounded-lg px-2 py-1.5 border border-slate-700 focus:outline-none focus:border-blue-500 w-14"
            />
          </label>

          <span className="text-xs text-slate-500">
            {filteredPlayers.length} players
          </span>
        </div>
      )}

      {/* Heatmap legend */}
      <div className="mb-3">
        <HeatmapLegend />
      </div>

      {/* Loading state */}
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
          nameLabel={mainTab === 'players' ? 'Player' : 'Team'}
          showTeam={mainTab === 'players'}
          showPosition={mainTab === 'players'}
        />
      )}
    </main>
  )
}
