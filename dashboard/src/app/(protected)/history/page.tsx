'use client'

import { useCallback, useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { HistoryFilters, type StatusFilter } from '@/components/history/HistoryFilters'
import { HistorySummary } from '@/components/history/HistorySummary'
import { BetList } from '@/components/history/BetList'
import { DfsEntrySummary } from '@/components/history/DfsEntrySummary'
import { DfsEntryList } from '@/components/history/DfsEntryList'
import { BetSourceFilter, type BetSource } from '@/components/shared/BetSourceFilter'
import { DirectionFilter, type DirectionFilterValue } from '@/components/shared/DirectionFilter'
import { type PaperBet } from '@/types/predictions'
import type { UserDfsEntryWithLegs } from '@/types/dfs-entries'
import { cn } from '@/lib/utils'
import { useSport } from '@/contexts/SportContext'

// Extended type to include is_recommended and bookmaker from joined daily_predictions
interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
  bookmaker?: string
}

type HistoryTab = 'my_bets' | 'model_history' | 'dfs_entries'
type DatePreset = '7d' | '30d' | '90d' | 'all'

function getDefaultStartDate(): string {
  const d = new Date()
  d.setDate(d.getDate() - 30)
  return d.toISOString().split('T')[0]
}

function getDefaultEndDate(): string {
  return new Date().toISOString().split('T')[0]
}

function formatDateLabel(start: string, end: string): string {
  const s = new Date(start + 'T00:00:00')
  const e = new Date(end + 'T00:00:00')
  const fmt = (d: Date) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  return `${fmt(s)} – ${fmt(e)}`
}

export default function HistoryPage() {
  const { sport, config } = useSport()
  const [bets, setBets] = useState<PaperBetWithRecommended[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [betSource, setBetSource] = useState<BetSource>('model') // Default to Model Picks
  const [activeTab, setActiveTab] = useState<HistoryTab>('my_bets')

  // Date range state
  const [startDate, setStartDate] = useState(getDefaultStartDate)
  const [endDate, setEndDate] = useState(getDefaultEndDate)

  // My Bets state
  const [myBets, setMyBets] = useState<PaperBet[]>([])
  const [myBetsLoading, setMyBetsLoading] = useState(false)
  const [myBetsFilter, setMyBetsFilter] = useState<StatusFilter>('all')
  const [directionFilter, setDirectionFilter] = useState<DirectionFilterValue>('both')
  const [myBetsDirectionFilter, setMyBetsDirectionFilter] = useState<DirectionFilterValue>('both')

  // DFS Entries state
  const [dfsEntries, setDfsEntries] = useState<UserDfsEntryWithLegs[]>([])
  const [dfsLoading, setDfsLoading] = useState(false)

  const applyPreset = (preset: DatePreset) => {
    const now = new Date()
    const end = now.toISOString().split('T')[0]
    setEndDate(end)
    if (preset === 'all') {
      setStartDate('2024-01-01')
    } else {
      const days = preset === '7d' ? 7 : preset === '30d' ? 30 : 90
      const d = new Date()
      d.setDate(d.getDate() - days)
      setStartDate(d.toISOString().split('T')[0])
    }
  }

  // Fetch model history (paper_bets)
  useEffect(() => {
    async function fetchData() {
      setLoading(true)
      const supabase = createClient()

      const { data: betsData, error: betsError } = await supabase
        .from(config.paperBetsTable)
        .select('id, prediction_id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl')
        .gte('game_date', startDate)
        .lte('game_date', endDate)
        .order('game_date', { ascending: false })

      if (!betsError && betsData) {
        // Get unique prediction IDs to fetch is_recommended status
        const predictionIds = [...new Set(betsData.map(b => b.prediction_id).filter(Boolean))]

        // Fetch is_recommended and bookmaker for these predictions
        const { data: predictionsData } = await supabase
          .from(config.predictionsTable)
          .select('id, is_recommended, bookmaker')
          .in('id', predictionIds)

        // Create maps for quick lookup (use string keys for consistent bigint handling)
        const recommendedMap = new Map<string, boolean>()
        const bookmakerMap = new Map<string, string>()
        if (predictionsData) {
          for (const p of predictionsData) {
            const key = String(p.id)
            recommendedMap.set(key, p.is_recommended ?? false)
            if (p.bookmaker) {
              bookmakerMap.set(key, p.bookmaker)
            }
          }
        }

        // Merge is_recommended and bookmaker into bets
        const enrichedBets = betsData.map(bet => {
          const key = String(bet.prediction_id)
          return {
            ...bet,
            is_recommended: recommendedMap.get(key) ?? false,
            bookmaker: bookmakerMap.get(key),
          }
        }) as PaperBetWithRecommended[]

        setBets(enrichedBets)
      }

      setLoading(false)
    }

    fetchData()
  }, [startDate, endDate, sport, config.paperBetsTable, config.predictionsTable])

  // Fetch user bets when switching to My Bets tab or date range changes
  useEffect(() => {
    if (activeTab !== 'my_bets') return

    async function fetchMyBets() {
      setMyBetsLoading(true)
      const supabase = createClient()

      const { data, error } = await supabase
        .from('user_bets')
        .select('id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl, book_at_bet, team_abbrev, opponent_abbrev, bet_context, user_confidence, placed_at')
        .in('stat_type', config.statTypes)
        .gte('game_date', startDate)
        .lte('game_date', endDate)
        .order('game_date', { ascending: false })

      if (!error && data) {
        // Map user_bets columns to PaperBet shape for BetList/HistorySummary reuse
        const mapped: PaperBet[] = data.map(row => ({
          id: row.id,
          game_date: row.game_date,
          player_id: row.player_id,
          player_name: row.player_name,
          stat_type: row.stat_type,
          line: Number(row.line),
          bet_direction: row.bet_direction,
          odds_at_bet: row.odds_at_bet ? Number(row.odds_at_bet) : -110,
          stake: row.stake ? Number(row.stake) : 0,
          edge: row.edge ? Number(row.edge) : 0,
          status: row.status,
          actual_value: row.actual_value != null ? Number(row.actual_value) : null,
          pnl: row.pnl != null ? Number(row.pnl) : null,
          bookmaker: row.book_at_bet,
          team_abbrev: row.team_abbrev ?? undefined,
          opponent_abbrev: row.opponent_abbrev ?? undefined,
          bet_context: row.bet_context ?? null,
          user_confidence: row.user_confidence ?? null,
          placed_at: row.placed_at ?? null,
        }))
        setMyBets(mapped)
      }

      setMyBetsLoading(false)
    }

    fetchMyBets()
  }, [activeTab, startDate, endDate, sport, config.statTypes])

  // Fetch DFS entries when switching to DFS tab or date range changes
  useEffect(() => {
    if (activeTab !== 'dfs_entries') return

    async function fetchDfsEntries() {
      setDfsLoading(true)
      const supabase = createClient()

      // Fetch entries
      const { data: entriesData, error: entriesError } = await supabase
        .from('user_dfs_entries')
        .select('*')
        .gte('entry_date', startDate)
        .lte('entry_date', endDate)
        .order('placed_at', { ascending: false })

      if (entriesError || !entriesData) {
        console.error('Failed to fetch DFS entries:', entriesError)
        setDfsEntries([])
        setDfsLoading(false)
        return
      }

      if (entriesData.length === 0) {
        setDfsEntries([])
        setDfsLoading(false)
        return
      }

      // Fetch legs for all entries
      const entryIds = entriesData.map(e => e.id)
      const { data: legsData, error: legsError } = await supabase
        .from('user_dfs_legs')
        .select('*')
        .in('entry_id', entryIds)

      if (legsError) {
        console.error('Failed to fetch DFS legs:', legsError)
      }

      // Group legs by entry_id
      const legsByEntry = new Map<number, typeof legsData>()
      if (legsData) {
        for (const leg of legsData) {
          if (!legsByEntry.has(leg.entry_id)) legsByEntry.set(leg.entry_id, [])
          legsByEntry.get(leg.entry_id)!.push(leg)
        }
      }

      const entries: UserDfsEntryWithLegs[] = entriesData.map(entry => ({
        ...entry,
        legs: legsByEntry.get(entry.id) ?? [],
      }))

      setDfsEntries(entries)
      setDfsLoading(false)
    }

    fetchDfsEntries()
  }, [activeTab, startDate, endDate])

  // Remove a pending DFS entry (cascade deletes legs)
  const handleRemoveDfsEntry = useCallback(async (entryId: number) => {
    const supabase = createClient()
    const { error } = await supabase
      .from('user_dfs_entries')
      .delete()
      .eq('id', entryId)

    if (error) {
      console.error('Failed to remove DFS entry:', error)
      return
    }

    setDfsEntries(prev => prev.filter(e => e.id !== entryId))
  }, [])

  // Filter bets by source (Model Picks = is_recommended from daily_predictions)
  const sourcedBets = betSource === 'model'
    ? bets.filter(b => b.is_recommended === true)
    : bets

  // Apply direction filter (before status filter, so summary reflects direction)
  const directionFilteredBets = directionFilter === 'both'
    ? sourcedBets
    : sourcedBets.filter(b => b.bet_direction === directionFilter)

  // Filter bets by status
  const filteredBets = filter === 'all'
    ? directionFilteredBets.filter(b => b.status !== 'pending' && b.status !== 'cancelled')
    : directionFilteredBets.filter(b => b.status === filter)

  // Apply direction filter to my bets (before status filter)
  const directionFilteredMyBets = myBetsDirectionFilter === 'both'
    ? myBets
    : myBets.filter(b => b.bet_direction === myBetsDirectionFilter)

  // Filter my bets by status (show pending in "All" view)
  const filteredMyBets = myBetsFilter === 'all'
    ? directionFilteredMyBets.filter(b => b.status !== 'cancelled')
    : directionFilteredMyBets.filter(b => b.status === myBetsFilter)

  // Remove a pending bet
  const handleRemoveBet = useCallback(async (betId: number) => {
    const supabase = createClient()
    const { error } = await supabase
      .from('user_bets')
      .delete()
      .eq('id', betId)

    if (error) {
      console.error('Failed to remove bet:', error)
      return
    }

    setMyBets(prev => prev.filter(b => b.id !== betId))
  }, [])

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Bet History</h1>
            <p className="text-slate-400">{formatDateLabel(startDate, endDate)}</p>
          </div>
          <div className="flex items-center gap-3">
            {/* Tab Toggle */}
            <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg border border-slate-700">
              <button
                onClick={() => setActiveTab('my_bets')}
                className={cn(
                  'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                  activeTab === 'my_bets'
                    ? 'bg-green-600 text-white'
                    : 'text-slate-400 hover:text-slate-200'
                )}
              >
                My Bets
              </button>
              <button
                onClick={() => setActiveTab('model_history')}
                className={cn(
                  'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                  activeTab === 'model_history'
                    ? 'bg-slate-700 text-slate-100'
                    : 'text-slate-400 hover:text-slate-200'
                )}
              >
                Model History
              </button>
              <button
                onClick={() => setActiveTab('dfs_entries')}
                className={cn(
                  'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                  activeTab === 'dfs_entries'
                    ? 'bg-purple-600 text-white'
                    : 'text-slate-400 hover:text-slate-200'
                )}
              >
                DFS Entries
              </button>
            </div>
            {activeTab === 'model_history' && (
              <BetSourceFilter activeSource={betSource} onSourceChange={setBetSource} />
            )}
          </div>
        </div>

        {/* Date range filter */}
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2">
            <input
              type="date"
              value={startDate}
              onChange={e => setStartDate(e.target.value)}
              className="px-2 py-1.5 bg-slate-800 border border-slate-700 rounded-md text-sm text-slate-200 focus:outline-none focus:border-blue-500"
            />
            <span className="text-slate-500 text-sm">to</span>
            <input
              type="date"
              value={endDate}
              onChange={e => setEndDate(e.target.value)}
              className="px-2 py-1.5 bg-slate-800 border border-slate-700 rounded-md text-sm text-slate-200 focus:outline-none focus:border-blue-500"
            />
          </div>
          <div className="flex items-center gap-1">
            {(['7d', '30d', '90d', 'all'] as DatePreset[]).map(preset => (
              <button
                key={preset}
                onClick={() => applyPreset(preset)}
                className="px-2.5 py-1 rounded text-xs font-medium bg-slate-800 border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 transition-colors"
              >
                {preset === 'all' ? 'All' : preset.toUpperCase()}
              </button>
            ))}
          </div>
        </div>

        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          {activeTab === 'my_bets' ? (
            <DirectionFilter activeDirection={myBetsDirectionFilter} onDirectionChange={setMyBetsDirectionFilter} />
          ) : (
            <DirectionFilter activeDirection={directionFilter} onDirectionChange={setDirectionFilter} />
          )}
          {activeTab === 'my_bets' ? (
            <HistoryFilters activeFilter={myBetsFilter} onFilterChange={setMyBetsFilter} />
          ) : (
            <HistoryFilters activeFilter={filter} onFilterChange={setFilter} />
          )}
        </div>
      </div>

      {/* My Bets Tab */}
      {activeTab === 'my_bets' && (
        <>
          {myBetsLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading your bets...</div>
            </div>
          ) : myBets.length === 0 ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-center">
                <p className="text-slate-400 text-lg">No bets taken yet</p>
                <p className="text-slate-500 text-sm mt-2">
                  Tap the checkmark on any prop card to track your bets
                </p>
              </div>
            </div>
          ) : (
            <>
              <HistorySummary bets={directionFilteredMyBets} />
              <BetList bets={filteredMyBets} onRemove={handleRemoveBet} />
            </>
          )}
        </>
      )}

      {/* Model History Tab */}
      {activeTab === 'model_history' && (
        <>
          {loading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading history...</div>
            </div>
          ) : (
            <>
              <HistorySummary bets={directionFilteredBets} />
              <BetList bets={filteredBets} />
            </>
          )}
        </>
      )}

      {/* DFS Entries Tab */}
      {activeTab === 'dfs_entries' && (
        <>
          {dfsLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading DFS entries...</div>
            </div>
          ) : (
            <>
              <DfsEntrySummary entries={dfsEntries} />
              <DfsEntryList entries={dfsEntries} onRemove={handleRemoveDfsEntry} />
            </>
          )}
        </>
      )}
    </main>
  )
}
