'use client'

import { useCallback, useState } from 'react'
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
import { useModelHistory, useMyBets, useDfsEntries, PAGE_SIZE } from '@/lib/hooks/useHistoryData'

// Extended type to include is_recommended and bookmaker from joined daily_predictions
interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
  bookmaker?: string
}

type HistoryTab = 'my_bets' | 'model_history' | 'dfs_entries'
type DatePreset = '7d' | '30d' | '90d' | 'all' | 'lifetime'

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
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [betSource, setBetSource] = useState<BetSource>('model')
  const [activeTab, setActiveTab] = useState<HistoryTab>('my_bets')

  // Date range state
  const [startDate, setStartDate] = useState(getDefaultStartDate)
  const [endDate, setEndDate] = useState(getDefaultEndDate)

  // Filters
  const [myBetsFilter, setMyBetsFilter] = useState<StatusFilter>('all')
  const [directionFilter, setDirectionFilter] = useState<DirectionFilterValue>('both')
  const [myBetsDirectionFilter, setMyBetsDirectionFilter] = useState<DirectionFilterValue>('both')

  // React Query hooks — all fetch with pagination built in
  const modelHistory = useModelHistory(startDate, endDate)
  const myBetsQuery = useMyBets(startDate, endDate)
  const dfsEntriesQuery = useDfsEntries(startDate, endDate)

  const bets = (modelHistory.data ?? []) as PaperBetWithRecommended[]
  const myBets = myBetsQuery.data ?? []
  const dfsEntries = dfsEntriesQuery.data ?? []

  const applyPreset = (preset: DatePreset) => {
    const now = new Date()
    const end = now.toISOString().split('T')[0]
    setEndDate(end)
    if (preset === 'all') {
      const d = new Date()
      d.setMonth(d.getMonth() - 6)
      setStartDate(d.toISOString().split('T')[0])
    } else if (preset === 'lifetime') {
      setStartDate('2020-01-01')
    } else {
      const days = preset === '7d' ? 7 : preset === '30d' ? 30 : 90
      const d = new Date()
      d.setDate(d.getDate() - days)
      setStartDate(d.toISOString().split('T')[0])
    }
  }

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

    dfsEntriesQuery.refetch()
  }, [dfsEntriesQuery])

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

    myBetsQuery.refetch()
  }, [myBetsQuery])

  const loading = activeTab === 'model_history' ? modelHistory.isLoading
    : activeTab === 'my_bets' ? myBetsQuery.isLoading
    : dfsEntriesQuery.isLoading

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
            {(['7d', '30d', '90d', 'all', 'lifetime'] as DatePreset[]).map(preset => (
              <button
                key={preset}
                onClick={() => applyPreset(preset)}
                className="px-2.5 py-1 rounded text-xs font-medium bg-slate-800 border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 transition-colors"
              >
                {preset === 'all' ? '6M' : preset === 'lifetime' ? 'ALL' : preset.toUpperCase()}
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
          {myBetsQuery.isLoading ? (
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
              {myBets.length >= PAGE_SIZE && (
                <div className="text-center py-4 text-sm text-slate-500">
                  Showing first {PAGE_SIZE} results. Narrow date range for more.
                </div>
              )}
            </>
          )}
        </>
      )}

      {/* Model History Tab */}
      {activeTab === 'model_history' && (
        <>
          {modelHistory.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading history...</div>
            </div>
          ) : (
            <>
              <HistorySummary bets={directionFilteredBets} />
              <BetList bets={filteredBets} />
              {bets.length >= PAGE_SIZE && (
                <div className="text-center py-4 text-sm text-slate-500">
                  Showing first {PAGE_SIZE} results. Narrow date range for more.
                </div>
              )}
            </>
          )}
        </>
      )}

      {/* DFS Entries Tab */}
      {activeTab === 'dfs_entries' && (
        <>
          {dfsEntriesQuery.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading DFS entries...</div>
            </div>
          ) : (
            <>
              <DfsEntrySummary entries={dfsEntries} />
              <DfsEntryList entries={dfsEntries} onRemove={handleRemoveDfsEntry} />
              {dfsEntries.length >= PAGE_SIZE && (
                <div className="text-center py-4 text-sm text-slate-500">
                  Showing first {PAGE_SIZE} results. Narrow date range for more.
                </div>
              )}
            </>
          )}
        </>
      )}
    </main>
  )
}
