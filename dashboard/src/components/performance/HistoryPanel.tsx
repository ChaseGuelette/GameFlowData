'use client'

import { useCallback, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { HistoryFilters, type StatusFilter } from '@/components/history/HistoryFilters'
import { HistorySummary } from '@/components/history/HistorySummary'
import { BetList } from '@/components/history/BetList'
import { EditBetModal } from '@/components/history/EditBetModal'
import { DirectionFilter, type DirectionFilterValue } from '@/components/shared/DirectionFilter'
import { DateRangeFilter, formatDateRangeLabel, getDefaultEndDate, getDefaultStartDate } from '@/components/shared/DateRangeFilter'
import { type PaperBet } from '@/types/predictions'
import { cn } from '@/lib/utils'
import { useModelHistory, useMyBets, PAGE_SIZE } from '@/lib/hooks/useHistoryData'
import type { Sport } from '@/lib/sport-config'

// Extended type to include is_recommended and bookmaker from joined daily_predictions
interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
  bookmaker?: string
}

type HistoryTab = 'my_bets' | 'model_history'
type PaperTradeFilter = 'all' | 'real' | 'paper'

export function HistoryPanel() {
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [activeTab, setActiveTab] = useState<HistoryTab>('my_bets')
  const [modelSport, setModelSport] = useState<Sport>('nba')
  const [editingBet, setEditingBet] = useState<PaperBet | null>(null)

  // Date range state
  const [startDate, setStartDate] = useState(getDefaultStartDate)
  const [endDate, setEndDate] = useState(getDefaultEndDate)

  // Filters
  const [myBetsFilter, setMyBetsFilter] = useState<StatusFilter>('all')
  const [directionFilter, setDirectionFilter] = useState<DirectionFilterValue>('both')
  const [myBetsDirectionFilter, setMyBetsDirectionFilter] = useState<DirectionFilterValue>('both')
  const [paperTradeFilter, setPaperTradeFilter] = useState<PaperTradeFilter>('all')

  // React Query hooks — all fetch with pagination built in
  const validRange = Boolean(startDate && endDate && startDate <= endDate)
  const modelHistory = useModelHistory(startDate, endDate, activeTab === 'model_history' && validRange, modelSport)
  const myBetsQuery = useMyBets(startDate, endDate, activeTab === 'my_bets')

  const bets = (modelHistory.data ?? []) as PaperBetWithRecommended[]
  const myBets = myBetsQuery.data ?? []


  // Apply direction filter (before status filter, so summary reflects direction)
  const directionFilteredBets = directionFilter === 'both'
    ? bets
    : bets.filter(b => b.bet_direction === directionFilter)

  // Filter bets by status
  const filteredBets = filter === 'all'
    ? directionFilteredBets.filter(b => b.status !== 'pending' && b.status !== 'cancelled')
    : directionFilteredBets.filter(b => b.status === filter)

  // Apply paper trade filter to my bets (before direction + status filters)
  const paperFilteredMyBets = paperTradeFilter === 'real'
    ? myBets.filter(b => !b.is_paper_trade)
    : paperTradeFilter === 'paper'
      ? myBets.filter(b => b.is_paper_trade)
      : myBets

  // Apply direction filter to my bets (before status filter)
  const directionFilteredMyBets = myBetsDirectionFilter === 'both'
    ? paperFilteredMyBets
    : paperFilteredMyBets.filter(b => b.bet_direction === myBetsDirectionFilter)

  // Filter my bets by status (show pending in "All" view)
  const filteredMyBets = myBetsFilter === 'all'
    ? directionFilteredMyBets.filter(b => b.status !== 'cancelled')
    : directionFilteredMyBets.filter(b => b.status === myBetsFilter)

  // Remove a bet (any status)
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

    const { data: { user } } = await supabase.auth.getUser()
    if (user) await supabase.rpc('rebuild_user_daily_log', { target_user_id: user.id })

    myBetsQuery.refetch()
  }, [myBetsQuery])

  const handleEditSaved = useCallback(() => {
    setEditingBet(null)
    myBetsQuery.refetch()
  }, [myBetsQuery])



  return (
    <section>
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <div>
            <h2 className="text-xl font-bold text-slate-50">Bet History</h2>
            <p className="text-slate-400">{validRange ? formatDateRangeLabel(startDate, endDate) : 'Choose a valid date range'}</p>
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
            </div>
            {activeTab === 'model_history' && (
              <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg border border-slate-700">
                {(['nba', 'mlb'] as Sport[]).map(sport => (
                  <button
                    key={sport}
                    onClick={() => setModelSport(sport)}
                    className={cn(
                      'px-3 py-1 rounded text-xs font-bold transition-colors',
                      modelSport === sport ? 'bg-blue-700 text-white' : 'text-slate-400 hover:text-slate-200'
                    )}
                  >
                    {sport.toUpperCase()}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Date range filter */}
        <DateRangeFilter startDate={startDate} endDate={endDate} onStartDateChange={setStartDate} onEndDateChange={setEndDate} />
        {!validRange && <div className="text-sm text-red-300">Start date must be on or before end date.</div>}

        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          {activeTab === 'my_bets' ? (
            <DirectionFilter activeDirection={myBetsDirectionFilter} onDirectionChange={setMyBetsDirectionFilter} />
          ) : (
            <DirectionFilter activeDirection={directionFilter} onDirectionChange={setDirectionFilter} />
          )}
          <div className="flex items-center gap-2">
            {activeTab === 'my_bets' && (
              <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg border border-slate-700">
                {(['all', 'real', 'paper'] as PaperTradeFilter[]).map(f => (
                  <button
                    key={f}
                    onClick={() => setPaperTradeFilter(f)}
                    className={cn(
                      'px-2.5 py-1 rounded text-xs font-medium transition-colors',
                      paperTradeFilter === f
                        ? f === 'paper' ? 'bg-blue-700 text-white' : 'bg-slate-600 text-slate-100'
                        : 'text-slate-400 hover:text-slate-200'
                    )}
                  >
                    {f === 'all' ? 'All' : f === 'real' ? 'Real' : 'Paper'}
                  </button>
                ))}
              </div>
            )}
            {activeTab === 'my_bets' ? (
              <HistoryFilters activeFilter={myBetsFilter} onFilterChange={setMyBetsFilter} />
            ) : (
              <HistoryFilters activeFilter={filter} onFilterChange={setFilter} />
            )}
          </div>
        </div>
      </div>

      {/* My Bets Tab */}
      {activeTab === 'my_bets' && (
        <>
          {myBetsQuery.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading your bets...</div>
            </div>
          ) : myBetsQuery.error ? (
            <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">
              Failed to load your bets: {myBetsQuery.error instanceof Error ? myBetsQuery.error.message : 'Unknown error'}
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
              <BetList bets={filteredMyBets} onRemove={handleRemoveBet} onEdit={setEditingBet} />
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
      {activeTab === 'model_history' && validRange && (
        <>
          {modelHistory.isLoading ? (
            <div className="flex items-center justify-center py-16">
              <div className="text-slate-400">Loading history...</div>
            </div>
          ) : modelHistory.error ? (
            <div className="bg-red-900/30 border border-red-700 rounded-lg px-5 py-4 text-sm text-red-300">
              Failed to load model history: {modelHistory.error instanceof Error ? modelHistory.error.message : 'Unknown error'}
            </div>
          ) : (
            <>
              <HistorySummary bets={directionFilteredBets} statTypes={modelSport === 'mlb' ? ['pitcher_strikeouts', 'batter_rbis', 'batter_hits'] : undefined} />
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


      {/* Edit Bet Modal */}
      {editingBet && (
        <EditBetModal
          bet={editingBet}
          onClose={() => setEditingBet(null)}
          onSaved={handleEditSaved}
        />
      )}


    </section>
  )
}
