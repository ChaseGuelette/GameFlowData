'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { HistoryFilters, type StatusFilter } from '@/components/history/HistoryFilters'
import { HistorySummary } from '@/components/history/HistorySummary'
import { BetList } from '@/components/history/BetList'
import { BetSourceFilter, type BetSource } from '@/components/shared/BetSourceFilter'
import { type PaperBet } from '@/types/predictions'
import { cn } from '@/lib/utils'

// Extended type to include is_recommended and bookmaker from joined daily_predictions
interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
  bookmaker?: string
}

type HistoryTab = 'my_bets' | 'model_history'

export default function HistoryPage() {
  const [bets, setBets] = useState<PaperBetWithRecommended[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [betSource, setBetSource] = useState<BetSource>('model') // Default to Model Picks
  const [activeTab, setActiveTab] = useState<HistoryTab>('my_bets')

  // My Bets state
  const [myBets, setMyBets] = useState<PaperBet[]>([])
  const [myBetsLoading, setMyBetsLoading] = useState(false)
  const [myBetsFilter, setMyBetsFilter] = useState<StatusFilter>('all')

  // Fetch model history (paper_bets)
  useEffect(() => {
    async function fetchData() {
      const supabase = createClient()

      // Fetch all resolved bets (last 30 days)
      const thirtyDaysAgo = new Date()
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30)
      const startDate = thirtyDaysAgo.toISOString().split('T')[0]

      const { data: betsData, error: betsError } = await supabase
        .from('paper_bets')
        .select('*')
        .gte('game_date', startDate)
        .order('game_date', { ascending: false })

      if (!betsError && betsData) {
        // Get unique prediction IDs to fetch is_recommended status
        const predictionIds = [...new Set(betsData.map(b => b.prediction_id).filter(Boolean))]

        // Fetch is_recommended and bookmaker for these predictions
        const { data: predictionsData } = await supabase
          .from('daily_predictions')
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
  }, [])

  // Fetch user bets when switching to My Bets tab
  useEffect(() => {
    if (activeTab !== 'my_bets') return
    if (myBets.length > 0) return // Already loaded

    async function fetchMyBets() {
      setMyBetsLoading(true)
      const supabase = createClient()

      const thirtyDaysAgo = new Date()
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30)
      const startDate = thirtyDaysAgo.toISOString().split('T')[0]

      const { data, error } = await supabase
        .from('user_bets')
        .select('*')
        .gte('game_date', startDate)
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
        }))
        setMyBets(mapped)
      }

      setMyBetsLoading(false)
    }

    fetchMyBets()
  }, [activeTab, myBets.length])

  // Filter bets by source (Model Picks = is_recommended from daily_predictions)
  const sourcedBets = betSource === 'model'
    ? bets.filter(b => b.is_recommended === true)
    : bets

  // Filter bets by status
  const filteredBets = filter === 'all'
    ? sourcedBets.filter(b => b.status !== 'pending' && b.status !== 'cancelled')
    : sourcedBets.filter(b => b.status === filter)

  // Filter my bets by status
  const filteredMyBets = myBetsFilter === 'all'
    ? myBets
    : myBets.filter(b => b.status === myBetsFilter)

  return (
    <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-col gap-4 mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Bet History</h1>
            <p className="text-slate-400">Last 30 days</p>
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
              <BetSourceFilter activeSource={betSource} onSourceChange={setBetSource} />
            )}
          </div>
        </div>
        <div className="flex justify-end">
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
              <HistorySummary bets={myBets} />
              <BetList bets={filteredMyBets} />
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
              <HistorySummary bets={sourcedBets} />
              <BetList bets={filteredBets} />
            </>
          )}
        </>
      )}
    </main>
  )
}
