'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { Navbar } from '@/components/layout/Navbar'
import { HistoryFilters, type StatusFilter } from '@/components/history/HistoryFilters'
import { HistorySummary } from '@/components/history/HistorySummary'
import { BetList } from '@/components/history/BetList'
import { type PaperBet } from '@/types/predictions'

export default function HistoryPage() {
  const [bets, setBets] = useState<PaperBet[]>([])
  const [loading, setLoading] = useState(true)
  const [filter, setFilter] = useState<StatusFilter>('all')
  const [bankroll, setBankroll] = useState<number | undefined>(undefined)

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
        setBets(betsData as PaperBet[])
      }

      // Fetch latest bankroll
      const { data: logData } = await supabase
        .from('paper_trading_daily_log')
        .select('bankroll_after')
        .order('game_date', { ascending: false })
        .limit(1)
        .single()

      if (logData?.bankroll_after) {
        setBankroll(logData.bankroll_after)
      }

      setLoading(false)
    }

    fetchData()
  }, [])

  // Filter bets by status
  const filteredBets = filter === 'all'
    ? bets.filter(b => b.status !== 'pending' && b.status !== 'cancelled')
    : bets.filter(b => b.status === filter)

  return (
    <div className="min-h-screen flex flex-col bg-slate-900">
      <Navbar bankroll={bankroll} />

      <main className="flex-1 max-w-7xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-2xl font-bold text-slate-50">Bet History</h1>
            <p className="text-slate-400">Last 30 days</p>
          </div>
          <HistoryFilters activeFilter={filter} onFilterChange={setFilter} />
        </div>

        {/* Content */}
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <div className="text-slate-400">Loading history...</div>
          </div>
        ) : (
          <>
            <HistorySummary bets={bets} />
            <BetList bets={filteredBets} />
          </>
        )}
      </main>
    </div>
  )
}
