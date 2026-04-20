'use client'

import { useCallback, useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import type { ArbPaperBet, ArbDailyLog, ArbSummary, ArbDateRange, VerifiedMarketLink } from '@/types/arb-scanner'

function getDateFilter(range: ArbDateRange): string | null {
  if (range === 'all') return null
  const now = new Date()
  if (range === 'today') {
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate())
    return today.toISOString()
  }
  const days = range === '7d' ? 7 : 30
  const d = new Date(now)
  d.setDate(d.getDate() - days)
  return d.toISOString()
}

export function useArbSummary() {
  const supabase = createClient()
  return useQuery<ArbSummary>({
    queryKey: ['arb-summary'],
    queryFn: async () => {
      const { data, error } = await supabase
        .from('arb_paper_bets')
        .select('status, pnl')
      if (error) throw error
      const bets = data ?? []
      const active = bets.filter((b) => b.status === 'placed').length
      const resolvedProfit = bets.filter((b) => b.status === 'resolved_profit').length
      const resolvedLoss = bets.filter((b) => b.status === 'resolved_loss').length
      const cancelled = bets.filter((b) => b.status === 'cancelled').length
      const totalResolved = resolvedProfit + resolvedLoss
      const totalPnl = bets.reduce((sum, b) => sum + (b.pnl ?? 0), 0)
      const winRate = totalResolved > 0 ? resolvedProfit / totalResolved : 0
      return {
        total_placed: bets.length,
        active_bets: active,
        total_resolved: totalResolved,
        resolved_profit: resolvedProfit,
        resolved_loss: resolvedLoss,
        total_cancelled: cancelled,
        total_pnl: totalPnl,
        win_rate: winRate,
      }
    },
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}

export function useArbBets(range: ArbDateRange) {
  const supabase = createClient()
  return useQuery<ArbPaperBet[]>({
    queryKey: ['arb-bets', range],
    queryFn: async () => {
      let query = supabase
        .from('arb_paper_bets')
        .select('*')
        .order('placed_at', { ascending: false })
      const dateFilter = getDateFilter(range)
      if (dateFilter) {
        query = query.gte('placed_at', dateFilter)
      }
      const { data, error } = await query
      if (error) throw error
      return (data ?? []) as ArbPaperBet[]
    },
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}

export function useArbDailyLogs(range: ArbDateRange) {
  const supabase = createClient()
  return useQuery<ArbDailyLog[]>({
    queryKey: ['arb-daily-logs', range],
    queryFn: async () => {
      let query = supabase
        .from('arb_paper_trading_daily_log')
        .select('*')
        .order('log_date', { ascending: false })
      const dateFilter = getDateFilter(range)
      if (dateFilter) {
        query = query.gte('log_date', dateFilter.split('T')[0])
      }
      const { data, error } = await query
      if (error) throw error
      return (data ?? []) as ArbDailyLog[]
    },
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}

export function useMatchQueue() {
  const supabase = createClient()
  const [links, setLinks] = useState<VerifiedMarketLink[]>([])
  const [loading, setLoading] = useState(true)

  const fetchLinks = useCallback(async () => {
    const { data } = await supabase
      .from('verified_market_links')
      .select('*')
      .eq('status', 'pending')
      .gte('match_confidence', 0.70)
      .order('match_confidence', { ascending: false })
      .limit(200)
    setLinks(data ?? [])
    setLoading(false)
  }, [supabase])

  useEffect(() => { fetchLinks() }, [fetchLinks])

  const decide = useCallback(async (id: number, action: 'approve' | 'reject') => {
    await fetch('/api/arb/verify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id, action }),
    })
    setLinks(prev => prev.filter(l => l.id !== id))
  }, [])

  return { links, loading, decide, refetch: fetchLinks }
}
