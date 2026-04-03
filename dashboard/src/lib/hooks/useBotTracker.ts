'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import type {
  KalshiBotSummary,
  KalshiLiveOrder,
  KalshiPaperBet,
  KalshiLiveDailyLog,
  KalshiPaperDailyLog,
  DateRange,
  BotTab,
} from '@/types/bot-tracker'

function getDateFilter(range: DateRange): string | null {
  if (range === 'all') return null
  const now = new Date()
  if (range === 'today') {
    return now.toISOString().split('T')[0]
  }
  const days = range === '7d' ? 7 : 30
  const d = new Date(now)
  d.setDate(d.getDate() - days)
  return d.toISOString().split('T')[0]
}

async function fetchBotSummary(): Promise<KalshiBotSummary> {
  const supabase = createClient()
  const { data, error } = await supabase.rpc('get_kalshi_bot_summary')
  if (error) throw error
  return data as KalshiBotSummary
}

async function fetchLiveOrders(range: DateRange): Promise<KalshiLiveOrder[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_live_orders')
    .select('*')
    .order('placed_at', { ascending: false })

  const minDate = getDateFilter(range)
  if (minDate) {
    query = query.gte('game_date', minDate)
  }

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiLiveOrder[]
}

async function fetchPaperBets(range: DateRange): Promise<KalshiPaperBet[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_paper_bets')
    .select('*')
    .order('placed_at', { ascending: false })

  const minDate = getDateFilter(range)
  if (minDate) {
    query = query.gte('game_date', minDate)
  }

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiPaperBet[]
}

async function fetchLiveDailyLogs(range: DateRange): Promise<KalshiLiveDailyLog[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_live_trading_daily_log')
    .select('*')
    .order('game_date', { ascending: false })

  const minDate = getDateFilter(range)
  if (minDate) {
    query = query.gte('game_date', minDate)
  }

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiLiveDailyLog[]
}

async function fetchPaperDailyLogs(range: DateRange): Promise<KalshiPaperDailyLog[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_paper_trading_daily_log')
    .select('*')
    .order('game_date', { ascending: false })

  const minDate = getDateFilter(range)
  if (minDate) {
    query = query.gte('game_date', minDate)
  }

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiPaperDailyLog[]
}

export function useBotSummary() {
  return useQuery({
    queryKey: ['bot-tracker', 'summary'],
    queryFn: fetchBotSummary,
    staleTime: 60 * 1000, // 1 minute
    refetchInterval: 60 * 1000,
  })
}

export function useBotOrders(tab: BotTab, range: DateRange) {
  return useQuery<(KalshiLiveOrder | KalshiPaperBet)[]>({
    queryKey: ['bot-tracker', 'orders', tab, range],
    queryFn: () => (tab === 'live' ? fetchLiveOrders(range) : fetchPaperBets(range)),
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}

export function useBotDailyLogs(tab: BotTab, range: DateRange) {
  return useQuery<(KalshiLiveDailyLog | KalshiPaperDailyLog)[]>({
    queryKey: ['bot-tracker', 'daily-logs', tab, range],
    queryFn: () => (tab === 'live' ? fetchLiveDailyLogs(range) : fetchPaperDailyLogs(range)),
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}
