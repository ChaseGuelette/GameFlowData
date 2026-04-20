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

function getDateFilter(range: DateRange, customDate?: string): { min: string | null; max: string | null } {
  if (range === 'custom' && customDate) return { min: customDate, max: customDate }
  if (range === 'all') return { min: null, max: null }
  const now = new Date()
  if (range === 'today') {
    const d = now.toISOString().split('T')[0]
    return { min: d, max: null }
  }
  const days = range === '7d' ? 7 : 30
  const d = new Date(now)
  d.setDate(d.getDate() - days)
  return { min: d.toISOString().split('T')[0], max: null }
}

async function fetchBotSummary(): Promise<KalshiBotSummary> {
  const supabase = createClient()
  const { data, error } = await supabase.rpc('get_kalshi_bot_summary')
  if (error) throw error
  return data as KalshiBotSummary
}

async function fetchLiveOrders(range: DateRange, customDate?: string): Promise<KalshiLiveOrder[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_live_orders')
    .select('*')
    .order('placed_at', { ascending: false })

  const { min, max } = getDateFilter(range, customDate)
  if (min) query = query.gte('game_date', min)
  if (max) query = query.lte('game_date', max)

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiLiveOrder[]
}

async function fetchPaperBets(range: DateRange, customDate?: string): Promise<KalshiPaperBet[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_paper_bets')
    .select('*')
    .order('placed_at', { ascending: false })

  const { min, max } = getDateFilter(range, customDate)
  if (min) query = query.gte('game_date', min)
  if (max) query = query.lte('game_date', max)

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiPaperBet[]
}

async function fetchLiveDailyLogs(range: DateRange, customDate?: string): Promise<KalshiLiveDailyLog[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_live_trading_daily_log')
    .select('*')
    .order('game_date', { ascending: false })

  const { min, max } = getDateFilter(range, customDate)
  if (min) query = query.gte('game_date', min)
  if (max) query = query.lte('game_date', max)

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as KalshiLiveDailyLog[]
}

async function fetchPaperDailyLogs(range: DateRange, customDate?: string): Promise<KalshiPaperDailyLog[]> {
  const supabase = createClient()
  let query = supabase
    .from('kalshi_paper_trading_daily_log')
    .select('*')
    .order('game_date', { ascending: false })

  const { min, max } = getDateFilter(range, customDate)
  if (min) query = query.gte('game_date', min)
  if (max) query = query.lte('game_date', max)

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

export function useBotOrders(tab: BotTab, range: DateRange, customDate?: string) {
  return useQuery<(KalshiLiveOrder | KalshiPaperBet)[]>({
    queryKey: ['bot-tracker', 'orders', tab, range, customDate],
    queryFn: () => (tab === 'live' ? fetchLiveOrders(range, customDate) : fetchPaperBets(range, customDate)),
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}

export function useBotDailyLogs(tab: BotTab, range: DateRange, customDate?: string) {
  return useQuery<(KalshiLiveDailyLog | KalshiPaperDailyLog)[]>({
    queryKey: ['bot-tracker', 'daily-logs', tab, range, customDate],
    queryFn: () => (tab === 'live' ? fetchLiveDailyLogs(range, customDate) : fetchPaperDailyLogs(range, customDate)),
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  })
}
