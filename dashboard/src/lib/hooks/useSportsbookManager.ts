'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'

export const PRESET_BOOKS = [
  'DraftKings',
  'FanDuel',
  'BetMGM',
  'Caesars Sportsbook',
  'PointsBet',
  'Kalshi',
  'Polymarket',
  'BetRivers',
  'ESPN Bet',
  'Bet365',
  'Hard Rock Bet',
  'Fanatics Sportsbook',
  'PrizePicks',
  'Underdog Fantasy',
  'Betr',
]

export interface Sportsbook {
  id: number
  book_name: string
  balance: number
  notes: string | null
  sort_order: number
}

/**
 * Computes the effective bankroll (override ?? sum of books),
 * writes it to user_profiles.bankroll, updates localStorage,
 * and dispatches a bankrollUpdate window event.
 */
export async function syncEffectiveBankroll(
  books: Sportsbook[],
  override: number | null
): Promise<void> {
  const autoSum = books.reduce((acc, b) => acc + b.balance, 0)
  const effective = override != null ? override : autoSum

  // Update localStorage
  localStorage.setItem('betting_bankroll', effective.toString())

  // Dispatch window event so useUserPreferences updates in-place
  window.dispatchEvent(new CustomEvent('bankrollUpdate', { detail: { bankroll: effective } }))

  // Write to DB
  const supabase = createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) return

  await supabase
    .from('user_profiles')
    .upsert({ user_id: user.id, bankroll: effective }, { onConflict: 'user_id' })
}

async function fetchSportsbooks(): Promise<Sportsbook[]> {
  const supabase = createClient()
  const { data, error } = await supabase
    .from('user_sportsbooks')
    .select('id, book_name, balance, notes, sort_order')
    .order('sort_order', { ascending: true })
    .order('created_at', { ascending: true })

  if (error) throw error
  return (data ?? []).map(row => ({
    id: row.id,
    book_name: row.book_name,
    balance: Number(row.balance),
    notes: row.notes,
    sort_order: row.sort_order,
  }))
}

export function useSportsbooks() {
  return useQuery({
    queryKey: ['sportsbooks'],
    queryFn: fetchSportsbooks,
  })
}

export function useAddSportsbook() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async ({ book_name, balance }: { book_name: string; balance: number }) => {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) throw new Error('Not authenticated')

      const { error } = await supabase
        .from('user_sportsbooks')
        .insert({ user_id: user.id, book_name, balance })

      if (error) throw error
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sportsbooks'] }),
  })
}

export function useUpdateSportsbook() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async ({ id, ...fields }: { id: number; balance?: number; notes?: string }) => {
      const supabase = createClient()
      const { error } = await supabase
        .from('user_sportsbooks')
        .update({ ...fields, updated_at: new Date().toISOString() })
        .eq('id', id)

      if (error) throw error
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sportsbooks'] }),
  })
}

export function useRemoveSportsbook() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: async (id: number) => {
      const supabase = createClient()
      const { error } = await supabase
        .from('user_sportsbooks')
        .delete()
        .eq('id', id)

      if (error) throw error
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ['sportsbooks'] }),
  })
}
