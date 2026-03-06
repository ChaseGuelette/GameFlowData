import { useCallback, useEffect, useRef, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { type Prediction } from '@/types/predictions'

// Calculate Kelly stake as fraction of bankroll (same logic as AnalysisModal)
function calculateKelly(modelProb: number, odds: number, kellyFraction: number): number {
  if (odds === 0 || modelProb <= 0 || modelProb >= 1) return 0
  const b = odds > 0 ? odds / 100 : 100 / Math.abs(odds)
  const f = (modelProb * (b + 1) - 1) / b
  if (f <= 0) return 0
  return Math.min(f * kellyFraction, 0.25)
}

/**
 * Hook for user bet tracking with cross-device sync via Supabase.
 * Replaces the old per-date localStorage approach.
 *
 * Returns a Set<string> of taken bet keys (`${player_id}-${stat}`)
 * and a toggle function that does optimistic UI updates + async DB writes.
 */
export function useUserBets(selectedDate: string, bankroll?: number, kellyFraction?: number) {
  const [takenBets, setTakenBets] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState(true)
  // Track the full bet rows so we can delete by id
  const betRowsRef = useRef<Map<string, { id: number }>>(new Map())

  // Fetch user's bets for the selected date
  useEffect(() => {
    let cancelled = false

    async function fetchBets() {
      setLoading(true)
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user || cancelled) { setLoading(false); return }

      const { data, error } = await supabase
        .from('user_bets')
        .select('id, player_id, stat_type')
        .eq('user_id', user.id)
        .eq('game_date', selectedDate)

      if (!error && data && !cancelled) {
        const keys = new Set<string>()
        const rows = new Map<string, { id: number }>()
        for (const row of data) {
          const key = `${row.player_id}-${row.stat_type}`
          keys.add(key)
          rows.set(key, { id: row.id })
        }
        setTakenBets(keys)
        betRowsRef.current = rows
      }
      if (!cancelled) setLoading(false)
    }

    fetchBets()
    return () => { cancelled = true }
  }, [selectedDate])

  // Toggle a bet: optimistic update → async upsert/delete
  const toggleBet = useCallback((prediction: Prediction) => {
    const key = `${prediction.player_id}-${prediction.stat}`

    // Determine direction from the card's displayed recommendation
    const overEdge = Number.isFinite(prediction.over_edge) ? prediction.over_edge : 0
    const underEdge = Number.isFinite(prediction.under_edge) ? prediction.under_edge : 0
    const isOver = overEdge > underEdge
    const direction = isOver ? 'over' : 'under'
    const bestOdds = isOver ? prediction.best_over_odds : prediction.best_under_odds
    const bestBook = isOver ? prediction.best_over_book : prediction.best_under_book
    const modelProb = isOver ? prediction.model_prob_over : prediction.model_prob_under
    const edge = isOver ? overEdge : underEdge

    const isTaken = takenBets.has(key)

    // Optimistic update
    setTakenBets(prev => {
      const next = new Set(prev)
      if (isTaken) next.delete(key)
      else next.add(key)
      return next
    })

    // Async DB operation
    ;(async () => {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) return

      if (isTaken) {
        // Remove bet
        const row = betRowsRef.current.get(key)
        if (row) {
          const { error } = await supabase
            .from('user_bets')
            .delete()
            .eq('id', row.id)

          if (error) {
            console.error('Failed to delete user bet:', error)
            // Rollback
            setTakenBets(prev => new Set(prev).add(key))
          } else {
            betRowsRef.current.delete(key)
          }
        }
      } else {
        // Insert bet
        // Calculate Kelly stake if bankroll & kelly settings are available
        let stake: number | null = null
        if (bankroll && kellyFraction && modelProb && bestOdds) {
          const kellyPct = calculateKelly(modelProb, bestOdds, kellyFraction)
          if (kellyPct > 0) {
            stake = Math.round(bankroll * kellyPct * 100) / 100
          }
        }

        const { data, error } = await supabase
          .from('user_bets')
          .upsert({
            user_id: user.id,
            prediction_id: prediction.id,
            game_date: prediction.prediction_date,
            player_id: prediction.player_id,
            player_name: prediction.player_name || `Player ${prediction.player_id}`,
            stat_type: prediction.stat,
            line: prediction.prop_line,
            bet_direction: direction,
            odds_at_bet: bestOdds ?? null,
            book_at_bet: bestBook ?? null,
            model_prob: modelProb ?? null,
            edge: edge ?? null,
            stake,
          }, {
            onConflict: 'user_id,game_date,player_id,stat_type',
          })
          .select('id')
          .single()

        if (error) {
          console.error('Failed to insert user bet:', error)
          // Rollback
          setTakenBets(prev => {
            const next = new Set(prev)
            next.delete(key)
            return next
          })
        } else if (data) {
          betRowsRef.current.set(key, { id: data.id })
        }
      }
    })()
  }, [takenBets, bankroll, kellyFraction])

  return { takenBets, toggleBet, loading }
}
