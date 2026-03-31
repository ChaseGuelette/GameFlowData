'use client'

import { useCallback, useMemo, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import {
  USER_DFS_SLIP_TYPES,
  type UserDfsSlipType,
  type SlipBuilderLeg,
} from '@/types/dfs-entries'
import type { EdgeMode } from '@/types/dfs'
import { calcParlayKelly, getEffectiveProb } from '@/lib/dfs-kelly'

interface SlipValidation {
  isValid: boolean
  requiredLegs: number
  currentLegs: number
  hasDuplicatePlayers: boolean
  message: string | null
}

export function useSlipBuilder(edgeMode: EdgeMode) {
  const [legs, setLegs] = useState<SlipBuilderLeg[]>([])
  const [slipType, setSlipType] = useState<UserDfsSlipType>('pp_2_power')
  const [placing, setPlacing] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Set of keys for quick lookup (row highlighting in DfsTable)
  const selectedLegKeys = useMemo(
    () => new Set(legs.map(l => l.key)),
    [legs],
  )

  // Auto-suggest slip type when leg count matches exactly one type
  const autoSuggestSlipType = useCallback((legCount: number) => {
    const matching = Object.entries(USER_DFS_SLIP_TYPES).filter(
      ([, cfg]) => cfg.legs === legCount,
    )
    if (matching.length === 1) {
      setSlipType(matching[0][0] as UserDfsSlipType)
    }
  }, [])

  const toggleLeg = useCallback((leg: SlipBuilderLeg) => {
    setError(null)
    setLegs(prev => {
      const existing = prev.findIndex(l => l.key === leg.key)
      if (existing >= 0) {
        const next = prev.filter((_, i) => i !== existing)
        return next
      }
      const next = [...prev, leg]
      // Auto-suggest on add
      setTimeout(() => autoSuggestSlipType(next.length), 0)
      return next
    })
  }, [autoSuggestSlipType])

  const removeLeg = useCallback((key: string) => {
    setError(null)
    setLegs(prev => prev.filter(l => l.key !== key))
  }, [])

  const clearAll = useCallback(() => {
    setLegs([])
    setError(null)
  }, [])

  // Validation
  const validation = useMemo<SlipValidation>(() => {
    const config = USER_DFS_SLIP_TYPES[slipType]
    const required = config.legs
    const current = legs.length

    // Check for duplicate players (same player_id in multiple legs)
    const playerIds = legs.map(l => l.player_id)
    const hasDuplicatePlayers = new Set(playerIds).size !== playerIds.length

    if (hasDuplicatePlayers) {
      return { isValid: false, requiredLegs: required, currentLegs: current, hasDuplicatePlayers, message: 'Duplicate player in slip' }
    }
    if (current < required) {
      return { isValid: false, requiredLegs: required, currentLegs: current, hasDuplicatePlayers, message: `Need ${required - current} more leg${required - current > 1 ? 's' : ''}` }
    }
    if (current > required) {
      return { isValid: false, requiredLegs: required, currentLegs: current, hasDuplicatePlayers, message: `Too many legs (${current}/${required})` }
    }

    // Check all legs have valid probs
    for (const leg of legs) {
      const prob = getEffectiveProb(leg, edgeMode)
      if (prob === null || prob <= 0) {
        return { isValid: false, requiredLegs: required, currentLegs: current, hasDuplicatePlayers, message: 'Missing probability for one or more legs' }
      }
    }

    return { isValid: true, requiredLegs: required, currentLegs: current, hasDuplicatePlayers, message: null }
  }, [legs, slipType, edgeMode])

  // Place entry to Supabase
  const placeEntry = useCallback(async (
    bankroll: number,
    kellyFraction: number,
    date: string,
  ): Promise<boolean> => {
    if (!validation.isValid) return false

    const kelly = calcParlayKelly(legs, slipType, bankroll, kellyFraction, edgeMode)
    if (!kelly || kelly.stake <= 0) {
      setError('No positive edge — Kelly stake is $0')
      return false
    }

    setPlacing(true)
    setError(null)

    try {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) {
        setError('Not authenticated')
        return false
      }

      const config = USER_DFS_SLIP_TYPES[slipType]

      // Insert entry
      const { data: entry, error: entryError } = await supabase
        .from('user_dfs_entries')
        .insert({
          user_id: user.id,
          entry_date: date,
          slip_type: slipType,
          platform: config.platform,
          num_legs: config.legs,
          stake: kelly.stake,
          combined_prob: kelly.combinedProb,
          kelly_fraction_used: kellyFraction,
          payout_multiplier: kelly.payoutMultiplier,
          edge_mode: edgeMode,
          status: 'pending',
        })
        .select('id')
        .single()

      if (entryError || !entry) {
        setError(entryError?.message ?? 'Failed to create entry')
        return false
      }

      // Insert legs
      const legRows = legs.map(leg => ({
        entry_id: entry.id,
        player_id: leg.player_id,
        player_name: leg.player_name,
        game_id: leg.game_id,
        stat_type: leg.stat_type,
        line: leg.line,
        direction: leg.direction,
        dfs_bookmaker: leg.dfs_bookmaker,
        model_prob: leg.model_prob,
        market_prob: leg.market_prob,
        edge: leg.edge,
        status: 'pending',
      }))

      const { error: legsError } = await supabase
        .from('user_dfs_legs')
        .insert(legRows)

      if (legsError) {
        // Rollback: delete the entry (cascade will clean up any partial legs)
        await supabase.from('user_dfs_entries').delete().eq('id', entry.id)
        setError(legsError.message)
        return false
      }

      // Success — clear slip
      setLegs([])
      return true
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unexpected error')
      return false
    } finally {
      setPlacing(false)
    }
  }, [legs, slipType, edgeMode, validation.isValid])

  return {
    legs,
    slipType,
    setSlipType,
    placing,
    error,
    selectedLegKeys,
    validation,
    toggleLeg,
    removeLeg,
    clearAll,
    placeEntry,
  }
}
