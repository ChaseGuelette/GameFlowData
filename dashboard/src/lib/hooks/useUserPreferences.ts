import { useCallback, useEffect, useRef, useState } from 'react'
import { createClient } from '@/lib/supabase/client'

export interface UserPreferences {
  userState: string
  bankroll: number
  initialBankroll: number
  kellyFraction: number
  useCustomKelly: boolean
  bankrollOverride: number | null
}

const DEFAULTS: UserPreferences = {
  userState: '',
  bankroll: 1000,
  initialBankroll: 1000,
  kellyFraction: 0.25,
  useCustomKelly: false,
  bankrollOverride: null,
}

/**
 * Hook for cross-device user preferences sync.
 * Loads instantly from localStorage, then syncs with Supabase user_profiles.
 * On change: writes to both localStorage (instant) and DB (async).
 */
export function useUserPreferences() {
  const [prefs, setPrefs] = useState<UserPreferences>(() => {
    if (typeof window === 'undefined') return DEFAULTS
    return {
      userState: localStorage.getItem('user_state') || '',
      bankroll: parseFloat(localStorage.getItem('betting_bankroll') || '') || 1000,
      initialBankroll: parseFloat(localStorage.getItem('betting_initial_bankroll') || '') || 1000,
      kellyFraction: parseFloat(localStorage.getItem('betting_kelly_fraction') || '') || 0.25,
      useCustomKelly: localStorage.getItem('betting_use_custom_kelly') === 'true',
      bankrollOverride: null,
    }
  })
  const [loading, setLoading] = useState(true)
  const userIdRef = useRef<string | null>(null)
  // Debounce timer for DB writes
  const writeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Fetch from DB on mount, merge with localStorage
  useEffect(() => {
    let cancelled = false

    async function syncFromDB() {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user || cancelled) { setLoading(false); return }
      userIdRef.current = user.id

      const { data, error } = await supabase
        .from('user_profiles')
        .select('user_state, bankroll, initial_bankroll, kelly_fraction, use_custom_kelly, bankroll_override')
        .eq('user_id', user.id)
        .maybeSingle()

      if (!error && data && !cancelled) {
        const dbPrefs: UserPreferences = {
          userState: data.user_state ?? prefs.userState,
          bankroll: data.bankroll != null ? Number(data.bankroll) : prefs.bankroll,
          initialBankroll: data.initial_bankroll != null ? Number(data.initial_bankroll) : prefs.initialBankroll,
          kellyFraction: data.kelly_fraction != null ? Number(data.kelly_fraction) : prefs.kellyFraction,
          useCustomKelly: data.use_custom_kelly ?? prefs.useCustomKelly,
          bankrollOverride: data.bankroll_override != null ? Number(data.bankroll_override) : null,
        }
        setPrefs(dbPrefs)
        // Update localStorage to match DB (DB is source of truth once fetched)
        localStorage.setItem('user_state', dbPrefs.userState)
        localStorage.setItem('betting_bankroll', dbPrefs.bankroll.toString())
        localStorage.setItem('betting_initial_bankroll', dbPrefs.initialBankroll.toString())
        localStorage.setItem('betting_kelly_fraction', dbPrefs.kellyFraction.toString())
        localStorage.setItem('betting_use_custom_kelly', dbPrefs.useCustomKelly.toString())
        // bankrollOverride is not stored in localStorage — always from DB
      }
      if (!cancelled) setLoading(false)
    }

    syncFromDB()
    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Write to DB (debounced)
  const writeToDB = useCallback((newPrefs: UserPreferences) => {
    if (writeTimerRef.current) clearTimeout(writeTimerRef.current)

    writeTimerRef.current = setTimeout(async () => {
      const supabase = createClient()
      const userId = userIdRef.current
      if (!userId) return

      const { error } = await supabase
        .from('user_profiles')
        .upsert({
          user_id: userId,
          user_state: newPrefs.userState,
          bankroll: newPrefs.bankroll,
          initial_bankroll: newPrefs.initialBankroll,
          kelly_fraction: newPrefs.kellyFraction,
          use_custom_kelly: newPrefs.useCustomKelly,
          bankroll_override: newPrefs.bankrollOverride,
        }, {
          onConflict: 'user_id',
        })

      if (error) {
        console.error('Failed to sync preferences to DB:', error)
      }
    }, 500) // 500ms debounce
  }, [])

  // Update a single preference field
  const updatePref = useCallback(<K extends keyof UserPreferences>(
    key: K,
    value: UserPreferences[K]
  ) => {
    setPrefs(prev => {
      const next = { ...prev, [key]: value }

      // Write to localStorage immediately
      switch (key) {
        case 'userState':
          localStorage.setItem('user_state', value as string)
          break
        case 'bankroll':
          localStorage.setItem('betting_bankroll', (value as number).toString())
          break
        case 'initialBankroll':
          localStorage.setItem('betting_initial_bankroll', (value as number).toString())
          break
        case 'kellyFraction':
          localStorage.setItem('betting_kelly_fraction', (value as number).toString())
          break
        case 'useCustomKelly':
          localStorage.setItem('betting_use_custom_kelly', (value as boolean).toString())
          break
        // bankrollOverride is DB-only, no localStorage entry
      }

      // Debounced DB write
      writeToDB(next)

      return next
    })
  }, [writeToDB])

  // Listen for bankrollUpdate events dispatched by syncEffectiveBankroll
  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent<{ bankroll: number }>
      setPrefs(prev => ({ ...prev, bankroll: ce.detail.bankroll }))
    }
    window.addEventListener('bankrollUpdate', handler)
    return () => window.removeEventListener('bankrollUpdate', handler)
  }, [])

  return { prefs, updatePref, loading }
}
