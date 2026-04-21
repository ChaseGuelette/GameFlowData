'use client'

import { createContext, useContext, useState, useCallback, useMemo, useEffect, type ReactNode } from 'react'
import { type Sport, type SportConfig, getSportConfig } from '@/lib/sport-config'

interface SportContextValue {
  sport: Sport
  setSport: (sport: Sport) => void
  config: SportConfig
}

const SportContext = createContext<SportContextValue | null>(null)

export function SportProvider({ children }: { children: ReactNode }) {
  // Always initialize to 'nba' so server and first client render match.
  // After hydration, read localStorage and update if the user had a different sport saved.
  const [sport, setSportRaw] = useState<Sport>('nba')

  useEffect(() => {
    const stored = localStorage.getItem('selectedSport')
    if (stored === 'nba' || stored === 'mlb') setSportRaw(stored)
  }, [])

  const setSport = useCallback((s: Sport) => {
    setSportRaw(s)
    localStorage.setItem('selectedSport', s)
  }, [])

  const config = useMemo(() => getSportConfig(sport), [sport])

  const value = useMemo(() => ({ sport, setSport, config }), [sport, setSport, config])

  return (
    <SportContext.Provider value={value}>
      {children}
    </SportContext.Provider>
  )
}

export function useSport(): SportContextValue {
  const ctx = useContext(SportContext)
  if (!ctx) throw new Error('useSport must be used within a SportProvider')
  return ctx
}
