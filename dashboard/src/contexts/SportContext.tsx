'use client'

import { createContext, useContext, useState, useCallback, useMemo, type ReactNode } from 'react'
import { type Sport, type SportConfig, getSportConfig } from '@/lib/sport-config'

interface SportContextValue {
  sport: Sport
  setSport: (sport: Sport) => void
  config: SportConfig
}

const SportContext = createContext<SportContextValue | null>(null)

function getInitialSport(): Sport {
  if (typeof window === 'undefined') return 'nba'
  const stored = localStorage.getItem('selectedSport')
  if (stored === 'nba' || stored === 'mlb') return stored
  return 'nba'
}

export function SportProvider({ children }: { children: ReactNode }) {
  const [sport, setSportRaw] = useState<Sport>(getInitialSport)

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
