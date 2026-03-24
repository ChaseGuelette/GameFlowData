'use client'

import { useState, useEffect, useRef } from 'react'
import { type GameStatusInfo } from '@/types/predictions'
import { useSport } from '@/contexts/SportContext'

export type GameStatusMap = Map<string, GameStatusInfo>

const POLL_INTERVAL_MS = 30_000

export function useGameStatus(isToday: boolean): GameStatusMap {
  const { config } = useSport()
  const [statusMap, setStatusMap] = useState<GameStatusMap>(new Map())
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    // Only poll scoreboard for sports that support it
    if (!isToday || !config.features.scoreboard) {
      setStatusMap(new Map())
      return
    }

    async function fetchScoreboard() {
      try {
        const resp = await fetch('/api/scoreboard')
        if (!resp.ok) return
        const data: Record<string, GameStatusInfo> = await resp.json()
        const map = new Map<string, GameStatusInfo>()
        for (const [gameId, info] of Object.entries(data)) {
          map.set(gameId, info)
        }
        setStatusMap(map)
      } catch {
        // Keep previous state on error
      }
    }

    fetchScoreboard()
    intervalRef.current = setInterval(fetchScoreboard, POLL_INTERVAL_MS)

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }
  }, [isToday, config.features.scoreboard])

  return statusMap
}
