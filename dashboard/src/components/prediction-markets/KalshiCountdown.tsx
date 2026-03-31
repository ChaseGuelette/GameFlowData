'use client'

import { useEffect, useState } from 'react'

interface KalshiCountdownProps {
  closeTime: string | null
}

export function KalshiCountdown({ closeTime }: KalshiCountdownProps) {
  const [timeLeft, setTimeLeft] = useState('')
  const [urgency, setUrgency] = useState<'normal' | 'warning' | 'critical'>('normal')

  useEffect(() => {
    if (!closeTime) {
      setTimeLeft('--')
      return
    }

    const update = () => {
      const now = Date.now()
      const close = new Date(closeTime).getTime()
      const diff = close - now

      if (diff <= 0) {
        setTimeLeft('Closed')
        setUrgency('critical')
        return
      }

      const hours = Math.floor(diff / (1000 * 60 * 60))
      const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))

      if (hours > 2) {
        setTimeLeft(`${hours}h ${minutes}m`)
        setUrgency('normal')
      } else if (hours > 0) {
        setTimeLeft(`${hours}h ${minutes}m`)
        setUrgency('warning')
      } else if (minutes > 30) {
        setTimeLeft(`${minutes}m`)
        setUrgency('warning')
      } else {
        const secs = Math.floor((diff % (1000 * 60)) / 1000)
        setTimeLeft(`${minutes}m ${secs}s`)
        setUrgency('critical')
      }
    }

    update()
    const interval = setInterval(update, 1000)
    return () => clearInterval(interval)
  }, [closeTime])

  const colorClass = {
    normal: 'text-green-400',
    warning: 'text-amber-400',
    critical: 'text-red-400',
  }[urgency]

  return <span className={`font-mono text-sm ${colorClass}`}>{timeLeft}</span>
}
