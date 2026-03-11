import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Format date as "Jan 16, 2026"
// Handles date-only strings (YYYY-MM-DD) without timezone shift
export function formatDate(date: string | Date): string {
  let d: Date
  if (typeof date === 'string') {
    // For date-only strings like "2026-02-10", parse components to avoid timezone shift
    // new Date("2026-02-10") interprets as UTC, which shifts when converted to local time
    if (/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      const [year, month, day] = date.split('-').map(Number)
      d = new Date(year, month - 1, day) // month is 0-indexed
    } else {
      d = new Date(date)
    }
  } else {
    d = date
  }
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

// Format date as "YYYY-MM-DD" for API queries (local timezone)
export function formatDateISO(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// Get today's date in YYYY-MM-DD format (local timezone)
export function getToday(): string {
  return formatDateISO(new Date())
}

// Format edge as percentage with sign (NaN-safe)
export function formatEdge(edge: number): string {
  if (!Number.isFinite(edge)) return '—'
  const sign = edge >= 0 ? '+' : ''
  return `${sign}${(edge * 100).toFixed(1)}%`
}

// Format probability as percentage (NaN-safe)
export function formatProb(prob: number): string {
  if (!Number.isFinite(prob)) return '—'
  return `${(prob * 100).toFixed(1)}%`
}

// Get NBA player headshot URL
export function getHeadshotUrl(playerId: number): string {
  return `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`
}

// Placeholder image for when headshot fails to load (inline SVG as data URL)
export const PLACEHOLDER_AVATAR = 'data:image/svg+xml,' + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect fill="#334155" width="100" height="100"/><circle cx="50" cy="35" r="20" fill="#64748b"/><ellipse cx="50" cy="85" rx="35" ry="25" fill="#64748b"/></svg>`)

// Determine edge tier for styling
export function getEdgeTier(edge: number): 'high' | 'medium' | 'low' {
  const absEdge = Math.abs(edge)
  if (absEdge >= 0.07) return 'high'
  if (absEdge >= 0.05) return 'medium'
  return 'low'
}

// Convert American odds to decimal
export function americanToDecimal(american: number): number {
  if (american > 0) {
    return 1 + american / 100
  } else {
    return 1 + 100 / Math.abs(american)
  }
}

// Convert American odds to implied probability
export function americanToImplied(american: number): number {
  if (american > 0) {
    return 100 / (american + 100)
  } else {
    return Math.abs(american) / (Math.abs(american) + 100)
  }
}

// Format game time for display (e.g., "7:30 PM")
export function formatGameTime(gameTime: string | undefined | null): string {
  if (!gameTime) return 'TBD'
  try {
    const date = new Date(gameTime)
    if (isNaN(date.getTime())) return 'TBD'
    return date.toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
    })
  } catch {
    return 'TBD'
  }
}

// Game status based on start time (~3 hour game duration)
const GAME_DURATION_MS = 3 * 60 * 60 * 1000

export function isGameLive(gameTime: string | undefined | null): boolean {
  if (!gameTime) return false
  try {
    const start = new Date(gameTime)
    if (isNaN(start.getTime())) return false
    const now = Date.now()
    return start.getTime() <= now && now < start.getTime() + GAME_DURATION_MS
  } catch {
    return false
  }
}

export function isGameDone(gameTime: string | undefined | null): boolean {
  if (!gameTime) return false
  try {
    const start = new Date(gameTime)
    if (isNaN(start.getTime())) return false
    return Date.now() >= start.getTime() + GAME_DURATION_MS
  } catch {
    return false
  }
}

// Centralized game status: uses live NBA data when available, falls back to time-based
import { type GameStatusInfo } from '@/types/predictions'

export function getGameStatus(
  gameId: string | undefined,
  gameTime: string | undefined | null,
  statusMap: Map<string, GameStatusInfo>
): { isLive: boolean; isDone: boolean; statusText: string } {
  // Check live scoreboard data first
  if (gameId && statusMap.size > 0) {
    const info = statusMap.get(gameId)
    if (info) {
      return {
        isLive: info.gameStatus === 2,
        isDone: info.gameStatus === 3,
        statusText: info.gameStatusText,
      }
    }
  }

  // Fall back to time-based estimation
  return {
    isLive: isGameLive(gameTime),
    isDone: isGameDone(gameTime),
    statusText: isGameLive(gameTime) ? 'Live' : isGameDone(gameTime) ? 'Final' : '',
  }
}

// =============================================================================
// Black-Litterman Blending Functions
// =============================================================================

// BL blending constants (from src/models/black_litterman.py)
const BL_Z_MAX = 1.0
const BL_MAX_WEIGHT = 0.50

// Calculate BL confidence from prediction distribution
// Higher confidence when line is far from model's prediction center
export function calculateBLConfidence(predMean: number, predStd: number, line: number): number {
  if (!predStd || predStd < 0.001) return 0
  const z = Math.abs(predMean - line) / predStd
  return Math.min(z / BL_Z_MAX, 1.0)
}

// Blend model probability with implied probability in log-odds space
export function blendProbability(
  modelProb: number,
  impliedProb: number,
  tau: number,
  confidence: number
): number {
  // Clamp to avoid log(0)
  const modelP = Math.max(0.01, Math.min(0.99, modelProb))
  const impliedP = Math.max(0.01, Math.min(0.99, impliedProb))

  // Convert to log-odds
  const modelLogit = Math.log(modelP / (1 - modelP))
  const impliedLogit = Math.log(impliedP / (1 - impliedP))

  // Blending weight (capped at max_weight)
  const w = Math.min(tau * confidence, BL_MAX_WEIGHT)

  // Blend in log-odds space
  const posteriorLogit = impliedLogit + w * (modelLogit - impliedLogit)

  // Convert back to probability
  return 1 / (1 + Math.exp(-posteriorLogit))
}
