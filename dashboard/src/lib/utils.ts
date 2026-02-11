import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Format date as "Jan 16, 2026"
export function formatDate(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date
  return d.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

// Format date as "YYYY-MM-DD" for API queries
export function formatDateISO(date: Date): string {
  return date.toISOString().split('T')[0]
}

// Get today's date in YYYY-MM-DD format
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
