'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { useRouter, usePathname } from 'next/navigation'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'
import { useAdmin } from '@/lib/hooks/useAdmin'
import { useSport } from '@/contexts/SportContext'
import type { Sport } from '@/lib/sport-config'

export function Navbar() {
  const router = useRouter()
  const pathname = usePathname()
  const supabase = createClient()
  const { prefs, loading: prefsLoading } = useUserPreferences()
  const { isAdmin } = useAdmin()
  const { sport, setSport, config } = useSport()
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)

  useEffect(() => {
    setMobileMenuOpen(false)
  }, [pathname])

  const handleLogout = async () => {
    await supabase.auth.signOut()
    router.push('/login')
    router.refresh()
  }

  const isActive = (path: string) => {
    if (path === '/dashboard') return pathname === '/dashboard'
    return pathname.startsWith(path)
  }

  const navLinkClasses = (path: string) => {
    const base = 'px-3 py-2 rounded-md text-sm font-medium transition-colors'
    if (isActive(path)) {
      return `${base} bg-blue-600 text-white`
    }
    return `${base} text-slate-400 hover:text-slate-100 hover:bg-slate-700`
  }

  const mobileNavLinkClasses = (path: string) => {
    const base = 'block w-full px-4 py-3 rounded-md text-sm font-medium transition-colors'
    if (isActive(path)) {
      return `${base} bg-blue-600 text-white`
    }
    return `${base} text-slate-400 hover:text-slate-100 hover:bg-slate-700`
  }

  return (
    <nav className="bg-slate-800 border-b border-slate-700">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo and Nav Links */}
          <div className="flex items-center space-x-8">
            <Link href="/dashboard" className="flex items-center space-x-2">
              <span className="text-2xl font-bold text-blue-500">GF</span>
              <span className="text-lg font-semibold text-slate-50">GameFlow</span>
            </Link>

            {/* Sport Toggle */}
            <div className="flex items-center bg-slate-900 rounded-lg p-0.5">
              {(['nba', 'mlb'] as Sport[]).map((s) => (
                <button
                  key={s}
                  onClick={() => setSport(s)}
                  className={`px-3 py-1 text-xs font-bold rounded-md transition-colors ${
                    sport === s
                      ? 'bg-blue-600 text-white'
                      : 'text-slate-400 hover:text-slate-200'
                  }`}
                >
                  {s.toUpperCase()}
                </button>
              ))}
            </div>

            <div className="hidden md:flex items-center space-x-1">
              <Link href="/dashboard" className={navLinkClasses('/dashboard')}>
                Props
              </Link>
              {config.features.dfs && (
                <Link href="/dfs" className={navLinkClasses('/dfs')}>
                  DFS
                </Link>
              )}
              <Link href="/history" className={navLinkClasses('/history')}>
                History
              </Link>
              <Link href="/performance" className={navLinkClasses('/performance')}>
                Performance
              </Link>
              <Link href="/track-record" className={navLinkClasses('/track-record')}>
                Track Record
              </Link>
              {config.features.statsVault && (
                <Link href="/stats" className={navLinkClasses('/stats')}>
                  Data Vault
                </Link>
              )}
              {config.features.predictionMarkets && (
                <Link href="/prediction-markets" className={navLinkClasses('/prediction-markets')}>
                  Markets
                </Link>
              )}
              {isAdmin && (
                <Link href="/bot-tracker" className={navLinkClasses('/bot-tracker')}>
                  Bot
                </Link>
              )}
            </div>
          </div>

          {/* Desktop: Bankroll and Logout */}
          <div className="hidden md:flex items-center space-x-4">
            {!prefsLoading && (
              <div className="text-right">
                <div className="text-xs text-slate-400">Balance</div>
                <div className="text-lg font-semibold text-green-500">
                  ${prefs.bankroll.toLocaleString()}
                </div>
              </div>
            )}
            <Link
              href="/account"
              className="text-slate-400 hover:text-slate-300 px-3 py-2 rounded-md text-sm font-medium"
            >
              Account
            </Link>
            <button
              onClick={handleLogout}
              className="text-slate-400 hover:text-slate-300 px-3 py-2 rounded-md text-sm font-medium"
            >
              Logout
            </button>
          </div>

          {/* Mobile: Hamburger button */}
          <button
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="md:hidden p-2 rounded-md text-slate-400 hover:text-slate-100 hover:bg-slate-700 transition-colors"
            aria-label="Toggle menu"
          >
            {mobileMenuOpen ? (
              <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            ) : (
              <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            )}
          </button>
        </div>
      </div>

      {/* Mobile menu panel */}
      {mobileMenuOpen && (
        <div className="md:hidden border-t border-slate-700 px-4 py-3 space-y-1">
          <Link href="/dashboard" className={mobileNavLinkClasses('/dashboard')}>
            Props
          </Link>
          {config.features.dfs && (
            <Link href="/dfs" className={mobileNavLinkClasses('/dfs')}>
              DFS
            </Link>
          )}
          <Link href="/history" className={mobileNavLinkClasses('/history')}>
            History
          </Link>
          <Link href="/performance" className={mobileNavLinkClasses('/performance')}>
            Performance
          </Link>
          <Link href="/track-record" className={mobileNavLinkClasses('/track-record')}>
            Track Record
          </Link>
          {config.features.statsVault && (
            <Link href="/stats" className={mobileNavLinkClasses('/stats')}>
              Data Vault
            </Link>
          )}
          {config.features.predictionMarkets && (
            <Link href="/prediction-markets" className={mobileNavLinkClasses('/prediction-markets')}>
              Markets
            </Link>
          )}
          {isAdmin && (
            <Link href="/bot-tracker" className={mobileNavLinkClasses('/bot-tracker')}>
              Bot
            </Link>
          )}

          {!prefsLoading && (
            <div className="px-3 py-2 border-t border-slate-700 mt-2 pt-2">
              <div className="text-xs text-slate-400">Balance</div>
              <div className="text-lg font-semibold text-green-500">
                ${prefs.bankroll.toLocaleString()}
              </div>
            </div>
          )}

          <div className="border-t border-slate-700 mt-2 pt-2 space-y-1">
            <Link href="/account" className={mobileNavLinkClasses('/account')}>
              Account
            </Link>
            <button
              onClick={handleLogout}
              className="block w-full text-left px-3 py-2 rounded-md text-sm font-medium text-slate-400 hover:text-slate-100 hover:bg-slate-700 transition-colors"
            >
              Logout
            </button>
          </div>
        </div>
      )}
    </nav>
  )
}
