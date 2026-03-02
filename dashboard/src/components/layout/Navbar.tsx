'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { useRouter, usePathname } from 'next/navigation'

export function Navbar() {
  const router = useRouter()
  const pathname = usePathname()
  const supabase = createClient()
  const [bankroll, setBankroll] = useState<number | undefined>(undefined)
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)

  useEffect(() => {
    async function fetchBankroll() {
      const { data } = await supabase
        .from('paper_trading_daily_log')
        .select('bankroll')
        .order('game_date', { ascending: false })
        .limit(1)
        .single()

      if (data?.bankroll) {
        setBankroll(data.bankroll)
      }
    }
    fetchBankroll()
  }, [supabase])

  // Close mobile menu on route change
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
    const base = 'block px-4 py-3 rounded-md text-base font-medium transition-colors'
    if (isActive(path)) {
      return `${base} bg-blue-600 text-white`
    }
    return `${base} text-slate-300 hover:text-white hover:bg-slate-700`
  }

  return (
    <>
      <nav className="bg-slate-800 border-b border-slate-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Hamburger button (mobile only) */}
            <button
              onClick={() => setMobileMenuOpen(true)}
              className="md:hidden inline-flex items-center justify-center p-2 rounded-md text-slate-400 hover:text-white hover:bg-slate-700 transition-colors"
              aria-label="Open menu"
            >
              <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            </button>

            {/* Logo and Nav Links */}
            <div className="flex items-center space-x-8">
              <Link href="/dashboard" className="flex items-center space-x-2">
                <span className="text-2xl font-bold text-blue-500">GF</span>
                <span className="text-lg font-semibold text-slate-50">GameFlow</span>
              </Link>

              <div className="hidden md:flex items-center space-x-1">
                <Link href="/dashboard" className={navLinkClasses('/dashboard')}>
                  Props
                </Link>
                <Link href="/dfs" className={navLinkClasses('/dfs')}>
                  DFS
                </Link>
                <Link href="/history" className={navLinkClasses('/history')}>
                  History
                </Link>
                <Link href="/performance" className={navLinkClasses('/performance')}>
                  Performance
                </Link>
                <Link href="/stats" className={navLinkClasses('/stats')}>
                  Data Vault
                </Link>
              </div>
            </div>

            {/* Bankroll and Logout */}
            <div className="flex items-center space-x-4">
              {bankroll !== undefined && (
                <div className="text-right hidden sm:block">
                  <div className="text-xs text-slate-400">Balance</div>
                  <div className="text-lg font-semibold text-green-500">
                    ${bankroll.toLocaleString()}
                  </div>
                </div>
              )}
              <Link
                href="/account"
                className="hidden md:block text-slate-400 hover:text-slate-300 px-3 py-2 rounded-md text-sm font-medium"
              >
                Account
              </Link>
              <button
                onClick={handleLogout}
                className="hidden md:block text-slate-400 hover:text-slate-300 px-3 py-2 rounded-md text-sm font-medium"
              >
                Logout
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Mobile menu overlay */}
      {mobileMenuOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/50 md:hidden"
          onClick={() => setMobileMenuOpen(false)}
        />
      )}

      {/* Mobile slide-out menu */}
      <div
        className={`fixed top-0 left-0 z-50 h-full w-72 bg-slate-900 border-r border-slate-700 transform transition-transform duration-300 ease-in-out md:hidden ${
          mobileMenuOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        {/* Menu header */}
        <div className="flex items-center justify-between px-4 h-16 border-b border-slate-700">
          <Link href="/dashboard" className="flex items-center space-x-2">
            <span className="text-2xl font-bold text-blue-500">GF</span>
            <span className="text-lg font-semibold text-slate-50">GameFlow</span>
          </Link>
          <button
            onClick={() => setMobileMenuOpen(false)}
            className="p-2 rounded-md text-slate-400 hover:text-white hover:bg-slate-700 transition-colors"
            aria-label="Close menu"
          >
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Bankroll (mobile) */}
        {bankroll !== undefined && (
          <div className="px-4 py-3 border-b border-slate-700">
            <div className="text-xs text-slate-400">Balance</div>
            <div className="text-lg font-semibold text-green-500">
              ${bankroll.toLocaleString()}
            </div>
          </div>
        )}

        {/* Nav links */}
        <div className="px-3 py-4 space-y-1">
          <Link href="/dashboard" className={mobileNavLinkClasses('/dashboard')}>
            Props
          </Link>
          <Link href="/dfs" className={mobileNavLinkClasses('/dfs')}>
            DFS
          </Link>
          <Link href="/history" className={mobileNavLinkClasses('/history')}>
            History
          </Link>
          <Link href="/performance" className={mobileNavLinkClasses('/performance')}>
            Performance
          </Link>
          <Link href="/stats" className={mobileNavLinkClasses('/stats')}>
            Data Vault
          </Link>
        </div>

        {/* Bottom section */}
        <div className="absolute bottom-0 left-0 right-0 px-3 py-4 border-t border-slate-700 space-y-1">
          <Link href="/account" className={mobileNavLinkClasses('/account')}>
            Account
          </Link>
          <button
            onClick={handleLogout}
            className="block w-full text-left px-4 py-3 rounded-md text-base font-medium text-slate-300 hover:text-white hover:bg-slate-700 transition-colors"
          >
            Logout
          </button>
        </div>
      </div>
    </>
  )
}
