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

            <div className="hidden md:flex items-center space-x-1">
              <Link href="/dashboard" className={navLinkClasses('/dashboard')}>
                Props
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
              <div className="text-right">
                <div className="text-xs text-slate-400">Balance</div>
                <div className="text-lg font-semibold text-green-500">
                  ${bankroll.toLocaleString()}
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
        </div>
      </div>
    </nav>
  )
}
