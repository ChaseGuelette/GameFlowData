'use client'

import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { useRouter } from 'next/navigation'

interface NavbarProps {
  bankroll?: number
}

export function Navbar({ bankroll }: NavbarProps) {
  const router = useRouter()
  const supabase = createClient()

  const handleLogout = async () => {
    await supabase.auth.signOut()
    router.push('/login')
    router.refresh()
  }

  return (
    <nav className="bg-slate-800 border-b border-slate-700">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo and Nav Links */}
          <div className="flex items-center space-x-8">
            <Link href="/" className="flex items-center space-x-2">
              <span className="text-2xl font-bold text-blue-500">GF</span>
              <span className="text-lg font-semibold text-slate-50">GameFlow</span>
            </Link>

            <div className="hidden md:flex items-center space-x-4">
              <Link
                href="/"
                className="text-slate-300 hover:text-slate-50 px-3 py-2 rounded-md text-sm font-medium"
              >
                Props
              </Link>
              <Link
                href="/history"
                className="text-slate-400 hover:text-slate-300 px-3 py-2 rounded-md text-sm font-medium"
              >
                History
              </Link>
              <Link
                href="/performance"
                className="text-slate-400 hover:text-slate-300 px-3 py-2 rounded-md text-sm font-medium"
              >
                Performance
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
