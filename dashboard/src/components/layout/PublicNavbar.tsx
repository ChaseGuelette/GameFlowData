'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { DISCORD_URL } from '@/lib/constants'

export function PublicNavbar() {
  const pathname = usePathname()

  const navLinkClasses = (path: string) => {
    const base = 'px-3 py-2 rounded-md text-sm font-medium transition-colors'
    if (pathname === path) return `${base} text-white`
    return `${base} text-slate-400 hover:text-slate-100`
  }

  return (
    <nav className="bg-slate-800 border-b border-slate-700">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <div className="flex items-center space-x-8">
            <Link href="/" className="flex items-center space-x-2">
              <span className="text-2xl font-bold text-blue-500">GF</span>
              <span className="text-lg font-semibold text-slate-50">GameFlow</span>
            </Link>
            <div className="hidden md:flex items-center space-x-1">
              <Link href="/picks" className={navLinkClasses('/picks')}>
                Picks
              </Link>
              <a
                href={DISCORD_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="px-3 py-2 rounded-md text-sm font-medium text-slate-400 hover:text-slate-100 transition-colors"
              >
                Discord
              </a>
            </div>
          </div>

          <div className="flex items-center space-x-3">
            <Link
              href="/login"
              className="px-4 py-2 text-sm font-medium text-slate-300 hover:text-white transition-colors"
            >
              Log In
            </Link>
            <Link
              href="/signup"
              className="px-4 py-2 text-sm font-medium bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors"
            >
              Sign Up Free
            </Link>
          </div>
        </div>
      </div>
    </nav>
  )
}
