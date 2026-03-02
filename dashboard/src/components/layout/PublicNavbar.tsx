'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { DISCORD_URL } from '@/lib/constants'

export function PublicNavbar() {
  const pathname = usePathname()
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)

  // Close mobile menu on route change
  useEffect(() => {
    setMobileMenuOpen(false)
  }, [pathname])

  const navLinkClasses = (path: string) => {
    const base = 'px-3 py-2 rounded-md text-sm font-medium transition-colors'
    if (pathname === path) return `${base} text-white`
    return `${base} text-slate-400 hover:text-slate-100`
  }

  const mobileNavLinkClasses = (path: string) => {
    const base = 'block px-4 py-3 rounded-md text-base font-medium transition-colors'
    if (pathname === path) return `${base} text-white bg-slate-700`
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
          <Link href="/" className="flex items-center space-x-2">
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

        {/* Nav links */}
        <div className="px-3 py-4 space-y-1">
          <Link href="/picks" className={mobileNavLinkClasses('/picks')}>
            Picks
          </Link>
          <a
            href={DISCORD_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="block px-4 py-3 rounded-md text-base font-medium text-slate-300 hover:text-white hover:bg-slate-700 transition-colors"
          >
            Discord
          </a>
        </div>

        {/* Bottom section */}
        <div className="absolute bottom-0 left-0 right-0 px-3 py-4 border-t border-slate-700 space-y-2">
          <Link
            href="/login"
            className="block px-4 py-3 rounded-md text-base font-medium text-slate-300 hover:text-white hover:bg-slate-700 text-center transition-colors"
          >
            Log In
          </Link>
          <Link
            href="/signup"
            className="block px-4 py-3 rounded-md text-base font-medium bg-blue-600 hover:bg-blue-700 text-white text-center transition-colors"
          >
            Sign Up Free
          </Link>
        </div>
      </div>
    </>
  )
}
