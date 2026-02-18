import Link from 'next/link'
import { DISCORD_URL } from '@/lib/constants'

export function Footer() {
  return (
    <footer className="bg-slate-800 border-t border-slate-700 py-8 mt-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center space-x-2">
            <span className="text-lg font-bold text-blue-500">GF</span>
            <span className="text-sm text-slate-400">GameFlow</span>
          </div>
          <div className="flex items-center space-x-6 text-sm text-slate-400">
            <Link href="/terms" className="hover:text-slate-200 transition-colors">
              Terms of Service
            </Link>
            <Link href="/privacy" className="hover:text-slate-200 transition-colors">
              Privacy Policy
            </Link>
            <a
              href={DISCORD_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-slate-200 transition-colors"
            >
              Discord
            </a>
          </div>
          <p className="text-xs text-slate-500">
            &copy; {new Date().getFullYear()} GameFlow. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  )
}
