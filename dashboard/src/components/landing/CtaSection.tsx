import Link from 'next/link'
import { DISCORD_URL } from '@/lib/constants'

export function CtaSection() {
  return (
    <section className="py-16 sm:py-24 bg-slate-900/50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
        <h2 className="text-3xl font-bold text-slate-50 mb-4">Free During Beta</h2>
        <p className="text-slate-400 max-w-xl mx-auto mb-8">
          GameFlow is free while we build out the platform. Sign up for full access
          to AI-powered predictions, and join our Discord for daily picks and discussion.
        </p>
        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
          <Link
            href="/signup"
            className="px-8 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors text-lg"
          >
            Sign Up Free
          </Link>
          <a
            href={DISCORD_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="px-8 py-3 border border-slate-600 hover:border-slate-500 text-slate-300 hover:text-white font-semibold rounded-lg transition-colors text-lg"
          >
            Join Discord
          </a>
        </div>
      </div>
    </section>
  )
}
