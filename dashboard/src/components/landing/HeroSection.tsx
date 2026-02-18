import Link from 'next/link'
import { DISCORD_URL } from '@/lib/constants'

export function HeroSection() {
  return (
    <section className="relative py-20 sm:py-32">
      <div className="max-w-4xl mx-auto text-center px-4">
        <h1 className="text-4xl sm:text-6xl font-bold text-slate-50 tracking-tight">
          AI-Powered{' '}
          <span className="text-blue-500">NBA Prop</span>{' '}
          Predictions
        </h1>
        <p className="mt-6 text-lg sm:text-xl text-slate-400 max-w-2xl mx-auto">
          Quantile regression models, Black-Litterman edge blending, and real-time
          line analysis. Make smarter prop bets backed by data.
        </p>
        <div className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4">
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
