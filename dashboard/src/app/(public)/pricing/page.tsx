import Link from 'next/link'
import { DISCORD_URL } from '@/lib/constants'

export default function PricingPage() {
  return (
    <div className="py-16 sm:py-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-slate-50">Pricing</h1>
          <p className="mt-4 text-lg text-slate-400">
            Free while we&apos;re in beta. Full access, no credit card required.
          </p>
        </div>

        {/* Beta Access Card */}
        <div className="max-w-md mx-auto bg-slate-800 border border-slate-700 rounded-xl p-8">
          <div className="text-center mb-6">
            <p className="text-sm font-medium text-blue-400 uppercase tracking-wide">Beta Access</p>
            <div className="mt-2 flex items-baseline justify-center gap-1">
              <span className="text-5xl font-bold text-slate-50">$0</span>
              <span className="text-slate-400">/mo</span>
            </div>
          </div>
          <ul className="space-y-3 mb-8 text-sm text-slate-300">
            <li className="flex items-center gap-2">
              <span className="text-green-400">&#10003;</span>
              AI-powered prop predictions
            </li>
            <li className="flex items-center gap-2">
              <span className="text-green-400">&#10003;</span>
              Black-Litterman edge blending
            </li>
            <li className="flex items-center gap-2">
              <span className="text-green-400">&#10003;</span>
              Model Picks (BL edge &ge;9%)
            </li>
            <li className="flex items-center gap-2">
              <span className="text-green-400">&#10003;</span>
              Paper trading &amp; performance tracking
            </li>
            <li className="flex items-center gap-2">
              <span className="text-green-400">&#10003;</span>
              Discord community access
            </li>
          </ul>
          <div className="flex flex-col gap-3">
            <Link
              href="/signup"
              className="block text-center px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
            >
              Sign Up Free
            </Link>
            <a
              href={DISCORD_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="block text-center px-6 py-3 border border-slate-600 hover:border-slate-500 text-slate-300 hover:text-white font-semibold rounded-lg transition-colors"
            >
              Join Discord
            </a>
          </div>
        </div>

        <p className="text-center text-sm text-slate-500 mt-8">
          Paid plans may be introduced in the future. Beta users will be notified in advance.
        </p>
      </div>
    </div>
  )
}
