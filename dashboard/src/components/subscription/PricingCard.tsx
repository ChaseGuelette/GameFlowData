import Link from 'next/link'

interface PricingCardProps {
  ctaHref?: string
  ctaLabel?: string
}

const features = [
  'Full prediction access (pts, reb, ast)',
  'Curated Model Picks daily',
  'Black-Litterman edge analysis',
  'Quantile distributions & confidence bands',
  'Paper trading performance tracking',
  'Bet history & stat breakdowns',
]

export function PricingCard({ ctaHref = '/signup', ctaLabel = 'Get Started' }: PricingCardProps) {
  return (
    <div className="bg-slate-800 border border-blue-500/50 rounded-xl p-8 max-w-md mx-auto">
      <div className="text-center mb-6">
        <h3 className="text-xl font-semibold text-slate-50">Pro</h3>
        <div className="mt-4">
          <span className="text-5xl font-bold text-slate-50">$19</span>
          <span className="text-2xl text-slate-400">.99</span>
          <span className="text-slate-400 ml-1">/mo</span>
        </div>
        <p className="text-sm text-slate-400 mt-2">Full access to all predictions and tools</p>
      </div>

      <ul className="space-y-3 mb-8">
        {features.map((feature) => (
          <li key={feature} className="flex items-start gap-3">
            <span className="text-green-400 mt-0.5 shrink-0">&#10003;</span>
            <span className="text-sm text-slate-300">{feature}</span>
          </li>
        ))}
      </ul>

      <Link
        href={ctaHref}
        className="block w-full py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors text-center"
      >
        {ctaLabel}
      </Link>
    </div>
  )
}
