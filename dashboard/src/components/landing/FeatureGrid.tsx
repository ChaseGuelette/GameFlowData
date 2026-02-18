const features = [
  {
    title: 'Quantile Regression',
    description: 'Full probability distributions for every player prop — not just point estimates. Know the range of outcomes.',
    icon: '📊',
  },
  {
    title: 'Black-Litterman Blending',
    description: 'Market-implied probabilities fused with model predictions for calibrated edge estimates.',
    icon: '⚡',
  },
  {
    title: 'Model Picks',
    description: 'Curated daily selections with BL Edge ≥9%. Backtested and optimized for consistent returns.',
    icon: '🎯',
  },
  {
    title: 'Paper Trading',
    description: 'Track model performance with simulated bankroll management. Full history and stat breakdowns.',
    icon: '📈',
  },
]

export function FeatureGrid() {
  return (
    <section className="py-16 sm:py-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-slate-50 text-center mb-12">
          Built for Serious Bettors
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature) => (
            <div
              key={feature.title}
              className="bg-slate-800 border border-slate-700 rounded-lg p-6 hover:border-slate-600 transition-colors"
            >
              <div className="text-3xl mb-4">{feature.icon}</div>
              <h3 className="text-lg font-semibold text-slate-50 mb-2">{feature.title}</h3>
              <p className="text-sm text-slate-400">{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
