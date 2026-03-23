{/*
  To replace placeholders with real screenshots:
  1. Add images to dashboard/public/screenshots/
  2. Import Image from 'next/image'
  3. Replace the placeholder div with:
     <Image src="/screenshots/predictions.png" alt={item.title} fill className="object-cover" />
*/}

const screenshots = [
  {
    title: 'Predictions',
    description: 'Daily AI-powered player prop predictions with quantile distributions.',
    icon: '📊',
  },
  {
    title: 'DFS Edge Finder',
    description: 'Find the highest-edge props across every stat and player.',
    icon: '🔍',
  },
  {
    title: 'Analysis Modal',
    description: 'Deep dive into any prediction with full model breakdowns.',
    icon: '📈',
  },
  {
    title: 'Performance Tracking',
    description: 'Paper trading results, ROI tracking, and bankroll management.',
    icon: '💰',
  },
]

export function ProductScreenshots() {
  return (
    <section className="py-16 sm:py-24 bg-slate-900/50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-slate-50 text-center mb-12">
          See the Dashboard in Action
        </h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {screenshots.map((item) => (
            <div
              key={item.title}
              className="bg-slate-800 border border-slate-700 rounded-xl p-6 hover:border-slate-600 transition-colors"
            >
              <div className="aspect-video rounded-lg overflow-hidden mb-4 relative bg-gradient-to-br from-slate-700/50 to-slate-800/50">
                <div className="absolute inset-0 opacity-10" style={{
                  backgroundImage: 'linear-gradient(to right, rgb(148 163 184 / 0.3) 1px, transparent 1px), linear-gradient(to bottom, rgb(148 163 184 / 0.3) 1px, transparent 1px)',
                  backgroundSize: '24px 24px',
                }} />
                <div className="absolute inset-0 flex flex-col items-center justify-center text-slate-500">
                  <span className="text-4xl mb-2">{item.icon}</span>
                  <span className="text-sm">Screenshot Coming Soon</span>
                </div>
              </div>
              <h3 className="text-lg font-semibold text-slate-50 mb-1">{item.title}</h3>
              <p className="text-sm text-slate-400">{item.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
