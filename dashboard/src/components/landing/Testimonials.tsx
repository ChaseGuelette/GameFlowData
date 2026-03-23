// =============================================
// EDIT TESTIMONIALS HERE
// Each testimonial: { name, handle?, text, rating (1-5) }
// =============================================
const testimonials = [
  {
    name: 'Marcus T.',
    handle: '@marcus_bets',
    text: "The quantile model picks up edges I'd never spot manually. My prop hit rate has gone way up since I started using GameFlow.",
    rating: 5,
  },
  {
    name: 'Jordan K.',
    text: 'Finally a tool that shows full distributions instead of just a number. The analysis breakdowns are insanely detailed.',
    rating: 5,
  },
  {
    name: 'Tyler R.',
    handle: '@tyler_dfs',
    text: "Been paper trading for two weeks and the ROI is real. Can't believe this is free during beta.",
    rating: 4,
  },
  {
    name: 'Alex M.',
    text: 'The DFS edge finder alone is worth it. I can scan every player prop in seconds and find the best value plays.',
    rating: 5,
  },
  {
    name: 'Chris D.',
    handle: '@chrisd_picks',
    text: 'Love the Discord community too. The daily picks discussion helps me understand the model logic better.',
    rating: 4,
  },
]

function StarRating({ rating }: { rating: number }) {
  return (
    <div className="flex gap-0.5 mb-3">
      {Array.from({ length: 5 }, (_, i) => (
        <svg
          key={i}
          className={`h-4 w-4 ${i < rating ? 'text-yellow-400' : 'text-slate-600'}`}
          fill="currentColor"
          viewBox="0 0 20 20"
        >
          <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
        </svg>
      ))}
    </div>
  )
}

export function Testimonials() {
  return (
    <section className="py-16 sm:py-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-slate-50 text-center mb-12">
          What Bettors Are Saying
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {testimonials.map((t) => (
            <div
              key={t.name}
              className="bg-slate-800 border border-slate-700 rounded-xl p-6 hover:border-slate-600 transition-colors"
            >
              <StarRating rating={t.rating} />
              <p className="text-sm text-slate-300 mb-4 leading-relaxed">&ldquo;{t.text}&rdquo;</p>
              <div className="flex items-center gap-3">
                <div className="h-9 w-9 rounded-full bg-blue-500/20 flex items-center justify-center text-blue-400 text-sm font-medium">
                  {t.name.split(' ').map((n) => n[0]).join('')}
                </div>
                <div>
                  <p className="text-sm font-medium text-slate-50">{t.name}</p>
                  {t.handle && (
                    <p className="text-xs text-slate-500">{t.handle}</p>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
