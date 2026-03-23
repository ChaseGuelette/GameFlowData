import Image from 'next/image'

const betSlips = [
  { src: '/bet-slips/slip-1.png', alt: 'Winning bet slip 1' },
  { src: '/bet-slips/slip-2.png', alt: 'Winning bet slip 2' },
  { src: '/bet-slips/slip-3.png', alt: 'Winning bet slip 3' },
  { src: '/bet-slips/slip-4.png', alt: 'Winning bet slip 4' },
  { src: '/bet-slips/slip-5.png', alt: 'Winning bet slip 5' },
  { src: '/bet-slips/slip-6.png', alt: 'Winning bet slip 6' },
]

function BetSlipPlaceholder() {
  return (
    <div className="aspect-[3/4] bg-gradient-to-br from-slate-700/50 to-slate-800/50 flex flex-col items-center justify-center text-slate-500">
      <span className="text-3xl mb-2">🎫</span>
      <span className="text-xs">Bet slip image</span>
    </div>
  )
}

export function WinningBetSlips() {
  // Set to true once real images are added to dashboard/public/bet-slips/
  const hasImages = false

  return (
    <section className="py-16 sm:py-24 bg-slate-900/50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-slate-50 text-center mb-12">
          Proven Results
        </h2>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
          {betSlips.map((slip) => (
            <div
              key={slip.alt}
              className="relative bg-slate-800 border border-slate-700 rounded-xl overflow-hidden hover:scale-[1.02] hover:border-slate-600 transition-all"
            >
              {hasImages ? (
                <Image
                  src={slip.src}
                  alt={slip.alt}
                  width={300}
                  height={400}
                  className="w-full h-auto"
                />
              ) : (
                <BetSlipPlaceholder />
              )}
              <span className="absolute top-3 right-3 px-2 py-0.5 bg-green-500/20 text-green-400 text-xs font-medium rounded-full">
                Won
              </span>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
