import Link from 'next/link'

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-slate-900 px-4">
      <div className="w-full max-w-md">
        <div className="text-center mb-8">
          <Link href="/" className="inline-flex items-center space-x-2">
            <span className="text-4xl font-bold text-blue-500">GF</span>
            <span className="text-2xl font-semibold text-slate-50">GameFlow</span>
          </Link>
          <p className="text-slate-400 mt-2">NBA Props Dashboard</p>
        </div>
        {children}
      </div>
    </div>
  )
}
