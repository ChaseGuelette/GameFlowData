import { Navbar } from '@/components/layout/Navbar'
import { SportProvider } from '@/contexts/SportContext'
import { QueryProvider } from '@/components/providers/QueryProvider'

export default function ProtectedLayout({ children }: { children: React.ReactNode }) {
  return (
    <QueryProvider>
      <SportProvider>
        <div className="min-h-screen flex flex-col">
          <Navbar />
          {children}
        </div>
      </SportProvider>
    </QueryProvider>
  )
}
