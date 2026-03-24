import { Navbar } from '@/components/layout/Navbar'
import { SportProvider } from '@/contexts/SportContext'

export default function ProtectedLayout({ children }: { children: React.ReactNode }) {
  return (
    <SportProvider>
      <div className="min-h-screen flex flex-col">
        <Navbar />
        {children}
      </div>
    </SportProvider>
  )
}
