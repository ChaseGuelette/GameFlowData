'use client'

import { DfsEntryCard } from './DfsEntryCard'
import type { UserDfsEntryWithLegs } from '@/types/dfs-entries'

interface DfsEntryListProps {
  entries: UserDfsEntryWithLegs[]
  onRemove?: (id: number) => void
}

export function DfsEntryList({ entries, onRemove }: DfsEntryListProps) {
  if (entries.length === 0) {
    return (
      <div className="flex items-center justify-center py-16">
        <div className="text-center">
          <p className="text-slate-400 text-lg">No DFS entries</p>
          <p className="text-slate-500 text-sm mt-2">
            Build slips on the DFS page and place entries to track them here
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-2">
      {entries.map(entry => (
        <DfsEntryCard key={entry.id} entry={entry} onRemove={onRemove} />
      ))}
    </div>
  )
}
