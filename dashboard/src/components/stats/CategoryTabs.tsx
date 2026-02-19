'use client'

import { cn } from '@/lib/utils'

interface CategoryTabsProps<T extends string> {
  categories: { value: T; label: string }[]
  active: T
  onChange: (cat: T) => void
}

export function CategoryTabs<T extends string>({ categories, active, onChange }: CategoryTabsProps<T>) {
  return (
    <div className="flex items-center space-x-1">
      {categories.map((cat) => (
        <button
          key={cat.value}
          onClick={() => onChange(cat.value)}
          className={cn(
            'px-3 py-1.5 text-xs font-medium rounded-full transition-colors',
            active === cat.value
              ? 'bg-slate-600 text-white'
              : 'text-slate-400 hover:text-slate-200 bg-slate-800 hover:bg-slate-700'
          )}
        >
          {cat.label}
        </button>
      ))}
    </div>
  )
}
