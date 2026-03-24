# Mobile UX

> Part of [[Product]]

## Current Status
Comprehensive mobile responsiveness overhaul completed (Session 3). Horizontal scrolling eliminated across all protected pages. DFS page now uses card-based layout on mobile.

## What Works Well
- PropCards are responsive
- Analysis Modal adapts to mobile viewport
- Navigation works on small screens
- Filter tabs are horizontally scrollable with hidden scrollbar (FilterTabs, HistoryFilters, CategoryTabs, HeatmapLegend)
- DFS table renders as compact cards on mobile, full table on desktop
- DFS filters collapse behind a toggle button on mobile
- HeatmapTable has responsive column widths — name column shrinks on mobile
- StatBreakdown table fits at 375px with tightened padding
- AnalysisModal L5 table has reduced cell padding on mobile

## Patterns Used
- **Scrollable tab bars**: `overflow-x-auto scrollbar-hide` + `whitespace-nowrap` on flex containers
- **Collapsible filters**: `useState` toggle with `sm:hidden` button, panel uses `hidden sm:flex` when collapsed
- **Mobile card / desktop table**: `block sm:hidden` for cards, `hidden sm:block` for table
- **Responsive padding**: `px-2 sm:px-3` or `px-1.5 sm:px-2` pattern
- **Responsive min-width**: `min-w-[100px] sm:min-w-[160px]` for sticky columns
- **Scrollbar-hide utility**: Defined in `globals.css`, works cross-browser

## CSS Utility
Added to `dashboard/src/app/globals.css`:
```css
.scrollbar-hide::-webkit-scrollbar { display: none; }
.scrollbar-hide { -ms-overflow-style: none; scrollbar-width: none; }
```

## Remaining Improvements
- No pagination on history/performance pages (can be slow on mobile with many records)
- React Query caching for faster cross-page navigation (SCALING.md Tier 2)

#mobile #product #ux
