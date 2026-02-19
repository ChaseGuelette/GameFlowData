import type { StatRow, PlayTypeGrouping } from '@/types/stats'

interface PlayTypeRow {
  team_id: number
  team_name: string
  team_abbreviation: string
  play_type: string
  type_grouping: string
  poss_pct: number | null
  ppp: number | null
  percentile: number | null
  [key: string]: unknown
}

/**
 * Pivots long-format play-type rows (330 rows = 30 teams x 11 play types)
 * into wide-format StatRows (30 rows, one per team) with synthetic columns
 * like `isolation_poss_pct`, `transition_ppp`, etc.
 *
 * poss_pct from the API is already a decimal (e.g., 0.059) — multiply by 100
 * so rawPct1 format displays "5.9%".
 */
export function pivotPlayTypes(
  rows: PlayTypeRow[],
  grouping: PlayTypeGrouping
): StatRow[] {
  const filtered = rows.filter((r) => r.type_grouping === grouping)

  // Group by team_id
  const byTeam = new Map<number, { name: string; abbrev: string; cols: Record<string, number | null> }>()

  for (const row of filtered) {
    const teamId = row.team_id
    if (!byTeam.has(teamId)) {
      byTeam.set(teamId, {
        name: row.team_name,
        abbrev: row.team_abbreviation,
        cols: {},
      })
    }
    const entry = byTeam.get(teamId)!
    const key = row.play_type.toLowerCase()

    // poss_pct: multiply by 100 for rawPct1 display
    entry.cols[`${key}_poss_pct`] = row.poss_pct != null ? row.poss_pct * 100 : null
    entry.cols[`${key}_ppp`] = row.ppp
    entry.cols[`${key}_percentile`] = row.percentile
  }

  const result: StatRow[] = []
  for (const [teamId, entry] of byTeam) {
    result.push({
      id: teamId,
      name: entry.name,
      teamAbbrev: entry.abbrev,
      ...entry.cols,
    })
  }

  return result
}
