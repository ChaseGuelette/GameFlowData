// US state → legal sportsbook mapping
// Offshore books (pinnacle, novig, prophetx, bovada) excluded from all states
// fliff excluded (sweepstakes platform, not a regulated sportsbook)

export const US_STATES: { value: string; label: string }[] = [
  { value: '', label: 'All States' },
  { value: 'AZ', label: 'Arizona' },
  { value: 'CO', label: 'Colorado' },
  { value: 'CT', label: 'Connecticut' },
  { value: 'DC', label: 'Washington DC' },
  { value: 'IL', label: 'Illinois' },
  { value: 'IN', label: 'Indiana' },
  { value: 'IA', label: 'Iowa' },
  { value: 'KS', label: 'Kansas' },
  { value: 'KY', label: 'Kentucky' },
  { value: 'LA', label: 'Louisiana' },
  { value: 'MA', label: 'Massachusetts' },
  { value: 'MD', label: 'Maryland' },
  { value: 'MI', label: 'Michigan' },
  { value: 'NC', label: 'North Carolina' },
  { value: 'NH', label: 'New Hampshire' },
  { value: 'NJ', label: 'New Jersey' },
  { value: 'NY', label: 'New York' },
  { value: 'OH', label: 'Ohio' },
  { value: 'OR', label: 'Oregon' },
  { value: 'PA', label: 'Pennsylvania' },
  { value: 'RI', label: 'Rhode Island' },
  { value: 'TN', label: 'Tennessee' },
  { value: 'VA', label: 'Virginia' },
  { value: 'VT', label: 'Vermont' },
  { value: 'WV', label: 'West Virginia' },
  { value: 'WY', label: 'Wyoming' },
]

// Bookmaker keys match raw_player_props_combined.bookmaker values
const MAJOR = ['draftkings', 'fanduel', 'betmgm', 'caesars', 'williamhill_us', 'betrivers', 'espnbet', 'fanatics'] as const

export const STATE_SPORTSBOOKS: Record<string, string[]> = {
  AZ: [...MAJOR],
  CO: [...MAJOR, 'pointsbet', 'bet365'],
  CT: ['draftkings', 'fanduel'],
  DC: ['draftkings', 'fanduel', 'betmgm', 'caesars'],
  IL: [...MAJOR, 'pointsbet'],
  IN: [...MAJOR, 'pointsbet', 'hardrockbet'],
  IA: ['draftkings', 'fanduel', 'betmgm', 'caesars', 'williamhill_us', 'betrivers', 'espnbet', 'pointsbet', 'hardrockbet'],
  KS: [...MAJOR],
  KY: [...MAJOR],
  LA: [...MAJOR],
  MA: [...MAJOR],
  MD: [...MAJOR],
  MI: [...MAJOR, 'pointsbet', 'hardrockbet'],
  NC: [...MAJOR],
  NH: ['draftkings'],
  NJ: [...MAJOR, 'pointsbet', 'hardrockbet', 'bet365', 'unibet'],
  NY: [...MAJOR],
  OH: [...MAJOR, 'pointsbet', 'hardrockbet', 'bet365'],
  OR: ['draftkings', 'betmgm'],
  PA: [...MAJOR, 'pointsbet', 'hardrockbet', 'unibet'],
  RI: ['draftkings', 'fanduel'],
  TN: [...MAJOR, 'hardrockbet'],
  VA: [...MAJOR, 'hardrockbet'],
  VT: ['draftkings', 'fanduel'],
  WV: ['draftkings', 'fanduel', 'betmgm', 'caesars', 'williamhill_us', 'betrivers', 'espnbet', 'hardrockbet'],
  WY: ['draftkings', 'fanduel'],
}

export const SPORTSBOOK_OPTIONS: { value: string; label: string }[] = [
  { value: '', label: 'All Books' },
  { value: 'draftkings', label: 'DraftKings' },
  { value: 'fanduel', label: 'FanDuel' },
  { value: 'betmgm', label: 'BetMGM' },
  { value: 'caesars', label: 'Caesars' },
  { value: 'williamhill_us', label: 'William Hill' },
  { value: 'betrivers', label: 'BetRivers' },
  { value: 'espnbet', label: 'ESPN Bet' },
  { value: 'fanatics', label: 'Fanatics' },
  { value: 'pointsbet', label: 'PointsBet' },
  { value: 'hardrockbet', label: 'Hard Rock' },
  { value: 'bet365', label: 'Bet365' },
]

export function getAllowedBookmakers(stateCode: string): string[] | null {
  if (!stateCode) return null // "All States" = no filtering
  return STATE_SPORTSBOOKS[stateCode] ?? null
}
