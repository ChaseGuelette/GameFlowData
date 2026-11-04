# NCAAB data sources

- Research checked: 2026-11-03
- Status: source-selection handoff; narrow historical/local-live probes only, not full-period or production certification.

[Handoff summary](README.md) | [Model architecture decision](model-architecture-decision.md) | [System architecture](architecture-overview.md)

## Initial source decision

| Purpose | Initial source/access | Boundary |
|---|---|---|
| Historical schedules and team/player game statistics | SportsDataverse's public ESPN-derived releases; direct Parquet or Python release loaders | Initial statistical backbone |
| New games, schedules and corrected box scores | Live ESPN requests, through a validated Python adapter/client | Verify historical/live field parity and deploy-host access |
| Historical and current betting quotes | Existing The Odds API account | Separate odds adapter; declared pregame snapshot policy |
| Richer lineup/possession research | Optional NCAA-derived SportsDataverse releases | Later enrichment; different IDs and reconstruction checks |
| Specific missing records or alternative API access | Optional CBBD | Targeted fallback, not a mandatory duplicate ingestion system |

**KenPom is excluded for now.** Do not add its subscription, adapter, ratings, or benchmarks to the initial dependency set. Kaggle is also outside the initial build. Commercial-product planning is not the purpose of this research phase.

## Understand the SportsDataverse repositories

SportsDataverse is the umbrella ecosystem. **hoopR** is the R basketball interface; **sportsdataverse-py** is a sibling Python interface. Producer repositories collect/process data, and `sportsdataverse-data` publishes release assets. Release loaders read prebuilt files; live provider functions call their upstream services. Neither language package is itself a hosted SQL database. [1] [2] [3]

```text
ESPN -> collection/processing -> published season files -> Python ingestion
  \-----------------------------------------------> live Python refresh

NCAA -> separate producer/release family -> optional enrichment
The Odds API -> separate historical/current quote ingestion
```

For repeated training/replay, store the selected records in GameFlow's own source/canonical tables and query those. Do not repeatedly download full seasons for every prediction. The initial implementation uses direct Parquet and small direct-HTTP Python adapters, avoiding an R or beta-SDK runtime dependency; a later SDK adapter must preserve fixture parity. Do not assume identically named fields or identifiers from different producers are interchangeable. A separate reviewed season-team classification manifest is required for D1 eligibility; scoreboard grouping alone is insufficient.

## Initial collection scope

- Five completed seasons: **2021-22, 2022-23, 2023-24, 2024-25, 2025-26**.
- Ongoing **2026-27** updates, with season labels explicitly mapped to ending years where the provider uses that convention.
- Start with schedules and team/player box scores; add reliable membership/role context where available.
- Collect only game-level odds markets needed by the initial spread/total/moneyline evaluation, not college player props.
- Add earlier feature warm-up history only where necessary. Collection depth and model-training window are different decisions.

This is a target, not a claim that these seasons are already ingested or that all fields/books are complete. The v1 design selects verified D1-versus-D1 non-exhibition games, supported postseason/neutral sites, and T-60 decisions with a T-5 closing reference. Season-specific classification evidence, book lists and execution budgets still require verification/configuration before acquisition.

## Historical statistics: verified evidence and limitations

The relevant public releases are the [schedule][4], [team-box][5], [player-box][6] and [roster][7] collections. Their schemas expose game/team/athlete identifiers and basic game/player statistics. Python can read the published Parquet files without introducing R into GameFlow. [1] [2] [3]

A bounded 2026-season sample was downloaded and independently rechecked during this discussion:

| Check | Observed result |
|---|---:|
| Schedule rows | 6,318 |
| Completed games in that schedule | 6,300 |
| Completed games with player-box rows | 6,300 |
| Completed games with team-box rows | 6,299 |
| Player-game rows / unique athletes | 196,876 / 12,481 |
| Duplicate valid player-game-team keys | 0 |
| Duplicate team-game keys | 0 |
| Missing athlete-ID rows | 11 |

Missing player minutes matched the DNP flag in that sample; null minutes are not automatically missing-source failures. The missing team box was game `401823003`, Youngstown State versus Penn State Shenango, dated 2025-11-10. These counts compare to that provider's schedule, not a separately certified bettable-game universe. [4] [5] [6]

A listed historical season is not proof of useful coverage. For example, the producer's player-box documentation lists only 26 rows in 2003 and 289 in 2004. Roster-file history is narrower than player-box history. [8] [9]

### Historical roster and availability caution

The player-core documentation warns that `current_team_id` is today's team and that mutable biography fields are overwritten; a season label may indicate participation rather than the vintage of the biography. Use historical game/team associations and prior appearances to construct role features, not today's profile joined onto old games.[10]

A complete historical pregame injury/availability feed was **not** established by this research. Actual target-game starters, DNP status and minutes are outcomes, not automatically available pregame features. Season membership, event time, publication time and data-revision time must remain distinct.

## Daily refresh is feasible, but production is unverified

On 2026-11-03, read-only requests from the local Windows environment returned:

- The 2026-11-02 ESPN scoreboard: 182 events marked final.
- The 2026-11-03 ESPN scoreboard: 29 events marked scheduled.
- Game `401906769`, Miami at Florida: both team boxes and both player-stat groups, including minutes, shooting, rebounds, assists and turnovers.

The requests returned HTTP 200. This verifies a small **local upstream HTTP** path, not an installed-wrapper integration, all-game completeness, sustained uptime, or access from Railway/the eventual deployment host. [11] [12] [13]

Proposed daily behavior: fetch schedules/results, acquire newly completed or corrected game records, upsert by verified keys, rebuild rolling features/team states, and check freshness before inference. Retain raw retrieval metadata, retry bounded transient failures, and block affected predictions on incomplete inputs. [Layer 7](layers/07-daily-orchestration-and-data-quality.md) specifies the initial three-day correction lookback, disabled schedules, leases, request ceilings and deployment acceptance.

## Odds source

Keep The Odds API as the authority for timestamped market inputs and evaluation prices. NCAAB featured-market history is advertised from mid-2020; the general archive documents ten-minute snapshots initially and five-minute snapshots from September 2022. Historical selection returns a snapshot equal to or earlier than the requested time. [14] [15]

The owner's account, historical entitlement, credits and bookmaker coverage were not inspected. No paid requests were made. Establish the actual book/date coverage and collection cost before an owner-launched historical backfill.

Preserve requested time, returned snapshot time, provider update times where present, game start, bookmaker, market, line and price. Match ESPN/NCAA games to odds events through a verified ID map. A game date or an undated embedded spread is not a substitute for a pregame quote timestamp.

## Optional sources, not initial blockers

### NCAA-derived SportsDataverse releases

A separate producer publishes reconstructed lineups, ten-player matchup stints, possessions and shots. Keep NCAA-native IDs and ESPN bridge IDs distinct.[16]

The published coverage index was parsed during research: its 2026 denominator contained 6,300 games, with lineup-record presence of 99.95% and matchup/possession-record presence of 99.86%. In 2010, matchup/possession presence was only 3.11%. These are record-presence measures, not proof of reconstruction accuracy or pregame availability.[17]

A roster encoder can start with prior player-game logs; full PBP/lineup ingestion is not an initial prerequisite.

### CBBD

CBBD is a viable structured API/export alternative with player/team boxes and season membership information. Its documented regular lineup history begins in 2024, and coverage varies by field/season. [18] [19]

Use it only for a concrete gap or a later source comparison. Its season-level rating endpoints are not dated pregame snapshots, and its betting-line service exposes no per-quote snapshot history; neither should silently replace our point-in-time team states or The Odds API quotes. [20] [21]

## Source acceptance questions before implementation

1. Which game universe and dates are eligible, and which provider IDs define each entity?
2. Which fields have matching definitions and availability in historical files and live responses?
3. What information is genuinely known before the prediction, versus reconstructed after the game?
4. Which source corrections must update stored history, and how are dataset versions pinned?
5. Does the live path work from the intended execution host?
6. Which bookmakers and decision times have usable paired quotes and independent closing observations?

The [ingestion](layers/01-data-ingestion.md), [database/linking](layers/02-database-and-identity-linking.md), and [data-quality](layers/07-daily-orchestration-and-data-quality.md) specifications now define these contracts and acceptance checks. Remaining provider coverage, book choices and deploy-host evidence are explicit implementation/operation gates, not facts established by this research.

## Sources

The observations above are dated research evidence, not perpetual provider guarantees. Public-file samples and API probes were read-only; the references are the primary source locations used to verify them.

[1]: https://raw.githubusercontent.com/sportsdataverse/hoopR/main/README.md
[2]: https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/README.md
[3]: https://sportsdataverse-py.sportsdataverse.org/docs/mbb/reference/loaders
[4]: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules
[5]: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores
[6]: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores
[7]: https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters
[8]: https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-data/main/docs/datasets/player_box.md
[9]: https://raw.githubusercontent.com/sportsdataverse/hoopR-mbb-data/main/docs/datasets/rosters.md
[10]: https://hoopr.sportsdataverse.org/reference/load_mbb_player_core.html
[11]: https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates=20261102&groups=50&limit=1000
[12]: https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates=20261103&groups=50&limit=1000
[13]: https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event=401906769
[14]: https://the-odds-api.com/historical-odds-data/
[15]: https://the-odds-api.com/sports/ncaab-odds.html
[16]: https://raw.githubusercontent.com/sportsdataverse/ncaa-mbb-hoops-data/main/README.md
[17]: https://raw.githubusercontent.com/sportsdataverse/ncaa-mbb-hoops-data/main/mbb/ncaa_mbb_schedule_coverage.parquet
[18]: https://api.collegebasketballdata.com/data-availability
[19]: https://api.collegebasketballdata.com/api-docs.json
[20]: https://raw.githubusercontent.com/CFBD/cbb-api/main/src/app/ratings/service.ts
[21]: https://raw.githubusercontent.com/CFBD/cbb-api/main/src/app/lines/service.ts
