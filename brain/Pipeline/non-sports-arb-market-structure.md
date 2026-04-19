# Non-Sports Arb: Market Structure Learnings

> Verified: April 19, 2026. Source: end-to-end diagnostic dry-run + DB inspection.

## TL;DR

Kalshi and Polymarket use **fundamentally different question formats** for many categories.
Matching only works when both platforms ask the same *type* of question (threshold level + direction).
Most Kalshi crypto and many FED/CPI questions do NOT meet this bar.

---

## 1. Kalshi Crypto Series — Range Brackets, Not Threshold Binaries

**Series**: KXBTC, KXBTCD, KXETH, KXETHD, KXDOGE, KXXRP

**What they look like**:
```
Ticker: KXBTC-26APR1909-B75050
Title:  "Bitcoin price range on Apr 19, 2026?"
```

The title contains **no direction word** (above/below) and **no dollar amount**. The price ($75,050) is
encoded in the ticker suffix only (`B75050`). Each market covers a narrow bracket (e.g., $75,050–$75,150).
You're betting on whether BTC lands in that specific $100 range at a specific time.

**Polymarket BTC markets look like**:
```
"Will the price of Bitcoin be above $86,000 on April 21?"
```

These are **binary threshold** markets — will the price be above/below a single level?

**Why they can't match**: Different financial instruments entirely.
- Kalshi: "Is BTC in the range $75,050–$75,150 at 9am?" (range bracket)
- Poly: "Is BTC above $80,000 on Apr 21?" (threshold binary)

Even if you lower the matching threshold, these will never correctly pair because they're asking
different questions. Matching them would produce fake arbs.

**Decision**: Removed from `KALSHI_NON_SPORTS_SERIES` and `KALSHI_SERIES_POLY_CONFIG`.
Do NOT re-add. Wastes ~90 seconds per arb scan, ~100k rows of DB, 0 valid matches.
Verified: 0/1,034 crypto Kalshi markets extracted successfully in structured extractor.

---

## 2. FED Rate Markets — Level vs. Change Mismatch

**Kalshi KXFED format** (threshold level):
```
Ticker: KXFED-26APR-T3P50
Title:  "Will the federal funds rate target be above 3.50% after the April 2026 meeting?"
```
Always asks: "will the rate be at/above level X after meeting Y?" — **level + date**.

**Polymarket FED formats** (two types):

*Type A — Change-based* (majority):
```
"Will the Fed decrease interest rates by 25 bps after April 2026?"
"Will the Fed cut rates at the May meeting?"
```
No specific rate level, just direction of change. **Cannot structurally match Kalshi.**
The `_parse_pct()` extractor won't find a percentage (bps ≠ %). Falls to SequenceMatcher fallback,
which also fails because the question wording is too different from Kalshi's level-based phrasing.

*Type B — Level-based* (minority, very high liquidity):
```
"Will the upper bound of the target federal funds rate be 2.75% at the end of 2026?"
```
Contains a specific rate level — **can potentially match Kalshi**. BUT: the direction word is often
missing ("be 2.75%" doesn't say above/below), so `_parse_direction()` returns None → extraction fails.

**Consequence**: Of 169 Poly FED markets scanned, only a small subset are level+direction questions
that match Kalshi's format. This is why FED produces few matches despite high market count on both sides.

**Future improvement**: Add "be X%" as an implicit ABOVE direction for FED/CPI level markets, or
add Type B Poly markets to a manual mapping table.

---

## 3. CPI Markets — Month-Specific Threshold, Good Format Alignment

**Kalshi KXCPI format**:
```
Ticker: KXCPI-26JUN-T0.2
Title:  "Will CPI rise more than 0.2% in June 2026?"
```
Clear: threshold (0.2%), direction (more than = ABOVE), period (June 2026). Extractor handles these.

**Polymarket CPI format** (when it matches):
```
"Will US CPI rise more than 0.2% in June 2026?"
```
When Poly phrases it the same way, this pairs perfectly.

**Why matches are still sparse**: Poly has only ~12 CPI-keyword markets in economics/politics.
They tend to cover a few popular thresholds (0.2%, 0.3%) for 1-2 months ahead. Kalshi has
granular markets at 0.0%, 0.1%, 0.2%, 0.3%, 0.4%, 0.5%+ for 3-4 months. Most Kalshi CPI
price points have no corresponding Poly market.

---

## 4. GDP Markets — Best-Aligned Format

**Kalshi KXGDP format**:
```
Ticker: KXGDP-26APR30-T3P5
Title:  "Will real GDP increase by more than 3.5% in Q1 2026?"
```

**Polymarket GDP format**:
```
"Will US GDP growth in Q1 2026 be greater than 3.5%?"
```

Structurally similar. Quarter+year parsing works on both sides. GDP produces the most reliable
matches of the three macro series.

**Limitation**: Kalshi only has ~12 KXGDP markets (Q1-Q4 2026 at various thresholds).
Poly has 95 GDP-keyword markets but many are for non-US countries (China, Eurozone, UK).
Country filtering could improve match precision here.

---

## 5. Poly Category Misclassification

Polymarket categorizes markets inconsistently. Verified examples:
- **International inflation markets** (Argentina, Brazil, Mexico CPI) appear in the `sports` category
- **FED press conference word-count markets** ("Will Powell say 'inflation' 60+ times?") appear in `other`
- **Level-based FED rate markets** are correctly in `economics`

**Consequence**: Adding `"other"` to poly_categories for FED/CPI/GDP gains ~45 markets,
but those markets are mostly word-count bets and miscategorized content — not rate-level
markets that can match Kalshi. Audited Apr 19 2026; not worth adding.

---

## 6. Current Matching Ceiling (as of Apr 19 2026)

| Series | Kalshi Markets | Poly Markets Scanned | Expected Match Ceiling | Actual Matches |
|--------|---------------|---------------------|----------------------|----------------|
| KXCPI  | 68            | ~12                 | ~5-10                | included in 15 |
| KXFED  | 120           | ~169                | ~5-15                | included in 15 |
| KXGDP  | 12            | ~95                 | ~5-10                | included in 15 |
| **Total** | **200**    | **~276**            | **~15-35**           | **15**         |

**15 matches is near the realistic ceiling** for the current Kalshi macro series inventory.
To grow match count substantially, new Kalshi series must be added (politics, geopolitics).

---

## 7. Full Kalshi Non-Sports Universe (Audited Apr 19 2026)

Discovered via `list_all_events(status='open')` — 5,366 open events, 2,267 unique non-sports series.

| Category | Kalshi Series | Poly Equivalent | Market Type | Matchable? |
|----------|--------------|-----------------|-------------|------------|
| Elections | 648 | politics (3,645) | Binary outcome | ✅ High priority |
| Politics | 322 | politics + other | Binary outcome | ✅ High priority |
| Economics | 177 | economics + other | Mixed — see below | ⚠️ Partial |
| Companies | 82 | other (tech) | Binary outcome | ✅ Medium priority |
| Climate/Weather | 67 | weather (2,696) | Range bracket | ❌ Range brackets |
| Science/Tech | 60 | other | Binary outcome | ✅ Medium priority |
| Financials | 52 | economics/other | Mixed | ⚠️ Partial |
| Entertainment | 333 | culture (348) | Binary outcome | ✅ Low priority |
| Commodities | 23 | other | Range bracket | ❌ Range brackets |
| Mentions | 41 | — | Text frequency | ❌ No Poly equivalent |
| Crypto (non-BTC/ETH) | 27 | crypto | Range bracket | ❌ Range brackets |
| Social | 17 | — | Niche | ❌ Too niche |
| World | 11 | other | Binary outcome | ✅ Low priority |
| Health | 7 | — | Niche | ❌ Too niche |

### Why Commodities/Weather/New-Crypto Are Also Range Brackets

Verified by title inspection:
- `KXGOLDD` → "Gold price on Apr 20, 2026 at 5pm EDT?" — same range bracket format as KXBTC
- `KXBRENTD` → "Brent crude oil price on Apr 20, 2026 at 5pm EDT?" — range bracket
- `KXHIGHCHI` → "Highest temperature in Chicago on Apr 20, 2026?" — range bracket
- `KXBNB` → "BNB price range on Apr 24, 2026?" — explicitly says "price range"

**Rule of thumb**: Any Kalshi series asking "what will X be on date Y?" is a range bracket.
Any series asking "will X happen?" is a binary event. Only binary events can match Polymarket.

### Economics (177 series) — Mixed

Beyond KXFED/KXCPI/KXGDP (already covered), the economics series include:
- `KXAAAGASM` — "Gas prices in the US in Apr 2026?" → range bracket ❌
- `KX3MTBILL` — "UST par yield curve (3M) at end of Q2 2026" → range bracket ❌
- `KXAAAGASMAXCA` — "How high will gas prices in California get this year?" → likely level threshold, potentially matchable but no clear Poly equivalent

Most Economics series beyond the core 3 are range brackets or too niche for Poly matching.

---

## 8. Elections + Politics Expansion — IMPLEMENTED Apr 19 2026

### Approach: Dynamic Category-Scrape Mode
Instead of hardcoding 970+ series, `list_all_events(status="open")` discovers all open series at scrape time, grouped by Kalshi category. Markets stored with `series_ticker = config_key` (`"_CAT_ELECTIONS"` or `"_CAT_POLITICS"`) so the matcher groups them under one config entry.

### Results
- **Scraper**: 649 elections series + 332 politics series = **6,239 new markets** (vs ~200 macro)
- **Arb scan runtime**: ~2 min (was 30+ min before volume/liquidity filters)
- **Matched pairs after disambiguation**: 144 (down from 364 raw)

### Volume/Liquidity Filter Decisions
Poly politics is extremely top-heavy — `liq > 1000` still returns 3,544 markets; only `liq > 50,000` reduces to 283.

| Config | min_kalshi_volume | min_poly_liquidity | Kalshi | Poly |
|--------|------------------|--------------------|--------|------|
| `_CAT_ELECTIONS` | 5,000 | 50,000 | ~1,131 | ~276 |
| `_CAT_POLITICS` | 500 | 5,000 | ~1,252 | ~662 |

### Candidate Disambiguation Logic
Korean and Spanish-romanized names share enough characters to score ≥ 0.50 (e.g. "Chong Won-o" vs "Kang Hoon-sik" = 0.50). Three-layer check in fuzzy fallback:
1. Both questions have "Will X win?" → require name similarity ≥ **0.65**
2. One side has "Will X win?", other doesn't → structural mismatch → reject
3. Neither has candidate → proceed with fuzzy score

### Known Remaining False Positives
1. **Same-race, different placement**: "Roberto finish 2nd" vs "Roberto Chiabra finish 2nd" — same "Roberto" in same race, different specific candidates. Fix: extend check to handle "finish Nth" patterns.
2. **Different verb, same person**: "Rubio receive pardon" vs "Rubio announce presidential run" — passes candidate check. Fix: action verb extraction.
3. **GDP country mismatch**: Structured extractor scores US GDP vs Mexico/Eurozone GDP as 1.0 (ignores country). Fix: add country field to `non_sports_extractor.py`.

---

## 9. Next Expansion: Finance, Entertainment, SCOTUS

From the full Kalshi universe audit (Section 7), remaining high-value matchable categories:

| Category | Kalshi Series | Poly Markets | Priority |
|----------|--------------|--------------|----------|
| Finance/Markets | ~52 | economics/other | **High** — S&P 500, Nasdaq, VIX binary thresholds |
| Entertainment | 333 | culture (348) | Medium — Oscars, Emmys, box office |
| SCOTUS/Law | ~20 | politics/other | Medium — binary case outcomes |
| Science/Tech | ~60 | other | Medium — AI milestones, SpaceX |
| Companies | ~82 | other | Low — CEO exits, acquisitions |

**Implementation**: Same `mode: "category_scrape"` pattern. Add `_CAT_FINANCE`, `_CAT_ENTERTAINMENT`, `_CAT_SCOTUS` entries to `KALSHI_NON_SPORTS_SERIES` and `KALSHI_SERIES_POLY_CONFIG`.

**Status**: Not yet implemented. Scoped Apr 19 2026.
