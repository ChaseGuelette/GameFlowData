# Kalshi Sportsbook-Reference Ranker Notes

Date: 2026-05-24
Status: design note / interpretation guide

## Purpose

This note captures how the MLB CLV ranking result should translate to a Kalshi-primary betting system.

The current sportsbook diagnostic found a meaningful ranker:

```text
selected_vs_candidate_mean_gap = selected_edge - candidate_mean_edge
```

For sportsbook execution, this asks whether the book selected by the strategy gave a better model edge than the average available candidate book for the same player/game/stat/line at the same decision time.

For Kalshi execution, the same concept should be reframed. Kalshi is the execution venue; sportsbooks are reference markets, not candidate execution venues.

## Relevant invariants

- Probabilities must come from the model / empirical sample distribution. Do not introduce Gaussian CDF shortcuts.
- Raw model edge has not earned Kelly or aggressive sizing by itself.
- Positive average CLV is not enough if ranking quality is unconfirmed.
- Kalshi policy remains NO-only unless explicitly running a controlled experiment.
- Unsupported Kalshi stat types remain blocked.
- Do not “fix” the Q10 miscalibration globally; this has repeatedly removed edge.

## Sportsbook ranker recap

For sportsbook bets:

```text
selected_edge = model_prob - selected_book_implied_prob
candidate_edge_i = model_prob - candidate_book_i_implied_prob
candidate_mean_edge = mean(candidate_edge_i)
selected_vs_candidate_mean_gap = selected_edge - candidate_mean_edge
```

Because `model_prob` appears in both the selected and candidate terms, the gap is mostly a price-quality signal:

```text
selected_vs_candidate_mean_gap
= (model_prob - selected_implied_prob) - mean(model_prob - candidate_implied_prob)
= mean(candidate_implied_prob) - selected_implied_prob
```

Interpretation:

- Positive gap: selected book is cheaper / better than the candidate market average.
- Near-zero gap: selected book is in line with the market.
- Negative gap: selected book is worse than the candidate market average.

This is why sharp books such as DraftKings, FanDuel, or Pinnacle can be more valuable as reference prices than as the final execution venue. A soft book with a better selected price can be the correct place to bet if the model and CLV validation support it.

## Kalshi reframing

For Kalshi, do not treat sportsbooks as alternate places to place the same bet. Treat them as reference markets.

Kalshi-primary selection should ask:

1. Does the model like the Kalshi executable price after fees/spread?
2. Does sportsbook consensus agree enough with the model direction?
3. Is Kalshi cheap relative to sportsbook consensus?
4. Is the Kalshi line actually comparable to the sportsbook line?
5. Is there enough liquidity and acceptable spread?

The analogous ranker should be named something like:

```text
kalshi_edge_advantage_vs_sportsbook_mean
```

or:

```text
kalshi_vs_sportsbook_reference_gap
```

## Kalshi ranker formula

For a NO bet:

```text
kalshi_no_edge = model_prob_no - kalshi_no_cost_probability
sportsbook_reference_edge = model_prob_no - sportsbook_consensus_no_vig_prob
kalshi_vs_sportsbook_reference_gap = kalshi_no_edge - sportsbook_reference_edge
```

This simplifies to:

```text
kalshi_vs_sportsbook_reference_gap
= sportsbook_consensus_no_vig_prob - kalshi_no_cost_probability
```

So the ranker is really measuring:

```text
How much cheaper is Kalshi NO than the sportsbook reference fair probability?
```

Example:

```text
Model NO probability:              0.52
Kalshi executable NO cost:          0.38
Sportsbook no-vig NO probability:   0.47

kalshi_no_edge = 0.52 - 0.38 = +0.14
sportsbook_reference_edge = 0.52 - 0.47 = +0.05
kalshi_vs_sportsbook_reference_gap = +0.14 - +0.05 = +0.09

Equivalent shortcut:
0.47 - 0.38 = +0.09
```

Interpretation:

Kalshi is selling NO about 9 cents cheaper than sportsbook consensus fair value.

## Decision matrix

Use both model edge and sportsbook-reference gap. They answer different questions.

| Model edge | Sportsbook-reference gap | Interpretation | Default action |
|---|---:|---|---|
| Positive | Positive | Model likes the bet and sportsbook consensus says Kalshi is cheap | Best candidate |
| Positive | Near zero | Model likes the bet, but Kalshi is not obviously cheap vs books | Possible, lower confidence |
| Positive | Negative | Model likes the bet, but sportsbooks imply Kalshi is expensive | Dangerous; requires historical proof |
| Negative | Positive | Kalshi is cheap vs books, but model disagrees | Skip unless explicitly testing market-arb logic |
| Negative | Negative | No model edge and no market-relative bargain | Skip |

## Required Kalshi-specific inputs

A Kalshi-primary ranker should include at least:

- `kalshi_fee_adjusted_edge`
- `kalshi_vs_sportsbook_consensus_gap`
- `sportsbook_alignment_score`
- `line_alignment_flag`
- `bid_ask_spread`
- `available_size` / liquidity
- `volume` / `open_interest`
- supported-stat flag
- side flag, with NO-only enforced by default

## Line alignment is mandatory

A sportsbook reference is only useful if it refers to the same threshold.

Safe comparison:

- Same player
- Same game/date
- Same stat
- Same line / threshold
- Same side semantics

Dangerous comparison:

- Kalshi line 1.5 vs sportsbook line 0.5
- Different stat definitions
- Missing or stale sportsbook line
- Sportsbook line available only after the Kalshi decision time

If lines do not match, either skip the candidate or apply an explicit alignment penalty. Do not silently compare unlike thresholds.

## Is this market decorrelation?

Partly, but the phrase can mislead.

The sportsbook ranker is not simply rewarding disagreement from the market. It is rewarding favorable execution price relative to the market, conditional on the model already identifying the bet as positive edge.

Better wording:

```text
market-relative price advantage
```

or:

```text
execution price advantage versus reference market
```

A large positive gap means the selected venue is away from the market in our favor. That can be a real opportunity, but it can also be stale data, line mismatch, bad liquidity, or a book-specific artifact. CLV/ROI validation is what separates useful mispricing from noise.

## Does this enable feature iteration?

Yes, but narrowly.

A meaningful CLV ranker means the selection policy is no longer completely unresolved. It gives us a measurable quality axis that can be used as a gate when comparing model or feature variants.

However, it does not prove raw model edge is calibrated for sizing, and it does not by itself justify Kelly.

Feature iteration can resume if variants are judged on:

- flat ROI
- mean CLV and CLV CI low
- ranker stability / Spearman CI low
- filtered ROI after applying the ranker
- drawdown
- book/venue concentration
- line-alignment coverage

Feature work should not optimize only for raw backtest ROI or raw model edge.

## Will this make bets more profitable, or just filter bad bets?

Initially, treat it as a bad-bet filter.

The first validated use is to remove bets where the selected venue is worse than the reference market or where Kalshi is expensive versus sportsbook consensus.

If filtered buckets show monotonic improvement in ROI, CLV, drawdown, and stability, then the same signal may later support confidence tiers or capped sizing.

The progression should be:

1. Filter bad bets.
2. Verify filtered ROI/CLV improves out of sample.
3. Verify monotonic buckets, not just top-vs-bottom separation.
4. Only then test confidence tiers / capped sizing.
5. Only much later consider Kelly-like sizing, and only if raw or transformed edge magnitude is validated as calibrated.

## Are filtering and confidence ranking the same idea?

They are related but not identical.

Filtering asks:

```text
Should this bet be allowed at all?
```

Ranking asks:

```text
Among allowed bets, which deserve more confidence or size?
```

A ranker can be good enough for filtering before it is good enough for sizing.

Example:

- Bottom bucket is clearly bad.
- Middle and upper buckets are all similarly good.

That supports a filter, not tiered sizing.

To support sizing, buckets should show stable monotonic improvement and the size proxy should map reliably to realized CLV/ROI.

## Recommended next experiments

### Sportsbook strategy

Run filtered diagnostics over:

```text
selected_vs_candidate_mean_gap >= 0.000
selected_vs_candidate_mean_gap >= 0.005
selected_vs_candidate_mean_gap >= 0.010
selected_vs_candidate_mean_gap >= 0.015
```

Measure:

- bet count
- ROI
- mean CLV / CI low
- Spearman CI low
- drawdown
- book concentration
- ESPNBet / ProphetX concentration

### Kalshi strategy

Build a Kalshi-specific reference ranker:

```text
kalshi_fee_adjusted_edge = model_prob_no - all_in_kalshi_no_cost
kalshi_vs_sportsbook_reference_gap = sportsbook_consensus_no_vig_prob - all_in_kalshi_no_cost
```

Then bucket by:

```text
kalshi_fee_adjusted_edge
kalshi_vs_sportsbook_reference_gap
line_alignment_flag
spread/liquidity buckets
```

Initial gate candidate:

```text
kalshi_fee_adjusted_edge > 0
kalshi_vs_sportsbook_reference_gap >= 0
line_alignment_flag = true
NO side only
supported stat only
```

Then test stricter thresholds only after preserving enough sample.

## Bottom line

The current ranker is meaningful because it found that venue-relative price quality predicts CLV better than raw model edge.

For sportsbooks, it can identify whether the selected book was better than the candidate market.

For Kalshi, the same idea should be reframed as whether Kalshi is cheap versus sportsbook consensus while still passing the model edge gate.

Use it first as a filter. Treat confidence tiers and sizing as later validation stages, not immediate conclusions.
