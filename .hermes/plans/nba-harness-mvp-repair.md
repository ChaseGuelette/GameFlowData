# NBA harness MVP repair scope

Owner-approved scope: correct drawdown, missing-quantile calibration and Sharpe; replace giant raw-props reads with the compact props source. No new model architecture, unrelated metric suite, schema migration, paid calls, backfill or full replay/training.

Coordinator implementation:
- Shared metrics: dated daily P&L, peak-relative daily drawdown, calendar-daily equity returns (365 annualization), per-quantile finite observation counts.
- Compact quote reader: `nba_player_prop_feature_lines` selects the exact frozen T-60 line/book. Historical prices must match the existing cached response; live prices must match `nba_player_props_inference`. No synthetic price or giant raw-table fallback.
- Reuse direct/prefetch harness and historical-source/quote-adapter paths. Keep policy/context/route gates. Include Pinnacle in existing allowlists and the new reader in source fingerprints.

Verification/stopping rule: affected existing tests, focused synthetic metric probes, and one real local cached-price pilot through source consumers. No additional reviewer rounds or full-history certification. Preserve concurrent injury/frontend work. No commit/push unless requested.
