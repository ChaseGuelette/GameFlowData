# MLB Paper Trading P&L Queries

Execute these 3 SQL queries against Supabase (authenticated role) and return EXACT results:

## Query 1 - Overall P&L by stat (excluding batter_rbis)
```sql
SELECT
    stat_type,
    COUNT(*) as total_bets,
    SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN status = 'loss' THEN 1 ELSE 0 END) as losses,
    ROUND(SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as win_pct,
    ROUND(SUM(pnl)::numeric, 2) as total_pnl,
    ROUND(AVG(stake)::numeric, 2) as avg_stake,
    ROUND(MAX(stake)::numeric, 2) as max_stake
FROM mlb_paper_bets
WHERE status IN ('win', 'loss')
GROUP BY stat_type
ORDER BY total_pnl DESC;
```

## Query 2 - Daily P&L for non-RBI stats
```sql
SELECT
    game_date,
    COUNT(*) as bets,
    SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END) as wins,
    ROUND(SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as win_pct,
    ROUND(SUM(pnl)::numeric, 2) as daily_pnl
FROM mlb_paper_bets
WHERE status IN ('win', 'loss')
  AND stat_type != 'batter_rbis'
GROUP BY game_date
ORDER BY game_date;
```

## Query 3 - Overall totals with and without RBIs
```sql
SELECT
    CASE WHEN stat_type = 'batter_rbis' THEN 'batter_rbis' ELSE 'all_other_stats' END as category,
    COUNT(*) as total_bets,
    SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END) as wins,
    ROUND(SUM(CASE WHEN status = 'win' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as win_pct,
    ROUND(SUM(pnl)::numeric, 2) as total_pnl,
    ROUND(SUM(stake)::numeric, 2) as total_staked
FROM mlb_paper_bets
WHERE status IN ('win', 'loss')
GROUP BY CASE WHEN stat_type = 'batter_rbis' THEN 'batter_rbis' ELSE 'all_other_stats' END
ORDER BY category;
```

Return all results EXACTLY as they come back from the database. Do NOT modify column names, order, or structure.
