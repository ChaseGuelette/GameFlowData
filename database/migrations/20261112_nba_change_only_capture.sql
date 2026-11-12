-- Prepared only. Apply after parent metadata reconciliation and consumer rollout.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE public.nba_props_live_captures (
    capture_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_game_id text NOT NULL,
    commence_time timestamptz NOT NULL,
    home_team text NOT NULL,
    away_team text NOT NULL,
    requested_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    source_kind text NOT NULL CHECK (source_kind = 'live'),
    policy_version text NOT NULL CHECK (policy_version = 'live_change_v1'),
    scope jsonb NOT NULL CHECK (jsonb_typeof(scope) = 'object'),
    groups jsonb NOT NULL CHECK (jsonb_typeof(groups) = 'array' AND jsonb_array_length(groups) <= 20000),
    CHECK (requested_at <= received_at AND received_at < commence_time),
    CHECK (octet_length(groups::text) <= 16000000),
    UNIQUE (api_game_id, received_at)
);
CREATE INDEX nba_props_live_captures_event_latest
    ON public.nba_props_live_captures (api_game_id, received_at DESC, capture_id DESC);
-- Deliberately no quote-reference FKs: existing 21-day quote pruning must not block.
-- Retention owner must prune receipts in the same expired-event window, after T60
-- materialization. Keep receipts/tombstones while their event's quotes can serve;
-- deleting the last empty receipt early would incorrectly reactivate fallback.
ALTER TABLE public.nba_props_live_captures ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.nba_props_live_captures FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT, INSERT ON public.nba_props_live_captures TO postgres;
GRANT USAGE, SELECT ON SEQUENCE public.nba_props_live_captures_capture_id_seq TO postgres;

CREATE VIEW public.nba_player_props_current WITH (security_invoker = true) AS
WITH latest AS (
    SELECT DISTINCT ON (api_game_id) * FROM public.nba_props_live_captures
    ORDER BY api_game_id, received_at DESC, capture_id DESC
)
SELECT q.*, c.capture_id, c.received_at AS confirmed_at,
    (g.value->>'market_last_update')::timestamptz AS confirmation_market_last_update,
    (g.value->>'bookmaker_last_update')::timestamptz AS confirmation_bookmaker_last_update
FROM latest c
CROSS JOIN LATERAL jsonb_array_elements(c.groups) g(value)
CROSS JOIN LATERAL jsonb_array_elements_text(g.value->'ids') member(id)
JOIN public.nba_player_props_inference q ON q.staging_id = member.id::bigint
    AND q.api_game_id = c.api_game_id AND q.commence_time = c.commence_time
    AND q.home_team = c.home_team AND q.away_team = c.away_team
WHERE c.received_at < c.commence_time
UNION ALL
SELECT q.*, NULL::bigint, q.observed_at, q.market_last_update, q.bookmaker_last_update
FROM public.nba_player_props_inference q
WHERE NOT EXISTS (SELECT 1 FROM public.nba_props_live_captures c WHERE c.api_game_id=q.api_game_id);
REVOKE ALL ON public.nba_player_props_current FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT ON public.nba_player_props_current TO postgres;

-- Private coherent serving projection. Original clocks remain on the current
-- view; snapshot_time here is explicitly serving confirmation, not a quote edit.
CREATE VIEW public.nba_sportsbook_current_pairs WITH (security_invoker = true) AS
WITH paired AS (
    SELECT q.player_id, q.game_id, q.bookmaker, q.market_key, q.line,
        q.api_game_id, q.commence_time, q.source_snapshot_time, q.confirmed_at,
        max(q.odds_american) FILTER (WHERE q.outcome_label='Over')::integer AS over_odds,
        max(q.odds_american) FILTER (WHERE q.outcome_label='Under')::integer AS under_odds
    FROM public.nba_player_props_current q
    WHERE q.player_id IS NOT NULL AND q.game_id IS NOT NULL
      AND q.bookmaker NOT IN ('prizepicks','underdog','pick6','betr_us_dfs','fliff')
      AND q.confirmed_at > now() - interval '48 hours' AND q.confirmed_at <= now()
      AND q.confirmed_at < q.commence_time
      AND q.source_snapshot_time <= now() AND q.observed_at <= now()
      AND q.source_snapshot_time < q.commence_time AND q.observed_at < q.commence_time
      AND (q.market_last_update IS NULL OR q.market_last_update <= now() AND q.market_last_update < q.commence_time)
      AND (q.bookmaker_last_update IS NULL OR q.bookmaker_last_update <= now() AND q.bookmaker_last_update < q.commence_time)
      AND (q.confirmation_market_last_update IS NULL OR q.confirmation_market_last_update <= q.confirmed_at)
      AND (q.confirmation_bookmaker_last_update IS NULL OR q.confirmation_bookmaker_last_update <= q.confirmed_at)
    GROUP BY q.player_id,q.game_id,q.bookmaker,q.market_key,q.line,q.api_game_id,
        q.commence_time,q.source_snapshot_time,q.confirmed_at
    HAVING count(*)=2 AND count(DISTINCT q.outcome_label)=2
       AND count(*) FILTER (WHERE q.outcome_label IN ('Over','Under'))=2
), ranked AS (
    SELECT *, row_number() OVER (PARTITION BY player_id,game_id,bookmaker,market_key,line
        ORDER BY confirmed_at DESC,source_snapshot_time DESC) AS rn FROM paired
)
SELECT player_id,game_id,bookmaker,market_key,line,over_odds,under_odds,
    confirmed_at AS snapshot_time,commence_time FROM ranked WHERE rn=1;
REVOKE ALL ON public.nba_sportsbook_current_pairs FROM PUBLIC, anon, authenticated, service_role;
GRANT SELECT ON public.nba_sportsbook_current_pairs TO postgres;

-- These three existing, scoped RPCs are the ONLY browser exposure. No capture
-- group/state payload or full immutable evidence JSON is sent to the browser.
CREATE OR REPLACE FUNCTION public.get_game_commence_times(p_game_ids text[])
RETURNS TABLE(game_id text, commence_time timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO public, pg_temp
SET statement_timeout TO '30s' AS $function$
    SELECT requested.game_id, quote.commence_time
    FROM unnest(p_game_ids) requested(game_id)
    CROSS JOIN LATERAL (
        SELECT p.commence_time FROM public.nba_player_props_current p
        WHERE p.game_id=requested.game_id AND p.confirmed_at > now()-interval '48 hours'
        ORDER BY p.confirmed_at DESC,p.staging_id DESC LIMIT 1
    ) quote;
$function$;
CREATE OR REPLACE FUNCTION public.get_sportsbook_lines(target_date date)
RETURNS TABLE(player_id bigint, game_id text, bookmaker text, market_key text,
    line numeric, over_odds integer, under_odds integer, snapshot_time timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO public, pg_temp
SET statement_timeout TO '30s' AS $function$
    SELECT p.player_id,p.game_id,p.bookmaker,p.market_key,p.line,p.over_odds,p.under_odds,p.snapshot_time
    FROM public.nba_sportsbook_current_pairs p
    WHERE p.commence_time >= (target_date::timestamp AT TIME ZONE 'America/New_York')
      AND p.commence_time < ((target_date+1)::timestamp AT TIME ZONE 'America/New_York');
$function$;
CREATE OR REPLACE FUNCTION public.get_sportsbook_lines_by_games(p_game_ids text[])
RETURNS TABLE(player_id bigint, game_id text, bookmaker text, market_key text,
    line numeric, over_odds integer, under_odds integer, snapshot_time timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO public, pg_temp
SET statement_timeout TO '30s' AS $function$
    SELECT p.player_id,p.game_id,p.bookmaker,p.market_key,p.line,p.over_odds,p.under_odds,p.snapshot_time
    FROM public.nba_sportsbook_current_pairs p WHERE p.game_id=ANY(p_game_ids);
$function$;
REVOKE ALL ON FUNCTION public.get_game_commence_times(text[]) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.get_sportsbook_lines(date) FROM PUBLIC, anon;
REVOKE ALL ON FUNCTION public.get_sportsbook_lines_by_games(text[]) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.get_game_commence_times(text[]) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_sportsbook_lines(date) TO authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_sportsbook_lines_by_games(text[]) TO authenticated, service_role;

-- Evidence only; t60_v1 still ranks the same expanded provider responses.
-- No historical rewrite and no new policy version/eligibility change.
ALTER TABLE public.nba_player_prop_feature_lines
    ADD COLUMN capture_id bigint,
    ADD COLUMN capture_received_at timestamptz,
    ADD COLUMN capture_market_last_update timestamptz,
    ADD COLUMN capture_bookmaker_last_update timestamptz;
ALTER TABLE public.nba_player_prop_feature_lines ADD CONSTRAINT nba_feature_capture_pregame CHECK (
    (capture_id IS NULL AND capture_received_at IS NULL
        AND capture_market_last_update IS NULL AND capture_bookmaker_last_update IS NULL)
    OR (capture_id IS NOT NULL AND capture_received_at IS NOT NULL
        AND source_kind='live' AND source_snapshot_time <= capture_received_at
        AND capture_received_at <= decision_time
        AND (capture_market_last_update IS NULL OR capture_market_last_update <= decision_time)
        AND (capture_bookmaker_last_update IS NULL OR capture_bookmaker_last_update <= decision_time))
);
COMMIT;
