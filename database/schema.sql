-- ============================================================================
-- NBA DATABASE SCHEMA - IMPORTANT NOTES
-- ============================================================================
--
-- GAME_ID FORMAT (10 characters):
--   Position 1-2:  "00" (standard padding)
--   Position 3-4:  Game type
--                    "22" = Regular Season
--                    "42" = Playoffs
--                    "52" = All-Star
--   Position 5-6:  Season year (e.g., "25" = 2024-25 season)
--   Position 7-10: Game number
--
--   Example: "0022500369" = Regular Season, 2024-25, Game #369
--
-- WARNING - CHRONOLOGICAL SORTING:
--   When sorting games chronologically, ALWAYS use:
--     ORDER BY game_date DESC
--
--   NEVER use:
--     ORDER BY game_id DESC
--
--   Reason: game_id is a STRING. Playoff IDs (004x, 005x) sort AFTER
--   regular season (002x) alphabetically, even if they're from years ago.
--   Example: "0052100201" (2022 All-Star) > "0022501224" (2026 Regular)
--
-- SEASON_ID FORMAT:
--   "22024" = 2023-24 Regular Season
--   "22025" = 2024-25 Regular Season
--   "42024" = 2023-24 Playoffs
--   etc.
--
-- ============================================================================

create table public.league_priors_history (
  season_id text not null,
  position_group text not null,
  snapshot_date date not null,
  league_off_rtg numeric(6, 2) null,
  league_reb_per100 numeric(6, 2) null,
  league_ast_per100 numeric(6, 2) null,
  league_stl_per100 numeric(6, 2) null,
  league_blk_per100 numeric(6, 2) null,
  league_threes_per100 numeric(6, 2) null,
  league_tov_per100 numeric(6, 2) null,
  league_fta_per100 numeric(6, 2) null,
  league_oreb_per100 numeric(6, 2) null,
  league_pf_per100 numeric(6, 2) null,
  total_possessions numeric(12, 2) null,
  total_games integer null,
  created_at timestamp without time zone null default now(),
  constraint league_priors_history_pkey primary key (season_id, position_group, snapshot_date)
) TABLESPACE pg_default;

create index IF not exists idx_league_priors_lookup on public.league_priors_history using btree (season_id, snapshot_date desc) TABLESPACE pg_default;

create table public.player_average_advanced_stats (
  player_id bigint not null,
  game_id text not null,
  season_id text not null,
  game_date date not null,
  team_id bigint not null,
  game_number bigint not null,
  games_l5 smallint not null,
  games_l15 smallint not null,
  games_szn smallint not null,
  avg_off_rtg_l5 numeric(6, 2) null,
  avg_off_rtg_l15 numeric(6, 2) null,
  avg_off_rtg_szn numeric(6, 2) null,
  avg_def_rtg_l5 numeric(6, 2) null,
  avg_def_rtg_l15 numeric(6, 2) null,
  avg_def_rtg_szn numeric(6, 2) null,
  avg_net_rtg_l5 numeric(6, 2) null,
  avg_net_rtg_l15 numeric(6, 2) null,
  avg_net_rtg_szn numeric(6, 2) null,
  avg_ts_pct_l5 numeric(5, 4) null,
  avg_ts_pct_l15 numeric(5, 4) null,
  avg_ts_pct_szn numeric(5, 4) null,
  avg_efg_pct_l5 numeric(5, 4) null,
  avg_efg_pct_l15 numeric(5, 4) null,
  avg_efg_pct_szn numeric(5, 4) null,
  avg_usg_pct_l5 numeric(5, 4) null,
  avg_usg_pct_l15 numeric(5, 4) null,
  avg_usg_pct_szn numeric(5, 4) null,
  avg_ast_ratio_l5 numeric(5, 2) null,
  avg_ast_ratio_l15 numeric(5, 2) null,
  avg_ast_ratio_szn numeric(5, 2) null,
  avg_ast_pct_l5 numeric(5, 4) null,
  avg_ast_pct_l15 numeric(5, 4) null,
  avg_ast_pct_szn numeric(5, 4) null,
  avg_ast_tov_l5 numeric(5, 2) null,
  avg_ast_tov_l15 numeric(5, 2) null,
  avg_ast_tov_szn numeric(5, 2) null,
  avg_tov_ratio_l5 numeric(5, 2) null,
  avg_tov_ratio_l15 numeric(5, 2) null,
  avg_tov_ratio_szn numeric(5, 2) null,
  avg_reb_pct_l5 numeric(5, 4) null,
  avg_reb_pct_l15 numeric(5, 4) null,
  avg_reb_pct_szn numeric(5, 4) null,
  avg_oreb_pct_l5 numeric(5, 4) null,
  avg_oreb_pct_l15 numeric(5, 4) null,
  avg_oreb_pct_szn numeric(5, 4) null,
  avg_dreb_pct_l5 numeric(5, 4) null,
  avg_dreb_pct_l15 numeric(5, 4) null,
  avg_dreb_pct_szn numeric(5, 4) null,
  avg_pace_l5 numeric(6, 2) null,
  avg_pace_l15 numeric(6, 2) null,
  avg_pace_szn numeric(6, 2) null,
  avg_poss_l5 numeric(6, 2) null,
  avg_poss_l15 numeric(6, 2) null,
  avg_poss_szn numeric(6, 2) null,
  avg_pie_l5 numeric(5, 4) null,
  avg_pie_l15 numeric(5, 4) null,
  avg_pie_szn numeric(5, 4) null,
  constraint pk_player_avg_advanced_stats primary key (player_id, game_id),
  constraint fk_player_adv_avg_player foreign KEY (player_id) references players (player_id) on update CASCADE on delete CASCADE,
  constraint fk_player_adv_avg_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_player_adv_avg_season on public.player_average_advanced_stats using btree (player_id, season_id) TABLESPACE pg_default;

create index IF not exists idx_player_adv_avg_date on public.player_average_advanced_stats using btree (game_date) TABLESPACE pg_default;

create index IF not exists idx_player_adv_avg_team on public.player_average_advanced_stats using btree (team_id, game_date) TABLESPACE pg_default;

create table public.player_average_game_stats (
  player_id bigint not null,
  game_id text not null,
  season_id text not null,
  game_date date not null,
  team_id bigint not null,
  game_number bigint not null,
  games_l5 smallint not null,
  games_l15 smallint not null,
  games_szn smallint not null,
  avg_min_l5 numeric(5, 2) null,
  avg_min_l15 numeric(5, 2) null,
  avg_min_szn numeric(5, 2) null,
  avg_fgm_l5 numeric(5, 2) null,
  avg_fgm_l15 numeric(5, 2) null,
  avg_fgm_szn numeric(5, 2) null,
  avg_fga_l5 numeric(5, 2) null,
  avg_fga_l15 numeric(5, 2) null,
  avg_fga_szn numeric(5, 2) null,
  avg_fg_pct_l5 numeric(5, 4) null,
  avg_fg_pct_l15 numeric(5, 4) null,
  avg_fg_pct_szn numeric(5, 4) null,
  avg_fg3m_l5 numeric(5, 2) null,
  avg_fg3m_l15 numeric(5, 2) null,
  avg_fg3m_szn numeric(5, 2) null,
  avg_fg3a_l5 numeric(5, 2) null,
  avg_fg3a_l15 numeric(5, 2) null,
  avg_fg3a_szn numeric(5, 2) null,
  avg_fg3_pct_l5 numeric(5, 4) null,
  avg_fg3_pct_l15 numeric(5, 4) null,
  avg_fg3_pct_szn numeric(5, 4) null,
  avg_ftm_l5 numeric(5, 2) null,
  avg_ftm_l15 numeric(5, 2) null,
  avg_ftm_szn numeric(5, 2) null,
  avg_fta_l5 numeric(5, 2) null,
  avg_fta_l15 numeric(5, 2) null,
  avg_fta_szn numeric(5, 2) null,
  avg_ft_pct_l5 numeric(5, 4) null,
  avg_ft_pct_l15 numeric(5, 4) null,
  avg_ft_pct_szn numeric(5, 4) null,
  avg_oreb_l5 numeric(5, 2) null,
  avg_oreb_l15 numeric(5, 2) null,
  avg_oreb_szn numeric(5, 2) null,
  avg_dreb_l5 numeric(5, 2) null,
  avg_dreb_l15 numeric(5, 2) null,
  avg_dreb_szn numeric(5, 2) null,
  avg_reb_l5 numeric(5, 2) null,
  avg_reb_l15 numeric(5, 2) null,
  avg_reb_szn numeric(5, 2) null,
  avg_ast_l5 numeric(5, 2) null,
  avg_ast_l15 numeric(5, 2) null,
  avg_ast_szn numeric(5, 2) null,
  avg_stl_l5 numeric(5, 2) null,
  avg_stl_l15 numeric(5, 2) null,
  avg_stl_szn numeric(5, 2) null,
  avg_blk_l5 numeric(5, 2) null,
  avg_blk_l15 numeric(5, 2) null,
  avg_blk_szn numeric(5, 2) null,
  avg_tov_l5 numeric(5, 2) null,
  avg_tov_l15 numeric(5, 2) null,
  avg_tov_szn numeric(5, 2) null,
  avg_pf_l5 numeric(5, 2) null,
  avg_pf_l15 numeric(5, 2) null,
  avg_pf_szn numeric(5, 2) null,
  avg_pts_l5 numeric(5, 2) null,
  avg_pts_l15 numeric(5, 2) null,
  avg_pts_szn numeric(5, 2) null,
  avg_plus_minus_l5 numeric(6, 2) null,
  avg_plus_minus_l15 numeric(6, 2) null,
  avg_plus_minus_szn numeric(6, 2) null,
  constraint pk_player_avg_game_stats primary key (player_id, game_id),
  constraint fk_player_avg_player foreign KEY (player_id) references players (player_id) on update CASCADE on delete CASCADE,
  constraint fk_player_avg_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_player_avg_season on public.player_average_game_stats using btree (player_id, season_id) TABLESPACE pg_default;

create index IF not exists idx_player_avg_date on public.player_average_game_stats using btree (game_date) TABLESPACE pg_default;

create index IF not exists idx_player_avg_team on public.player_average_game_stats using btree (team_id, game_date) TABLESPACE pg_default;

create index IF not exists idx_pags_player_date on public.player_average_game_stats using btree (player_id, game_date desc) TABLESPACE pg_default;

create table public.player_game_advanced_stats (
  game_id text not null,
  player_id bigint not null,
  team_id bigint not null,
  first_name text null,
  family_name text null,
  name_i text null,
  player_slug text null,
  position text null,
  comment text null,
  jersey_num text null,
  team_city text null,
  team_name text null,
  team_tricode text null,
  team_slug text null,
  minutes numeric null,
  estimated_offensive_rating numeric null,
  offensive_rating numeric null,
  estimated_defensive_rating numeric null,
  defensive_rating numeric null,
  estimated_net_rating numeric null,
  net_rating numeric null,
  assist_percentage numeric null,
  assist_to_turnover numeric null,
  assist_ratio numeric null,
  offensive_rebound_percentage numeric null,
  defensive_rebound_percentage numeric null,
  rebound_percentage numeric null,
  estimated_team_turnover_percentage numeric null,
  turnover_ratio numeric null,
  effective_field_goal_percentage numeric null,
  true_shooting_percentage numeric null,
  usage_percentage numeric null,
  estimated_usage_percentage numeric null,
  estimated_pace numeric null,
  pace numeric null,
  possessions numeric null,
  pie numeric null,
  did_not_play boolean null default false,
  created_at timestamp with time zone null default now(),
  pace_per40 numeric null,
  constraint pk_advanced_player_game_stats primary key (game_id, player_id),
  constraint fk_adv_player_person foreign KEY (player_id) references players (player_id) on update CASCADE on delete CASCADE,
  constraint fk_adv_player_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_adv_player_game_id on public.player_game_advanced_stats using btree (game_id) TABLESPACE pg_default;

create index IF not exists idx_adv_player_person_id on public.player_game_advanced_stats using btree (player_id) TABLESPACE pg_default;

create index IF not exists idx_adv_player_team_id on public.player_game_advanced_stats using btree (team_id) TABLESPACE pg_default;

create index IF not exists idx_adv_player_person_team on public.player_game_advanced_stats using btree (player_id, team_id) TABLESPACE pg_default;

create index IF not exists idx_adv_player_dnp on public.player_game_advanced_stats using btree (did_not_play) TABLESPACE pg_default
where
  (did_not_play = false);

create table public.player_game_stats (
  season_id text null,
  player_id bigint not null,
  game_id text not null,
  matchup text null,
  wl text null,
  min bigint null,
  fgm bigint null,
  fga bigint null,
  fg_pct double precision null,
  fg3m bigint null,
  fg3a bigint null,
  fg3_pct double precision null,
  ftm bigint null,
  fta bigint null,
  ft_pct double precision null,
  oreb bigint null,
  dreb bigint null,
  reb bigint null,
  ast bigint null,
  stl bigint null,
  blk bigint null,
  tov bigint null,
  pf bigint null,
  pts bigint null,
  plus_minus bigint null,
  video_available bigint null,
  team_id bigint null,
  comment text null,
  did_not_play boolean null default false,
  game_date date null,
  constraint pk_player_game_stats primary key (player_id, game_id),
  constraint fk_player_game_stats_player foreign KEY (player_id) references players (player_id) on update CASCADE on delete CASCADE,
  constraint fk_player_game_stats_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE,
  constraint fk_player_game_stats_team_game foreign KEY (team_id, game_id) references team_game_stats (team_id, game_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_player_game_lookup on public.player_game_stats using btree (player_id, game_id) TABLESPACE pg_default;

create index IF not exists idx_player_game_season on public.player_game_stats using btree (season_id) TABLESPACE pg_default;

create index IF not exists idx_player_game_date on public.player_game_stats using btree (game_date) TABLESPACE pg_default;

create table public.player_position_history (
  player_id bigint not null,
  team_id bigint not null,
  snapshot_date date not null,
  season_id text null,
  primary_position text null,
  position_group text null,
  position_confidence numeric(4, 3) null,
  total_games_in_window integer null,
  created_at timestamp with time zone null default now(),
  constraint pk_player_position_history primary key (player_id, snapshot_date),
  constraint fk_pos_hist_player foreign KEY (player_id) references players (player_id) on update CASCADE on delete CASCADE,
  constraint fk_pos_hist_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_pos_hist_lookup on public.player_position_history using btree (player_id, snapshot_date desc) TABLESPACE pg_default;

create table public.players (
  player_id bigint not null,
  player_name text null,
  primary_position text null,
  position_group text null,
  constraint pk_players primary key (player_id)
) TABLESPACE pg_default;

create table public.raw_game_lines_staging (
  staging_id bigserial not null,
  api_game_id text not null,
  bookmaker text not null,
  market_key text not null,
  outcome_label text not null,
  line numeric null,
  odds_american integer not null,
  commence_time timestamp with time zone null,
  home_team text null,
  away_team text null,
  inserted_at timestamp with time zone null default now(),
  market_last_update timestamp with time zone null,
  bookmaker_last_update timestamp with time zone null,
  bookmaker_name text null,
  snapshot_time timestamp with time zone null,
  nba_game_id text null,
  nba_home_team_id bigint null,
  nba_away_team_id bigint null,
  constraint raw_game_lines_staging_pkey primary key (staging_id)
) TABLESPACE pg_default;

create index IF not exists idx_raw_lines_nba_game_id on public.raw_game_lines_staging using btree (nba_game_id) TABLESPACE pg_default;

create index IF not exists idx_staging_game_lines_dedupe on public.raw_game_lines_staging using btree (
  api_game_id,
  bookmaker,
  market_key,
  outcome_label,
  line
) TABLESPACE pg_default;

create table public.raw_player_props_combined (
  staging_id bigserial not null,
  api_game_id text not null,
  api_player_name text not null,
  bookmaker text not null,
  market_key text not null,
  outcome_label text not null,
  line numeric null,
  odds_american integer not null,
  commence_time timestamp with time zone null,
  home_team text null,
  away_team text null,
  inserted_at timestamp with time zone null default now(),
  market_last_update timestamp with time zone null,
  bookmaker_last_update timestamp with time zone null,
  bookmaker_name text null,
  snapshot_time timestamp with time zone null,
  game_id text null,
  player_id bigint null,
  team_id bigint null,
  constraint raw_player_props_combined_pkey primary key (staging_id)
) TABLESPACE pg_default;

create index IF not exists idx_combined_dedupe on public.raw_player_props_combined using btree (
  api_game_id,
  api_player_name,
  bookmaker,
  market_key,
  line
) TABLESPACE pg_default;

create index IF not exists idx_props_api_game_id on public.raw_player_props_combined using btree (api_game_id) TABLESPACE pg_default;

create table public.raw_player_props_combined (
  staging_id bigserial not null,
  api_game_id text not null,
  api_player_name text not null,
  bookmaker text not null,
  market_key text not null,
  outcome_label text not null,
  line numeric null,
  odds_american integer not null,
  commence_time timestamp with time zone null,
  home_team text null,
  away_team text null,
  inserted_at timestamp with time zone null default now(),
  market_last_update timestamp with time zone null,
  bookmaker_last_update timestamp with time zone null,
  bookmaker_name text null,
  snapshot_time timestamp with time zone null,
  game_id text null,
  player_id bigint null,
  team_id bigint null,
  constraint raw_player_props_combined_pkey primary key (staging_id)
) TABLESPACE pg_default;

create index IF not exists idx_combined_dedupe on public.raw_player_props_combined using btree (
  api_game_id,
  api_player_name,
  bookmaker,
  market_key,
  line
) TABLESPACE pg_default;

create index IF not exists idx_props_api_game_id on public.raw_player_props_combined using btree (api_game_id) TABLESPACE pg_default;

create table public.raw_player_props_combined (
  staging_id bigserial not null,
  api_game_id text not null,
  api_player_name text not null,
  bookmaker text not null,
  market_key text not null,
  outcome_label text not null,
  line numeric null,
  odds_american integer not null,
  commence_time timestamp with time zone null,
  home_team text null,
  away_team text null,
  inserted_at timestamp with time zone null default now(),
  market_last_update timestamp with time zone null,
  bookmaker_last_update timestamp with time zone null,
  bookmaker_name text null,
  snapshot_time timestamp with time zone null,
  game_id text null,
  player_id bigint null,
  team_id bigint null,
  constraint raw_player_props_combined_pkey primary key (staging_id)
) TABLESPACE pg_default;

create index IF not exists idx_combined_dedupe on public.raw_player_props_combined using btree (
  api_game_id,
  api_player_name,
  bookmaker,
  market_key,
  line
) TABLESPACE pg_default;

create index IF not exists idx_props_api_game_id on public.raw_player_props_combined using btree (api_game_id) TABLESPACE pg_default;

create table public.team_allowed_by_position (
  team_id bigint not null,
  game_id text not null,
  game_date date not null,
  position_group text not null,
  games_l5 smallint null,
  games_l15 smallint null,
  games_szn smallint null,
  poss_faced_l5 numeric(8, 2) null,
  poss_faced_l15 numeric(8, 2) null,
  poss_faced_szn numeric(10, 2) null,
  pts_allowed_l5 numeric(8, 2) null,
  pts_allowed_l15 numeric(8, 2) null,
  pts_allowed_szn numeric(10, 2) null,
  reb_allowed_l5 numeric(8, 2) null,
  reb_allowed_l15 numeric(8, 2) null,
  reb_allowed_szn numeric(10, 2) null,
  ast_allowed_l5 numeric(8, 2) null,
  ast_allowed_l15 numeric(8, 2) null,
  ast_allowed_szn numeric(10, 2) null,
  stl_allowed_l5 numeric(8, 2) null,
  stl_allowed_l15 numeric(8, 2) null,
  stl_allowed_szn numeric(10, 2) null,
  blk_allowed_l5 numeric(8, 2) null,
  blk_allowed_l15 numeric(8, 2) null,
  blk_allowed_szn numeric(10, 2) null,
  threes_allowed_l5 numeric(8, 2) null,
  threes_allowed_l15 numeric(8, 2) null,
  threes_allowed_szn numeric(10, 2) null,
  tov_forced_l5 numeric(8, 2) null,
  tov_forced_l15 numeric(8, 2) null,
  tov_forced_szn numeric(10, 2) null,
  fta_allowed_l5 numeric(8, 2) null,
  fta_allowed_l15 numeric(8, 2) null,
  fta_allowed_szn numeric(10, 2) null,
  oreb_allowed_l5 numeric(8, 2) null,
  oreb_allowed_l15 numeric(8, 2) null,
  oreb_allowed_szn numeric(10, 2) null,
  pf_allowed_l5 numeric(8, 2) null,
  pf_allowed_l15 numeric(8, 2) null,
  pf_allowed_szn numeric(10, 2) null,
  off_rtg_allowed_l5 numeric(6, 2) null,
  off_rtg_allowed_l15 numeric(6, 2) null,
  off_rtg_allowed_szn numeric(6, 2) null,
  reb_per100_allowed_l5 numeric(6, 2) null,
  reb_per100_allowed_l15 numeric(6, 2) null,
  reb_per100_allowed_szn numeric(6, 2) null,
  ast_per100_allowed_l5 numeric(6, 2) null,
  ast_per100_allowed_l15 numeric(6, 2) null,
  ast_per100_allowed_szn numeric(6, 2) null,
  stl_per100_allowed_l5 numeric(6, 2) null,
  stl_per100_allowed_l15 numeric(6, 2) null,
  stl_per100_allowed_szn numeric(6, 2) null,
  blk_per100_allowed_l5 numeric(6, 2) null,
  blk_per100_allowed_l15 numeric(6, 2) null,
  blk_per100_allowed_szn numeric(6, 2) null,
  threes_per100_allowed_l5 numeric(6, 2) null,
  threes_per100_allowed_l15 numeric(6, 2) null,
  threes_per100_allowed_szn numeric(6, 2) null,
  tov_per100_forced_l5 numeric(6, 2) null,
  tov_per100_forced_l15 numeric(6, 2) null,
  tov_per100_forced_szn numeric(6, 2) null,
  fta_per100_allowed_l5 numeric(6, 2) null,
  fta_per100_allowed_l15 numeric(6, 2) null,
  fta_per100_allowed_szn numeric(6, 2) null,
  oreb_per100_allowed_l5 numeric(6, 2) null,
  oreb_per100_allowed_l15 numeric(6, 2) null,
  oreb_per100_allowed_szn numeric(6, 2) null,
  pf_per100_allowed_l5 numeric(6, 2) null,
  pf_per100_allowed_l15 numeric(6, 2) null,
  pf_per100_allowed_szn numeric(6, 2) null,
  created_at timestamp without time zone null default now(),
  constraint team_allowed_by_position_pkey primary key (team_id, game_id, position_group),
  constraint team_allowed_by_position_team_id_fkey foreign KEY (team_id) references teams (team_id)
) TABLESPACE pg_default;

create index IF not exists idx_tabp_team_pos_date on public.team_allowed_by_position using btree (team_id, position_group, game_date desc) TABLESPACE pg_default;

create index IF not exists idx_team_allowed_lookup on public.team_allowed_by_position using btree (team_id, position_group, game_date desc) TABLESPACE pg_default;

create table public.team_average_game_stats (
  team_id bigint not null,
  game_id text not null,
  season_id text not null,
  game_date date not null,
  game_number bigint not null,
  games_l5 smallint not null,
  games_l15 smallint not null,
  games_szn smallint not null,
  avg_pts_l5 numeric(6, 2) null,
  avg_pts_l15 numeric(6, 2) null,
  avg_pts_szn numeric(6, 2) null,
  avg_fgm_l5 numeric(5, 2) null,
  avg_fgm_l15 numeric(5, 2) null,
  avg_fgm_szn numeric(5, 2) null,
  avg_fga_l5 numeric(5, 2) null,
  avg_fga_l15 numeric(5, 2) null,
  avg_fga_szn numeric(5, 2) null,
  avg_fg_pct_l5 numeric(5, 4) null,
  avg_fg_pct_l15 numeric(5, 4) null,
  avg_fg_pct_szn numeric(5, 4) null,
  avg_fg3m_l5 numeric(5, 2) null,
  avg_fg3m_l15 numeric(5, 2) null,
  avg_fg3m_szn numeric(5, 2) null,
  avg_fg3a_l5 numeric(5, 2) null,
  avg_fg3a_l15 numeric(5, 2) null,
  avg_fg3a_szn numeric(5, 2) null,
  avg_fg3_pct_l5 numeric(5, 4) null,
  avg_fg3_pct_l15 numeric(5, 4) null,
  avg_fg3_pct_szn numeric(5, 4) null,
  avg_ftm_l5 numeric(5, 2) null,
  avg_ftm_l15 numeric(5, 2) null,
  avg_ftm_szn numeric(5, 2) null,
  avg_fta_l5 numeric(5, 2) null,
  avg_fta_l15 numeric(5, 2) null,
  avg_fta_szn numeric(5, 2) null,
  avg_ft_pct_l5 numeric(5, 4) null,
  avg_ft_pct_l15 numeric(5, 4) null,
  avg_ft_pct_szn numeric(5, 4) null,
  avg_oreb_l5 numeric(5, 2) null,
  avg_oreb_l15 numeric(5, 2) null,
  avg_oreb_szn numeric(5, 2) null,
  avg_dreb_l5 numeric(5, 2) null,
  avg_dreb_l15 numeric(5, 2) null,
  avg_dreb_szn numeric(5, 2) null,
  avg_reb_l5 numeric(5, 2) null,
  avg_reb_l15 numeric(5, 2) null,
  avg_reb_szn numeric(5, 2) null,
  avg_ast_l5 numeric(5, 2) null,
  avg_ast_l15 numeric(5, 2) null,
  avg_ast_szn numeric(5, 2) null,
  avg_stl_l5 numeric(5, 2) null,
  avg_stl_l15 numeric(5, 2) null,
  avg_stl_szn numeric(5, 2) null,
  avg_blk_l5 numeric(5, 2) null,
  avg_blk_l15 numeric(5, 2) null,
  avg_blk_szn numeric(5, 2) null,
  avg_tov_l5 numeric(5, 2) null,
  avg_tov_l15 numeric(5, 2) null,
  avg_tov_szn numeric(5, 2) null,
  avg_pf_l5 numeric(5, 2) null,
  avg_pf_l15 numeric(5, 2) null,
  avg_pf_szn numeric(5, 2) null,
  avg_plus_minus_l5 numeric(6, 2) null,
  avg_plus_minus_l15 numeric(6, 2) null,
  avg_plus_minus_szn numeric(6, 2) null,
  avg_off_rtg_l5 numeric(6, 2) null,
  avg_off_rtg_l15 numeric(6, 2) null,
  avg_off_rtg_szn numeric(6, 2) null,
  avg_def_rtg_l5 numeric(6, 2) null,
  avg_def_rtg_l15 numeric(6, 2) null,
  avg_def_rtg_szn numeric(6, 2) null,
  avg_net_rtg_l5 numeric(6, 2) null,
  avg_net_rtg_l15 numeric(6, 2) null,
  avg_net_rtg_szn numeric(6, 2) null,
  avg_ts_pct_l5 numeric(5, 4) null,
  avg_ts_pct_l15 numeric(5, 4) null,
  avg_ts_pct_szn numeric(5, 4) null,
  avg_efg_pct_l5 numeric(5, 4) null,
  avg_efg_pct_l15 numeric(5, 4) null,
  avg_efg_pct_szn numeric(5, 4) null,
  avg_pace_l5 numeric(6, 2) null,
  avg_pace_l15 numeric(6, 2) null,
  avg_pace_szn numeric(6, 2) null,
  avg_poss_l5 numeric(6, 2) null,
  avg_poss_l15 numeric(6, 2) null,
  avg_poss_szn numeric(6, 2) null,
  avg_ast_ratio_l5 numeric(5, 2) null,
  avg_ast_ratio_l15 numeric(5, 2) null,
  avg_ast_ratio_szn numeric(5, 2) null,
  avg_tov_ratio_l5 numeric(5, 2) null,
  avg_tov_ratio_l15 numeric(5, 2) null,
  avg_tov_ratio_szn numeric(5, 2) null,
  avg_reb_pct_l5 numeric(5, 4) null,
  avg_reb_pct_l15 numeric(5, 4) null,
  avg_reb_pct_szn numeric(5, 4) null,
  avg_oreb_pct_l5 numeric(5, 4) null,
  avg_oreb_pct_l15 numeric(5, 4) null,
  avg_oreb_pct_szn numeric(5, 4) null,
  avg_dreb_pct_l5 numeric(5, 4) null,
  avg_dreb_pct_l15 numeric(5, 4) null,
  avg_dreb_pct_szn numeric(5, 4) null,
  avg_pie_l5 numeric(5, 4) null,
  avg_pie_l15 numeric(5, 4) null,
  avg_pie_szn numeric(5, 4) null,
  constraint pk_team_avg_game_stats primary key (team_id, game_id),
  constraint fk_team_avg_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_team_avg_season on public.team_average_game_stats using btree (team_id, season_id) TABLESPACE pg_default;

create index IF not exists idx_team_avg_date on public.team_average_game_stats using btree (game_date) TABLESPACE pg_default;

create table public.team_game_stats (
  season_id text null,
  team_id bigint not null,
  team_abbreviation text null,
  team_name text null,
  game_id text not null,
  team_matchup text null,
  team_wl text null,
  team_min bigint null,
  team_pts bigint null,
  team_fgm bigint null,
  team_fga bigint null,
  team_fg_pct double precision null,
  team_fg3m bigint null,
  team_fg3a bigint null,
  team_fg3_pct double precision null,
  team_ftm bigint null,
  team_fta bigint null,
  team_ft_pct double precision null,
  team_oreb bigint null,
  team_dreb bigint null,
  team_reb bigint null,
  team_ast bigint null,
  team_stl bigint null,
  team_blk bigint null,
  team_tov bigint null,
  team_pf bigint null,
  team_plus_minus double precision null,
  season text null,
  opponent_id bigint null,
  minutes numeric null,
  offensive_rating numeric null,
  defensive_rating numeric null,
  net_rating numeric null,
  estimated_offensive_rating numeric null,
  estimated_defensive_rating numeric null,
  estimated_net_rating numeric null,
  effective_field_goal_percentage numeric null,
  true_shooting_percentage numeric null,
  pie numeric null,
  possessions numeric null,
  pace numeric null,
  estimated_pace numeric null,
  pace_per40 numeric null,
  assist_ratio numeric null,
  turnover_ratio numeric null,
  assist_to_turnover numeric null,
  estimated_team_turnover_percentage numeric null,
  assist_percentage numeric null,
  rebound_percentage numeric null,
  offensive_rebound_percentage numeric null,
  defensive_rebound_percentage numeric null,
  game_date date null,
  constraint pk_team_game_stats primary key (team_id, game_id),
  constraint fk_team_game_stats_opponent foreign KEY (opponent_id) references teams (team_id) on update CASCADE on delete CASCADE,
  constraint fk_team_game_stats_team foreign KEY (team_id) references teams (team_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_team_game_season on public.team_game_stats using btree (season_id) TABLESPACE pg_default;

create index IF not exists idx_team_game_date on public.team_game_stats using btree (game_date) TABLESPACE pg_default;

create index IF not exists idx_team_game_stats_opponent on public.team_game_stats using btree (opponent_id) TABLESPACE pg_default;


create table public.teams (
  team_id bigint not null,
  team_name text null,
  constraint pk_teams primary key (team_id)
) TABLESPACE pg_default;

