from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import math

seasonStartDates = {
    '2020-21' : '12/22/2020',
    '2021-22' : '10/19/2021',
    '2022-23' : '10/18/2022',
    '2023-24' : '10/24/2023',
    '2024-25' : '10/22/2024'
}


def identify_nba_season(date_str):
    """
    Identifies which NBA season a date falls into.
    NBA seasons typically run from October to June of the following year.
    
    Args:
        date_str (str): Date string in various formats:
            - 'YYYY-MM-DD'
            - 'MM-DD-YYYY'
            - 'YYYY/MM/DD'
            - 'MM/DD/YYYY'
        
    Returns:
        str: Season identifier in format 'YYYY-YY' (e.g., '2024-25')
        None: If the date doesn't fall within any of the listed seasons
    """
    # Available seasons
    years_list = ['2024-25', '2023-24', '2022-23', '2021-22', '2020-21', '2019-20']
    
    # Parse the input date with multiple format attempts
    date_obj = None
    formats_to_try = [
        '%Y-%m-%d',  # YYYY-MM-DD
        '%m-%d-%Y',  # MM-DD-YYYY
        '%Y/%m/%d',  # YYYY/MM/DD
        '%m/%d/%Y',  # MM/DD/YYYY
        '%d-%m-%Y',  # DD-MM-YYYY
        '%d/%m/%Y'   # DD/MM/YYYY
    ]
    
    for fmt in formats_to_try:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue
    
    if date_obj is None:
        raise ValueError(f"Unable to parse date: {date_str}. Supported formats: YYYY-MM-DD, MM-DD-YYYY, etc.")
    
    # Get year and month
    year = date_obj.year
    month = date_obj.month
    
    # Determine NBA season
    # If month is from October to December, we're in the first year of the season
    # If month is from January to September, we're in the second year of the season
    if month >= 10:  # October to December
        season_start = year
        season_end = year + 1
    else:  # January to September
        season_start = year - 1
        season_end = year
    
    # Create season string and check if it's in our list
    season = f"{season_start}-{str(season_end)[-2:]}"
    
    if season in years_list:
        return season
    else:
        # Check if the season is simply not in our predefined list
        # For dates outside our predefined ranges
        return season

def calculate_recent_scores(player_data, year, date, engine):
    player_id = player_data['PLAYER_ID']
    name = player_data['PLAYER_NAME']
    
    with engine.connect() as conn:
        with conn.begin():
            query10 = text(f"""
                WITH recent_games AS (
                    SELECT *
                    FROM "all_player_game_stats_2024-25"
                    WHERE "PLAYER_ID" = :player_id
                    AND TO_DATE("GAME_DATE", 'MON DD, YYYY') < TO_DATE(:date, 'MM/DD/YYYY')
                    ORDER BY TO_DATE("GAME_DATE", 'MON DD, YYYY') DESC
                    LIMIT 10
                )
                SELECT
                    "PLAYER_ID",
                    AVG("PTS") as "AVG_PTS",
                    AVG("PLUS_MINUS") as "AVG_PLUS_MINUS",
                    AVG("MIN") as "AVG_MIN",
                    AVG("FGA") as "AVG_FGA", 
                    AVG("FTA") as "AVG_FTA",
                    AVG("TOV") as "AVG_TOV",
                    AVG("OREB") as "AVG_OREB",
                    MAX(TO_DATE("GAME_DATE", 'MON DD, YYYY')) as "LATEST_GAME_DATE",
                    MIN(TO_DATE("GAME_DATE", 'MON DD, YYYY')) as "EARLIEST_GAME_DATE",
                    COUNT(*) as "GAMES_PLAYED"
                FROM recent_games
                GROUP BY "PLAYER_ID"
            """)
            query5 = text(f"""
                WITH recent_games AS (
                    SELECT *
                    FROM "all_player_game_stats_2024-25"
                    WHERE "PLAYER_ID" = :player_id
                    AND TO_DATE("GAME_DATE", 'MON DD, YYYY') < TO_DATE(:date, 'MM/DD/YYYY')
                    ORDER BY TO_DATE("GAME_DATE", 'MON DD, YYYY') DESC
                    LIMIT 5
                )
                SELECT
                    "PLAYER_ID",
                    AVG("PTS") as "AVG_PTS",
                    AVG("PLUS_MINUS") as "AVG_PLUS_MINUS",
                    AVG("MIN") as "AVG_MIN",
                    AVG("FGA") as "AVG_FGA", 
                    AVG("FTA") as "AVG_FTA",
                    AVG("TOV") as "AVG_TOV",
                    AVG("OREB") as "AVG_OREB",
                    MAX(TO_DATE("GAME_DATE", 'MON DD, YYYY')) as "LATEST_GAME_DATE",
                    MIN(TO_DATE("GAME_DATE", 'MON DD, YYYY')) as "EARLIEST_GAME_DATE",
                    COUNT(*) as "GAMES_PLAYED"
                FROM recent_games
                GROUP BY "PLAYER_ID"
            """)
            recent_games = pd.read_sql_query(query10, engine, params={'player_id': player_id, 'date' : date})
            more_recent_games = pd.read_sql_query(query5, engine, params={'player_id': player_id, 'date' : date})

    #now that we have the recent games for this player, calculate net rating
    possessions = 0.96 * recent_games['AVG_FGA'].values[0] + 0.44 * recent_games['AVG_FTA'].values[0] - recent_games['AVG_OREB'].values[0] + recent_games['AVG_TOV'].values[0]

    offensive_rating = (recent_games['AVG_PTS'].values[0] / possessions) 
    defensive_rating = (recent_games['AVG_PTS'].values[0] - recent_games['AVG_PLUS_MINUS'].values[0]) / possessions
    net_ratingTen = offensive_rating - defensive_rating

    #-----------------------------
    possessionsFive = 0.96 * more_recent_games['AVG_FGA'].values[0] + 0.44 * more_recent_games['AVG_FTA'].values[0] - more_recent_games['AVG_OREB'].values[0] + more_recent_games['AVG_TOV'].values[0]

    offensive_ratingFive = (more_recent_games['AVG_PTS'].values[0] / possessionsFive) 
    defensive_ratingFive = (more_recent_games['AVG_PTS'].values[0] - more_recent_games['AVG_PLUS_MINUS'].values[0]) / possessionsFive
    net_ratingFive= offensive_ratingFive - defensive_ratingFive

    #calculate true shooting pct
    true_shootingTen = recent_games['AVG_PTS'].values[0] /  (2 * (recent_games['AVG_FGA'].values[0] + 0.44 * recent_games['AVG_FTA'].values[0]))
    #-----------------------------
    true_shootingFive = more_recent_games['AVG_PTS'].values[0] /  (2 * (more_recent_games['AVG_FGA'].values[0] + 0.44 * more_recent_games['AVG_FTA'].values[0]))
    
    #calculate usage rate
    usage_rateTen = ((recent_games['AVG_FGA'].values[0] + 0.44 * recent_games['AVG_FTA'].values[0] + recent_games['AVG_TOV'].values[0]) / recent_games['AVG_MIN'].values[0])
    #-----------------------------
    usage_rateFive = ((more_recent_games['AVG_FGA'].values[0] + 0.44 * more_recent_games['AVG_FTA'].values[0] + more_recent_games['AVG_TOV'].values[0]) / more_recent_games['AVG_MIN'].values[0])

    #calculate turnover rate
    turnover_rateTen = (recent_games['AVG_TOV'].values[0] / (recent_games['AVG_FGA'].values[0] + 0.44 * recent_games['AVG_FTA'].values[0] + recent_games['AVG_TOV'].values[0]))
    #-----------------------------
    turnover_rateFive = (more_recent_games['AVG_TOV'].values[0] / (more_recent_games['AVG_FGA'].values[0] + 0.44 * more_recent_games['AVG_FTA'].values[0] + more_recent_games['AVG_TOV'].values[0]))

    #calculate minutes per game
    minutes_per_gameTen = recent_games['AVG_MIN'].values[0] / recent_games['GAMES_PLAYED'].values[0]
    #-----------------------------
    minutes_per_gameFive = more_recent_games['AVG_MIN'].values[0] / more_recent_games['GAMES_PLAYED'].values[0]

    # print(net_ratingTen)
    # print(true_shootingTen)
    # print(usage_rateTen)
    # print(turnover_rateTen)
    # print(minutes_per_gameTen)
    # print()
    recentFormTen = (0.4 * net_ratingTen) + (0.2 * true_shootingTen) + (0.15 * usage_rateTen) + (0.1 * turnover_rateTen) + (0.15 * minutes_per_gameTen)

    # print(net_ratingFive)
    # print(true_shootingFive)
    # print(usage_rateFive)
    # print(turnover_rateFive)
    # print(minutes_per_gameFive)
    # print()
    recentFormFive = (0.4 * net_ratingFive) + (0.2 * true_shootingFive) + (0.15 * usage_rateFive) + (0.1 * turnover_rateFive) + (0.15 * minutes_per_gameFive)

    finalScore = (0.6 * recentFormTen) + (0.4 * recentFormFive)
    # print(finalScore)
    # print()
    return finalScore

def calculate_player_metrics(engine, year, date, star_players, key_rotation_players):
    primary_weights = {
        'PIE' : 0.175,
        'USG' : 0.125,
        'NetRating' : .1
    }
    
    secondary_weights = {
        'TrueShooting' : .125,
        'TurnoverRate' : .05,
        'Availability' : .1,
        'Mins' : .15 #this can be adjusted to chnage to minutes consistency 
    }
    clutch_weights = {
        'CLUTCH_SCORE_PCT' : .5,
        'CLUTCH_USAGE_RATE' : .25,
        'CLUTCH_NET_RATING' : .25
    }

    def calculate_primary_scores(player_data, player_GP, team_GP):
        score = 0
        score += float(player_data['PIE']) * primary_weights['PIE']
        score += float(player_data['E_USG_PCT']) * primary_weights['USG']
        
        #adjust the net rating based on how many games have been played
        netRating = player_data['NET_RATING'] * primary_weights['NetRating']
        a = 0.75
        #this is the adjusted net rating based on number of games played
        #note that when we make this for a full team performance, we need to get the number of games the team has played
        netRating = netRating * (player_GP/team_GP) ** a
        score += netRating
        return score
    
    def calculate_secondary_scores(player_data, player_GP, team_GP):
        score = 0
        score += float(player_data['TS_PCT']) * secondary_weights['TrueShooting']
        score += float(player_data['E_TOV_PCT']) * secondary_weights['TurnoverRate']
        #player minutes should be split between trends in recent games and season averages
        score += player_data['MIN'] * secondary_weights['Mins'] #in the future we want to make thier average just part of it
        
        # score += availability * secondary_weights['AVAILABILITY']
        return score

    def calculate_clutch_scores(player_data, player_GP, team_GP):
        score = 0
        #get raw stats
        scorePct = float(player_data['CLUTCH_SCORE_PCT']) * clutch_weights['CLUTCH_SCORE_PCT']
        usgRate = float(player_data['CLUTCH_USAGE_RATE']) * clutch_weights['CLUTCH_USAGE_RATE']
        netRating = float(player_data['CLUTCH_NET_RATING']) * clutch_weights['CLUTCH_NET_RATING']
        #setup adjustment constants 
        a = 0.75
        #scale scores to avoid small sample outliers 
        score += scorePct * (player_GP/team_GP) ** a
        score += usgRate * (player_GP/team_GP) ** a
        score += netRating * (player_GP/team_GP) ** a

        if math.isnan(score):
            return 0
        else:
            return score
    
    def calculate_player_scores(players_df, baseWeight = 0.5, clutchWeight = 0.3, recentWeight = 0.2):
        scores = []
        for index, player in players_df.iterrows():
            
            #get player and team games played
            player_id = player['PLAYER_ID']
            team_id = player['TEAM_ID']
            with engine.connect() as conn:
                with conn.begin():
                    player_query = text(f"""
                        select * from "all_player_game_stats_{year}"
                        where "PLAYER_ID" = :player_id
                    """)
                    team_query = text(f"""
                        select * from "{year}_historic_game_data"
                        where "TEAM_ID" = :team_id
                    """)
                    #get player number of games
                    player_games = pd.read_sql_query(player_query, engine, params = {'player_id' : player_id})
                    player_GP = len(player_games)
                    #get team number of games 
                    team_games = pd.read_sql_query(team_query, engine, params = {'team_id' : team_id})
                    team_GP = len(team_games)
            
            primaryScore = calculate_primary_scores(player, player_GP, team_GP)
            secondaryScore = calculate_secondary_scores(player, player_GP, team_GP)
            clutchScore = calculate_clutch_scores(player, player_GP, team_GP)
            recentScore = calculate_recent_scores(player, year, date, engine)
    
            final_score = (primaryScore + secondaryScore) * baseWeight
            final_score += clutchScore * clutchWeight
            final_score += recentScore * recentWeight
    
            scores.append({
                'PLAYER_NAME': player['PLAYER_NAME'],
                'PLAYER_ID': player['PLAYER_ID'],
                'SCORE': final_score,
                'PRIMARY_CONTRIBUTION': primaryScore * baseWeight,
                'SECONDARY_CONTRIBUTION': secondaryScore * baseWeight,
                'CLUTCH_CONTRIBUTION': clutchScore * clutchWeight,
                'RECENT_CONTRIBUTION': recentScore * recentWeight,
                'CATEGORY': 'Star' if player['MIN'] > 25  else 'Rotation' if player['MIN'] < 25 and player['MIN'] > 15 else 'Loser'
            })
    
        return pd.DataFrame(scores)
            
    
    starScores = calculate_player_scores(star_players)
    rotationScores = calculate_player_scores(key_rotation_players)
    
    allScores = pd.concat([starScores, rotationScores])
    allScores = allScores.sort_values('SCORE', ascending=False)
    
    return allScores

def get_team_player_scores(engine, teamName, date):
    year = identify_nba_season(date)
    
    team_name = teamName
    season = identify_nba_season(date)
    startDate = seasonStartDates[season]
    # retreive players we want stats for 
    # take those players stats from the advanced table
    with engine.connect() as conn:
        with conn.begin():
            query = text("""
                WITH player_advanced_stats AS (
                    SELECT 
                        ps."PLAYER_ID"::TEXT,
                        ps."PLAYER_NAME",
                        ps."TEAM_ID"::BIGINT,
                        COUNT(*) AS num_periods,
                        AVG(NULLIF(ps."PIE", '')::NUMERIC) AS "PIE",
                        AVG(NULLIF(ps."TS_PCT", '')::NUMERIC) AS "TS_PCT",
                        AVG(NULLIF(ps."NET_RATING", '')::NUMERIC) AS "NET_RATING",
                        AVG(NULLIF(ps."E_NET_RATING", '')::NUMERIC) AS "E_NET_RATING",
                        AVG(NULLIF(ps."E_USG_PCT", '')::NUMERIC) AS "E_USG_PCT",
                        AVG(NULLIF(ps."E_TOV_PCT", '')::NUMERIC) AS "E_TOV_PCT",
                        AVG(NULLIF(ps."MIN", '')::NUMERIC) AS "MIN"
                    FROM "advanced_player_stats_by_date" ps
                    WHERE TO_DATE(ps."DATE_TO", 'MM/DD/YYYY') < TO_DATE(:date, 'MM/DD/YYYY')
                    AND TO_DATE(ps."DATE_FROM", 'MM/DD/YYYY') > TO_DATE(:seasonStartDate, 'MM/DD/YYYY')
                    GROUP BY ps."PLAYER_ID", ps."PLAYER_NAME", ps."TEAM_ID"
                ),
                player_clutch_stats AS (
                    SELECT 
                        pc."PLAYER_ID"::TEXT,
                        pc."PLAYER_NAME",
                        COUNT(*) AS GAME_COUNT,
                        AVG(NULLIF(pc."CLUTCH_SCORE_PCT", '')::NUMERIC) AS "CLUTCH_SCORE_PCT",
                        AVG(NULLIF(pc."CLUTCH_USAGE_RATE", '')::NUMERIC) AS "CLUTCH_USAGE_RATE",
                        AVG(NULLIF(pc."CLUTCH_NET_RATING", '')::NUMERIC) AS "CLUTCH_NET_RATING"
                    FROM "advanced_player_clutch_stats_by_date" pc
                    WHERE TO_DATE(pc."DATE_TO", 'MM/DD/YYYY') < TO_DATE(:date, 'MM/DD/YYYY')
                    AND TO_DATE(pc."DATE_FROM", 'MM/DD/YYYY') > TO_DATE(:seasonStartDate, 'MM/DD/YYYY')
                    GROUP BY pc."PLAYER_ID", pc."PLAYER_NAME"
                )
                SELECT
                    t."TEAM_NAME"::TEXT,
                    a."TEAM_ID",
                    a."PLAYER_ID",
                    a."PLAYER_NAME",
                    a.num_periods,
                    a."PIE",
                    a."TS_PCT",
                    a."NET_RATING",
                    a."E_NET_RATING",
                    a."E_USG_PCT",
                    a."E_TOV_PCT",
                    a."MIN",
                    c.game_count,
                    c."CLUTCH_SCORE_PCT",
                    c."CLUTCH_USAGE_RATE",
                    c."CLUTCH_NET_RATING"
                FROM player_advanced_stats a
                LEFT JOIN player_clutch_stats c
                    ON a."PLAYER_ID" = c."PLAYER_ID"
                JOIN "teams" t  -- Assuming you have a simple teams lookup table
                    ON a."TEAM_ID" = t."TEAM_ID"
                WHERE t."TEAM_NAME" = :team_name
            """)
            #starters
            player_info = pd.read_sql_query(query, engine, params={'team_name': team_name, 'date' : date, 'seasonStartDate' : startDate})

            star_mask = player_info['MIN'] > 30
            key_rotation_mask = (player_info['MIN'] < 30) & (player_info['MIN'] > 15)

            star_players = player_info[star_mask]
            key_rotation_players = player_info[key_rotation_mask]
                
    # #get high impact stats - player impact rating, net rating, usg pct, plus/minus adjsuted for minutes
    # #get efficiency stats - true shooting, turnover rate 
    # #get availability - least important

    # #apply weights to key points 
    # #star players 1x weight, key rotation .75 weight 
    # #bench .5 weight 

    player_scores = calculate_player_metrics(engine, year, date, star_players, key_rotation_players)
    return player_scores