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
def identify_nba_season(date_str):

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

"""
    Calculate a player's recent performance score based on their last 5 and 10 games.
    
    This function retrieves game statistics for a player from a database and calculates
    a composite performance score using various basketball metrics including net rating,
    true shooting percentage, usage rate, turnover rate, and minutes per game.
    
    Parameters:
    -----------
    player_data : dict
        Dictionary containing player information with at minimum the keys:
        - 'PLAYER_ID' : int or str, the unique identifier for the player
        - 'PLAYER_NAME' : str, the name of the player
    
    year : str
        The season year in format 'YYYY-YY' (e.g., '2024-25')
    
    date : str
        The cutoff date in 'MM/DD/YYYY' format to retrieve games before this date
    
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries
    
    Returns:
    --------
    float
        The calculated performance score representing the player's recent form.
        Higher values indicate better recent performance.
    
    Notes:
    ------
    The performance score is calculated using these metrics with the following weights:
    - 10-game composite (60% of final score):
      - Net Rating: 40%
      - True Shooting %: 20%
      - Usage Rate: 15%
      - Turnover Rate: 10%
      - Minutes Per Game: 15%
    
    - 5-game composite (40% of final score):
      - Same metrics and weights as 10-game composite
    
    Performance metrics are calculated as follows:
    - Possessions = 0.96 * AVG_FGA + 0.44 * AVG_FTA - AVG_OREB + AVG_TOV
    - Offensive Rating = AVG_PTS / possessions
    - Defensive Rating = (AVG_PTS - AVG_PLUS_MINUS) / possessions
    - Net Rating = offensive_rating - defensive_rating
    - True Shooting % = AVG_PTS / (2 * (AVG_FGA + 0.44 * AVG_FTA))
    - Usage Rate = (AVG_FGA + 0.44 * AVG_FTA + AVG_TOV) / AVG_MIN
    - Turnover Rate = AVG_TOV / (AVG_FGA + 0.44 * AVG_FTA + AVG_TOV)
    - Minutes Per Game = AVG_MIN / GAMES_PLAYED
    
    Example:
    --------
    >>> player = {'PLAYER_ID': 1627936, 'PLAYER_NAME': 'Jaylen Brown'}
    >>> date = '03/01/2025'
    >>> year = '2024-25'
    >>> score = calculate_recent_scores(player, year, date, db_engine)
    >>> print(f"{player['PLAYER_NAME']}'s score: {score:.2f}")
    Jaylen Brown's score: 18.45
"""
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


"""
    Calculate comprehensive player performance metrics combining primary stats, secondary stats,
    clutch performance, and recent game performance for NBA players.
    
    This function evaluates player performance using a weighted scoring system that considers
    various basketball metrics and returns a sorted DataFrame of all players with their
    calculated scores and contribution breakdowns.
    
    Parameters:
    -----------
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries
    
    year : str
        The season year in format 'YYYY-YY' (e.g., '2024-25')
    
    date : str
        The cutoff date in 'MM/DD/YYYY' format used for recent performance calculations
    
    star_players : pandas.DataFrame
        DataFrame containing statistics for team star players with columns including:
        'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'PIE', 'E_USG_PCT', 'NET_RATING', 
        'TS_PCT', 'E_TOV_PCT', 'MIN', 'CLUTCH_SCORE_PCT', 'CLUTCH_USAGE_RATE', 
        'CLUTCH_NET_RATING'
    
    key_rotation_players : pandas.DataFrame
        DataFrame containing statistics for key rotation players with the same
        columns as star_players
    
    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing calculated performance metrics for all players with columns:
        - 'PLAYER_NAME': Player name
        - 'PLAYER_ID': Player ID
        - 'SCORE': Overall calculated performance score
        - 'PRIMARY_CONTRIBUTION': Score contribution from primary stats
        - 'SECONDARY_CONTRIBUTION': Score contribution from secondary stats
        - 'CLUTCH_CONTRIBUTION': Score contribution from clutch performance
        - 'RECENT_CONTRIBUTION': Score contribution from recent games
        - 'CATEGORY': Player classification ('Star', 'Rotation', or 'Loser')
        
        The DataFrame is sorted by 'SCORE' in descending order.
    
    Notes:
    ------
    The function calculates player scores using four component metrics:
    
    1. Primary Score - Based on Player Impact Estimate (PIE), Usage Rate, and Net Rating
       Weights:
       - PIE: 17.5%
       - Usage Rate: 12.5%
       - Net Rating: 10% (adjusted based on games played)
    
    2. Secondary Score - Based on True Shooting, Turnover Rate, and Minutes
       Weights:
       - True Shooting %: 12.5%
       - Turnover Rate: 5%
       - Minutes: 15%
    
    3. Clutch Score - Based on clutch scoring, usage, and net rating
       Weights:
       - Clutch Scoring %: 50%
       - Clutch Usage Rate: 25%
       - Clutch Net Rating: 25%
       (All clutch metrics are scaled based on games played)
    
    4. Recent Score - Based on performance in recent games
       (Calculated by the calculate_recent_scores function)
    
    The final score is a weighted combination of these components:
    - Primary + Secondary: 50%
    - Clutch: 30%
    - Recent: 20%
    
    Player categories are determined by minutes played:
    - Star: > 25 minutes
    - Rotation: 15-25 minutes
    - Loser: < 15 minutes
    
    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost:5432/nba_database')
    >>> year = '2024-25'
    >>> date = '03/01/2025'
    >>> star_players_df = pd.read_sql("SELECT * FROM player_stats WHERE MIN > 25", engine)
    >>> rotation_players_df = pd.read_sql("SELECT * FROM player_stats WHERE MIN BETWEEN 15 AND 25", engine)
    >>> player_metrics = calculate_player_metrics(engine, year, date, star_players_df, rotation_players_df)
    >>> print(player_metrics[['PLAYER_NAME', 'SCORE', 'CATEGORY']].head())
"""
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

"""
    Retrieve and calculate performance scores for players on a specific NBA team up to a given date.
    
    This function fetches advanced and clutch statistics for players on the specified team,
    categorizes them based on minutes played, and calculates comprehensive performance scores
    using the calculate_player_metrics function.
    
    Parameters:
    -----------
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries
    
    teamName : str
        The name of the NBA team to analyze (e.g., 'Boston Celtics')
    
    date : str
        The cutoff date in 'MM/DD/YYYY' format to retrieve stats up to
        
    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing calculated performance metrics for the team's players with columns:
        - 'PLAYER_NAME': Player name
        - 'PLAYER_ID': Player ID
        - 'SCORE': Overall calculated performance score
        - 'PRIMARY_CONTRIBUTION': Score contribution from primary stats
        - 'SECONDARY_CONTRIBUTION': Score contribution from secondary stats
        - 'CLUTCH_CONTRIBUTION': Score contribution from clutch performance
        - 'RECENT_CONTRIBUTION': Score contribution from recent games
        - 'CATEGORY': Player classification ('Star', 'Rotation', or 'Loser')
        
        The DataFrame is sorted by 'SCORE' in descending order.
    
    Notes:
    ------
    The function performs the following operations:
    1. Identifies the current NBA season based on the provided date
    2. Retrieves the season's start date from a predefined dictionary
    3. Executes a SQL query to fetch:
       - Advanced player statistics (PIE, TS%, NET_RATING, etc.)
       - Clutch performance statistics
    4. Categorizes players based on minutes played:
       - Star players: > 30 minutes per game
       - Key rotation players: 15-30 minutes per game
    5. Passes these categorized players to calculate_player_metrics to compute performance scores
    
    The advanced statistics are retrieved from the "advanced_player_stats_by_date" table,
    and clutch statistics from the "advanced_player_clutch_stats_by_date" table.
    Only data between the season start date and the specified cutoff date is considered.
    
    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost:5432/nba_database')
    >>> date = '03/01/2025'
    >>> celtics_scores = get_team_player_scores(engine, 'Boston Celtics', date)
    >>> print(celtics_scores[['PLAYER_NAME', 'SCORE', 'CATEGORY']].head())
"""
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