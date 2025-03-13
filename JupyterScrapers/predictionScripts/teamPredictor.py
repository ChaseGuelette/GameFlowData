from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import math
from dotenv import load_dotenv
import os
from nba_api.stats.static import teams


load_dotenv()

yearsList = ['2024-25', '2023-24', '2022-23', '2021-22', '2020-21', '2019-20']

def identify_nba_season(date_str):
    """
    Identifies which NBA season a date falls into.
    NBA seasons typically run from October to June of the following year.
    
    Args:
        date_str (str): Date string in format 'YYYY-MM-DD' or 'YYYY-M-D'
        
    Returns:
        str: Season identifier in format 'YYYY-YY' (e.g., '2024-25')
        None: If the date doesn't fall within any of the listed seasons
    """
    # Available seasons
    years_list = ['2024-25', '2023-24', '2022-23', '2021-22', '2020-21', '2019-20']
    
    # Parse the input date
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        try:
            # Try alternative format if the standard one fails
            date_obj = datetime.strptime(date_str, '%Y-%-m-%-d')
        except ValueError:
            # If both formats fail, try to be more flexible
            parts = date_str.split('-')
            if len(parts) == 3:
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                date_obj = datetime(year, month, day)
            else:
                raise ValueError(f"Unable to parse date: {date_str}. Expected format: YYYY-MM-DD or YYYY-M-D")
    
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
        # Check if we possibly have a season not in the standard format
        full_season = f"{season_start}-{season_end}"
        for listed_season in years_list:
            start, end = listed_season.split('-')
            if len(end) == 2:
                end = f"20{end}"
            if full_season == f"{start}-{end}":
                return listed_season
                
        return None
    
#function for recent form
#offensive and defensice ratings 
#rolling averages for the last 5, 10, 15 game windows 
#@params : teamName= 'Atlanta Hawks', date = '2025-02-20'
def calculate_recent_form_scores(engine, date, teamName):
    year = identify_nba_season(date)
    #print(year)

    games_df = pd.DataFrame()
    
    with engine.connect() as conn:
        with conn.begin():
            query = text(f"""
                with recent_games as (
                	select *, 
                	ROW_NUMBER() OVER (
                		PARTITION BY "TEAM_ID"
                		ORDER BY "GAME_DATE" DESC
                	) as game_number
                from "{year}_historic_game_data"
                where "GAME_DATE" <= :game_date
                )
                select 
                	"TEAM_ID",
                	"TEAM_NAME",
                	"GAME_ID",
                	"GAME_DATE",
                    "TEAM_ABBREVIATION",
                    "FT_PCT"
                from recent_games
                where "TEAM_NAME" = :team_name
                and game_number <= :window
                ORDER BY "GAME_DATE" DESC;
            """)
            query2 = text(f"""
                select * 
                from "{year}_team_advanced_game_data"
                where "GAME_ID" = :game_id
            """)
            recent_games = pd.read_sql_query(query, engine, params={'team_name': teamName, 'window': 15, 'game_date' : date})
            gameIds = recent_games['GAME_ID'].tolist()
            abvs = recent_games['TEAM_ABBREVIATION'].tolist()
            abv = abvs[0]

        #query all the games from the games table: 2024-25_team_advanced_game_data   
        for i in range(15):
            gameId = gameIds[i]
            curr_game = pd.read_sql_query(query2, engine, params={'game_id' : gameId})
            individual_team_df = curr_game.loc[curr_game['TEAM_ABBREVIATION'] == abv]
            games_df = pd.concat([games_df, individual_team_df], ignore_index=True)

    #now we want to calculate rolling averages for the most recent games 
    def calculate_net_rating(games_df):
        netRatingFive = 0
        netRatingTen = 0
        netRatingFifteen = 0
        #5 window
        for index, row in games_df.iloc[0:5].iterrows():
            netRatingFive += row["NET_RATING"]
        netRatingFive = netRatingFive/5
        #print(netRatingFive)
            
        #10 window
        for index, row in games_df.iloc[0:10].iterrows():
            netRatingTen += row["NET_RATING"]
        netRatingTen = netRatingTen/10
        #print(netRatingTen)
    
        #15 window 
        # for index, row in games_df.iloc[0:15].iterrows():
        #     netRatingFifteen += row["NET_RATING"]
        # netRatingFifteen = netRatingFifteen/15
        # print(netRatingFifteen)
    
        netRating = (0.6 * netRatingTen) + (0.4 * netRatingFive)
        print(f"Net Rating Score: {netRating}")
        
        return netRating
        
    def calculate_four_factors(games_df, recent_games):

        #calculate the 4 factors in 5 game range 
        ftr5 = 0
        oreb5 = 0
        dreb5 = 0
        tov5 = 0
        efg5  = 0
        for index, row in games_df.iloc[0:5].iterrows():
            oreb5  += row["OREB_PCT"]
            dreb5  += row["DREB_PCT"]
            tov5 += row["TM_TOV_PCT"]
            efg5  += row["EFG_PCT"]

        for index, row in recent_games.iloc[0:5].iterrows():
            ftr5 += row["FT_PCT"]

        ftr5 = ftr5/5
        oreb5 = oreb5/5
        dreb5 = dreb5/5
        tov5 = tov5/5
        efg5  = efg5/5

        #perform same operation for a 10 game frame 
        ftr10 = 0
        oreb10 = 0
        dreb10 = 0
        tov10 = 0
        efg10 = 0
        for index, row in games_df.iloc[0:10].iterrows():
            oreb10  += row["OREB_PCT"]
            dreb10  += row["DREB_PCT"]
            tov10 += row["TM_TOV_PCT"]
            efg10  += row["EFG_PCT"]

        for index, row in recent_games.iloc[0:10].iterrows():
            ftr10 += row["FT_PCT"]
            
        ftr10 = ftr10/10
        oreb10 = oreb10/10
        dreb10 = dreb10/10
        tov10 = tov10/10
        efg10 = efg10/10
        
        FreeThrowRate = (0.6 * ftr10) + (0.4 * ftr5)
        OREB_PCT = (0.6 * oreb10) + (0.4 * oreb5)
        DREB_PCT = (0.6 * dreb10) + (0.4 * dreb5)
        TOV = ((0.6 * tov10) + (0.4 * tov5))/100
        EFG = (0.6 * efg10) + (0.4 * efg5)
        # print(FreeThrowRate)
        # print(OREB_PCT)
        # print(DREB_PCT)
        # print(TOV)
        # print(EFG)
        fourFactorsScore = (0.6 * EFG) - (0.25*TOV) + ((0.1*OREB_PCT) + (0.1*DREB_PCT)) + (0.15 * FreeThrowRate)
        print(f"Recent Four Factors Score: {fourFactorsScore}")
        return fourFactorsScore
        
    recentNet = calculate_net_rating(games_df)
    recentFour = calculate_four_factors(games_df, recent_games)

    recentFormScore = (0.75 * recentNet) + (0.25 * recentFour)
    
    return recentFormScore
    


#function for 4 factors 
#offensive and defensice ratings 
#rolling averages for the last 5, 10, 15 game windows 
#@params : teamName= 'Atlanta Hawks', date = '2025-02-20'
def calculate_net_four(engine, date, teamName):
    year = identify_nba_season(date)

    games_df = pd.DataFrame()
    
    with engine.connect() as conn:
        with conn.begin():
            query = text(f"""
                 select 
                	"TEAM_ID",
                	"TEAM_NAME",
                	"GAME_ID",
                	"GAME_DATE",
                	"TEAM_ABBREVIATION",
                	"FT_PCT"
                from "{year}_historic_game_data"
                where "TEAM_NAME" = :team_name
                and "GAME_DATE" <= :date
                ORDER BY "GAME_DATE" DESC;
            """)
            query2 = text(f"""
                select * 
                from "{year}_team_advanced_game_data"
                where "GAME_ID" = :game_id
            """)
            recent_games = pd.read_sql_query(query, engine, params={'team_name': teamName, 'date' : date})
            gameIds = recent_games['GAME_ID'].tolist()
            abvs = recent_games['TEAM_ABBREVIATION'].tolist()
            abv = abvs[0]

            for gameId in gameIds:
                #gameId = gameIds[i]
                curr_game = pd.read_sql_query(query2, engine, params={'game_id' : gameId})
                individual_team_df = curr_game.loc[curr_game['TEAM_ABBREVIATION'] == abv]
                games_df = pd.concat([games_df, individual_team_df], ignore_index=True)

    def calculate_net_rating(games_df):
        netRating = 0
        #iterate through all rows to average netRating
        for index, row in games_df.iterrows():
            netRating += row["NET_RATING"]
        netRating = netRating/len(games_df)
        
        print(f"Net Rating Score: {netRating}")
        return netRating

    def calculate_four_factors(games_df, recent_games):
        FreeThrowRate = 0
        OREB_PCT = 0
        DREB_PCT = 0
        TOV = 0
        EFG = 0
        for index, row in games_df.iterrows():
            OREB_PCT += row["OREB_PCT"]
            DREB_PCT += row["DREB_PCT"]
            TOV += row["TM_TOV_PCT"]
            EFG += row["EFG_PCT"]
        
        for index, row in recent_games.iterrows():
            FreeThrowRate += row["FT_PCT"]

        OREB_PCT = OREB_PCT/len(games_df)
        DREB_PCT = DREB_PCT/len(games_df)
        TOV = TOV/len(games_df)
        EFG = EFG/len(games_df)
        FreeThrowRate = FreeThrowRate/len(recent_games)
        
        fourFactorsScore = (0.6 * EFG) - (0.25*TOV) + ((0.1*OREB_PCT) + (0.1*DREB_PCT)) + (0.15 * FreeThrowRate)
        print(f"Recent Four Factors Score: {fourFactorsScore}")
        return fourFactorsScore

    seasonNetRating = calculate_net_rating(games_df)
    seasonFourFactors = calculate_four_factors(games_df, recent_games)
    
    return seasonNetRating, seasonFourFactors
    

def get_game_info_query(year):
    tableName = f"{year}_team_advanced_game_data"
    return text(f"""
                SELECT 
                    a."GAME_ID",
                    
                    -- Team 1 (DET) Columns
                    a."TEAM_ID" AS T1_TEAM_ID,
                    a."TEAM_NAME" AS T1_TEAM_NAME,
                    a."TEAM_ABBREVIATION" AS T1_TEAM_ABBREVIATION,
                    a."TEAM_CITY" AS T1_TEAM_CITY,
                    a."MIN" AS T1_MIN,
                    a."E_OFF_RATING" AS T1_E_OFF_RATING,
                    a."OFF_RATING" AS T1_OFF_RATING,
                    a."E_DEF_RATING" AS T1_E_DEF_RATING,
                    a."DEF_RATING" AS T1_DEF_RATING,
                    a."E_NET_RATING" AS T1_E_NET_RATING,
                    a."NET_RATING" AS T1_NET_RATING,
                    a."AST_PCT" AS T1_AST_PCT,
                    a."AST_TOV" AS T1_AST_TOV,
                    a."AST_RATIO" AS T1_AST_RATIO,
                    a."OREB_PCT" AS T1_OREB_PCT,
                    a."DREB_PCT" AS T1_DREB_PCT,
                    a."REB_PCT" AS T1_REB_PCT,
                    a."E_TM_TOV_PCT" AS T1_E_TM_TOV_PCT,
                    a."TM_TOV_PCT" AS T1_TM_TOV_PCT,
                    a."EFG_PCT" AS T1_EFG_PCT,
                    a."TS_PCT" AS T1_TS_PCT,
                    a."USG_PCT" AS T1_USG_PCT,
                    a."E_USG_PCT" AS T1_E_USG_PCT,
                    a."E_PACE" AS T1_E_PACE,
                    a."PACE" AS T1_PACE,
                    a."PACE_PER40" AS T1_PACE_PER40,
                    a."POSS" AS T1_POSS,
                    a."PIE" AS T1_PIE,
                
                    -- Team 2 (ATL) Columns
                    b."TEAM_ID" AS T2_TEAM_ID,
                    b."TEAM_NAME" AS T2_TEAM_NAME,
                    b."TEAM_ABBREVIATION" AS T2_TEAM_ABBREVIATION,
                    b."TEAM_CITY" AS T2_TEAM_CITY,
                    b."MIN" AS T2_MIN,
                    b."E_OFF_RATING" AS T2_E_OFF_RATING,
                    b."OFF_RATING" AS T2_OFF_RATING,
                    b."E_DEF_RATING" AS T2_E_DEF_RATING,
                    b."DEF_RATING" AS T2_DEF_RATING,
                    b."E_NET_RATING" AS T2_E_NET_RATING,
                    b."NET_RATING" AS T2_NET_RATING,
                    b."AST_PCT" AS T2_AST_PCT,
                    b."AST_TOV" AS T2_AST_TOV,
                    b."AST_RATIO" AS T2_AST_RATIO,
                    b."OREB_PCT" AS T2_OREB_PCT,
                    b."DREB_PCT" AS T2_DREB_PCT,
                    b."REB_PCT" AS T2_REB_PCT,
                    b."E_TM_TOV_PCT" AS T2_E_TM_TOV_PCT,
                    b."TM_TOV_PCT" AS T2_TM_TOV_PCT,
                    b."EFG_PCT" AS T2_EFG_PCT,
                    b."TS_PCT" AS T2_TS_PCT,
                    b."USG_PCT" AS T2_USG_PCT,
                    b."E_USG_PCT" AS T2_E_USG_PCT,
                    b."E_PACE" AS T2_E_PACE,
                    b."PACE" AS T2_PACE,
                    b."PACE_PER40" AS T2_PACE_PER40,
                    b."POSS" AS T2_POSS,
                    b."PIE" AS T2_PIE
                
                FROM "{tableName}" a
                JOIN "{tableName}" b 
                    ON a."GAME_ID" = b."GAME_ID"
                WHERE a."TEAM_ABBREVIATION" = :badAbv 
                  AND b."TEAM_ABBREVIATION" = :goodAbv
              AND a."TEAM_ID" != b."TEAM_ID";
            """)

def get_game_id_query(year):
    return text(f"""
                select 
                	"TEAM_ID",
                	"TEAM_NAME",
                	"GAME_ID",
                	"GAME_DATE",
                	"TEAM_ABBREVIATION"
                from "{year}_historic_game_data"
                where "TEAM_NAME" = :team_name
                and "GAME_ID" = :gameId
            """)
    
#yearsList = ['2024-25', '2023-24', '2022-23', '2021-22', '2020-21', '2019-20']

#historic head to head matchups 
def get_historical_matchups(engine, date, goodTeam, badTeam):
    
    yearsList = ['2024-25', '2023-24', '2022-23', '2021-22', '2020-21', '2019-20']
    year = identify_nba_season(date)

    #tableName = f"{yearsList[0]}_team_advanced_game_data"
    i = 0
    for currYear in yearsList:
        if year == currYear:
            currYearIndex = i
        i += 1
    
    nba_teams = teams.get_teams()
    team_mapping = {}
    
    for team in nba_teams:
        team_mapping[team['full_name']] = team['abbreviation']
    team_mapping['Brooklyn Nets'] = 'BRK'
    team_mapping['Phoenix Suns'] = 'PHO'
    team_mapping['Charlotte Hornets'] = 'CHO'

    goodAbv = team_mapping[goodTeam]
    badAbv = team_mapping[badTeam]
    print(goodAbv)
    print(badAbv)

    games_df = pd.DataFrame()
    
    with engine.connect() as conn:
        with conn.begin():
            query = get_game_info_query(year)
            query2 = get_game_id_query(year)
            
            recent_games = pd.read_sql_query(query, 
                                            engine, params={'badAbv': badAbv, 'goodAbv': goodAbv})
            gameIds = recent_games['GAME_ID'].tolist()

            #get all the games dates
            for gameId in gameIds:
                #gameId = gameIds[i]
                curr_game = pd.read_sql_query(query2, engine, params={'gameId' : gameId, 'team_name' : goodTeam})
                games_df = pd.concat([games_df, curr_game], ignore_index=True)

            valid_games = []
            checked_valid = []
            #check that the date is within our range 
            for index, row in games_df.iterrows():
                validDate = datetime.strptime(date, "%Y-%m-%d")
                currDate = datetime.strptime(row["GAME_DATE"], "%Y-%m-%d")
                if currDate < validDate:
                    valid_games.append(row["GAME_ID"])

            print(valid_games)

            #check if the number of valid games is less than 3 - in which case we need to go 
            #back one year to find more matchups
            games_df2 = pd.DataFrame()
            if len(valid_games) < 3:
                #tableName=f"{yearsList[1]}_team_advanced_game_data"
                year = yearsList[currYearIndex+1]
                query3 = get_game_info_query(year)
                query4 = get_game_id_query(year)
            

                #create a new tableName, and requery on the previous year 
                #tableName = f"{yearsList[1]}_team_advanced_game_data"
                recent_gamesTwo =  pd.read_sql_query(query3, engine, params={'badAbv': badAbv, 'goodAbv': goodAbv})
                gameIdsTwo = recent_gamesTwo['GAME_ID'].tolist()
                
                #get all the games dates for new table
                for gameId in gameIdsTwo:
                    #gameId = gameIds[i]
                    curr_game = pd.read_sql_query(query4, engine, params={'gameId' : gameId, 'team_name' : goodTeam})
                    games_df2 = pd.concat([games_df2, curr_game], ignore_index=True)
                print(games_df2)
                valid_games2 = []
                checked_valid = []
                #check that the date is within our range again
                for index, row in games_df2.iterrows():
                    validDate = datetime.strptime(date, "%Y-%m-%d")
                    currDate = datetime.strptime(row["GAME_DATE"], "%Y-%m-%d")
                    if currDate < validDate:
                        valid_games2.append(row["GAME_ID"])

                #now we have a new valid games table with more games. 
                print(len(valid_games2))
                print(valid_games2)
                #first find how many more we need 
                numNeeded = 3 - len(valid_games)
                # Add the games we already had
                for game in valid_games:
                    checked_valid.append(game)
                
                # Add new games, but only as many as are available up to numNeeded
                for i in range(min(numNeeded, len(valid_games2))):
                    checked_valid.append(valid_games2[i])
                    
            elif len(valid_games) > 3:
                for i in range(3):
                    checked_valid.append(valid_games[i])

            print(checked_valid)

    #ok so at this point we have checked valid, which has all the valid game ids
    #we also have 2 tables, which have corresponding ids for the games that are valid. 
    #so we just need to get the table of valid games 
    final_valid_games_df = pd.DataFrame()
    for gameId in checked_valid:
        #check each game id in the first game df 
        for index, row in recent_games.iterrows():
            if row["GAME_ID"] == gameId:
                final_valid_games_df = pd.concat([final_valid_games_df, pd.DataFrame([row])], ignore_index=True)
        #check each game id in second games df 
        for index, row in recent_gamesTwo.iterrows():
            if row["GAME_ID"] == gameId:
                final_valid_games_df = pd.concat([final_valid_games_df, pd.DataFrame([row])], ignore_index=True)
                    
    print(len(recent_games))
    print(len(final_valid_games_df))
    return final_valid_games_df

def calculate_historic_matchups(engine, date, goodTeam, badTeam, avg_games=3, weighted=False):
    matchups_df = get_historical_matchups(engine, date, goodTeam, badTeam)

    # Initializing cumulative stats
    total_net_rating_diff = 0
    total_efg_diff = 0
    total_ts_pct_diff = 0
    total_ast_tov_diff = 0
    total_tov_pct_diff = 0
    total_oreb_pct_diff = 0
    total_dreb_pct_diff = 0
    total_pace_diff = 0
    total_pie_diff = 0
    
    #positive values = badTeam doing better - team1
    #negative values = goodTeam doing better - team2
    for index, row in matchups_df.iterrows():
        weight = (avg_games - index) if weighted else 1  # Ensure most recent game gets highest weight
        
        total_net_rating_diff += weight * (row['t1_net_rating'] - row['t2_net_rating'])
        total_efg_diff += weight * (row['t1_efg_pct'] - row['t2_efg_pct'])
        total_ts_pct_diff += weight * (row['t1_ts_pct'] - row['t2_ts_pct'])
        total_ast_tov_diff += weight * (row['t1_ast_tov'] - row['t2_ast_tov'])
        total_tov_pct_diff += weight * (row['t1_tm_tov_pct'] - row['t2_tm_tov_pct'])
        total_oreb_pct_diff += weight * (row['t1_oreb_pct'] - row['t2_oreb_pct'])
        total_dreb_pct_diff += weight * (row['t1_dreb_pct'] - row['t2_dreb_pct'])
        total_pace_diff += weight * (row['t1_pace'] - row['t2_pace'])
        total_pie_diff += weight * (row['t1_pie'] - row['t2_pie'])

    normalizer = sum(range(1, avg_games + 1)) if weighted else avg_games
    if normalizer > 0:
        total_net_rating_diff /= normalizer
        total_efg_diff /= normalizer
        total_ts_pct_diff /= normalizer
        total_ast_tov_diff /= normalizer
        total_tov_pct_diff /= normalizer
        total_oreb_pct_diff /= normalizer
        total_dreb_pct_diff /= normalizer
        total_pace_diff /= normalizer
        total_pie_diff /= normalizer
        
        
    # Return results in a structured format
    return {
        'net_rating_diff': total_net_rating_diff,
        'efg_diff': total_efg_diff,
        'true_shooting_pct_diff': total_ts_pct_diff,
        'assist_to_turnover_pct_diff': total_ast_tov_diff,
        'turnover_pct_diff': total_tov_pct_diff,
        'oreb_pct_diff': total_oreb_pct_diff,
        'dreb_pct_diff': total_dreb_pct_diff,
        'pace_diff': total_pace_diff,
        'pie_diff': total_pie_diff }


DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
pd.set_option('display.max_columns', 58)

calculate_historic_matchups(engine, '2025-1-20', 'Atlanta Hawks', 'Detroit Pistons', 3, True)

teamNetRating, seasonFourFactors = calculate_net_four(engine, '2025-01-20', 'Atlanta Hawks')

recentFormScore = calculate_recent_form_scores(engine, '2024-12-12', 'Atlanta Hawks')
print(f"This is the recent form Score: {recentFormScore}")