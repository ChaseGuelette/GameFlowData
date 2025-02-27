#lets start with individual player metrics. 
from sqlalchemy import create_engine, text
import pandas as pd
import math

def calculate_recent_scores(player_data):
    player_id = player_data['PLAYER_ID']
    name = player_data['PLAYER_NAME']
        
    #create our engine for creating sql entries - this wont work right now 
    engine = create_engine('postgresql://chase:yourpassword@localhost:5433/TeamData')
        
    with engine.connect() as conn:
        with conn.begin():
            query10 = text("""
                with recent_games as (
                	select *,
                		ROW_NUMBER() OVER (
                			PARTITION BY "PLAYER_ID"
                			ORDER BY "GAME_DATE" DESC
                		) as game_number
                	from "all_player_game_stats"
                )
                select
                	"PLAYER_ID",
                    AVG("PTS") as "AVG_PTS",
                    AVG("PLUS_MINUS") as "AVG_PLUS_MINUS",
                    AVG("MIN") as "AVG_MIN",
                    AVG("FGA") as "AVG_FGA", 
                    AVG("FTA") as "AVG_FTA",
                    AVG("TOV") as "AVG_TOV",
                    AVG("OREB") as "AVG_OREB",
                    COUNT(*) as "GAMES_PLAYED"
                from recent_games
                where "PLAYER_ID" = :player_id
                and game_number <= 10
                GROUP BY "PLAYER_ID"
            """)
            query5 = text("""
                with recent_games as (
                	select *,
                		ROW_NUMBER() OVER (
                			PARTITION BY "PLAYER_ID"
                			ORDER BY "GAME_DATE" DESC
                		) as game_number
                	from "all_player_game_stats"
                )
                select
                	"PLAYER_ID",
                    AVG("PTS") as "AVG_PTS",
                    AVG("PLUS_MINUS") as "AVG_PLUS_MINUS",
                    AVG("MIN") as "AVG_MIN",
                    AVG("FGA") as "AVG_FGA", 
                    AVG("FTA") as "AVG_FTA",
                    AVG("TOV") as "AVG_TOV",
                    AVG("OREB") as "AVG_OREB",
                    COUNT(*) as "GAMES_PLAYED"
                from recent_games
                where "PLAYER_ID" = :player_id
                and game_number <= 5
                GROUP BY "PLAYER_ID"
            """)
            recent_games = pd.read_sql_query(query10, engine, params={'player_id': player_id})
            more_recent_games = pd.read_sql_query(query5, engine, params={'player_id': player_id})

    #now that we have the recent games for this player, calculate net rating
    possessions = 0.96 * recent_games['AVG_FGA'].values[0] + 0.44 * recent_games['AVG_FTA'].values[0] - recent_games['AVG_OREB'].values[0] + recent_games['AVG_TOV'].values[0]

    offensive_rating = (recent_games['AVG_PTS'].values[0] / possessions) 
    defensive_rating = (recent_games['AVG_PTS'].values[0] - recent_games['AVG_PLUS_MINUS'].values[0]) / possessions
    net_ratingTen = offensive_rating - defensive_rating

    #-----------------------------
    possessionsFive = 0.96 * more_recent_games['AVG_FGA'].values[0] + 0.44 * more_recent_games['AVG_FTA'].values[0] - more_recent_games['AVG_OREB'].values[0] + more_recent_games['AVG_TOV'].values[0]

    offensive_ratingFive = (more_recent_games['AVG_PTS'].values[0] / possessions) 
    defensive_ratingFive = (more_recent_games['AVG_PTS'].values[0] - more_recent_games['AVG_PLUS_MINUS'].values[0]) / possessions
    net_ratingFive= offensive_rating - defensive_rating

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

def calculate_player_metrics(star_players, key_rotation_players):
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

    def calculate_primary_scores(player_data):
        score = 0
        score += float(player_data['PIE']) * primary_weights['PIE']
        score += float(player_data['E_USG_PCT']) * primary_weights['USG']
        
        #adjust the net rating based on how many games have been played
        netRating = player_data['NET_RATING'] * primary_weights['NetRating']
        GP = player_data['GP']
        a = 0.75
        #this is the adjusted net rating based on number of games played
        #note that when we make this for a full team performance, we need to get the number of games the team has played
        netRating = netRating * (GP/54) ** a
        score += netRating
        return score
    
    def calculate_secondary_scores(player_data):
        score = 0
        score += float(player_data['TS_PCT']) * secondary_weights['TrueShooting']
        score += float(player_data['E_TOV_PCT']) * secondary_weights['TurnoverRate']
        #player minutes should be split between trends in recent games and season averages
        score += player_data['MIN'] * secondary_weights['Mins'] #in the future we want to make thier average just part of it
        
        # score += availability * secondary_weights['AVAILABILITY']
        return score

    def calculate_clutch_scores(player_data):
        score = 0
        #get raw stats
        scorePct = float(player_data['CLUTCH_SCORE_PCT']) * clutch_weights['CLUTCH_SCORE_PCT']
        usgRate = float(player_data['CLUTCH_USAGE_RATE']) * clutch_weights['CLUTCH_USAGE_RATE']
        netRating = float(player_data['CLUTCH_NET_RATING']) * clutch_weights['CLUTCH_NET_RATING']
        #setup adjustment constants 
        GP = player_data['GP']
        a = 0.75
        #scale scores to avoid small sample outliers 
        score += scorePct * (GP/54) ** a
        score += usgRate * (GP/54) ** a
        score += netRating * (GP/54) ** a

        if math.isnan(score):
            return 0
        else:
            return score
    
    def calculate_player_scores(players_df, baseWeight = 0.5, clutchWeight = 0.3, recentWeight = 0.2):
        scores = []
        for index, player in players_df.iterrows():
            
            
            primaryScore = calculate_primary_scores(player)
            secondaryScore = calculate_secondary_scores(player)
            clutchScore = calculate_clutch_scores(player)
            recentScore = calculate_recent_scores(player)
    
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
                'CATEGORY': 'Star' if player['MIN'] > 25 else 'Rotation'
            })
    
        return pd.DataFrame(scores)
            
    
    starScores = calculate_player_scores(star_players)
    rotationScores = calculate_player_scores(key_rotation_players)
    
    allScores = pd.concat([starScores, rotationScores])
    allScores = allScores.sort_values('SCORE', ascending=False)
    
    return allScores


def get_team_player_scores(teamName):
    #create our engine for creating sql entries - this wont work right now 
    engine = create_engine('postgresql://chase:yourpassword@localhost:5433/TeamData')
    
    team_name = teamName
    
    #retreive players we want stats for 
    #take those players stats from the advanced table
    with engine.connect() as conn:
        with conn.begin():
            queries = {
                'hawks_roster_estimates': """
                	SELECT 
                    	t."TEAM_NAME",
                    	p.*,
                    	c."CLUTCH_SCORE_PCT",
                    	c."CLUTCH_USAGE_RATE",
                    	c."CLUTCH_NET_RATING",
                    	m."GP" as TOTAL_GAMES,
                    	m."MIN" as TOTAL_MINUTES,
                    	m."PTS" as AVG_POINTS,
                    	m."AST" as AVG_ASSISTS
                    FROM "all_teams_misc_stats" t
                    JOIN "all_players_estimated_stats" p
                    	ON t."TEAM_ID" = p."TEAM_ID"
                    LEFT JOIN "player_clutch_stats" c
                    	ON p."PLAYER_ID" = c."PLAYER_ID"
                    LEFT JOIN "all_players_misc_stats" m
                    	ON p."PLAYER_ID" = m."PLAYER_ID"
                    WHERE t."TEAM_NAME" = :team_name
    
                """,
                'hawks_roster_advanced': """
                	SELECT 
                    	t."TEAM_NAME",
                    	p.*,
                    	c."CLUTCH_SCORE_PCT",
                    	c."CLUTCH_USAGE_RATE",
                    	c."CLUTCH_NET_RATING",
                    	m."GP" as TOTAL_GAMES,
                    	m."MIN" as TOTAL_MINUTES
                    FROM "all_teams_misc_stats" t
                    JOIN "all_players_advanced_stats" p
                    	ON t."TEAM_ID" = p."TEAM_ID"
                    LEFT JOIN "player_clutch_stats" c
                    	ON p."PLAYER_ID" = c."PLAYER_ID"
                    LEFT JOIN "all_players_misc_stats" m
                    	ON p."PLAYER_ID" = m."PLAYER_ID"
                    WHERE t."TEAM_NAME" = :team_name
                """
            }
            #starters
            dfs = {name: pd.read_sql_query(text(query), engine, params={'team_name': team_name})
                  for name, query in queries.items()}
            #players with over 25 minutes of game time
            dfs['all_star_player'] = dfs['hawks_roster_advanced'][
                (dfs['hawks_roster_advanced']['MIN'] > 25)]
            
            #rotation players 
            dfs['key_rotation_player'] = dfs['hawks_roster_advanced'][
                (dfs['hawks_roster_advanced']['MIN'] > 15) & 
                (dfs['hawks_roster_advanced']['MIN'] < 25)]
    
    #get high impact stats - player impact rating, net rating, usg pct, plus/minus adjsuted for minutes
    #get efficiency stats - true shooting, turnover rate 
    #get availability - least important
    
    #stats for all star players
    dfs['all_star_estimates'] = pd.merge(
        dfs['hawks_roster_estimates'],
        dfs['all_star_player'][['PLAYER_ID','PLAYER_NAME', 'PIE', 'TS_PCT', 'MIN', 'NET_RATING', 'GP', 'CLUTCH_SCORE_PCT', 'CLUTCH_USAGE_RATE', 'CLUTCH_NET_RATING']]#these are the columns we are adding from all_star_player
    )[['PLAYER_ID', 'PLAYER_NAME', 'PIE', 'E_NET_RATING', 'E_USG_PCT', 'TS_PCT', 'E_TOV_PCT', 'MIN', 'NET_RATING', 'GP', 'CLUTCH_SCORE_PCT', 'CLUTCH_USAGE_RATE', 'CLUTCH_NET_RATING']] #these are the columns we are keeping from the final dataframe
    
    #stats for key rotation players
    dfs['key_rotation_estimates'] = pd.merge(
        dfs['hawks_roster_estimates'],
        dfs['key_rotation_player'][['PLAYER_ID', 'PLAYER_NAME', 'PIE', 'TS_PCT', 'MIN', 'NET_RATING', 'GP', 'CLUTCH_SCORE_PCT', 'CLUTCH_USAGE_RATE', 'CLUTCH_NET_RATING']]
    )[['PLAYER_ID', 'PLAYER_NAME', 'PIE', 'E_NET_RATING', 'E_USG_PCT', 'TS_PCT', 'E_TOV_PCT', 'MIN', 'NET_RATING', 'GP', 'CLUTCH_SCORE_PCT', 'CLUTCH_USAGE_RATE', 'CLUTCH_NET_RATING']]
    
    #apply weights to key points 
    #star players 1x weight, key rotation .75 weight 
    #bench .5 weight 
    
    star_players = dfs['all_star_estimates']
    key_rotation_players = dfs['key_rotation_estimates']

    player_scores = calculate_player_metrics(star_players, key_rotation_players)
    return player_scores