from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from nba_api.stats.static import teams
from sqlalchemy import text

load_dotenv()

yearsList = ["2024-25", "2023-24", "2022-23", "2021-22", "2020-21", "2019-20"]

"""
    Identifies which NBA season a date falls into.
    NBA seasons typically run from October to June of the following year.

    Args:
        date_str (str): Date string in format 'YYYY-MM-DD' or 'YYYY-M-D'

    Returns:
        str: Season identifier in format 'YYYY-YY' (e.g., '2024-25')
        None: If the date doesn't fall within any of the listed seasons
"""


def identify_nba_season(date_str):
    # Available seasons
    years_list = ["2024-25", "2023-24", "2022-23", "2021-22", "2020-21", "2019-20"]

    # Parse the input date
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            # Try alternative format if the standard one fails
            date_obj = datetime.strptime(date_str, "%Y-%-m-%-d")
        except ValueError:
            # If both formats fail, try to be more flexible
            parts = date_str.split("-")
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
            start, end = listed_season.split("-")
            if len(end) == 2:
                end = f"20{end}"
            if full_season == f"{start}-{end}":
                return listed_season

        return None


"""
    Calculate a team's recent form score based on their performance in the last 15 games.

    This function analyzes a team's recent performance by calculating rolling averages
    of net rating and the four factors (shooting, turnovers, rebounding, free throws)
    across 5-game and 10-game windows, then combines them into a weighted recent form score.

    Parameters:
    -----------
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries

    date : str
        The cutoff date in format 'YYYY-MM-DD' or 'MM/DD/YYYY' to retrieve games before

    teamName : str
        The name of the NBA team to analyze (e.g., 'Atlanta Hawks')

    Returns:
    --------
    float
        The calculated recent form score representing the team's performance in recent games.
        Higher values indicate better recent performance.

    Notes:
    ------
    The function calculates two primary components:

    1. Net Rating Score (75% of final score)
       - Calculates average net rating over 5-game and 10-game windows
       - Weights: 5-game (40%), 10-game (60%)

    2. Four Factors Score (25% of final score)
       Components with weights:
       - Effective Field Goal % (60%)
       - Turnover % (-25%, negative impact)
       - Rebounding % (20%, combined offensive and defensive)
       - Free Throw % (15%)

    Each Four Factors component is also calculated using weighted 5-game (40%)
    and 10-game (60%) averages.

    The function retrieves game data from two tables:
    - "{year}_historic_game_data" - Basic game information and free throw percentage
    - "{year}_team_advanced_game_data" - Advanced statistics including net rating and four factors

    The NBA season year is automatically identified based on the provided date.

    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost:5432/nba_database')
    >>> date = '2025-02-20'
    >>> hawks_form = calculate_recent_form_scores(engine, date, 'Atlanta Hawks')
    >>> print(f"Atlanta Hawks recent form score: {hawks_form:.2f}")
    Atlanta Hawks recent form score: 3.85
"""


def calculate_recent_form_scores(engine, date, teamName):
    year = identify_nba_season(date)
    # print(year)

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
            recent_games = pd.read_sql_query(
                query, engine, params={"team_name": teamName, "window": 15, "game_date": date}
            )
            gameIds = recent_games["GAME_ID"].tolist()
            abvs = recent_games["TEAM_ABBREVIATION"].tolist()
            abv = abvs[0]

        # query all the games from the games table: 2024-25_team_advanced_game_data
        for i in range(15):
            gameId = gameIds[i]
            curr_game = pd.read_sql_query(query2, engine, params={"game_id": gameId})
            individual_team_df = curr_game.loc[curr_game["TEAM_ABBREVIATION"] == abv]
            games_df = pd.concat([games_df, individual_team_df], ignore_index=True)

    # now we want to calculate rolling averages for the most recent games
    def calculate_net_rating(games_df):
        netRatingFive = 0
        netRatingTen = 0
        # 5 window
        for index, row in games_df.iloc[0:5].iterrows():
            netRatingFive += row["NET_RATING"]
        netRatingFive = netRatingFive / 5
        # print(netRatingFive)

        # 10 window
        for index, row in games_df.iloc[0:10].iterrows():
            netRatingTen += row["NET_RATING"]
        netRatingTen = netRatingTen / 10
        # print(netRatingTen)

        # 15 window
        # for index, row in games_df.iloc[0:15].iterrows():
        #     netRatingFifteen += row["NET_RATING"]
        # netRatingFifteen = netRatingFifteen/15
        # print(netRatingFifteen)

        netRating = (0.6 * netRatingTen) + (0.4 * netRatingFive)
        print(f"Net Rating Score: {netRating}")

        return netRating

    def calculate_four_factors(games_df, recent_games):
        # calculate the 4 factors in 5 game range
        ftr5 = 0
        oreb5 = 0
        dreb5 = 0
        tov5 = 0
        efg5 = 0
        for index, row in games_df.iloc[0:5].iterrows():
            oreb5 += row["OREB_PCT"]
            dreb5 += row["DREB_PCT"]
            tov5 += row["TM_TOV_PCT"]
            efg5 += row["EFG_PCT"]

        for index, row in recent_games.iloc[0:5].iterrows():
            ftr5 += row["FT_PCT"]

        ftr5 = ftr5 / 5
        oreb5 = oreb5 / 5
        dreb5 = dreb5 / 5
        tov5 = tov5 / 5
        efg5 = efg5 / 5

        # perform same operation for a 10 game frame
        ftr10 = 0
        oreb10 = 0
        dreb10 = 0
        tov10 = 0
        efg10 = 0
        for index, row in games_df.iloc[0:10].iterrows():
            oreb10 += row["OREB_PCT"]
            dreb10 += row["DREB_PCT"]
            tov10 += row["TM_TOV_PCT"]
            efg10 += row["EFG_PCT"]

        for index, row in recent_games.iloc[0:10].iterrows():
            ftr10 += row["FT_PCT"]

        ftr10 = ftr10 / 10
        oreb10 = oreb10 / 10
        dreb10 = dreb10 / 10
        tov10 = tov10 / 10
        efg10 = efg10 / 10

        FreeThrowRate = (0.6 * ftr10) + (0.4 * ftr5)
        OREB_PCT = (0.6 * oreb10) + (0.4 * oreb5)
        DREB_PCT = (0.6 * dreb10) + (0.4 * dreb5)
        TOV = ((0.6 * tov10) + (0.4 * tov5)) / 100
        EFG = (0.6 * efg10) + (0.4 * efg5)
        # print(FreeThrowRate)
        # print(OREB_PCT)
        # print(DREB_PCT)
        # print(TOV)
        # print(EFG)
        fourFactorsScore = (0.6 * EFG) - (0.25 * TOV) + ((0.1 * OREB_PCT) + (0.1 * DREB_PCT)) + (0.15 * FreeThrowRate)
        print(f"Recent Four Factors Score: {fourFactorsScore}")
        return fourFactorsScore

    recentNet = calculate_net_rating(games_df)
    recentFour = calculate_four_factors(games_df, recent_games)

    recentFormScore = (0.75 * recentNet) + (0.25 * recentFour)

    return recentFormScore


"""
    Calculate a team's season-long Net Rating and Four Factors metrics.

    This function retrieves all games for a specified team up to a given date and calculates
    the average Net Rating and Four Factors score across those games. Unlike the
    calculate_recent_form_scores function, this calculates averages across all games in the
    season rather than using weighted recent windows.

    Parameters:
    -----------
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries

    date : str
        The cutoff date in format 'YYYY-MM-DD' or 'MM/DD/YYYY' to retrieve games before

    teamName : str
        The name of the NBA team to analyze (e.g., 'Atlanta Hawks')

    Returns:
    --------
    tuple : (float, float)
        A tuple containing two values:
        - seasonNetRating : Average Net Rating across all games
        - seasonFourFactors : Calculated Four Factors score across all games

        Higher values for both metrics indicate better team performance.

    Notes:
    ------
    The function calculates two primary components:

    1. Net Rating
       - Simple average of Net Rating across all games in the season up to the specified date

    2. Four Factors Score
       Components with weights:
       - Effective Field Goal % (60%)
       - Turnover % (-25%, negative impact)
       - Rebounding % (20%, combined offensive and defensive)
       - Free Throw % (15%)

    The function retrieves game data from two tables:
    - "{year}_historic_game_data" - Basic game information and free throw percentage
    - "{year}_team_advanced_game_data" - Advanced statistics including net rating and four factors

    The NBA season year is automatically identified based on the provided date.

    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost:5432/nba_database')
    >>> date = '2025-02-20'
    >>> net_rating, four_factors = calculate_net_four(engine, date, 'Atlanta Hawks')
    >>> print(f"Atlanta Hawks season metrics - Net Rating: {net_rating:.2f}, Four Factors: {four_factors:.2f}")
    Atlanta Hawks season metrics - Net Rating: 2.75, Four Factors: 3.12
"""


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
            recent_games = pd.read_sql_query(query, engine, params={"team_name": teamName, "date": date})
            gameIds = recent_games["GAME_ID"].tolist()
            abvs = recent_games["TEAM_ABBREVIATION"].tolist()
            abv = abvs[0]

            for gameId in gameIds:
                # gameId = gameIds[i]
                curr_game = pd.read_sql_query(query2, engine, params={"game_id": gameId})
                individual_team_df = curr_game.loc[curr_game["TEAM_ABBREVIATION"] == abv]
                games_df = pd.concat([games_df, individual_team_df], ignore_index=True)

    def calculate_net_rating(games_df):
        netRating = 0
        # iterate through all rows to average netRating
        for index, row in games_df.iterrows():
            netRating += row["NET_RATING"]
        netRating = netRating / len(games_df)

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

        OREB_PCT = OREB_PCT / len(games_df)
        DREB_PCT = DREB_PCT / len(games_df)
        TOV = TOV / len(games_df)
        EFG = EFG / len(games_df)
        FreeThrowRate = FreeThrowRate / len(recent_games)

        fourFactorsScore = (0.6 * EFG) - (0.25 * TOV) + ((0.1 * OREB_PCT) + (0.1 * DREB_PCT)) + (0.15 * FreeThrowRate)
        print(f"Recent Four Factors Score: {fourFactorsScore}")
        return fourFactorsScore

    seasonNetRating = calculate_net_rating(games_df)
    seasonFourFactors = calculate_four_factors(games_df, recent_games)

    return seasonNetRating, seasonFourFactors


"""
    Generate a SQL query to retrieve detailed game information for a matchup between two teams.

    This function creates a parameterized SQL query that joins a team's advanced game data
    with itself to return statistics for both teams in a specific matchup. The query prefixes
    all columns with T1_ for the first team and T2_ for the second team for clear distinction.

    Parameters:
    -----------
    year : str
        The NBA season year in format 'YYYY-YY' (e.g., '2024-25') used to determine
        the database table name

    Returns:
    --------
    sqlalchemy.sql.elements.TextClause
        A parameterized SQL query that can be executed with parameters:
        - badAbv : Team abbreviation for the first team (e.g., 'DET')
        - goodAbv : Team abbreviation for the second team (e.g., 'ATL')

    Notes:
    ------
    The query returns a comprehensive set of advanced metrics for both teams including:
    - Team identification (ID, name, abbreviation, city)
    - Offensive metrics (offensive rating, effective field goal %, true shooting %)
    - Defensive metrics (defensive rating)
    - Efficiency metrics (net rating, assist %, turnover %)
    - Rebounding metrics (offensive rebound %, defensive rebound %, total rebound %)
    - Pace metrics (possessions, pace)
    - Overall impact metrics (PIE - Player Impact Estimate)

    The query uses the table named "{year}_team_advanced_game_data" and requires two
    parameters when executed:
    - badAbv: The abbreviation of the first team
    - goodAbv: The abbreviation of the second team

    The function name suggests the first team might be performing worse than the second,
    but the query simply distinguishes them as T1 and T2 in the results.
"""


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


"""
    Retrieve historical matchup data between two NBA teams up to a specified date.

    This function searches for recent head-to-head games between the specified teams,
    looking across the current season and potentially previous seasons if needed to find
    at least 3 matchups (or as many as are available). It returns detailed advanced statistics
    for each matchup found.

    Parameters:
    -----------
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries

    date : str
        The cutoff date in format 'YYYY-MM-DD' to retrieve games before

    goodTeam : str
        The full name of the first NBA team (e.g., 'Atlanta Hawks')

    badTeam : str
        The full name of the second NBA team (e.g., 'Detroit Pistons')

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing detailed advanced statistics for the most recent head-to-head
        matchups between the specified teams, with a maximum of 3 games. Each row represents
        one game with columns prefixed with T1_ for the first team (badTeam) and T2_ for the
        second team (goodTeam).

    Notes:
    ------
    The function performs the following operations:

    1. Identifies the current NBA season based on the provided date
    2. Converts team names to their abbreviations using a mapping
    3. Searches for matchups in the current season's data
    4. If fewer than 3 games are found, looks in the previous season's data
    5. Collects up to 3 of the most recent games before the cutoff date

    The function searches across seasons from 2019-20 through 2024-25, starting with
    the current season determined by the provided date.

    Team abbreviation mappings include special cases for:
    - Brooklyn Nets (BRK)
    - Phoenix Suns (PHO)
    - Charlotte Hornets (CHO)

    The function uses two helper functions:
    - get_game_info_query: To retrieve advanced game statistics
    - get_game_id_query: To retrieve game dates and IDs

    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost:5432/nba_database')
    >>> date = '2025-02-20'
    >>> matchups = get_historical_matchups(engine, date, 'Atlanta Hawks', 'Detroit Pistons')
    >>> print(f"Found {len(matchups)} historical matchups")
    >>> for i, game in matchups.iterrows():
    >>>     print(f"Game ID: {game['GAME_ID']}")
    >>>     print(f"{game['T1_TEAM_NAME']} vs {game['T2_TEAM_NAME']}")"
"""


def get_historical_matchups(engine, date, goodTeam, badTeam):
    yearsList = ["2024-25", "2023-24", "2022-23", "2021-22", "2020-21", "2019-20"]
    year = identify_nba_season(date)

    # tableName = f"{yearsList[0]}_team_advanced_game_data"
    i = 0
    for currYear in yearsList:
        if year == currYear:
            currYearIndex = i
        i += 1

    nba_teams = teams.get_teams()
    team_mapping = {}

    for team in nba_teams:
        team_mapping[team["full_name"]] = team["abbreviation"]
    team_mapping["Brooklyn Nets"] = "BRK"
    team_mapping["Phoenix Suns"] = "PHO"
    team_mapping["Charlotte Hornets"] = "CHO"

    goodAbv = team_mapping[goodTeam]
    badAbv = team_mapping[badTeam]
    print(goodAbv)
    print(badAbv)

    games_df = pd.DataFrame()

    with engine.connect() as conn:
        with conn.begin():
            query = get_game_info_query(year)
            query2 = get_game_id_query(year)

            recent_games = pd.read_sql_query(query, engine, params={"badAbv": badAbv, "goodAbv": goodAbv})
            gameIds = recent_games["GAME_ID"].tolist()

            # get all the games dates
            for gameId in gameIds:
                # gameId = gameIds[i]
                curr_game = pd.read_sql_query(query2, engine, params={"gameId": gameId, "team_name": goodTeam})
                games_df = pd.concat([games_df, curr_game], ignore_index=True)

            valid_games = []
            checked_valid = []
            # check that the date is within our range
            for index, row in games_df.iterrows():
                validDate = datetime.strptime(date, "%Y-%m-%d")
                currDate = datetime.strptime(row["GAME_DATE"], "%Y-%m-%d")
                if currDate < validDate:
                    valid_games.append(row["GAME_ID"])

            print(valid_games)

            # check if the number of valid games is less than 3 - in which case we need to go
            # back one year to find more matchups
            games_df2 = pd.DataFrame()
            if len(valid_games) < 3:
                # tableName=f"{yearsList[1]}_team_advanced_game_data"
                year = yearsList[currYearIndex + 1]
                query3 = get_game_info_query(year)
                query4 = get_game_id_query(year)

                # create a new tableName, and requery on the previous year
                # tableName = f"{yearsList[1]}_team_advanced_game_data"
                recent_gamesTwo = pd.read_sql_query(query3, engine, params={"badAbv": badAbv, "goodAbv": goodAbv})
                gameIdsTwo = recent_gamesTwo["GAME_ID"].tolist()

                # get all the games dates for new table
                for gameId in gameIdsTwo:
                    # gameId = gameIds[i]
                    curr_game = pd.read_sql_query(query4, engine, params={"gameId": gameId, "team_name": goodTeam})
                    games_df2 = pd.concat([games_df2, curr_game], ignore_index=True)
                print(games_df2)
                valid_games2 = []
                checked_valid = []
                # check that the date is within our range again
                for index, row in games_df2.iterrows():
                    validDate = datetime.strptime(date, "%Y-%m-%d")
                    currDate = datetime.strptime(row["GAME_DATE"], "%Y-%m-%d")
                    if currDate < validDate:
                        valid_games2.append(row["GAME_ID"])

                # now we have a new valid games table with more games.
                print(len(valid_games2))
                print(valid_games2)
                # first find how many more we need
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

    # ok so at this point we have checked valid, which has all the valid game ids
    # we also have 2 tables, which have corresponding ids for the games that are valid.
    # so we just need to get the table of valid games
    final_valid_games_df = pd.DataFrame()
    for gameId in checked_valid:
        # check each game id in the first game df
        for index, row in recent_games.iterrows():
            if row["GAME_ID"] == gameId:
                final_valid_games_df = pd.concat([final_valid_games_df, pd.DataFrame([row])], ignore_index=True)
        # check each game id in second games df
        for index, row in recent_gamesTwo.iterrows():
            if row["GAME_ID"] == gameId:
                final_valid_games_df = pd.concat([final_valid_games_df, pd.DataFrame([row])], ignore_index=True)

    print(len(recent_games))
    print(len(final_valid_games_df))
    return final_valid_games_df


"""
    Retrieve historical matchup data between two NBA teams up to a specified date.

    This function searches for recent head-to-head games between the specified teams,
    looking across the current season and potentially previous seasons if needed to find
    at least 3 matchups (or as many as are available). It returns detailed advanced statistics
    for each matchup found.

    Parameters:
    -----------
    engine : SQLAlchemy Engine
        Database connection engine to execute SQL queries

    date : str
        The cutoff date in format 'YYYY-MM-DD' to retrieve games before

    goodTeam : str
        The full name of the first NBA team (e.g., 'Atlanta Hawks')

    badTeam : str
        The full name of the second NBA team (e.g., 'Detroit Pistons')

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing detailed advanced statistics for the most recent head-to-head
        matchups between the specified teams, with a maximum of 3 games. Each row represents
        one game with columns prefixed with T1_ for the first team (badTeam) and T2_ for the
        second team (goodTeam).

    Notes:
    ------
    The function performs the following operations:

    1. Identifies the current NBA season based on the provided date
    2. Converts team names to their abbreviations using a mapping
    3. Searches for matchups in the current season's data
    4. If fewer than 3 games are found, looks in the previous season's data
    5. Collects up to 3 of the most recent games before the cutoff date

    The function searches across seasons from 2019-20 through 2024-25, starting with
    the current season determined by the provided date.

    Team abbreviation mappings include special cases for:
    - Brooklyn Nets (BRK)
    - Phoenix Suns (PHO)
    - Charlotte Hornets (CHO)

    The function uses two helper functions:
    - get_game_info_query: To retrieve advanced game statistics
    - get_game_id_query: To retrieve game dates and IDs

    Example:
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('postgresql://user:password@localhost:5432/nba_database')
    >>> date = '2025-02-20'
    >>> matchups = get_historical_matchups(engine, date, 'Atlanta Hawks', 'Detroit Pistons')
    >>> print(f"Found {len(matchups)} historical matchups")
    >>> for i, game in matchups.iterrows():
    >>>     print(f"Game ID: {game['GAME_ID']}")
    >>>     print(f"{game['T1_TEAM_NAME']} vs {game['T2_TEAM_NAME']}")"
"""


def calculate_historical_matchups(engine, date, goodTeam, badTeam, avg_games=3, weighted=False):
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

    # positive values = badTeam doing better - team1
    # negative values = goodTeam doing better - team2
    for index, row in matchups_df.iterrows():
        weight = (avg_games - index) if weighted else 1  # Ensure most recent game gets highest weight

        total_net_rating_diff += weight * (row["t1_net_rating"] - row["t2_net_rating"])
        total_efg_diff += weight * (row["t1_efg_pct"] - row["t2_efg_pct"])
        total_ts_pct_diff += weight * (row["t1_ts_pct"] - row["t2_ts_pct"])
        total_ast_tov_diff += weight * (row["t1_ast_tov"] - row["t2_ast_tov"])
        total_tov_pct_diff += weight * (row["t1_tm_tov_pct"] - row["t2_tm_tov_pct"])
        total_oreb_pct_diff += weight * (row["t1_oreb_pct"] - row["t2_oreb_pct"])
        total_dreb_pct_diff += weight * (row["t1_dreb_pct"] - row["t2_dreb_pct"])
        total_pace_diff += weight * (row["t1_pace"] - row["t2_pace"])
        total_pie_diff += weight * (row["t1_pie"] - row["t2_pie"])

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
        "net_rating_diff": total_net_rating_diff,
        "efg_diff": total_efg_diff,
        "true_shooting_pct_diff": total_ts_pct_diff,
        "assist_to_turnover_pct_diff": total_ast_tov_diff,
        "turnover_pct_diff": total_tov_pct_diff,
        "oreb_pct_diff": total_oreb_pct_diff,
        "dreb_pct_diff": total_dreb_pct_diff,
        "pace_diff": total_pace_diff,
        "pie_diff": total_pie_diff,
    }


# DATABASE_URL = os.getenv("DATABASE_URL")
# engine = create_engine(DATABASE_URL)
# pd.set_option('display.max_columns', 58)

# calculate_historic_matchups(engine, '2025-1-20', 'Atlanta Hawks', 'Detroit Pistons', 3, True)

# teamNetRating, seasonFourFactors = calculate_net_four(engine, '2025-01-20', 'Atlanta Hawks')

# recentFormScore = calculate_recent_form_scores(engine, '2024-12-12', 'Atlanta Hawks')
# print(f"This is the recent form Score: {recentFormScore}")
