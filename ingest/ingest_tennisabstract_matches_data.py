from ingest.utils.functions.sql import (
    create_connection,
    get_table_column_list,
    ingest_df_to_sql,
)
from ingest.utils.functions.tennisabstract.matches import (
    get_match_data_scraped,
    get_match_data_url,
    get_match_point_data_scraped,
    get_match_url_list as get_match_url_list_tennisabstract,
)
from ingest.utils.functions.tennisabstract.players import (
    create_player_url,
    scrape_player_data,
)
from ingest.utils.functions.tennisabstract.tournaments import (
    create_tournament_url,
    get_tournament_data_scraped,
)
import logging
import os
import pandas as pd

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # set 'global' constants
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    matches_bulk_load_flag = True

    # set constants for match data
    matches_target_table_name = 'tennisabstract_matches'
    matches_temp_table_name = matches_target_table_name
    matches_unique_column_list = ['match_url',]

    # set constants for match point data
    match_points_target_table_name = 'tennisabstract_match_points'
    match_points_temp_table_name = match_points_target_table_name
    match_points_unique_column_list = ['match_url', 'point_number',]

    # set constants for player data
    players_target_table_name = 'tennisabstract_players'
    players_temp_table_name = players_target_table_name
    players_unique_column_list = ['player_url',]

    # set constants for tournament data
    tournaments_target_table_name = 'tennisabstract_tournaments'
    tournaments_temp_table_name = players_target_table_name
    tournaments_unique_column_list = ['tournament_url',]

    # get list of match urls from source
    match_url_list_tennisabstract = get_match_url_list_tennisabstract()

    # get match urls (depends on bulk load flag)

    if matches_bulk_load_flag == True:
        match_url_list = match_url_list_tennisabstract[:5] # set to list from source
    else:
        # get list of match urls from database
        conn = create_connection() # create connection
        match_url_list_db = get_table_column_list(
            connection=conn,
            schema_name=target_schema_name,
            table_name=matches_target_table_name,
            column_name_list=matches_unique_column_list,
        )
        conn.close() # close connection

        # only include source urls that are NOT in database
        match_url_list = list(filter(lambda url_dict: url_dict not in match_url_list_db, match_url_list_tennisabstract))
    logging.info(f"Found {len(match_url_list)} matches.")

    # loop through match urls
    for i, match_url_dict in enumerate(match_url_list):

        match_url = match_url_dict['match_url']
        logging.info(f"({i+1}/{len(match_url_list)}) Getting match data for match url: {match_url}")

        # get data from match url
        match_url_dict = get_match_data_url(match_url=match_url)
        logging.info(f"Got match url data for match url: {match_url}")

        ## MATCH DATA START ##

        # get data from match scraping
        match_scrape_dict = get_match_data_scraped(
            match_url=match_url,
            retries=3,
            delay=3
        )

        # continue with match data logic if data is returned from scraping
        if match_scrape_dict != {}:

            logging.info(f"Data found for match url: {match_url}")

            # combine match data
            match_data_dict = {
                **match_url_dict,
                **match_scrape_dict,
            }

            # load data to database
            match_data_df = pd.DataFrame([match_data_dict]) # create dataframe
            conn = create_connection() # create connection
            ingest_df_to_sql(
                connection=conn,
                df=match_data_df,
                target_schema_name=target_schema_name,
                target_table_name=matches_target_table_name,
                temp_schema_name=temp_schema_name,
                temp_table_name=matches_temp_table_name,
                unique_column_list=matches_unique_column_list
            )
            conn.close() # close connection

        ## MATCH DATA END ##

        ## MATCH POINT DATA START ##

        # get data from match point scraping
        match_point_scraped_list = get_match_point_data_scraped(
            match_url=match_url,
            retries=3,
            delay=3
        )

        # continue with match point data logic if data is returned from scraping
        if match_point_scraped_list != []:

            logging.info(f"Match point data found for match url: {match_url}")

            # load data to database
            match_point_data_df = pd.DataFrame(match_point_scraped_list) # create dataframe
            conn = create_connection() # create connection
            ingest_df_to_sql(
                connection=conn,
                df=match_point_data_df,
                target_schema_name=target_schema_name,
                target_table_name=match_points_target_table_name,
                temp_schema_name=temp_schema_name,
                temp_table_name=match_points_temp_table_name,
                unique_column_list=match_points_unique_column_list
            )
            conn.close() # close connection

        ## MATCH POINT DATA END ##

        ## PLAYER DATA START ##

        # get list of player names from match data
        player_name_list = [
            match_url_dict['match_player_one'],
            match_url_dict['match_player_two'],
        ]

        # initialize player data list
        player_data_list = []

        # loop through player names
        for player_name in player_name_list:
            
            # create player url dict
            player_name_clean = player_name.replace('_', ' ')
            player_gender = match_url_dict['match_gender']
            player_url_dict = {
                'player_name': player_name_clean,
                'player_gender': player_gender,
            }

            # create player url
            player_url = create_player_url(
                player_dict=player_url_dict
            )
            player_url_dict['player_url'] = player_url
            logging.info(f"Getting player data for player url: {player_url}")

            # get data from player scraping
            player_scrape_dict = scrape_player_data(
                player_url=player_url,
                retries=3,
                delay=5,
            )

            # continue with player data logic if data is returned from scraping
            if player_scrape_dict != {}:

                logging.info(f"Data found for player url: {player_url}")

                # combine player data
                player_data_dict = {
                    **player_url_dict,
                    **player_scrape_dict,
                }

                # append to player list
                player_data_list.append(player_data_dict)

        # load data to database
        player_data_df = pd.DataFrame(player_data_list) # create dataframe
        conn = create_connection() # create connection
        ingest_df_to_sql(
            connection=conn,
            df=player_data_df,
            target_schema_name=target_schema_name,
            target_table_name=players_target_table_name,
            temp_schema_name=temp_schema_name,
            temp_table_name=players_temp_table_name,
            unique_column_list=players_unique_column_list
        )
        conn.close() # close connection

        ## PLAYER DATA END ##

        ## TOURNAMENT DATA START ##
        
        # create tournament url dict
        tournament_year = match_url_dict['match_date'][:4] # get year from date
        tournament_name = match_url_dict['match_tournament'].replace('_', ' ')
        tournament_gender = match_url_dict['match_gender']
        tournament_url_dict = {
            'tournament_year': tournament_year,
            'tournament_name': tournament_name,
            'tournament_gender': tournament_gender,
        }

        # create tournament url
        tournament_url = create_tournament_url(
            tournament_dict=tournament_url_dict
        )
        tournament_url_dict['tournament_url'] = tournament_url
        logging.info(f"Getting tournament data for tournament url: {tournament_url}")

        # get data from tournament scraping
        tournament_scrape_dict = get_tournament_data_scraped(
            tournament_url=tournament_url,
            retries=3,
            delay=3
        )

        # continue with tournament data logic if data is returned from scraping
        if tournament_scrape_dict != {}:

            logging.info(f"Data found for tournament url: {tournament_url}")

            # combine tournament data
            tournament_data_dict = {
                **tournament_url_dict,
                **tournament_scrape_dict,
            }

            # load data to database
            tournament_data_df = pd.DataFrame([tournament_data_dict]) # create dataframe
            conn = create_connection() # create connection
            ingest_df_to_sql(
                connection=conn,
                df=tournament_data_df,
                target_schema_name=target_schema_name,
                target_table_name=tournaments_target_table_name,
                temp_schema_name=temp_schema_name,
                temp_table_name=tournaments_temp_table_name,
                unique_column_list=tournaments_unique_column_list
            )
            conn.close() # close connection

        ## TOURNAMENT DATA END ##


if __name__ == "__main__":
    main()