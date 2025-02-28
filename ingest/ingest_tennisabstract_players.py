from datetime import (
    datetime,
    timezone,
)
from ingest.utils.functions.sql import (
    create_connection,
    load_df_to_sql,
)
from ingest.utils.functions.tennisabstract.matches import (
    get_match_data_url,
    get_match_url_list as get_match_url_list_tennisabstract,
)
from ingest.utils.functions.tennisabstract.players import (
    create_player_url,
    scrape_player_data,
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

    # set constants
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    target_table_name = 'tennisabstract_players'
    temp_table_name = target_table_name

    # get list of match urls from source
    match_url_list = get_match_url_list_tennisabstract()

    # initialize player url list
    player_url_list = []

    # loop through match urls
    for match_dict in match_url_list:

        # get match dict from url
        match_url = match_dict['match_url']
        match_url_dict = get_match_data_url(match_url=match_url)

        # get list of player names from match data
        player_name_list = [
            match_url_dict['match_player_one'],
            match_url_dict['match_player_two'],
        ]

        # loop through player names
        for player_name in player_name_list:
            
            # create player url dict
            player_name_clean = player_name.replace('_', ' ')
            player_gender = match_url_dict['match_gender']
            player_url_dict = {
                'player_name': player_name_clean,
                'player_gender': player_gender,
            }

            # append to player url dict
            player_url_list.append(player_url_dict)

    # get distinct list of player dicts
    player_url_df = pd.DataFrame(player_url_list).drop_duplicates()
    player_url_list = player_url_df.to_dict(orient='records')

    # loop through player url list
    player_data_list = []
    for i, player_url_dict in enumerate(player_url_list):

        logging.info(f"({i+1}/{len(player_url_list)}) Getting player data")

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
            delay=3,
        )

        # continue with player data logic if data is returned from scraping
        if player_scrape_dict != {}:

            logging.info(f"Data found for player url: {player_url}")

            # combine player data
            player_data_dict = {
                **player_url_dict,
                **player_scrape_dict,
                **{'load_datetime': datetime.now(timezone.utc)},
            }

            # append to player list
            player_data_list.append(player_data_dict)

    # load to database if not empty
    if player_data_list != []:
    
        # load data to database
        player_data_df = pd.DataFrame(player_data_list) # create dataframe
        conn = create_connection() # create connection
        load_df_to_sql(
            connection=conn,
            df=player_data_df,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            temp_schema_name=temp_schema_name,
            temp_table_name=temp_table_name
        )
        conn.close() # close connection

if __name__ == "__main__":
    main()