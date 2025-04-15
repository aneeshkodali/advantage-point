from ingest.utils.functions.sql import (
    create_and_load_table,
    create_connection,
    create_or_alter_target_table,
    drop_table,
    insert_into_target_table,
    # load_df_to_sql,
    truncate_table,
)
from ingest.utils.functions.tennisabstract.matches import (
    get_match_data_url,
    get_match_url_list as get_match_url_list_tennisabstract,
)
from ingest.utils.functions.tennisabstract.players import (
    create_player_url,
    scrape_player_data,
    create_playwright_page,
    scrape_player_data_playwright,
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

    # create playwright page
    playwright, browser, page = create_playwright_page()

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

        # # get data from player scraping
        # player_scrape_dict = scrape_player_data(
        #     player_url=player_url,
        #     retries=3,
        #     delay=3,
        # )

        # get data from player scraping
        player_scrape_dict = scrape_player_data_playwright(
            player_url=player_url,
            page=page,
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
            }

            # append to player list
            player_data_list.append(player_data_dict)

    # load to database if not empty
    if player_data_list != []:
    
        # create dataframe
        player_data_df = pd.DataFrame(player_data_list) # create dataframe
        player_data_df = player_data_df.where(pd.notnull(player_data_df), None) # convert null values to SQL-compatible null values

        # create connection
        conn = create_connection()

        # drop temp table
        drop_table(
            connection=conn,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )
        
        # create temp table
        create_and_load_table(
            connection=conn,
            df=player_data_df,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )

        # create or alter target table
        create_or_alter_target_table(
            connection=conn,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=temp_schema_name,
            source_table_name=temp_table_name
        )

        # truncate target table
        truncate_table(
            connection=conn,
            schema_name=target_schema_name,
            table_name=target_table_name
        )

        # insert into target table
        insert_into_target_table(
            connection=conn,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=temp_schema_name,
            source_table_name=temp_table_name
        )
        
        conn.close() # close connection

    browser.close()
    playwright.stop()

if __name__ == "__main__":
    main()