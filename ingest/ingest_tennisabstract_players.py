from concurrent.futures import ThreadPoolExecutor, as_completed
from ingest.utils.functions.scrape import (
    create_chromedriver,
)
from ingest.utils.functions.sql import (
    create_connection,
    get_table_column_list,
    ingest_df_to_sql,
)
from ingest.utils.functions.tennisabstract.players import (
    get_player_url_list as get_player_url_list_tennisabstract,
    get_player_data_scraped,
)
import logging
import os
import pandas as pd
import random
import time


def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # create driver
    webdriver_path = os.getenv('CHROMEDRIVER_PATH')
    driver = create_chromedriver(webdriver_path=webdriver_path)

    # set constants for use in function
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    target_table_name = 'tennisabstract_players'
    temp_table_name = target_table_name
    unique_column_list = ['player_url',]

    # create connection
    conn = create_connection()

    # get list of players
    player_url_list_tennisabstract = [
        player['player_url']
        for player in get_player_url_list_tennisabstract()
    ]
    player_url_list_db = get_table_column_list(
        connection=conn,
        schema_name=target_schema_name,
        table_name=target_table_name,
        column_name_list=unique_column_list,
    )
    player_url_list = list(filter(lambda player_dict: player_dict['player_url'] not in player_url_list_db, player_url_list_tennisabstract))
    logging.info(f"Found {len(player_url_list)} players.")

    # loop through players
    # initialize chunk logic
    i = 0
    chunk_size = 100
    max_workers = 5
    for i in range(0, len(player_url_list), chunk_size):

        player_url_chunk_list = player_url_list[i:i + chunk_size]

        chunk_size_start = i
        chunk_size_end = min(i + chunk_size, len(player_url_list))
        logging.info(f"Chunking: {chunk_size_start} to {chunk_size_end}")

        # initialize data list
        player_data_list = []

        # loop through chunk urls
        for idx, player_url_dict in enumerate(player_url_chunk_list, start=i):
            player_url = player_url_dict['player_url']
            logging.info(f"Starting {idx+1} of {len(player_url_list)}.")
            logging.info(f"player url: {player_url}")
            player_data_scraped = get_player_data_scraped(
                driver=driver,
                player_url=player_url,
                retries=3,
                delay=3
            )
            player_data_dict = {
                **player_url_dict,
                **player_data_scraped,
            }
            player_data_list.append(player_data_dict)
            logging.info(f"Fetched data for: {player_url}")
            time.sleep(random.uniform(1, 3))

        # create dataframe
        player_data_df = pd.DataFrame(player_data_list)

        # ingest dataframe to sql
        ingest_df_to_sql(
            connection=conn,
            df=player_data_df,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            temp_schema_name=temp_schema_name,
            temp_table_name=temp_table_name,
            unique_column_list=unique_column_list
        )

    # close connection
    conn.close()


if __name__ == "__main__":
    main()