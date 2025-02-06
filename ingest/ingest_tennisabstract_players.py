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
    get_player_list as get_player_list_tennisabstract,
    get_player_data_scraped,
)
import logging
import os
import pandas as pd
import random
import time

def get_player_data(player_dict):

    # create driver
    webdriver_path = os.getenv('CHROMEDRIVER_PATH')
    driver = create_chromedriver(webdriver_path=webdriver_path)

    player_url = player_dict['player_url']

    logging.info(f"Scraping data for: {player_url}")

    player_data_scraped_dict = get_player_data_scraped(
        driver=driver,
        player_url=player_url,
        retries=3,
        delay=3
    )

    player_data_dict = {
        **player_dict,
        **player_data_scraped_dict,
    }

    driver.quit()

    return player_data_dict



def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # set constants for use in function
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    target_table_name = 'tennisabstract_players'
    temp_table_name = target_table_name
    unique_column_list = ['player_url',]


    # get list of players
    player_list_tennisabstract = get_player_list_tennisabstract()

    conn = create_connection()
    player_url_list_db = get_table_column_list(
        connection=conn,
        schema_name=target_schema_name,
        table_name=target_table_name,
        column_name_list=unique_column_list,
    )
    conn.close()

    player_list = list(filter(lambda player_dict: player_dict['player_url'] not in player_url_list_db, player_list_tennisabstract))[:5]
    logging.info(f"Found {len(player_list)} players.")

    # loop through players
    # initialize chunk logic
    i = 0
    chunk_size = 25
    max_workers = 5
    for i in range(0, len(player_list), chunk_size):

        player_chunk_list = player_list[i:i + chunk_size]

        chunk_size_start = i
        chunk_size_end = min(i + chunk_size, len(player_list))
        logging.info(f"Chunking: {chunk_size_start} to {chunk_size_end}")

        # initialize data list
        player_data_list = []

        # parallel scraping
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(get_player_data, player): player for player in player_chunk_list}

            for future in as_completed(futures):
                logging.info(f"future: {future}")
                logging.info(f"future.result: {future.result()}")
                logging.info(f"futures.future: {futures[future]}")
                # player_dict = futures[future]
                # try:
                #     result = future.result()  # Get the result of `get_player_data`
                #     if result:
                #         player_data_list.append(result)
                #         logging.info(f"Successfully fetched data for: {player_dict['player_url']}")
                # except Exception as e:
                #     logging.error(f"Error processing {player_dict['player_url']}: {e}")

        # create dataframe
        player_data_df = pd.DataFrame(player_data_list)

        # ingest dataframe to sql
        conn = create_connection()
        ingest_df_to_sql(
            connection=conn,
            df=player_data_df,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            temp_schema_name=temp_schema_name,
            temp_table_name=temp_table_name,
            unique_column_list=unique_column_list
        )
        conn.close()


if __name__ == "__main__":
    main()