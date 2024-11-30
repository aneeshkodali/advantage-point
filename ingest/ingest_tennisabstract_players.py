from concurrent.futures import ThreadPoolExecutor, as_completed
from ingest.utils.functions.scrape import (
    create_chromedriver,
)
from ingest.utils.functions.sql import (
    create_connection,
    get_table_column_list,
    ingest_df_to_sql,
)
from ingest.utils.functions.tennisabstract import (
    get_player_url_list as get_player_url_list_tennisabstract,
    get_player_data,
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
    alter_table_drop_column_flag = False
    merge_table_delete_row_flag = False

    # create connection
    conn = create_connection()

    # get list of players
    player_url_list_tennisabstract = get_player_url_list_tennisabstract()
    # player_url_list_db = get_table_column_list(
    #     connection=conn,
    #     schema_name=target_schema_name,
    #     table_name=target_table_name,
    #     column_name_list=unique_column_list,
    #     where_clause_list=['audit_field_active_flag = TRUE',]
    # )
    # player_url_list = list(filter(lambda url_dict: url_dict not in player_url_list_db, player_url_list_tennisabstract))[:20]
    player_url_list = player_url_list_tennisabstract[:20]
    logging.info(f"Found {len(player_url_list)} players.")

    # loop through players
    # initialize chunk logic
    i = 0
    chunk_size = 10
    max_workers = 5
    for i in range(0, len(player_url_list), chunk_size):

        player_url_chunk_list = player_url_list[i:i + chunk_size]

        chunk_size_start = i
        chunk_size_end = min(i + chunk_size, len(player_url_list))
        logging.info(f"Chunking: {chunk_size_start} to {chunk_size_end}")

        # initialize data list
        player_data_list = []

        # Use ThreadPoolExecutor to scrape player data in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks directly to executor
            future_to_url = {
                executor.submit(get_player_data, driver, player_url_dict['player_url'], retries=3, delay=3): idx
                for idx, player_url_dict in enumerate(player_url_chunk_list, start=chunk_size_start)
            }

            # Process results as they complete
            for future in as_completed(future_to_url):
                url_index = future_to_url[future]  # Get the original index of the URL
                player_url_dict = player_url_chunk_list[url_index - chunk_size_start]
                try:
                    result = future.result()  # Get the result of `get_player_data`
                    if result:
                        player_data_list.append(result)
                        logging.info(
                            f"Successfully fetched data for URL {url_index + 1}/{len(player_url_list)}: {player_url_dict['player_url']}"
                        )
                except Exception as e:
                    logging.info(
                        f"Failed to fetch data for URL {url_index + 1}/{len(player_url_list)}: {player_url_dict['player_url']} - Error: {e}"
                    )

            
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
            unique_column_list=unique_column_list,
            drop_column_flag=alter_table_drop_column_flag,
            delete_row_flag=merge_table_delete_row_flag
        )

    # close connection
    conn.close()


if __name__ == "__main__":
    main()