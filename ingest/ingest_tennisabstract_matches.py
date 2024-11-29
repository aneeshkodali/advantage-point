from concurrent.futures import ThreadPoolExecutor, as_completed
from ingest.utils.functions.sql import (
    create_connection,
    get_table_column_list,
    ingest_df_to_sql,
)
from ingest.utils.functions.tennisabstract import (
    get_match_url_list as get_match_url_list_tennisabstract,
    get_match_data,
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

    # set constants for use in function
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    target_table_name = 'tennisabstract_matches'
    temp_table_name = target_table_name
    unique_column_list = ['match_url',]
    alter_table_drop_column_flag = False
    merge_table_delete_row_flag = False

    # create connection
    conn = create_connection()

    # get list of matches
    match_url_list_tennisabstract = get_match_url_list_tennisabstract(
        # driver=driver
    )
    match_tennisabstract_url_list_db = get_table_column_list(
        connection=conn,
        schema_name=target_schema_name,
        table_name=target_table_name,
        column_name_list=unique_column_list,
        where_clause_list=['audit_field_active_flag = TRUE', 'match_title IS NOT NULL',]
    )
    match_url_list = list(filter(lambda url_dict: url_dict not in match_tennisabstract_url_list_db, match_url_list_tennisabstract))[:20]
    logging.info(f"Found {len(match_url_list)} matches.")

    # loop through matches
    # initialize chunk logic
    i = 0
    chunk_size = 100
    max_workers = 5
    for i in range(0, len(match_url_list), chunk_size):

        match_url_chunk_list = match_url_list[i:i + chunk_size]

        chunk_size_start = i
        chunk_size_end = min(i + chunk_size, len(match_url_list))
        logging.info(f"Chunking: {chunk_size_start} to {chunk_size_end}")

        # initialize data list
        match_data_list = []

        # # loop through chunk urls
        # for idx, match_url_dict in enumerate(match_url_chunk_list, start=i):

        #     match_url = match_url_dict['match_url']

        #     logging.info(f"Starting {idx+1} of {len(match_url_list)}.")
        #     logging.info(f"match url: {match_url}")

        #     match_data_dict = get_match_data(
        #         match_url=match_url,
        #         retries=3,
        #         delay=3
        #     )
        #     time.sleep(random.uniform(0.5, 1.2))

        #     match_data_list.append(match_data_dict)
        #     logging.info(f"Fetched data for: {match_url}")

        # Use ThreadPoolExecutor to scrape match data in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks directly to executor
            future_to_url = {
                executor.submit(get_match_data, match_url_dict['match_url'], retries=3, delay=3): idx
                for idx, match_url_dict in enumerate(match_url_chunk_list, start=chunk_size_start)
            }

            # Process results as they complete
            for future in as_completed(future_to_url):
                url_index = future_to_url[future]  # Get the original index of the URL
                match_url_dict = match_url_chunk_list[url_index - chunk_size_start]
                try:
                    result = future.result()  # Get the result of `get_match_data`
                    if result:
                        match_data_list.append(result)
                        logging.info(
                            f"Successfully fetched data for URL {url_index + 1}/{len(match_url_list)}: {match_url_dict['match_url']}"
                        )
                except Exception as e:
                    logging.info(
                        f"Failed to fetch data for URL {url_index + 1}/{len(match_url_list)}: {match_url_dict['match_url']} - Error: {e}"
                    )

            
        # create dataframe
        match_data_df = pd.DataFrame(match_data_list)

        # ingest dataframe to sql
        ingest_df_to_sql(
            connection=conn,
            df=match_data_df,
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