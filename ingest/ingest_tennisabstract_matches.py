# from bs4 import BeautifulSoup
# from selenium import webdriver
from ingest.utils.functions.selenium_fn import create_chromedriver
from ingest.utils.functions.sql import (
    create_connection,
    drop_table,
    create_and_load_table,
    create_or_alter_target_table,
    # get_table_column_list,
    merge_target_table,
)
from ingest.utils.functions.tennisabstract import (
    get_match_url_list,
    get_match_data,
)
# from typing import (
#     Dict,
#     List,
# )
import logging
import os
import pandas as pd






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

    # initialize driver
    webdriver_path = os.getenv('CHROMEDRIVER_PATH')
    driver = create_chromedriver(webdriver_path=webdriver_path)

    # get list of matches
    match_tennisabstract_url_list_source = get_match_tennisabstract_url_list_source(
        driver=driver
    )
    # match_tennisabstract_url_list_db = get_table_column_list(
    #     connection=conn,
    #     schema_name=target_schema_name,
    #     table_name=target_table_name,
    #     column_name_list=unique_column_list,
    #     where_clause_list=['audit_field_active_flag = TRUE']
    # )
    # match_tennisabstract_url_list = list(filter(lambda url_dict: url_dict not in match_tennisabstract_url_list_db, match_tennisabstract_url_list_source))
    match_tennisabstract_url_list = match_tennisabstract_url_list_source
    logging.info(f"Found {len(match_tennisabstract_url_list)} matches.")

    # loop through matches
    # initialize chunk logic
    i = 0
    chunk_size = 100
    for i in range(0, len(match_tennisabstract_url_list), chunk_size):

        match_url_chunk_list = match_tennisabstract_url_list[i:i + chunk_size]

        chunk_size_start = i
        chunk_size_end = min(i + chunk_size, len(match_tennisabstract_url_list))
        logging.info(f"Chunking: {chunk_size_start} to {chunk_size_end}")

        # initialize data list
        match_data_list = []

        # loop through chunk urls
        for idx, match_url_dict in enumerate(match_url_chunk_list, start=i):

            match_url = match_url_dict['match_url']

            logging.info(f"Starting {idx+1} of {len(match_tennisabstract_url_list)}.")
            logging.info(f"match url: {match_url}")

            match_data_dict = get_match_tennisabstract_data(
                driver=driver,
                match_url=match_url
            )

            match_data_list.append(match_data_dict)
            logging.info(f"Fetched data for: {match_url}")
        
        # create dataframe
        match_data_df = pd.DataFrame(match_data_list)

        # create or replace temp table and insert
        logging.info(f"Loading temp table {temp_schema_name}.{temp_table_name} for chunk: {chunk_size_start} to {chunk_size_end}")
        
        # drop temp table
        drop_table(
            connection=conn,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )
        
        create_and_load_table(
            connection=conn,
            df=match_data_df,
            schema_name=temp_schema_name,
            table_name=temp_table_name
        )
        logging.info(f"Loaded temp table {temp_schema_name}.{temp_table_name} for chunk: {chunk_size_start} to {chunk_size_end}")

        # create or alter target table
        logging.info(f"Loading target table {target_schema_name}.{target_table_name} for chunk: {chunk_size_start} to {chunk_size_end}")
        create_or_alter_target_table(
            connection=conn,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=temp_schema_name,
            source_table_name=temp_table_name,
            drop_column_flag=alter_table_drop_column_flag
        )
        logging.info(f"Loaded target table {target_schema_name}.{target_table_name} for chunk: {chunk_size_start} to {chunk_size_end}")

        # merge into target table
        logging.info(f"Merging records into target table {target_schema_name}.{target_table_name} for chunk: {chunk_size_start} to {chunk_size_end}")
        merge_target_table(
            connection=conn,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=temp_schema_name,
            source_table_name=temp_table_name,
            unique_column_list=unique_column_list,
            delete_row_flag=merge_table_delete_row_flag
        )
        logging.info(f"Merged records into target table {target_schema_name}.{target_table_name} for chunk: {chunk_size_start} to {chunk_size_end}")


    # close connection
    conn.close()


if __name__ == "__main__":
    main()