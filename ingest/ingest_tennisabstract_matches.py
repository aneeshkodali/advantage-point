from datetime import (
    datetime,
    timezone,
)
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
    get_match_data_scraped,
    get_match_data_url,
    get_match_url_list as get_match_url_list_tennisabstract,
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
    target_table_name = 'tennisabstract_matches'
    temp_table_name = target_table_name

    # get list of match urls from source
    match_url_list = get_match_url_list_tennisabstract()[:5]

    # loop through match urls
    match_data_list = []
    for i, match_dict in enumerate(match_url_list):

        match_url = match_dict['match_url']
        logging.info(f"({i+1}/{len(match_url_list)}) Getting match data for match url: {match_url}")

        # get data from match url
        match_url_dict = get_match_data_url(match_url=match_url)
        logging.info(f"Got match url data for match url: {match_url}")

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
                **{'load_datetime': datetime.now(timezone.utc)},
            }

            # append to list
            match_data_list.append(match_data_dict)

    # load data to database
    if match_data_list != []:

        # create dataframe
        match_data_df = pd.DataFrame([match_data_list]) # create dataframe
        match_data_df = match_data_df.where(pd.notnull(match_data_df), None) # convert null values to SQL-compatible null values

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
            df=match_data_df,
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

        # # merge into target table
        # merge_target_table(
        #     connection=connection,
        #     target_schema_name=target_schema_name,
        #     target_table_name=target_table_name,
        #     source_schema_name=temp_schema_name,
        #     source_table_name=temp_table_name,
        #     unique_column_list=unique_column_list
        # )

        # insert into target table
        insert_into_target_table(
            connection=conn,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            source_schema_name=temp_schema_name,
            source_table_name=temp_table_name
        )

        # load_df_to_sql(
        #     connection=conn,
        #     df=match_data_df,
        #     target_schema_name=target_schema_name,
        #     target_table_name=target_table_name,
        #     temp_schema_name=temp_schema_name,
        #     temp_table_name=temp_table_name
        # )

        # close connection
        conn.close()


if __name__ == "__main__":
    main()