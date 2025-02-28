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
    get_match_point_data_scraped,
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
    target_table_name = 'tennisabstract_match_points'
    temp_table_name = target_table_name

    # get list of match urls from source
    match_url_list = get_match_url_list_tennisabstract()

    # loop through match urls
    match_point_data_list = []
    for i, match_url_dict in enumerate(match_url_list):

        match_url = match_url_dict['match_url']
        logging.info(f"({i+1}/{len(match_url_list)}) Getting match point data for match url: {match_url}")

        # get data from match point scraping
        match_point_scraped_list = get_match_point_data_scraped(
            match_url=match_url,
            retries=3,
            delay=3
        )

        # continue with match point data logic if data is returned from scraping
        if match_point_scraped_list != []:

            logging.info(f"Match point data found for match url: {match_url}")

            # add load date
            match_point_scraped_list = [
                {
                    **match_point_scraped_dict,
                    **{'load_datetime': datetime.now(timezone.utc)},
                }
                for match_point_scraped_dict in match_point_scraped_list
            ]

            # append to match data list
            match_point_data_list.extend(match_point_scraped_list)

    
    # load data to database
    if match_point_data_list != []:

        # create dataframe
        match_point_data_df = pd.DataFrame(match_point_data_list) # create dataframe
        match_point_data_df = match_point_data_df.where(pd.notnull(match_point_data_df), None) # convert null values to SQL-compatible null values

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
            df=match_point_data_df,
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

if __name__ == "__main__":
    main()