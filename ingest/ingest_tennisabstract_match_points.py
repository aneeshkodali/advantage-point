from datetime import (
    datetime,
    timezone,
)
from ingest.utils.functions.sql import (
    create_connection,
    load_df_to_sql,
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
    target_table_name = 'tennisabstract_match_points'

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

            # load data to database
            match_point_data_df = pd.DataFrame(match_point_scraped_list) # create dataframe
            conn = create_connection() # create connection
            load_df_to_sql(
                connection=conn,
                df=match_point_data_df,
                target_schema_name=target_schema_name,
                target_table_name=target_table_name,
            )
            conn.close() # close connection

if __name__ == "__main__":
    main()