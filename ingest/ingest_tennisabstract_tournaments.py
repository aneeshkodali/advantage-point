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
from ingest.utils.functions.tennisabstract.tournaments import (
    create_tournament_url,
    get_tournament_data_scraped,
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
    target_table_name = 'tennisabstract_tournaments'
    temp_table_name = target_table_name

    # get list of match urls from source
    match_url_list = get_match_url_list_tennisabstract()

    # initialize tournament url list
    tournament_url_list = []

    # loop through match urls
    for match_dict in match_url_list:

        # get match dict from url
        match_url = match_dict['match_url']
        match_url_dict = get_match_data_url(match_url=match_url)

        # create tournament url dict
        tournament_year = match_url_dict['match_date'][:4] # get year from date
        tournament_name = match_url_dict['match_tournament'].replace('_', ' ')
        tournament_gender = match_url_dict['match_gender']
        tournament_url_dict = {
            'tournament_year': tournament_year,
            'tournament_name': tournament_name,
            'tournament_gender': tournament_gender,
        }

        # append to tournament url list
        tournament_url_list.append(tournament_url_dict)

    # get distinct list of tournament dicts
    tournament_url_df = pd.DataFrame(tournament_url_list).drop_duplicates()
    tournament_url_list = tournament_url_df.to_dict(orient='records')

    # loop through tournament url list
    tournament_data_list = []
    for i, tournament_url_dict in enumerate(tournament_url_list):

        logging.info(f"({i+1}/{len(tournament_url_list)}) Getting tournament data")

        # create tournament url
        tournament_url = create_tournament_url(
            tournament_dict=tournament_url_dict
        )
        tournament_url_dict['tournament_url'] = tournament_url
        logging.info(f"Getting tournament data for tournament url: {tournament_url}")

        # get data from tournament scraping
        tournament_scrape_dict = get_tournament_data_scraped(
            tournament_url=tournament_url,
            retries=3,
            delay=3,
        )

        # continue with tournament data logic if data is returned from scraping
        if tournament_scrape_dict != {}:

            logging.info(f"Data found for tournament url: {tournament_url}")

            # combine tournament data
            tournament_data_dict = {
                **tournament_url_dict,
                **tournament_scrape_dict,
                **{'load_datetime': datetime.now(timezone.utc)},
            }

            # append to tournament list
            tournament_data_list.append(tournament_data_dict)

    # load to database if not empty
    if tournament_data_list != []:
    
        # load data to database
        tournament_data_df = pd.DataFrame(tournament_data_list) # create dataframe
        conn = create_connection() # create connection
        load_df_to_sql(
            connection=conn,
            df=tournament_data_df,
            target_schema_name=target_schema_name,
            target_table_name=target_table_name,
            temp_schema_name=temp_schema_name,
            temp_table_name=temp_table_name
        )
        conn.close() # close connection

if __name__ == "__main__":
    main()