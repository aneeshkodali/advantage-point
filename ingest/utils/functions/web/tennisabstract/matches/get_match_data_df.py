from typing import (
    List,
)
from utils.functions.web.tennisabstract.matches.get_match_data_scraped import get_match_data_scraped
from utils.functions.web.tennisabstract.matches.get_match_data_url import get_match_data_url
import logging
import pandas as pd

def get_match_data_df(
    match_url_list: List
) -> pd.DataFrame:
    """
    Arguments:
    - match_url_list: List of match urls

    Create pandas dataframe from list.
    """

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

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
            }

            # append to list
            match_data_list.append(match_data_dict)

    # load data to database
    if match_data_list != []:

        # create dataframe
        match_data_df = pd.DataFrame.from_records(match_data_list) # create dataframe
        match_data_df = match_data_df.where(pd.notnull(match_data_df), None) # convert null values to SQL-compatible null values

        return match_data_df

    return pd.DataFrame()