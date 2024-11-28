from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from utils.functions.selenium_fn import create_chromedriver
from utils.functions.sql import (
    create_connection,
    drop_table,
    create_and_load_table,
    create_or_alter_target_table,
    # get_table_column_list,
    merge_target_table,
)
from typing import (
    Dict,
    List,
)
import logging
import os
import pandas as pd

def get_match_tennisabstract_url_list_source(
    driver: webdriver
) -> List[Dict]:
    """
    Arguments:
    - driver: Selenium webdriver

    Returns list of match urls from source (url)
    """

    # retrieve url page source
    match_list_url = 'https://www.tennisabstract.com/charting/'
    driver.get(match_list_url)
    response_page_source = driver.page_source

    # parse page source
    soup = BeautifulSoup(response_page_source, 'html.parser')
    # links are hrefs in last <p>
    p_tag_match = soup.find_all('p')[-1]
    match_href_list = [f"https://www.tennisabstract.com/charting/{a['href']}" for a in p_tag_match.find_all('a', href=True)]

    match_url_list = [
        {'match_url': match_url} for match_url in match_href_list
    ]
    return match_url_list

def fetch_match_tennisabstract_data_scraped(
    driver: webdriver,
    match_url: str,
    retries: int = 3,
    delay: int = 5
) -> Dict:
    """
    Arguments:
    - driver: Selenium webdriver
    - match_url: match link

    Returns dictionary of match information from url
    """

    # initialize data
    match_dict = {}

    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            driver.get(match_url)

            # wait for the page to fully render
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            # get the match title (<h2>)
            try:
                match_title = driver.find_element(By.XPATH, "//tbody//h2").text
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable match_title: {e}")
                match_title = None
            match_dict["match_title"] = match_title

            # get the match result (b)
            try:
                match_result = driver.find_element(By.XPATH, "//tbody//b").text
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable match_result: {e}")
                match_result = None
            match_dict["match_result"] = match_result

            # check if all values in dict are None -> return empty dict
            if all(value is None for value in match_dict.values()):
                logging.info(f"All values None for {match_url}. Returning empty dictionary.")
                return {}

            # return dictionary if data successfully extracted
            return match_dict

        except (TimeoutException, WebDriverException) as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {match_url}: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay*attempt} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {match_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}


def get_match_tennisabstract_data(
    driver: webdriver,
    match_url: str
) -> Dict:
    """
    Arguments:
    - driver: Selenium webdriver
    - match_url: match link

    Returns dictionary of complete match information from url
    """

    # initialize match data dictionary
    match_data_dict = {}
    match_data_dict['match_url'] = match_url

    # parse url
    match_url_parsed_list = match_url.split('charting/')[1].replace('.html', '').split('-')
    match_date = match_url_parsed_list[0]
    match_data_dict['match_date'] = match_date
    match_gender = match_url_parsed_list[1]
    match_data_dict['match_gender'] = match_gender
    match_tournament = match_url_parsed_list[2]
    match_data_dict['match_tournament'] = match_tournament
    match_round = match_url_parsed_list[3]
    match_data_dict['match_round'] = match_round
    match_player_one = match_url_parsed_list[4]
    match_data_dict['match_player_one'] = match_player_one
    match_player_two = match_url_parsed_list[5]
    match_data_dict['match_player_two'] = match_player_two

    # get match data from webscrape
    match_data_dict_scraped = fetch_match_tennisabstract_data_scraped(
        driver=driver,
        match_url=match_url,
        retries=3,
        delay=5
    )

    # combine dictionaries
    match_data_dict = {
        **match_data_dict,
        **match_data_dict_scraped,
    }

    return match_data_dict


def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # set constants for use in function
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    target_table_name = 'matches_tennisabstract'
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