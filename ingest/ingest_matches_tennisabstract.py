from bs4 import BeautifulSoup
from selenium import webdriver
from utils.functions.selenium_fn import create_chromedriver
from utils.functions.sql import (
    create_connection,
    get_table_column_list,
)
from typing import (
    Dict,
    List,
)
import logging
import os

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
    unique_column_list = ['match_url', 'point_number']
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
    match_tennisabstract_url_list_db = get_table_column_list(
        connection=conn,
        schema_name=target_schema_name,
        table_name=target_table_name,
        column_name_list=unique_column_list,
        where_clause_list=['audit_field_active_flag = TRUE']
    )
    match_tennisabstract_url_list = list(filter(lambda url_dict: url_dict not in match_tennisabstract_url_list_db, match_tennisabstract_url_list_source))
    logging.info(f"Found {len(match_tennisabstract_url_list)} matches.")



if __name__ == "__main__":
    main()