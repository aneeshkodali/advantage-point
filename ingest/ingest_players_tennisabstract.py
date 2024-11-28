from bs4 import BeautifulSoup
from typing import (
    Dict,
    List
)
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from utils.functions.sql import (
    create_connection,
    drop_table,
    create_and_load_table,
    create_or_alter_target_table,
    # get_table_column_list,
    merge_target_table,
)
from utils.functions.scrape import (
    scrape_page_source_var
)
from utils.functions.selenium_fn import create_chromedriver
import ast
import logging
import os
import pandas as pd
import psycopg2
import re

def create_player_tennisabstract_url(
    player_gender: str,
    player_name: str
) -> str:
    """
    Arguments:
    - player_gender: gender (M or W)
    - player_name: player full name

    Returns url
    """

    # format parts of url string
    url_player_str = 'player' if player_gender == 'M' else 'wplayer'
    url_player_name = player_name.replace(' ', '')

    player_url = f"https://www.tennisabstract.com/cgi-bin/{url_player_str}.cgi?p={url_player_name}"

    return player_url


def get_player_tennisabstract_url_list_source(
    driver: webdriver
) -> List[Dict]:
    """
    Arguments:
    - driver: Selenium webdriver

    Returns list of player urls from source (url)
    """

    # retrieve url page source
    player_list_url = 'https://www.tennisabstract.com/jsplayers/mwplayerlist.js'
    driver.get(player_list_url)
    response_page_source = driver.page_source

    # retrieve list-like string
    regex_var = 'playerlist'
    # regex_pattern = fr"var {regex_var}\s?=\s?(?P<{regex_var}>.*?);"
    # regex_var_match = re.search(regex_pattern, response_page_source)
    # val = regex_var_match.group(regex_var)
    val = scrape_page_source_var(
        page_source=response_page_source,
        var=regex_var
    )
    player_list = ast.literal_eval(val)

    # loop through each element and create url
    player_url_list = []
    for player in player_list:

        # each element in list is of format: (<gender>) <name>)
        regex_pattern = r'(?P<gender>\((.*?)\))\s*(?P<name>.*)'
        regex_match = re.search(regex_pattern, player)
        gender = regex_match.group('gender').strip('()')
        name = regex_match.group('name')

        # create url
        player_url = create_player_tennisabstract_url(
            player_gender=gender,
            player_name=name
        )
        player_url_dict = {}
        player_url_dict['player_url'] = player_url
        player_url_list.append(player_url_dict)

    return player_url_list

def fetch_player_tennisabstract_data_scraped(
    driver: webdriver,
    player_url: str
) -> Dict:
    """
    Arguments:
    - driver: Selenium webdriver
    - player_url: player link

    Returns dictionary of player information from url
    """

    # initialize data to be retrieved
    response_var_list = ['nameparam', 'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast', 'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate', 'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link', 'itf_id', 'atp_id', 'dc_id', 'wiki_id']
    data_dict = {var: None for var in response_var_list}

    # navigate to the page
    driver.get(player_url)

    # wait for the page to fully render (ensure JavaScript is executed)
    WebDriverWait(driver, 10).until(
        # lambda d: d.execute_script("return document.readyState") == "complete"
        EC.presence_of_element_located((By.XPATH, "//script[@language='JavaScript']"))
    )

    # get the fully rendered page source
    # response_page_source = driver.page_source
    script_tag = driver.find_element(By.XPATH, "//script[@language='JavaScript']")
    script_content = script_tag.get_attribute("innerHTML")


    # get variables
    for var in response_var_list:
        # try:
        #     val = driver.execute_script(f"return {var};")
        #     data_dict[var] = val
        # except Exception as e:
        #     logging.info(f"Error obtaining value for {var}: {e}")

        try:
            val = scrape_page_source_var(
                # page_source=response_page_source,
                page_source=script_content,
                var=var
            )
            logging.info(f"{var}: {val}")
            data_dict[var] = val
        except Exception as e:
            logging.info(f"Error encountered when getting data for variable {var}: {e}")

    # # try:

    # #     # get url page source
    # #     driver.get(player_url)

    # #     # Wait for the page to fully render (ensure JavaScript is executed)
    # #     WebDriverWait(driver, 10).until(
    # #         lambda d: d.execute_script("return document.readyState") == "complete"
    # #     )
    # # except Exception as e:
    # #     logging.info(f"Page did not fully load for URL {player_url}: {e}")
    # #     return data_dict  # Return dictionary with keys but all values set to None

    # # # get the fully rendered page source
    # # response_page_source = driver.page_source

    # # for regex_var in response_var_list:
    # #     regex_pattern = fr"var {regex_var}\s?=\s?(?P<{regex_var}>.*);"
    # #     regex_var_match = re.search(regex_pattern, response_page_source)
    # #     if regex_var_match:
    # #         val = regex_var_match.group(regex_var)
    # #         data_dict[regex_var] = val

    # try:

    #     # get url page source
    #     driver.get(player_url)
    #     # WebDriverWait(driver, 10).until(
    #     #     lambda d: d.execute_script("return document.readyState") == "complete"
    #     # )
        
    #     response_page_source = driver.page_source

    #     # find the script tag
    #     soup = BeautifulSoup(response_page_source, 'html.parser')
    #     script_tag = soup.find('script', attrs={'language': 'JavaScript'})
    #     script_text = script_tag.text

    #     # loop through variable list and add values to dict
    #     for regex_var in response_var_list:
    #         regex_pattern = fr"var {regex_var}\s?=\s?(?P<{regex_var}>.*);"
    #         regex_var_match = re.search(regex_pattern, script_text)
    #         if regex_var_match:
    #             val = regex_var_match.group(regex_var)
    #             data_dict[regex_var] = val

    # except Exception as e:
    #     logging.info(f"Error encountered when getting data for {player_url}: {e}")
    #     return data_dict

    return data_dict

def main():
    
    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # set constants for use in function
    target_schema_name = os.getenv('SCHEMA_INGESTION')
    temp_schema_name = os.getenv('SCHEMA_INGESTION_TEMP')
    target_table_name = 'players_tennisabstract'
    temp_table_name = target_table_name
    unique_column_list = ['player_url',]
    alter_table_drop_column_flag = False
    merge_table_delete_row_flag = False

    # create connection
    conn = create_connection()

    # initialize driver
    webdriver_path = os.getenv('CHROMEDRIVER_PATH')
    driver = create_chromedriver(webdriver_path=webdriver_path)

    # get list of players
    player_tennisabstract_url_list_source = get_player_tennisabstract_url_list_source(
        driver=driver
    )
    # player_tennisabstract_url_list_db = get_table_column_list(
    #     connection=conn,
    #     schema_name=target_schema_name,
    #     table_name=target_table_name,
    #     column_name_list=unique_column_list,
    #     where_clause_list=['audit_field_active_flag = TRUE']
    # )
    # player_tennisabstract_url_list = list(filter(lambda url_dict: url_dict not in player_tennisabstract_url_list_db, player_tennisabstract_url_list_source))
    player_tennisabstract_url_list = player_tennisabstract_url_list_source[:30]
    logging.info(f"Found {len(player_tennisabstract_url_list)} players.")

    # loop through players
    # initialize chunk logic
    i = 0
    chunk_size = 1000
    for i in range(0, len(player_tennisabstract_url_list), chunk_size):

        player_url_chunk_list = player_tennisabstract_url_list[i:i + chunk_size]

        chunk_size_start = i
        chunk_size_end = min(i + chunk_size, len(player_tennisabstract_url_list))
        logging.info(f"Chunking: {chunk_size_start} to {chunk_size_end}")

        # initialize data list
        player_data_list = []

        # loop through chunk urls
        for idx, player_url_dict in enumerate(player_url_chunk_list, start=i):

            player_url = player_url_dict['player_url']

            logging.info(f"Starting {idx+1} of {len(player_tennisabstract_url_list)}.")
            logging.info(f"player url: {player_url}")

            # initialize player data dictionary
            player_data_dict = {}
            player_data_dict['player_url'] = player_url
            player_data_dict['player_gender'] = 'W' if 'wplayer' in player_url else 'M'

            # get player data from webscrape
            player_data_dict_scraped = fetch_player_tennisabstract_data_scraped(
                driver=driver,
                player_url=player_url
            )

            # combine dictionaries
            player_data_dict = {
                **player_data_dict,
                **player_data_dict_scraped,
            }

            player_data_list.append(player_data_dict)
            logging.info(f"Fetched data for: {player_url}")
        
        # create dataframe
        player_data_df = pd.DataFrame(player_data_list)

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
            df=player_data_df,
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