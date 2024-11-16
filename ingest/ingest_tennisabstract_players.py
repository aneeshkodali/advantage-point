from typing import (
    Dict,
    List
)
from selenium import webdriver
from utils.functions.sql import (
    create_connection,
    get_table_column_list,
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
) -> List[str]:
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
    regex_pattern = fr"var {regex_var}\s?=\s?(?P<{regex_var}>.*?);"
    regex_var_match = re.search(regex_pattern, response_page_source)
    val = regex_var_match.group(regex_var)
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
        player_url_list.append(player_url)

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

    # get url page source
    driver.get(player_url)
    response_page_source = driver.page_source

    # initialize data to be retrieved
    data_dict = {}
    response_var_list = ['fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast', 'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate', 'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link', 'itf_id', 'atp_id', 'dc_id', 'wiki_id']
    for regex_var in response_var_list:
        regex_pattern = fr"var {regex_var}\s?=\s?(?P<{regex_var}>.*?);"
        regex_var_match = re.search(regex_pattern, response_page_source)
        if regex_var_match:
            val = regex_var_match.group(regex_var)
        else:
            val = None
        data_dict[regex_var] = val

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
    target_table_name = 'hub_player'
    temp_table_name = 'player'

    # create connection
    conn = create_connection()

    # initialize driver
    webdriver_path = os.getenv('CHROMEDRIVER_PATH')
    driver = create_chromedriver(webdriver_path=webdriver_path)

    # get list of players
    player_tennisabstract_url_list_source = get_player_tennisabstract_url_list_source(
        driver=driver
    )
    player_tennisabstract_url_list_db = get_table_column_list(
        connection=conn,
        schema_name=target_schema_name,
        table_name=target_table_name,
        column_name='player_url'
    )
    player_tennisabstract_url_list = list(filter(lambda url: url not in player_tennisabstract_url_list_db, player_tennisabstract_url_list_source))
    logging.info(f"Found {len(player_tennisabstract_url_list)} players.")

    # loop through players
    # initialize list
    player_data_list = []
    for i, player_url in enumerate(player_tennisabstract_url_list):

        logging.info(f"Starting {i+1} of {len(player_tennisabstract_url_list)}.")
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

    return player_data_list

    # close connection
    conn.close()

if __name__ == "__main__":
    main()