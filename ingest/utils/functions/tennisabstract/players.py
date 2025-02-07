from ingest.utils.functions.scrape import (
    make_request,
    scrape_javascript_var,
)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from typing import (
    Dict,
    List,
)
import ast
import logging
import re
import time

def create_player_url(
    player_dict: Dict
) -> str:
    """
    Arguments:
    - player_dict: Dictionary of player data

    Returns player url
    """

    # parse dictionary
    player_name = player_dict['player_name']
    player_gender = player_dict['player_gender']

    # format parts of url string
    url_player_str = 'player' if player_gender == 'M' else 'wplayer'
    url_player_name = player_name.replace(' ', '')

    player_url = f"https://www.tennisabstract.com/cgi-bin/{url_player_str}.cgi?p={url_player_name}"

    return player_url

def get_player_list() -> List[Dict]:
    """
    Returns list of player urls from source (url)
    """

    # retrieve url page
    player_list_url = 'https://www.tennisabstract.com/jsplayers/mwplayerlist.js'
    response = make_request(player_list_url)
    
    # retrieve list-like string
    player_list_val = scrape_javascript_var(
                content=response.text,
                var='playerlist'
    )
    # convert to list
    player_list = ast.literal_eval(player_list_val)

    # loop through each player and extract initial data
    player_data_list = []
    for player in player_list:

        # each element in list is of format: (<gender>) <name>)
        regex_pattern = r'(?P<gender>\((.*?)\))\s*(?P<name>.*)'
        regex_match = re.search(regex_pattern, player)
        gender = regex_match.group('gender').strip('()')
        name = regex_match.group('name')

        # create dictionary
        player_data_dict = {}
        player_data_dict['player_name'] = name
        player_data_dict['player_gender'] = gender
        player_data_list.append(player_data_dict)

    return player_data_list

# def get_player_data_scraped(
#     driver: webdriver,
#     player_url: str,
#     retries: int,
#     delay: int
# ) -> Dict:
#     """
#     Arguments:
#     - driver: Selenium webdriver
#     - player_url: player link
#     - retries: Number of retry attempts
#     - delay: Time (in seconds) between retries

#     Returns dictionary of player information from url
#     """

#     # initialize data
#     # initialize data to be retrieved
#     response_var_list = ['nameparam', 'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast', 'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate', 'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link', 'itf_id', 'atp_id', 'dc_id', 'wiki_id', 'elo_rating', 'elo_rank',]
#     player_dict = {var: None for var in response_var_list}

#     attempt = 0

#     while attempt < retries:

#         try:

#             # navigate to the page
#             driver.get(player_url)

#             # wait for the page to fully render (ensure JavaScript is executed)
#             WebDriverWait(driver, 10).until(
#                 EC.presence_of_element_located((By.XPATH, "//script[@language='JavaScript']"))
#             )
#             # locate script tag
#             script_tag = driver.find_element(By.XPATH, "//script[@language='JavaScript']")
#             script_content = script_tag.get_attribute("innerHTML")

#             for var in response_var_list:
#                 try:
#                     val = scrape_javascript_var(
#                         content=script_content,
#                         var=var
#                     )
#                     player_dict[var] = val
#                 except Exception as e:
#                     logging.info(f"Error encountered when getting data for variable {var}: {e}")

#             # check if all values in dict are None -> return empty dict
#             if all(value is None for value in player_dict.values()):
#                 logging.info(f"All values None for {player_url} - Returning empty dictionary.")
#                 return {}

#             # return dictionary if data successfully extracted
#             return player_dict

#         except Exception as e:
#             attempt += 1
#             logging.warning(f"Attempt {attempt} failed for {player_url}: {e}")
#             if attempt < retries:
#                 logging.info(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)  # Delay before retrying
#             else:
#                 logging.error(f"Max retries reached for {player_url}.")

#     # Return empty dictionary if all retries fail
#     logging.info(f"Returning empty dictionary")
#     return {}

def scrape_player_data(
    player_url: str,
    retries: int,
    delay: int
) -> Dict:
    """
    Arguments:
    - player_url: player link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of player information from url
    """

    # initialize data to be retrieved
    response_var_list = ['nameparam', 'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast', 'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate', 'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link', 'itf_id', 'atp_id', 'dc_id', 'wiki_id', 'elo_rating', 'elo_rank',]
    player_dict = {var: None for var in response_var_list}


    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            response = make_request(url=player_url)
            response_text = response.text
                
            for var in response_var_list:
                try:
                    val = scrape_javascript_var(
                        content=response_text,
                        var=var
                    )
                    player_dict[var] = val
                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")

            # check if all values in dict are None
            if all(value is None for value in player_dict.values()):
                
                logging.info(f"All values None for {player_url}")
                
                # Log possible causes
                logging.debug(f"Page Content Length: {len(response_text)}")
                logging.debug(f"Response Status Code: {response.status_code}")
                
                # Log which variables were not found
                missing_vars = [var for var, val in player_dict.items() if val is None]
                logging.debug(f"Missing variables: {missing_vars}")

                attempt += 1
                
                # retry if possible
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...Attempt {attempt} for {player_url}")
                    time.sleep(delay)
                    continue
                else:
                    logging.error(f"Max retries reached for {player_url}...Returning empty dictionary")
                return {}

            # return dictionary if data successfully extracted
            return player_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {player_url}: {e}")

            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {player_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}
