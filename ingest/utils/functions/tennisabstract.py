from bs4 import BeautifulSoup
from ingest.utils.functions.scrape import (
    make_request,
    scrape_javascript_var,
    scrape_page_source_var,
)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from typing import (
    Dict,
    List,
)
import ast
import logging
import re
import time

def get_match_url_list(
) -> List[Dict]:
    """
    Arguments:
    - driver: Selenium webdriver

    Returns list of match urls from source (url)
    """

    # retrieve url page source
    match_list_url = 'https://www.tennisabstract.com/charting/'
    response = make_request(url=match_list_url)

    # parse page source
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # links are hrefs in last <p>
    p_tag_match = soup.find_all('p')[-1]
    match_href_list = [f"https://www.tennisabstract.com/charting/{a['href']}" for a in p_tag_match.find_all('a', href=True)]

    match_url_list = [
        {'match_url': match_url} for match_url in match_href_list
    ]

    return match_url_list

def get_match_data_url(
    match_url: str
) -> Dict:
    """
    Arguments:
    - match_url: match link

    Returns dictionary of match information from url
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

    return match_data_dict

def get_match_data_scraped(
    match_url: str,
    retries: int,
    delay: int
) -> Dict:
    """
    Arguments:
    - match_url: match link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of match information from url
    """

    # initialize data
    match_dict = {}

    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            response = make_request(url=match_url)
            soup = BeautifulSoup(response.text, 'html.parser')
                
            # get the match title (<title>): <match info>: <player1> vs <player2> Detailed Stats | Tennis Abstract
            try:
                match_title = soup.find('title').text.split(' Detailed Stats | Tennis Abstract')[0]
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable match_title: {e}")
                match_title = None
            match_dict["match_title"] = match_title

            # get the match result (b): <winner> d. <loser> score
            try:
                match_result = soup.find('b', string=re.compile(r".+\sd\.\s.+\s.+")).text
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable match_result: {e}")
                match_result = None
            match_dict["match_result"] = match_result

            # check if all values in dict are None -> return empty dict
            if all(value is None for value in match_dict.values()):
                logging.info(f"All values None for {match_url} - Returning empty dictionary.")
                return {}

            # return dictionary if data successfully extracted
            return match_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {match_url}: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {match_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}

def get_match_data(
    match_url: str,
    retries: int,
    delay: int 
) -> Dict:
    """
    Arguments:
    - match_url: match link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of match information from url
    """


    # get match data from url
    match_data_dict_url = get_match_data_url(
        match_url=match_url
    )
    # get match data from webscrape
    match_data_dict_scraped = get_match_data_scraped(
        match_url=match_url,
        retries=retries,
        delay=delay
    )

    # combine dictionaries
    match_data_dict = {
        **match_data_dict_url,
        **match_data_dict_scraped,
    }

    return match_data_dict

def get_match_point_data(
    match_url: str,
    retries: int,
    delay: int
) -> List:
    """
    Arguments:
    - match_url: match link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns list of dictionaries of match point data
    """

    # initialize data
    match_point_list = []

    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            response = make_request(url=match_url)
           
            # get the pointlog data
            pointlog_raw = scrape_javascript_var(
                content=response.text,
                var='pointlog'
            )

            try:
                # extract the data (after 1st tr - headers)
                pointlog_soup = BeautifulSoup(pointlog_raw, 'html.parser')
                pointlog_tr_list = pointlog_soup.find_all('tr')[1:]

                # filter out empty rows
                pointlog_tr_list = [
                    tr for tr in pointlog_tr_list 
                    if all(td.get_text(strip=True) for td in tr.find_all('td'))
                ]

                # loop through tr list
                for index, tr in enumerate(pointlog_tr_list):
                    tr_td_list = tr.find_all('td')
                    point_data = {
                        'match_url': match_url,
                        'point_number': index + 1,
                        'server': tr_td_list[0].get_text(strip=True),
                        'sets': tr_td_list[1].get_text(strip=True),
                        'games': tr_td_list[2].get_text(strip=True),
                        'points': tr_td_list[3].get_text(strip=True),
                        'point_description': tr_td_list[4].get_text(strip=True),
                    }
                    match_point_list.append(point_data)
                return match_point_list
            except Exception as e:
                logging.info(f"Error getting point data for {match_url}: {e}")
                return []

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {match_url}: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {match_url}.")

    # Return empty list if all retries fail
    logging.info(f"Returning empty list")
    return []

def create_player_url(
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

def get_player_url_list() -> List[Dict]:
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

    # loop through each element and create url
    player_url_list = []
    for player in player_list:

        # each element in list is of format: (<gender>) <name>)
        regex_pattern = r'(?P<gender>\((.*?)\))\s*(?P<name>.*)'
        regex_match = re.search(regex_pattern, player)
        gender = regex_match.group('gender').strip('()')
        name = regex_match.group('name')

        # create url
        player_url = create_player_url(
            player_gender=gender,
            player_name=name
        )
        player_url_dict = {}
        player_url_dict['player_url'] = player_url
        player_url_list.append(player_url_dict)

    return player_url_list

def get_player_data_url(
    player_url: str
) -> Dict:
    """
    Arguments:
    - player_url: player link

    Returns dictionary of player information from url
    """

    # get player data
    player_data_dict = {}
    player_data_dict['player_url'] = player_url
    player_data_dict['player_gender'] = 'W' if 'wplayer' in player_url else 'M'

    return player_data_dict

def get_player_data_scraped(
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

    # initialize data
    # initialize data to be retrieved
    response_var_list = ['nameparam', 'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast', 'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate', 'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link', 'itf_id', 'atp_id', 'dc_id', 'wiki_id']
    player_dict = {var: None for var in response_var_list}

    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            response = make_request(url=player_url)
                
            for var in response_var_list:
                try:
                    val = scrape_javascript_var(
                        page_source=response.text,
                        var=var
                    )
                    player_dict[var] = val
                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")

            # check if all values in dict are None -> return empty dict
            if all(value is None for value in player_dict.values()):
                logging.info(f"All values None for {player_url} - Returning empty dictionary.")
                return {}

            # return dictionary if data successfully extracted
            return match_dict

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

def get_player_data(
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


    # get player data from url
    player_data_dict_url = get_player_data_url(
        player_url=player_url
    )
    # get player data from webscrape
    player_data_dict_scraped = get_player_data_scraped(
        player_url=player_url,
        retries=retries,
        delay=delay
    )

    # combine dictionaries
    player_data_dict = {
        **player_data_dict_url,
        **player_data_dict_scraped,
    }

    return player_data_dict
