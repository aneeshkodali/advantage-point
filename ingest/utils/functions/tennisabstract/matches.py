from bs4 import BeautifulSoup
from ingest.utils.functions.scrape import (
    make_request,
)
from typing import (
    Dict,
    List,
)
import logging
import re
import time

def get_match_url_list() -> List[Dict]:
    """
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