from bs4 import BeautifulSoup
from selenium import webdriver
from typing import (
    Dict,
    List
)
from utils.functions.selenium_fn import create_chromedriver
import logging
import os
import re

def get_match_url_list_source(
    driver: webdriver
) -> List[str]:
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

    return match_href_list

def fetch_match_data_scraped(
    driver: webdriver,
    match_url: str
) -> Dict:
    """
    Arguments:
    - driver: Selenium webdriver
    - match_url: match link

    Returns dictionary of match information from url
    """

    # initialize dictionary
    match_dict = {}

    # get url page source
    driver.get(match_url)
    response_page_source = driver.page_source

    # parse page
    soup = BeautifulSoup(response_page_source, 'html.parser')

    # Find the <b> tag after a <table> tag
    table_tag = soup.find('table')  # Find the first <table> tag
    if table_tag:
        # Get the next <b> tag after the table
        match_result_tag = table_tag.find_next('b')
        if match_result_tag:
           match_result =  match_result_tag.get_text().strip()
    else:
        match_result = None
    match_dict['match_result'] =  match_result

    # Find the point log
    pointlog_regex_pattern = fr"var pointlog\s?=\s?(?P<pointlog>.*?);"
    pointlog_regex_var_match = re.search(pointlog_regex_pattern, response_page_source)
    pointlog_raw = pointlog_regex_var_match.group('pointlog')

    # extract the data
    pointlog_soup = BeautifulSoup(pointlog_raw, 'html.parser')
    pointlog_tr_list = pointlog_soup.find_all('tr')
    pointlog_data_list = []
    for index, tr in enumerate(pointlog_tr_list):
        tr_td_list = tr.find_all('td')
        point_data = {
            'point_number': index + 1,
            'server': tr_td_list[0].get_text(strip=True),
            'sets': tr_td_list[1].get_text(strip=True),
            'games': tr_td_list[2].get_text(strip=True),
            'points': tr_td_list[3].get_text(strip=True),
            'point_description': tr_td_list[4].get_text(strip=True),
        }
        pointlog_data_list.append(point_data)

    match_dict['match_pointlog'] = pointlog_data_list

    return match_dict

def main():
    
    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # initialize driver
    webdriver_path = os.getenv('CHROMEDRIVER_PATH')
    driver = create_chromedriver(webdriver_path=webdriver_path)

    # get list of matches
    match_url_list = get_match_url_list_source(
        driver=driver
    )[:1]
    logging.info(f"Found {len(match_url_list)} matches.")

    # loop through matches
    # initialize list
    match_data_list = []
    for i, match_url in enumerate(match_url_list):

        logging.info(f"Starting {i+1} of {len(match_url_list)}.")
        logging.info(f"match url: {match_url}")

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
        match_data_dict['match_player_one'] = match_player_one.replace('_', ' ')
        match_player_two = match_url_parsed_list[5].replace('_', ' ')
        match_data_dict['match_player_two'] = match_player_two
        
        # get match data from webscrape
        match_data_dict_scraped = fetch_match_data_scraped(
            driver=driver,
            match_url=match_url
        )

        # combine dictionaries
        match_data_dict = {
            **match_data_dict,
            **match_data_dict_scraped,
        }

        # recreate dictionary such that each entry is point
        for i, point_dict in enumerate(match_data_dict['match_pointlog'][:10]):
            logging.info(f"Adding point number {i+1} for match: {match_url}")
            point_dict = {
                **point_dict,
                **{k:v for (k, v) in match_data_dict.items() if k != 'match_pointlog'}
            }

            match_data_list.append(point_dict)
        logging.info(f"Fetched data for: {match_url}")

    return match_data_list

if __name__ == "__main__":
    main()