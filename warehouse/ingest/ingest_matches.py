from bs4 import BeautifulSoup
from selenium import webdriver
from typing import (
    Dict,
    List
)
from utils.functions.selenium_fn import create_chromedriver
import json
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
    pointlog_regex_var_match = re.search(regex_pattern, response_page_source)
    if pointlog_regex_var_match:
        # Parse pointlog JSON data
        pointlog_json = pointlog_regex_var_match.group('pointlog')
        try:
            pointlog_data = json.loads(pointlog_json)
        except json.JSONDecodeError:
            pointlog_data = []
    else:
        pointlog_data = []

    # Check if pointlog data exists and is list-like
    if isinstance(pointlog_data, list) and pointlog_data:
        # Find column headers (keys) from the first row
        header_row = soup.find('table').find('tr')
        headers = [th.get_text().strip() for th in header_row.find_all('th')]

        # Convert pointlog data to a list of dictionaries with indices
        pointlog = [
            {**{headers[i]: point[i] for i in range(len(headers))}, 'point_number': idx + 1}
            for idx, point in enumerate(pointlog_data)
        ]
    else:
        pointlog = []

    match_dict['match_pointlog'] = pointlog

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
        for i, point_dict in enumerate(match_data_dict['pointlog'])[:10]:
            logging.info(f"Adding point number {i+1} for match: {match_url}")
            point_dict = {
                **point_dict,
                **{k:v for (k, v) in match_data_dict.items() if k != 'pointlog'}
            }

            match_data_list.append(point_dict)
        logging.info(f"Fetched data for: {match_url}")

    return match_data_list

if __name__ == "__main__":
    main()