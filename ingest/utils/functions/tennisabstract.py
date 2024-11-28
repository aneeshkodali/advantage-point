from bs4 import BeautifulSoup
from ingest.utils.functions.scrape import (
    scrape_page_source_var
)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from typing import (
    Dict,
    List,
)
import logging

def get_match_url_list(
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
    driver: webdriver,
    match_url: str,
    retries: int,
    delay: int
) -> Dict:
    """
    Arguments:
    - driver: Selenium webdriver
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

def get_match_data(
    driver: webdriver,
    match_url: str,
    retries: int,
    delay: int 
) -> Dict:

    # get match data from url
    match_data_dict_url = get_match_data_url(
        match_url=match_url
    )
    # get match data from webscrape
    match_data_dict_scraped = get_match_data_scraped(
        driver=driver,
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
    driver: webdriver,
    match_url: str,
    retries: int,
    delay: int
) -> List:
    """
    Arguments:
    - driver: Selenium webdriver
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
            driver.get(match_url)

            # wait for the pointlog to render
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return typeof pointlog !== 'undefined'")
            )
            response_page_source = driver.page_source

            # get the pointlog data
            pointlog_raw = scrape_page_source_var(
                page_source=response_page_source,
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
                    logging.info(f"Point {index+1} out of {len(pointlog_tr_list)}")
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
            except Exception as e:
                logging.info(f"Error getting point data for {match_url}: {e}")
                return []

        except (TimeoutException, WebDriverException) as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {match_url}: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay*attempt} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {match_url}.")

    # Return empty list if all retries fail
    logging.info(f"Returning empty list")
    return []

