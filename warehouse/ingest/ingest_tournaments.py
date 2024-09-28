from typing import (
    Dict,
    List
)
from utils.functions.scrape import scrape_page_source_var
from utils.functions.selenium_fn import create_webdriver
import logging
import re

def get_tournament_tr_list(driver) -> List:
    """
    Arguments:
    - driver: selenium webdriver

    Returns list of initial tournament data
    """

    # open url in browser
    tournament_base_url = 'https://www.minorleaguesplits.com/tennisabstract/cgi-bin/jstourneys'
    driver.get(tournament_base_url)

    tournament_table = driver.find_element(By.TAG_NAME, 'table')

    # find <tr> elements (excluding 1st 3)
    tournament_tr_list = tournament_table.find_elements(By.TAG_NAME, 'tr')[3:]
    
    return tournament_tr_list

def parse_tournament_tr(tournament_tr) -> Dict:
    """
    Arguments:
    - tournament_tr: selenium webelement corresponding to a <tr>

    Returns dictionary to tournament data
    """

    # find <a> element
    tournament_a = tournament_tr.find_element(By.TAG_NAME, 'a')
    # extract href
    tournament_href = tournament_a.get_attribute('href')
    # extract text
    tournament_text = tournament_a.text

    # text is of the format: {optional W_}{YYYY}{tournament}.js
    regex_pattern = r'(?P<prefix>W_)?(?P<year>\d{4})(?P<name>.+)\.js'
    regex_match = re.search(regex_pattern, tournament_text)

    # create dict of initial data
    tournament_dict = {}
    tournament_dict['tournament_url'] = tournament_href
    tournament_dict['tournament_url_suffix'] = tournament_text

    if regex_match:
        
        tournament_dict['tournament_gender'] = 'W' if regex_match.group('prefix') is not None else 'M' # M if prefix does not exist
        tournament_dict['tournament_year'] = regex_match.group('year')
        tournament_dict['tournament_name'] = regex_match.group('name').replace('_', ' ')

    return tournament_dict

def scrape_tournament_data(
    driver,
    tournament_url: str,
) -> Dict:
    """
    Arguments:
    - driver: selenium webdriver
    - tournament_url: tournament URL

    Returns dictionary of scraped tournament data
    """

    # initialize driver
    driver = create_webdriver()

    # open url in browser
    driver.get(tournament_url)

    # retrieve the page source
    response_page_source = driver.page_source

    # initialize variable list
    tournament_var_list = ['tyear', 'tdate', 'tname', 'tsurf', 'tlev', 'tpoints', 'rdate', 'lastyear', 'lastyearlink', 'nextyear', 'nextyearlink',]
    
    # create dictionary
    tournament_dict = {
        var:scrape_page_source_var(page_source=response_page_source, var=var) for var in tournament_var_list
    }

    return tournament_dict

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # initialize driver
    driver = create_webdriver()

    # get list of tournaments
    tournament_tr_list = get_tournament_tr_list(driver=driver)
    logging.info(f"Found {len(tournament_tr_list)} tournaments.")

    # loop through tournaments
    for i, tournament_tr in enumerate(tournament_tr_list):

        logging.info(f"Starting {i+1} of {len(tournament_tr_list)}.")
        
        # create dict of tournament info from <tr> element
        tournament_dict_parsed = parse_tournament_tr(tournament_tr)
        tournament_url_suffix = tournament_dict_parsed['tournament_url_suffix']
        logging.info(f"Begin parsing {tournament_url_suffix}")
        
        # scrape rest of tournament data
        tournament_url = tournament_dict_parsed['tournament_url']
        tournament_dict_scraped = scrape_tournament_data(
            driver=driver,
            tournament_url=tournament_url
        )
        
        # combine data
        tournament_dict = {
            **tournament_dict_parsed,
            **tournament_dict_scraped
        }
        logging.info(f"Finished parsing {tournament_url_suffix}")

    # quit driver
    driver.quit()

        # create or replace temp table and insert

        # create or alter target table

        # merge into target table

    

if __name__ == "__main__":
    main()