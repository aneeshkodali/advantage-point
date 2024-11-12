from bs4 import BeautifulSoup
from typing import (
    List
)

def get_match_list_source(
    driver
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
    match_href_list = [a['href'] for a in p_tag_match.find_all('a', href=True)]

    return match_href_list

def main():
    pass

if __name__ == "__main__":
    main()