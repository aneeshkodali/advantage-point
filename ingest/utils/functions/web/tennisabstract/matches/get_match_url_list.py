from bs4 import BeautifulSoup
from typing import (
    Dict,
    List,
)
from utils.functions.web.scrape.make_request import make_request

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