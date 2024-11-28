from typing import Optional
import random
import requests
import re

def make_request(
    url: str
):
    """
    Arguments:
    - url: Url for request
    Returns a response
    """

    # add list of agents
    user_agent_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.77 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.77 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.5481.77 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 15_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148 Safari/604.1",
    ]

    # create request header
    headers = {
        "User-Agent": random.choice(user_agent_list),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    # make request
    response = requests.get(url, headers=headers)

    return response

def scrape_page_source_var(
    page_source: str,
    var: str
) -> Optional[str]:
    """
    Arguments:
    - page_source: web page source
    - var: variable to scrape
    
    Returns variable value or none.
    """

    # within page source, variable is of value: var {var}{optional space}={optional space}{value};
    regex_pattern = fr"var {var}\s?=\s?(?P<{var}>.*);"
    regex_var_match = re.search(regex_pattern, page_source)
    val = regex_var_match.group(var) if regex_var_match else None

    return val