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
    user_agent_list_desktop = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.127 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
    ]

    # user_agent_list_mobile = [
    #     "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
    #     "Mozilla/5.0 (Linux; Android 11; Pixel 4 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Mobile Safari/537.36",
    #     "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    #     "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    #     "Mozilla/5.0 (Linux; Android 8.0; Nexus 6P Build/OPR6.170623.017) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36"
    # ]

    user_agent_list = user_agent_list_desktop

    # create request header
    headers = {
        "User-Agent": random.choice(user_agent_list),
        "Referer": "https://www.google.com",
    }

    # make request
    response = requests.get(url, headers=headers)

    return response

def scrape_javascript_var(
    content,
    var: str
) -> Optional[str]:
    """
    Arguments:
    - content: web page content
    - var: variable to scrape
    
    Returns variable value or none.
    """

    # within page source, variable is of value: var {var}{optional space}={optional space}{value};
    regex_pattern = fr"var {var}\s?=\s?(?P<{var}>.*);"
    regex_var_match = re.search(regex_pattern, content)
    val = regex_var_match.group(var) if regex_var_match else None

    return val