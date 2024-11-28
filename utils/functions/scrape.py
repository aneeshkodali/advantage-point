from typing import Optional
import re

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
    # regex_pattern = fr"var {var}\s?=\s?(?P<{var}>.*);"
    regex_pattern = fr"var {var}\s?=\s?(?P<{var}>.*?)(?=;)$"
    regex_var_match = re.search(regex_pattern, page_source, re.MULTILINE)
    val = regex_var_match.group(var) if regex_var_match else None

    return val