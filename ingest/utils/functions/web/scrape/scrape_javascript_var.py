from typing import (
    Optional
)
import re

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