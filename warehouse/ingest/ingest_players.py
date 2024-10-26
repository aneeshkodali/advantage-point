import ast
import re

def create_player_url(
    player_gender: str,
    player_name: str
) -> str:
    """
    Arguments:
    - player_gender: gender (M or W)
    - player_name: player full name

    Returns url
    """

    # format parts of url string
    url_player_str = 'player' if player_gender == 'M' else 'wplayer'
    url_player_name = player_name.replace(' ', '')

    player_url = f"https://www.tennisabstract.com/cgi-bin/{url_player_str}.cgi?p={url_player_name}"

    return player_url


def get_player_list_source(
    driver,
    player_list_url: str
) -> List:
    """
    Arguments:
    - driver: Selenium webdriver
    - player_list_url: url that contains list of players

    Returns list of player urls from source (url)
    """

    # retrieve url page source
    driver.get(player_list_url)
    response_page_source = driver.page_source

    # retrieve list-like string
    regex_var = 'playerlist'
    regex_pattern = fr"var {regex_var}\s?=\s?(?P<{regex_var}>.*?);"
    regex_var_match = re.search(regex_pattern, response_page_source)
    val = regex_var_match.group(regex_var)
    player_list = ast.literal_eval(val)

    # loop through each element and create url
    player_url_list = []
    for player in player_list:

        # each element in list is of format: (<gender>) <name>)
        regex_pattern = r'(?P<gender>\((.*?)\))\s*(?P<name>.*)'
        regex_match = re.search(regex_pattern, player)
        gender = regex_match.group('gender').strip('()')
        name = regex_match.group('name')

        # create url
        player_url = create_player_url(
            player_gender=gender,
            player_name=name
        )
        player_url_list.append(player_url)

def get_player_list_ingest():
    """
    Returns list of player urls from ingest table
    """
    pass

def main():
    pass

if __name__ == "__main__":
    main()