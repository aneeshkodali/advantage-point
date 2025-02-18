from typing import (
    Dict,
)

def create_tournament_url(
    tournament_dict: Dict
) -> str:
    """
    Arguments:
    - tournament_dict: Dictionary of tournament data

    Returns tournament url
    """

    # parse dictionary
    tournament_year = tournament_dict['tournament_year']
    tournament_name = tournament_dict['tournament_name'].replace(' ', '')
    tournament_gender = tournament_dict['tournament_gender']

    # format parts of url string
    url_tournament_str = 'tourney' if tournament_gender == 'M' else 'wtourney'
    url_tournament_name = f"{tournament_year}{tournament_name}"
    url_tournament_gender_prefix = '' if tournament_gender == 'M' else 'W_'

    tournament_url = f"https://www.tennisabstract.com/cgi-bin/{url_tournament_str}.cgi?t={url_tournament_gender_prefix}{url_tournament_name}"
    
    return tournament_url