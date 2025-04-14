{% macro generate_match_point_business_key(
    match_date_col,
    match_gender_col,
    match_tournament_col,
    match_round_col,
    match_players_col,
    match_point_col
) %}

    concat_ws(
        '||',
        cast({{ match_date_col }} as text),
        {{ match_gender_col }},
        {{ match_tournament_col }},
        {{ match_round_col }},
        {{ match_players_col }},
        lpad(cast({{ match_point_col }} as text), 4, '0')
    )
    
{% endmacro %}
