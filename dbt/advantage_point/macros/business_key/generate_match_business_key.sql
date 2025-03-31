{% macro generate_match_business_key(
    match_date_col,
    match_gender_col,
    match_tournament_col,
    match_round_col,
    match_player_array_col
) %}

    concat_ws(
        '||',
        cast({{ match_date_col }} as string),
        {{ match_gender_col }},
        {{ match_tournament_col }},
        {{ match_round_col }},
        cast({{ match_player_array_col }} as string)
    )
    
{% endmacro %}
