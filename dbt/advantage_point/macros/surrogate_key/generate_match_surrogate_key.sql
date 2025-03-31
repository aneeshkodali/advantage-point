{% macro generate_match_surrogate_key(
    match_date_col,
    match_gender_col,
    match_tournament_col,
    match_round_col,
    match_player_array_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            match_date_col,
            match_gender_col,
            match_tournament_col,
            match_round_col,
            match_player_array_col
        ]
    ) }}
{% endmacro %}
