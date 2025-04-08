{% macro generate_match_player_surrogate_key(
    match_business_key_col,
    player_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            match_business_key_col,
            player_business_key_col
        ]
    ) }}

{% endmacro %}
