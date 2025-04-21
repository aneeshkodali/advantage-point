{% macro generate_game_surrogate_key(
    game_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [game_business_key_col]
    ) }}

{% endmacro %}
