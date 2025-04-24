{% macro generate_point_game_surrogate_key(
    point_business_key_col,
    game_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            point_business_key_col,
            game_business_key_col
        ]
    ) }}

{% endmacro %}
