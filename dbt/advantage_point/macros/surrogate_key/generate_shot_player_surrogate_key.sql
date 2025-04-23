{% macro generate_shot_player_surrogate_key(
    shot_business_key_col,
    player_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            shot_business_key_col,
            player_business_key_col
        ]
    ) }}

{% endmacro %}
