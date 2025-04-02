{% macro generate_player_surrogate_key(
    player_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [player_business_key_col]
    ) }}

{% endmacro %}
