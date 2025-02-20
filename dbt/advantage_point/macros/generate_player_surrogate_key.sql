{% macro generate_player_surrogate_key(player_name_col, player_gender_col) %}
    {{ dbt_utils.generate_surrogate_key([player_name_col, player_gender_col]) }}
{% endmacro %}
