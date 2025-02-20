{% macro generate_player_surrogate_key(player_name_col, player_gender_col) %}
    {{ generate_surrogate_key(player_name_col, player_gender_col) }}
{% endmacro %}
