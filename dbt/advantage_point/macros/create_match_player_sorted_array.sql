{% macro create_match_player_sorted_array(player_array) %}
    array_sort({{ player_array }})
{% endmacro %}