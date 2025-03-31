{% macro sort_player_array(player_array) %}
    array_agg(
        array_sort({{ player_array }}),
        ', '
    )
{% endmacro %}