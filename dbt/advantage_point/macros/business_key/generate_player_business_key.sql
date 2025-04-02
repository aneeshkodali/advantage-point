{% macro generate_player_business_key(
    player_name_col,
    player_gender_col
) %}

    concat_ws(
        '||',
        {{ player_name_col }},
        {{ player_gender_col }}
    )
    
{% endmacro %}
