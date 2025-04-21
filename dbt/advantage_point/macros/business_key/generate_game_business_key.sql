{% macro generate_game_business_key(
    bk_match_col,
    game_col
) %}

    concat_ws(
        '||',
        {{ bk_match_col }},
        lpad(cast({{ game_col }} as text), 4, '0')
    )
    
{% endmacro %}
