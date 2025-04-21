{% macro generate_set_business_key(
    bk_match_col,
    set_col
) %}

    concat_ws(
        '||',
        {{ bk_match_col }},
        cast({{ set_col }} as text)
    )
    
{% endmacro %}
