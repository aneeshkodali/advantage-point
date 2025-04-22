{% macro generate_point_business_key(
    bk_match_col,
    point_number_col
) %}

    concat_ws(
        '||',
        {{ bk_match_col }},
        lpad(cast({{ point_number_col }} as text), 4, '0')
    )
    
{% endmacro %}
