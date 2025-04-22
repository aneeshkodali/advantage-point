{% macro generate_shot_business_key(
    bk_point_col,
    shot_number_col
) %}

    concat_ws(
        '||',
        {{ bk_point_col }},
        lpad(cast({{ shot_number_col }} as text), 4, '0')
    )
    
{% endmacro %}
