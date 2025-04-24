{% macro generate_shot_point_surrogate_key(
    shot_business_key_col,
    point_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            shot_business_key_col,
            point_business_key_col
        ]
    ) }}

{% endmacro %}
