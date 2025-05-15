{% macro generate_point_set_surrogate_key(
    point_business_key_col,
    set_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            point_business_key_col,
            set_business_key_col
        ]
    ) }}

{% endmacro %}
