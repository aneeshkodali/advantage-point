{% macro generate_match_point_surrogate_key(
    match_point_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [match_point_business_key_col]
    ) }}

{% endmacro %}
