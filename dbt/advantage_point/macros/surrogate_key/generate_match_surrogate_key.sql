{% macro generate_match_surrogate_key(
    match_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [match_business_key_col]
    ) }}

{% endmacro %}
