{% macro generate_set_surrogate_key(
    set_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [set_business_key_col]
    ) }}

{% endmacro %}
