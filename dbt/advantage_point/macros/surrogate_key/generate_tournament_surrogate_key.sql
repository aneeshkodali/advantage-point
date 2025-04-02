{% macro generate_tournament_surrogate_key(
    tournament_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [tournament_business_key_col]
    ) }}

{% endmacro %}
