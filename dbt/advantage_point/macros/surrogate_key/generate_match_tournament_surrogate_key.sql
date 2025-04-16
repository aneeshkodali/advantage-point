{% macro generate_match_tournament_surrogate_key(
    match_business_key_col,
    tournament_business_key_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            match_business_key_col,
            tournament_business_key_col
        ]
    ) }}

{% endmacro %}
