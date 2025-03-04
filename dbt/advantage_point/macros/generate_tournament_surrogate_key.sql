{% macro generate_tournament_surrogate_key(
    tournament_year_col,
    tournament_gender_col,
    tournament_name_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            tournament_year_col,
            tournament_gender_col,
            tournament_name_col
        ]
    ) }}
{% endmacro %}
