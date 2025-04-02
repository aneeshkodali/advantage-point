{% macro generate_tournament_business_key(
    tournament_year_col,
    tournament_gender_col,
    tournament_name_col
) %}

    concat_ws(
        '||',
        cast({{ tournament_year_col }} as text),
        {{ tournament_gender_col }},
        {{ tournament_name_col }}
    )
    
{% endmacro %}
