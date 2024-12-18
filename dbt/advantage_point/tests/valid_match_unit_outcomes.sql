with boolean_agg as (
    select
        {{ group_by_columns | join(', ') }},
        sum(case when {{ boolean_column }} = true then 1 else 0 end) as boolean_agg
    from {{ ref(model.name) }}
    group by {{ group_by_columns | join(', ') }}
)
select *
from boolean_agg
where boolean_agg != 1
