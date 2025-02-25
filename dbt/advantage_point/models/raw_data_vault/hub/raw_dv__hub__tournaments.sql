{{
    config(
        unique_key='hk_tournament'
    )
}}

with

tennisabstract_tournaments as (
    select
        *,
        'tennisabstract' as record_source
    from {{ ref('stg__tennisabstract__tournaments') }}
),

-- union data
tournaments_union as (
    select
        tournament_year,
        tournament_gender,
        tournament_name,
        record_source,
        hk_tournament,
        row_number() over (partition by hk_tournament order by record_source) as rn -- assing row number
    from (
        select
            tournament_year,
            tournament_gender,
            tournament_name,
            {{ generate_tournament_surrogate_key(
                tournament_year_col='tournament_year',
                tournament_gender_col='tournament_gender',
                tournament_name_col='tournament_name'
            ) }} as hk_tournament,
            record_source
        from tennisabstract_tournaments
    ) as t_union
),

final as (
    select
        hk_tournament,
        tournament_year,
        tournament_gender,
        tournament_name,
        current_timestamp as load_datetime,
        record_source
    from tournaments_union
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and hk_tournament not in (
            select hk_tournament from {{ this }}
        ) -- filter for new pk records
        {% endif %}
)

select * from final