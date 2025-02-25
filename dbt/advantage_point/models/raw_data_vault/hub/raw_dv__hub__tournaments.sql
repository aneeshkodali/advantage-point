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
    (
        select
            tournament_year,
            tournament_gender,
            tournament_name,
            record_source
        from tennisabstract_tournaments
    )
),

final as (
    select distinct
        {{ generate_tournament_surrogate_key(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        ) }} as hk_tournament,
        tournament_year,
        tournament_gender,
        tournament_name,
        current_timestamp as load_datetime,
        record_source
    from tournaments_union
    {% if is_incremental() %}
    where hk_tournament not in (select hk_tournament from {{ this }})
    {% endif %}
)

select * from final