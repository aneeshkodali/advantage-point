{{
    config(
        unique_key='hk_tournament'
    )
}}

with

tennisabstract_tournaments as (
    select
        {{ generate_tournament_surrogate_key(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        ) }} as hk_tournament,
        tournament_year,
        tournament_gender,
        tournament_name,
        current_timestamp as load_dts,
        'tennisabstract' as record_source
    from {{ ref('stg__tennisabstract__tournaments') }}
)

select * from tennisabstract_tournaments