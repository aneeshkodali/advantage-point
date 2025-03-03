{{
    config(
        unique_key='hk_tournament'
    )
}}

with

tennisabstract_tournaments as (
    select
        tournament_year,
        tournament_gender,
        tournament_name,

        'tennisabstract__tournaments' as record_source
    from {{ ref('stg__tennisabstract__tournaments') }}
),

-- union data
records_union as (
    (select * from tennisabstract_tournaments)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_tournament_surrogate_key(
                tournament_year_col='tournament_year',
                tournament_gender_col='tournament_gender',
                tournament_name_col='tournament_name'
        ) }} as hk_tournament
    from records_union
),

-- add row number to order records
records_rownum as (
    select
        *,
        row_number() over (partition by hk_tournament order by record_source) as rn -- assing row number
    from records_hkey
),

final as (
    select
        hk_tournament,

        tournament_year,
        tournament_gender,
        tournament_name,
        
        current_timestamp as load_datetime,
        record_source
    from records_rownum
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and hk_tournament not in (
            select hk_tournament from {{ this }}
        ) -- filter for new pk records
        {% endif %}
)

select * from final