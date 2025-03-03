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
        hub.*,
        row_number() over (partition by hub.hk_tournament order by hub_rec_src.sort_order) as rn -- assing row number
    from records_hkey as hub
    left join hub_record_sources as hub_rec_src on 1=1
        and hub_rec_src.hub_name = 'hub__tournaments'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_tournament,

        current_timestamp as load_datetime,
        record_source,

        tournament_year,
        tournament_gender,
        tournament_name
        
    from records_rownum
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.tournament_year = final.tournament_year
                and existing.tournament_gender = final.tournament_gender
                and existing.tournament_name = final.tournament_name
        ) -- filter for new pk records
        {% endif %}
)

select * from final