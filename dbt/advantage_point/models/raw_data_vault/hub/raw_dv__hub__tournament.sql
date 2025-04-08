{{
    config(
        unique_key='hk_tournament'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

tennisabstract_tournaments as (
    select
        bk_tournament,
        record_source,

        tournament_year,
        tournament_gender,
        tournament_name
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
            tournament_business_key_col='bk_tournament'
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
        and hub_rec_src.hub_name = 'raw_dv__hub__tournament'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_tournament,
        bk_tournament,
        current_timestamp as load_datetime,
        record_source,

        tournament_year,
        tournament_gender,
        tournament_name
        
    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_tournament = incr.hk_tournament
        ) -- filter for new pk records
        {% endif %}
)

select * from final