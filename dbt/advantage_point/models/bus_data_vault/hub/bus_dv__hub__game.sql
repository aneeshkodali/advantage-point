{{
    config(
        unique_key='hk_game'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

sat_point as (
    select distinct
        bk_game,
        bus_dv_source_model,

        bk_match,
        game_number_in_match
    from {{ ref('bus_dv__sat__point') }}
),

-- union data
records_union as (
    (select * from sat_point)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_game_surrogate_key(
            game_business_key_col='bk_game'
        ) }} as hk_game
    from records_union
),

-- add row number to order records
records_rownum as (
    select
        hub.*,
        row_number() over (partition by hub.hk_game order by hub_rec_src.sort_order) as rn -- assing row number
    from records_hkey as hub
    left join hub_record_sources as hub_rec_src on 1=1
        and hub_rec_src.hub_name = 'bus_dv__hub__game'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_game,
        bk_game,
        current_timestamp as bus_dv_load_datetime,
        bus_dv_source_model,
        
        bk_match,
        game_number_in_match
    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_game = incr.hk_game
        ) -- filter for new pk records
        {% endif %}
)

select * from final