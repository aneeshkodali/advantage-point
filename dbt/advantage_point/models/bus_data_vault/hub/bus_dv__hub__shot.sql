{{
    config(
        unique_key='hk_shot'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

int_shot as (
    select
        bk_shot,
        bus_dv_source_model,

        bk_point,
        shot_number_in_point
    from {{ ref('bus_dv__int__shot') }}
),

-- union data
records_union as (
    (select * from int_shot)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_shot_surrogate_key(
            shot_business_key_col='bk_shot'
        ) }} as hk_shot
    from records_union
),

-- add row number to order records
records_rownum as (
    select
        hub.*,
        row_number() over (partition by hub.hk_shot order by hub_rec_src.sort_order) as rn -- assing row number
    from records_hkey as hub
    left join hub_record_sources as hub_rec_src on 1=1
        and hub_rec_src.hub_name = 'bus_dv__hub__shot'
        and hub.bus_dv_source_model = hub_rec_src.record_source
),

final as (
    select
        hk_shot,
        bk_shot,
        current_timestamp as bus_dv_load_datetime,
        bus_dv_source_model as record_source,
        
        bk_point,
        shot_number_in_point
    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_shot = incr.hk_shot
        ) -- filter for new pk records
        {% endif %}
)

select * from final