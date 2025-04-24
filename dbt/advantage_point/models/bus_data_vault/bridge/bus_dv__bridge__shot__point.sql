{{
    config(
        unique_key='lk_shot_point'
    )
}}

with

link_record_sources as (
    select * from {{ ref('stg__seed__link_record_sources') }}
),

int_shot as (
    select * from {{ ref('bus_dv__int__shot') }}
),

hub_shot as (
    select * from {{ ref('bus_dv__hub__shot') }}
),

-- add hub key
records_lkey as (
    select
        {{ generate_shot_point_surrogate_key(
            shot_business_key_col='lnk.bk_shot',
            point_business_key_col='lnk.bk_point'
        ) }} as lk_shot_point,

        hub_shot.hk_shot,
        hub_point.hk_point,

        lnk.bk_shot,
        lnk.bk_point,

        lnk.shot_number,
        lnk.shot_number_in_point,
        lnk.record_source
    from int_shot as lnk
    left join hub_shot on lnk.bk_shot = hub_shot.bk_shot
    left join hub_point on lnk.hk_point = hub_point.hk_point
),

-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_shot_point order by link_rec_src.sort_order) as rn -- assing row number
    from records_lkey as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'bus_dv__bridge__shot__point'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_shot_point,
        current_timestamp as load_datetime,
        record_source,

        hk_shot,
        bk_shot,
        hk_point,
        bk_point,

        shot_number,
        shot_number_in_point

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_shot_point = incr.lk_shot_point
        ) -- filter for new pk records
        {% endif %}
)

select * from final