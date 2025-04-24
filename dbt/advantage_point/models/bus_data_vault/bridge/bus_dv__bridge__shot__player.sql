{{
    config(
        unique_key='lk_shot_player'
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

hub_player as (
    select * from {{ ref('raw_dv__hub__player') }}
),

-- get list of point servers
point_server as (
    select * from {{ ref('bus_dv__int__point__player') }}
    where is_point_server = true
),

-- get list of point receivers
point_receiver as (
    select * from {{ ref('bus_dv__int__point__player') }}
    where is_point_server = false
),

-- join models
joined as (
    select
        int_shot.bk_shot,
        int_shot.shot_number,
        case
            when shot_number % 2 != 0 then point_server.hk_player
            else point_receiver.hk_player
        end as hk_player,
        int_shot.bus_dv_source_model as record_source
    from int_shot
    left join point_server on int_shot.hk_point = point_server.hk_point
    left join point_receiver on int_shot.hk_point = point_receiver.hk_point
),

-- add hub key
records_lkey as (
    select
        {{ generate_shot_player_surrogate_key(
            shot_business_key_col='lnk.bk_shot',
            player_business_key_col='hub_player.bk_player'
        ) }} as lk_shot_player,

        hub_shot.hk_shot,
        lnk.hk_player,

        lnk.bk_shot,
        hub_player.bk_player,

        lnk.shot_number,
        lnk.record_source
    from joined as lnk
    left join hub_shot on lnk.bk_shot = hub_shot.bk_shot
    left join hub_player on lnk.hk_player = hub_player.hk_player
),


-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_shot_player order by link_rec_src.sort_order) as rn -- assing row number
    from records_lkey as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'bus_dv__bridge__shot__player'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_shot_player,
        current_timestamp as load_datetime,
        record_source,

        hk_shot,
        bk_shot,
        hk_player,
        bk_player,

        shot_number

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_shot_player = incr.lk_shot_player
        ) -- filter for new pk records
        {% endif %}
)

select * from final