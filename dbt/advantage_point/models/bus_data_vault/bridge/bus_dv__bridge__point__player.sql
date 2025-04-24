{{
    config(
        unique_key='lk_point_player'
    )
}}

with

link_record_sources as (
    select * from {{ ref('stg__seed__link_record_sources') }}
),

link_point_player as (
    select * from {{ ref('bus_dv__int__point__player') }}
),

bridge_shot_point as (
    select * from {{ ref('bus_dv__bridge__shot__point') }}
),

bridge_shot_player as (
    select * from {{ ref('bus_dv__bridge__shot__player') }}
)

 pit_point as (
    select
        *
    from {{ ref('bus_dv__pit__point') }}
),

-- add is_point_ending_player (did player hit last shot)
point_player_is_point_ending_player as (
    select
        link_point.player.lk_point_player,
        link_point_player.hk_point,
        link_point_player.bk_point,
        link_point_player.hk_player,
        link_point_player.bk_player,
        link_point_player.is_point_server,
        link_point_player.hk_player = bridge_shot_player.hk_player as is_point_ending_player,
        pit_point.point_result,
        link_point_player.bus_dv_source_model as record_source
    from link_point_player
    -- join point to get number of shots, result
    left join pit_point on link_point_player.hk_point = pit_point.hk_point
    -- join shot_point to get last shot
    left join bridge_shot_point on 1=1
        and link_point_player.hk_point = bridge_shot_point.hk_point
        and pit_point.point_length = bridge_shot_point.shot_number
    -- join shot_player to get player who hit last shot
    left join bridge_shot_player on bridge_shot_point.hk_shot = bridge_shot_player.hk_shot
),

-- add logic for if player won point
player_point_is_point_winner as (
    select
        lk_point_player,
        hk_point,
        bk_point,
        hk_player,
        bk_player,
        is_point_server,
        is_point_ending_player,
        record_source,

        case
            -- if player hit last shot
            when is_point_ending_player = true then
                case
                    -- if 'winner'-like shot
                    when point_result in ('ace', 'service winner', 'winner') then true
                    -- if 'error'-like shot
                    when point_result in ('double fault', 'forced error', 'unforced error') then false
                    else null
                end
            -- if player did NOT hit last shot --> opponent hit last shot
            when is_point_ending_player = false then
                case
                    -- if 'winner'-like shot
                    when point_result in ('ace', 'service winner', 'winner') then false
                    -- if 'error'-like shot
                    when point_result in ('double fault', 'forced error', 'unforced error') then true
                    else null
                end
            else null
        end as is_point_winner
    from point_player_is_point_ending_player
),

-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_point_player order by link_rec_src.sort_order) as rn -- assing row number
    from player_point_is_point_winner as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'bus_dv__bridge__point__player'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_point_player,
        current_timestamp as load_datetime,
        record_source,

        hk_point,
        bk_point,
        hk_player,
        bk_player,
        is_point_server,
        is_point_ending_player,
        is_point_winner

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_point_player = incr.lk_point_player
        ) -- filter for new pk records
        {% endif %}
)

select * from final