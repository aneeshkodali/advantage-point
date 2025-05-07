with

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

-- join to hk
int_shot_player_hk as (
    select
        hub_shot.hk_shot,
        int_shot_player.bk_shot,
        hub_player.hk_player,
        hub_player.bk_player,
        int_shot_player.shot_number
    from joined as int_shot_player
    left join hub_shot on int_shot_player.bk_shot = hub_shot.bk_shot
    left join hub_player on int_shot_player.hk_player = hub_player.hk_player
),

final as (
    select
        hk_shot,
        hk_player
        bk_shot,
        bk_player,
        
        shot_number,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__shot__player' as bus_dv_source_model
    from int_shot_player_hk
)

select * from final