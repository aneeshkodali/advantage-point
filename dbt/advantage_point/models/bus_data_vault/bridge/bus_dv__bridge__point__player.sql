with

link_point_server as (
    select
        lk_point_server,
        hk_point,
        hk_server,

        load_datetime
    from (
        {{ get_latest_record(
            model_ref=ref('raw_dv__link__point__server'),
            partition_by_col='lk_point_server',
            order_by_col='load_datetime'
        ) }}
    ) as p_s
),

 pit_point as (
    select
        *
    from {{ ref('bus_dv__pit__point') }}
),

pit_player as (
    select
        *
    from {{ ref('bus_dv__pit__player') }}
),

bridge_match_player as (
    select
        *
    from {{ ref('bus_dv__bridge__match__player') }}
),

hub_player as (
    select
        *
    from {{ ref('raw_dv__hub__player') }}
),

hub_point as (
    select
        *
    from {{ ref('raw_dv__hub__point') }}
),

link_match_point as (
    select
        *
    from {{ ref('raw_dv__link__match__point') }}
),

-- prep server rows
point_player_server as (
    select
        lk_point_server as lk_point_player,
        hk_point,
        hk_server as hk_player,
        true as is_server,
        load_datetime as link_load_datetime
    from link_point_server
),

-- prep receiver rows
-- creates new lk_point_player values since receiver rows do not exist in raw data vault
point_player_receiver as (
    select
        {{ generate_point_player_surrogate_key(
            point_business_key_col='hub_point.bk_point',
            player_business_key_col='hub_player.bk_player'
        ) }} as lk_point_player,
        link_point_server.hk_point,
        bridge_match_player.hk_player,
        false as is_server,
        link_point_server.load_datetime as link_load_datetime
    from link_point_server
    -- join match_point to point_server on point to get match_point
    left join bridge_match_point on link_point_server.hk_point = bridge_match_point.hk_point
    -- join match_player to match_point to get point_player (WHERE clause filters out server)
    left join bridge_match_player on bridge_match_point.hk_match = bridge_match_player.hk_match
    -- join to get bk for hk generation
    left join hub_point on bridge_match_point.hk_point = hub_point.hk_point
    -- join to get bk for hk generation
    left join hub_player on bridge_match_player.hk_player = hub_player.hk_player
    where 1=1
        -- filter out where player is server
        and link_point_server.hk_server != bridge_match_player.hk_player

),

-- union
point_player_union as (
    select
        bridge_point_player.lk_point_player,
        bridge_point_player.hk_point,
        bridge_point_player.hk_player,
        bridge_point_player.is_server,
        bridge_point_player.link_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__bridge__point__player' as bus_dv_source_model
    from (
        (
            select
                lk_point_player,
                hk_point,
                hk_player,
                is_server,
                link_load_datetime
            from point_player_server
        )
        union all
        (
            select
                lk_point_player,
                hk_point,
                hk_player,
                is_server,
                link_load_datetime
            from point_player_receiver
        )
    ) as bridge_point_player
    left join pit_point on bridge_point_player.hk_point = pit_point.hk_point
    left join pit_player on bridge_point_player.hk_player = pit_player.hk_player
)

select * from point_player_union