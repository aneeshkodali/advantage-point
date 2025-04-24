with

link_point_server as (
    select * from {{ ref('raw_dv__link__point__server') }}
),

link_match_player as (
    select * from {{ ref('raw_dv__link__match__player') }}
),

link_match_point as (
    select * from {{ ref('raw_dv__link__match__point') }}
),

hub_player as (
    select * from {{ ref('raw_dv__hub__player') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

-- prep server rows
point_player_server as (
    select
        lk_point_server as lk_point_player,
        hk_point,
        bk_point,
        hk_server as hk_player,
        bk_server as bk_player,
        true as is_point_server,
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
        link_point_server.bk_point,
        link_match_player.hk_player,
        link_match_player.bk_player,
        false as is_point_server,
        link_point_server.load_datetime as link_load_datetime
    from link_point_server
    -- join match_point to point_server on point to get match_point
    left join link_match_point on link_point_server.hk_point = link_match_point.hk_point
    -- join match_player to match_point to get point_player (WHERE clause filters out server)
    left join link_match_player on link_match_point.hk_match = link_match_player.hk_match
    -- join to get bk for hk generation
    left join hub_point on link_match_point.hk_point = hub_point.hk_point
    -- join to get bk for hk generation
    left join hub_player on link_match_player.hk_player = hub_player.hk_player
    where 1=1
        -- filter out where player is server
        and link_point_server.hk_server != link_match_player.hk_player

),

-- union
point_player_union as (
    select
        point_player.lk_point_player,
        point_player.hk_point,
        point_player.bk_point,
        point_player.hk_player,
        point_player.bk_player,
        point_player.is_point_server,
        point_player.link_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__point__player' as bus_dv_source_model
    from (
        (
            select
                lk_point_player,
                hk_point,
                bk_point,
                hk_player,
                bk_player,
                is_point_server,
                link_load_datetime
            from point_player_server
        )
        union all
        (
            select
                lk_point_player,
                hk_point,
                bk_point,
                hk_player,
                bk_player,
                is_point_server,
                link_load_datetime
            from point_player_receiver
        )
    ) as point_player
)

select * from point_player_union