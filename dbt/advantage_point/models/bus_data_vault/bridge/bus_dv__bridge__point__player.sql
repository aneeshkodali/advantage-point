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
    from {{ ref('bus_dv__pit__match') }}
),

pit_player as (
    select
        *
    from {{ ref('bus_dv__pit__player') }}
),

joined as (
    select
        link_point_server.lk_point_server,
        link_point_server.hk_point,
        link_point_server.hk_server,

        link_point_server.load_datetime as link_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__bridge__point__server' as bus_dv_source_model
        
    from link_point_server
    left join pit_point on link_point_server.hk_point = pit_point.hk_point
    left join pit_player as pit_server on link_point_server.hk_server = pit_server.hk_player
)

select * from joined

