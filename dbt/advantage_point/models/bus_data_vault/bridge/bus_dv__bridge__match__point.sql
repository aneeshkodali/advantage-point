with

link_match_point as (
    select
        lk_match_point,
        hk_match,
        hk_point,

        load_datetime
    from (
        {{ get_latest_record(
            model_ref=ref('raw_dv__link__match__point'),
            partition_by_col='lk_match_point',
            order_by_col='load_datetime'
        ) }}
    ) as m_p
),

pit_match as (
    select
        *
    from {{ ref('bus_dv__pit__match') }}
),

pit_point as (
    select
        *
    from {{ ref('bus_dv__pit__point') }}
),

joined as (
    select
        link_match_point.lk_match_point,
        link_match_point.hk_match,
        link_match_point.hk_point,

        link_match_point.load_datetime as link_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__bridge__match__point' as bus_dv_source_model
    from link_match_point
    left join pit_match on link_match_point.hk_match = pit_match.hk_match
    left join pit_point on link_match_point.hk_point = pit_point.hk_point
)

select * from joined