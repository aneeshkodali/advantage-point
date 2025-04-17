with

point_agg as (
    select
        hk_point,
        count(*) as player_count,
        sum(cast(is_server as int)) as server_count
    from {{ ref('bus_dv__bridge__point__player') }}
    group by 1
)

select *
from point_agg
where (player_count != 2 or server_count != 1)
