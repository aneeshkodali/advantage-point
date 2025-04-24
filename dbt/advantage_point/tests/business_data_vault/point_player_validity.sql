with

point_agg as (
    select
        hk_point,
        count(*) as player_count,
        sum(cast(is_point_server as int)) as is_point_server_count,
        sum(cast(is_point_ending_player as int)) as is_point_ending_player_count,
        sum(cast(is_point_winner as int)) as is_point_winner_count
    from {{ ref('bus_dv__bridge__point__player') }}
    group by 1
)

select *
from point_agg
where (
    player_count != 2
    or is_point_server_count != 1
    or is_point_ending_player_count != 1
    or is_point_winner_count != 1
)
