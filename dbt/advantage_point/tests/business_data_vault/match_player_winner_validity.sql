-- tests/business_vault/test_match_player_winner_validity.sql

with

match_agg as (
    select
        hk_match,
        count(*) as player_count,
        sum(cast(is_winner as int)) as winner_count
    from {{ ref('bus_dv__bridge__match__player') }}
    group by 1
)

select *
from match_agg
where (player_count != 2 or winner_count != 1)
