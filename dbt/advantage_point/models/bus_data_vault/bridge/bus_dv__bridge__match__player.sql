with

-- contains match-player relation
link_match_player as (
    select
        lk_match_player,
        hk_match,
        hk_player,

        load_datetime
    from (
        {{ get_latest_record(
            model_ref=ref('raw_dv__link__match__player'),
            partition_by_col='lk_match_player',
            order_by_col='load_datetime'
        ) }}
    ) as m_p
),

 pit_match as (
    select
        *,
        split_part(match_result, ' d.', 1) as match_winner
    from {{ ref('bus_dv__pit__match') }}
),

pit_player as (
    select * from {{ ref('bus_dv__pit__player') }}
),

joined as (
    select
        link_match_player.lk_match_player,
        link_match_player.hk_match,
        link_match_player.hk_player,
        pit_match.match_winner = pit_player.player_name as is_winner,

        link_match_player.load_datetime as link_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__bridge__match__player' as bus_dv_source_model
        
    from link_match_player
    left join pit_match on link_match_player.hk_match = pit_match.hk_match
    left join pit_player on link_match_player.hk_player = pit_player.hk_player
)

select * from joined

