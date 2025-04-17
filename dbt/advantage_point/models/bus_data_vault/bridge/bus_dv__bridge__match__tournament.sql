with

link_match_tournament as (
    select
        lk_match_tournament,
        hk_match,
        hk_tournament,

        load_datetime
    from (
        {{ get_latest_record(
            model_ref=ref('raw_dv__link__match__tournament'),
            partition_by_col='lk_match_tournament',
            order_by_col='load_datetime'
        ) }}
    ) as m_t
),

 pit_match as (
    select
        *,
        split_part(match_result, ' d.', 1) as match_winner
    from {{ ref('bus_dv__pit__match') }}
),

pit_tournament as (
    select * from {{ ref('bus_dv__pit__tournament') }}
),

joined as (
    select
        link_match_tournament.lk_match_tournament,
        link_match_tournament.hk_match,
        link_match_tournament.hk_tournament,

        link_match_tournament.load_datetime as link_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__bridge__match__tournament' as bus_dv_source_model
        
    from link_match_tournament
    left join pit_match on link_match_tournament.hk_match = pit_match.hk_match
    left join pit_tournament on link_match_tournament.hk_tournament = pit_tournament.hk_tournament
)

select * from joined

