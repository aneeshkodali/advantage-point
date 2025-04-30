with

int_set as (
    select * from {{ ref('bus_dv__int__set') }}
),

tournament_format_history as (
    select * from {{ ref('bus_dv__int__tournament_format_history') }}
),

link_match_tournament as (
    select * from {{ ref('raw_dv__link__match__tournament') }}
),

-- calculate is_deciding_set
sets_is_deciding_set as (
    select
        s.*,
        s.set_number_in_match = tfh.best_of_sets as is_deciding_set
    from int_set as s
    -- join match_tournament to get tournament
    left join link_match_tournament as m_t on s.hk_match = m_t.hk_match
    -- join tournament_format_history to get best_of_sets
    left join tournament_format_history as tfh on m_t.hk_tournament = tfh.hk_tournament
),

final as (
    select
        hk_match,
        bk_set,
        is_deciding_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__set__match' as bus_dv_source_model
    from sets_is_deciding_set
)

select * from final