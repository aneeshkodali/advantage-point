with

int_point as (
    select * from {{ ref('bus_dv__int__point') }}
),

tournament_format_history as (
    select * from {{ ref('bus_dv__int__tournament_format_history') }}
),

link_match_tournament as (
    select * from {{ ref('raw_dv__link__match__tournament') }}
),

-- get last point in set
set_last_point as (
    select
        bk_set,
        max(point_number_in_match) as point_number_in_match
    from int_point
    group by 1
),

-- calculate additional set details
set_details as (
    select
        s.*,
        
        -- is set deciding set
        s.set_number_in_match = tfh.best_of_sets as is_deciding_set,

        -- is set a tiebreak set
        case
            -- is set deciding/final set
            when s.set_number_in_match = tfh.best_of_sets then
                case
                    -- is tiebreak played in final set
                    when tf.final_set_tiebreak = true then
                        case
                            -- is game score equal to final set tiebreak trigger
                            when s.game_score_in_set = tfh.final_set_tiebreak_trigger_game then true
                            when s.game_score_in_set != tfh.final_set_tiebreak_trigger_game then false
                            else null
                        end
                    else null
                end
            when s.set_number_in_match != tfh.best_of_sets then
                case
                    -- is game score equal to tiebreak trigger
                    when s.game_score_in_set = tfh.tiebreak_trigger_game then true
                    when s.game_score_in_set != tfh.tiebreak_trigger_game then false
                    else null
                end
            else null
        end as is_tiebreak_set
                    

    from int_point as s
    -- join match_tournament to get tournament
    left join link_match_tournament as m_t on s.hk_match = m_t.hk_match
    -- join tournament_format_history to get best_of_sets
    left join tournament_format_history as tfh on m_t.hk_tournament = tfh.hk_tournament
),

-- filter for last point in set
set_point_filtered as (
    select
        s.*
    from set_details as s
    inner join set_max_point as s_m_p on 1=1
        and s.bk_set = s_m_p.bk_set
        and s.point_number_in_match = s_m_p.point_number_in_match
),

final as (
    select
        hk_match,
        bk_match,
        set_number_in_match,
        bk_set,
        is_deciding_set,
        is_tiebreak_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__set' as bus_dv_source_model
    from set_point_filtered
)

select * from final