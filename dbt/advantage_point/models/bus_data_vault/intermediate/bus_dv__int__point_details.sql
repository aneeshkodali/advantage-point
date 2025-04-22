with

sat_point_details as (
    select * from {{ ref('raw_dv__sat__point_details') }}
),

-- get side (of court)
point_side as (
    select
        *,
        case
            -- determine side based on non-tiebreaker scores
            when point_score_in_game in (
                '0-0', '0-30',
                '15-15', '15-40',
                '30-0', '30-30',
                '40-40'
            ) then 'deuce'
            when point_score_in_game in (
                '0-15', '0-40',
                '15-0', '15-30',
                '30-15', '30-40',
                'AD-40', '40-AD'
            ) then 'ad'
            -- determine side based on tiebreak scores
            when (
                split_part(point_score_in_game, '-', 1)::int
                + split_part(point_score_in_game, '-', 2)::int
            ) % 2 = 0 then 'deuce'
            when (
                split_part(point_score_in_game, '-', 1)::int
                + split_part(point_score_in_game, '-', 2)::int
            ) % 2 != 0 then 'ad'
        else null
        end as point_side
    from sat_point_details
),

-- determine 'point type' boolean fields
point_type as (
    select
        *,

        -- determine break point
        case
            when point_score_in_game in (
                '0-40', '15-40', '30-40', '40-AD'
            ) then true
            else false
        end as is_break_point,

        -- determine game point
        case
            when point_score_in_game in (
                '40-0', '40-15', '40-30', 'AD-40'
            ) then true
            else false
        end as is_game_point

    from point_side
),

final as (
    select
        hk_point,
        point_score_in_game,
        point_side,
        is_break_point,
        is_game_point,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__point_details' as bus_dv_source_model
    from point_type
)

select * from final