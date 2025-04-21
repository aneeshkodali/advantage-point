with

pit_point as (
    select
        *
    from {{ ref('bus_dv__pit__point') }}
),

bridge_match_point as (
    select
        *
    from {{ ref('bus_dv__bridge__match__point') }}
),

hub_match as (
    select
        *
    from {{ ref('raw_dv__hub__match') }}
),

hub_point as (
    select
        *
    from {{ ref('raw_dv__hub__point') }}
),

-- parse data from point columns
columns_parse as (
    select
        *,
        cast(split_part(set_score_in_match, '-', 1) as int) as set_score_in_match_server,
        cast(split_part(set_score_in_match, '-', 2) as int) as set_score_in_match_receiver,
        cast(split_part(game_score_in_set, '-', 1) as int) as game_score_in_set_server,
        cast(split_part(game_score_in_set, '-', 2) as int) as game_score_in_set_receiver,
        split_part(point_score_in_game, '-', 1) as point_score_in_game_server,
        split_part(point_score_in_game, '-', 2) as point_score_in_game_receiver,
        array_length(
            string_to_array(
                point_description,
                '; '
            ),
            1
        ) as point_length,
        -- shot_1; shot_2; ...; shot_n, <point_result><optional . then string>
        -- get text before period
        split_part(
            -- get last text after comma
            split_part(
                -- get last point in description
                split_part(
                    point_description,
                    '; ',
                    -1
                ),
                ',',
                -1
            ),
            '.',
            1
        ) as point_result
    from pit_point
),

-- add scores
scores_add as (
    select
        *,
        set_score_in_match_server + set_score_in_match_receiver + 1 as set_number_in_match,
        game_score_in_set_server + game_score_in_set_receiver + 1 as game_number_in_set,
        -- convert 'AD' to numeric
        cast(replace(point_score_in_game_server, 'AD', '41') as int) as point_score_in_game_server_int,
        cast(replace(point_score_in_game_receiver, 'AD', '41') as int) as point_score_in_game_receiver_int
    from columns_parse
),

-- get running counts
-- join in match and point to get partition/order calculations
running_numbers as (
    select
        ext_sat_points.*,
        bridge_match_point.hk_match,
        hub_match.bk_match,
        hub_point.point_number_in_match,
        dense_rank() over (
            partition by bridge_match_point.hk_match
            order by ext_sat_points.set_number_in_match, ext_sat_points.game_number_in_set
        ) as game_number_in_match,
        row_number() over (
            partition by bridge_match_point.hk_match, ext_sat_points.set_number_in_match
            order by ext_sat_points.game_number_in_set, hub_point.point_number_in_match
        ) as point_number_in_set,
        row_number() over (
            partition by bridge_match_point.hk_match, ext_sat_points.set_number_in_match, ext_sat_points.game_number_in_set
            order by hub_point.point_number_in_match
        ) as point_number_in_game
    from scores_add as ext_sat_points
    left join bridge_match_point on ext_sat_points.hk_point = bridge_match_point.hk_point
    left join hub_point on ext_sat_points.hk_point = hub_point.hk_point
    left join hub_match on bridge_match_point.hk_match = hub_match.hk_match
),

-- generate bk
entity_bks as (
    select
        *,
        {{ generate_set_business_key(
            bk_match_col='bk_match',
            set_col='set_number_in_match'
        ) }} as bk_set,
        {{ generate_game_business_key(
            bk_match_col='bk_match',
            game_col='game_number_in_match'
        ) }} as bk_game
    from running_numbers
),

-- calculate rally length
rally_length as (
    select
        *,
        case
        -- exclude 'errors'
        when point_result in ('double fault', 'forced error', 'unforced error') then point_length - 1
        -- include 'winners'
        when point_result in ('ace', 'service winner', 'winner') then point_length
        else null
        end as rally_length
    from entity_bks
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
            when point_score_in_game_server_int + point_score_in_game_receiver_int % 2 = 0 then 'deuce'
            when point_score_in_game_server_int + point_score_in_game_receiver_int % 2 != 0 then 'ad'
            else null
        end as point_side
    from rally_length
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
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_description,
        point_details_hash_diff,
        point_details_load_datetime,
        set_score_in_match_server,
        set_score_in_match_receiver,
        game_score_in_set_server,
        game_score_in_set_receiver,
        point_score_in_game_server,
        point_score_in_game_server_int,
        point_score_in_game_receiver,
        point_score_in_game_receiver_int,
        point_length,
        case
            when point_result in ('ace', 'double fault', 'forced error', 'service winner', 'unforced error', 'winner')
            then point_result
            else null
        end as point_result,
        set_number_in_match,
        game_number_in_set,
        hk_match,
        bk_match,
        bk_set,
        bk_game,
        point_number_in_match,
        game_number_in_match,
        point_number_in_set,
        point_number_in_game,
        rally_length,
        point_side,
        is_break_point,
        is_game_point,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__sat__point' as bus_dv_source_model
    from point_type
)

select * from final