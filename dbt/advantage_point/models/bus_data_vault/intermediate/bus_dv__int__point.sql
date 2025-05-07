{{ config(
  materialized='table'
)}}

with

sat_point_details as (
    select * from {{ ref('raw_dv__sat__point_details') }}
),

link_match_point as (
    select * from {{ ref('raw_dv__link__match__point') }}
),

hub_match as (
    select * from {{ ref('raw_dv__hub__match') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

int_shot as (
    select * from {{ ref('bus_dv__int__shot') }}
    where shot_result != 'fault'
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
        split_part(point_score_in_game, '-', 2) as point_score_in_game_receiver
    from sat_point_details
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
        hub_match.bk_match,
        hub_point.bk_point,
        hub_point.point_number_in_match,
        dense_rank() over (
            partition by link_match_point.hk_match
            order by ext_sat_points.set_number_in_match, ext_sat_points.game_number_in_set
        ) as game_number_in_match,
        row_number() over (
            partition by link_match_point.hk_match, ext_sat_points.set_number_in_match
            order by ext_sat_points.game_number_in_set, hub_point.point_number_in_match
        ) as point_number_in_set,
        row_number() over (
            partition by link_match_point.hk_match, ext_sat_points.set_number_in_match, ext_sat_points.game_number_in_set
            order by hub_point.point_number_in_match
        ) as point_number_in_game
    from scores_add as ext_sat_points
    left join link_match_point on ext_sat_points.hk_point = link_match_point.hk_point
    left join hub_point on ext_sat_points.hk_point = hub_point.hk_point
    left join hub_match on link_match_point.hk_match = hub_match.hk_match
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
    from entity_bks
),

-- rank rows to determine last shot in rally (=1)
int_shot_last_shot as (
    select
        *,
        row_number() over (
            partition by hk_point
            order by shot_number desc, shot_number_in_point desc
        ) as last_shot_row_number
    from int_shot
),

-- filter for last rows
int_shot_filtered as (
    select
        *
    from int_shot_last_shot
    where last_shot_row_number = 1
),

-- get point rally info
point_rally as (
    select
        points.*,
        shots.shot_number as point_length,
        shots.shot_result as point_result,
        case
            when shot_result in ('ace', 'service winner', 'winner') then shot_number
            when shot_result in ('double fault', 'forced error', 'unforced error') then shot_number - 1
            else null
        end as rally_length,
    from point_side as points
    left join int_shots_filtered as shots on points.hk_point = shots.hk_point
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

    from point_rally
),

final as (
    select
        hk_point,
        bk_point,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        set_score_in_match_server,
        set_score_in_match_receiver,
        game_score_in_set_server,
        game_score_in_set_receiver,
        point_score_in_game_server,
        point_score_in_game_receiver,
        set_number_in_match,
        game_number_in_set,
        point_score_in_game_server_int,
        point_score_in_game_receiver_int,
        hk_match,
        bk_match,
        point_number_in_match,
        game_number_in_match,
        point_number_in_set,
        point_number_in_game,
        bk_set,
        bk_game,
        point_side,
        point_length,
        point_result,
        rally_length,
        is_break_point,
        is_game_point,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__point' as bus_dv_source_model
    from point_type
)

select * from final