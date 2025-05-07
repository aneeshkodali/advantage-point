with

int_point as (
    select * from {{ ref('bus_dv__int__point') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

hub_game as (
    select * from {{ ref('bus_dv__hub__game') }}
),

-- select columns
point_game as (
    select
        hk_point,
        bk_point,
        point_number_in_game,
        bk_game,
        is_break_point,
        is_game_point
    from int_point
),

-- join to hk
point_game_hk as (
    select
        hub_point.hk_point,
        int_point_game.bk_point,
        hub_game.hk_game,
        int_point_game.bk_game,
        int_point_game.point_number_in_game,
        int_point_game.is_break_point,
        int_point_game.is_game_point
    from point_game as int_point_game
    left join hub_point on int_point_game.hk_point = hub_point.hk_point
    left join hub_game on int_point_game.bk_game = hub_game.bk_game
),

final as (
    select
        hk_point,
        hk_game,
        bk_point,
        bk_game,

        point_number_in_game,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__point__game' as bus_dv_source_model
    from point_game_hk
)

select * from final