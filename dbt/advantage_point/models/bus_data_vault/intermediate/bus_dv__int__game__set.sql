with

int_point as (
    select * from {{ ref('bus_dv__int__point') }}
),

game_set as (
    select distinct
        game_number_in_set,
        bk_set,
        bk_game
    from int_point
),

final as (
    select
        bk_set,
        bk_game,
        game_number_in_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__game__set' as bus_dv_source_model
    from game_set
)

select * from final