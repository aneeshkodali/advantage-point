with

point_scores_parsed as (
    select * from {{ ref('bus_dv__int__point_scores_parsed') }}
),

game_set as (
    select distinct
        game_number_in_set,
        bk_set,
        bk_game
    from point_scores_parsed
),

final as (
    select
        bk_set,
        bk_game,
        game_number_in_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__game__set' as bus_dv_source_model
)

select * from final