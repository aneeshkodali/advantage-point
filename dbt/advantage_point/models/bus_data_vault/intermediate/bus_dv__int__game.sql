with

int_point_scores_parsed as (
    select * from {{ ref('bus_dv__int__point_scores_parsed') }}
),

final as (
    select distinct
        hk_match,
        bk_match,
        game_number_in_set,
        game_number_in_match,
        bk_game,

        bus_dv_load_datetime,
        'bus_dv__int__game' as bus_dv_source_model
    from int_point_scores_parsed
)

select * from final