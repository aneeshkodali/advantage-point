with

int_point as (
    select * from {{ ref('bus_dv__int__point') }}
),

final as (
    select distinct
        hk_match,
        bk_match,
        game_number_in_match,
        bk_game,

        bus_dv_load_datetime,
        'bus_dv__int__game' as bus_dv_source_model
    from int_point
)

select * from final