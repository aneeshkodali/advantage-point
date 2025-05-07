with

int_shot as (
    select * from {{ ref('bus_dv__int__shot') }}
),

hub_shot as (
    select * from {{ ref('bus_dv__hub__shot') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

-- join to hk
int_shot_point_hk as (
    select
        hub_point.hk_point,
        int_shot_point.bk_point,
        hub_shot.hk_shot,
        int_shot_point.bk_shot,
        int_shot_point.shot_number_in_point,
        int_shot_point.shot_number
    from int_shot as int_shot_point
    left join hub_point on int_shot_point.hk_point = hub_point.hk_point
    left join hub_shot on int_shot_point.bk_shot = hub_shot.bk_shot
),

final as (
    select
        hk_point,
        hk_shot,
        bk_point,
        bk_shot,

        shot_number_in_point,
        shot_number,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__shot__point' as bus_dv_source_model
    from int_shot_point_hk
)

select * from final