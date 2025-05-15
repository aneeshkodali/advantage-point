with

int_point as (
    select * from {{ ref('bus_dv__int__point') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

hub_set as (
    select * from {{ ref('bus_dv__hub__set') }}
),

-- select columns
point_set as (
    select
        hk_point,
        bk_point,
        point_number_in_set,
        bk_set
    from int_point
),

-- join to hk
point_set_hk as (
    select
        hub_point.hk_point,
        int_point_set.bk_point,
        hub_set.hk_set,
        int_point_set.bk_set,
        int_point_set.point_number_in_set
    from point_set as int_point_set
    left join hub_point on int_point_set.hk_point = hub_point.hk_point
    left join hub_set on int_point_set.bk_set = hub_set.bk_set
),

final as (
    select
        hk_point,
        hk_set,
        bk_point,
        bk_set,

        point_number_in_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__point__set' as bus_dv_source_model
    from point_set_hk
)

select * from final