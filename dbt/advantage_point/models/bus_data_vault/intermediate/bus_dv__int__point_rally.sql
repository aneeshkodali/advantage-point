with

-- rank rows to determine last shot in rally (=1)
int_shot as (
    select
        *,
        row_number() over (
            partition by hk_point
            order by shot_number desc, shot_number_in_point desc
        ) as last_shot_row_number
    from {{ ref('bus_dv__int__shot') }}
),

-- filter for last rows
int_shot_filtered as (
    select
        *
    from int_shot
    where last_shot_row_number = 1
),

final as (
    select
        hk_point,
        shot_number as point_length,
        shot_result as point_result,
        case
            when shot_result in ('ace', 'service winner', 'winner') then shot_number
            when shot_result in ('double fault', 'forced error', 'unforced error') then shot_number - 1
            else null
        end as rally_length,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__point_rally' as bus_dv_source_model
    from int_shot_filtered
)

select * from final

