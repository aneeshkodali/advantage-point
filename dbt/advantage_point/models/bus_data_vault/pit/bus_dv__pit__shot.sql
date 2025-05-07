with

shot_details as (
    select
        hk_shot,
        load_datetime,
        hash_diff,
        record_source,

        shot_number,
        shot_direction,
        shot_result,
        shot_type
    from (
        {{ get_latest_record(
            model_ref=ref('bus_dv__sat__shot_details'),
            partition_by_col='hk_shot',
            order_by_col='load_datetime'
        ) }}
    ) as s
),

-- get unique list of hk
hk_union as (
    (select hk_shot from shot_details)
),

final as (
    select
        hk_union.hk_shot,

        shot_details.shot_number,
        shot_details.shot_direction,
        shot_details.shot_result,
        shot_details.shot_type,
        shot_details.hash_diff as shot_details_hash_diff,
        shot_details.load_datetime as shot_details_load_datetime,
        
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__shot' as bus_dv_source_model
    from hk_union
    left join shot_details using (hk_shot)
)

select * from final