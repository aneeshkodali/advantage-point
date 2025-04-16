with

point_details as (
    select
        hk_point,
        load_datetime,
        hash_diff,
        record_source,

        point_server,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_description
    from (
        {{ get_latest_record(
            model_ref=ref('raw_dv__sat__point_details'),
            partition_by_col='hk_point',
            order_by_col='load_datetime'
        ) }}
    ) as p
),

-- get unique list of hk
hk_union as (
    (select hk_point from point_details)
),

final as (
    select
        hk_union.hk_point,

        point_details.point_server,
        point_details.set_score_in_match,
        point_details.game_score_in_set,
        point_details.point_score_in_game,
        point_details.point_description,
        point_details.hash_diff as point_details_hash_diff,
        point_details.load_datetime as point_details_load_datetime,
        
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__point' as bus_dv_source_model
    from hk_union
    left join point_details using (hk_point)
)

select * from final