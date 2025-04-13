with

match_details as (
    select *
    from {{ get_latest_record(
        model_ref="ref('raw_dv__sat__match_details')",
        partition_by_col='hk_match',
        order_by_col='load_datetime'
    ) }}
),

final as (
    select
        hk_match,
        hk_tournament,
        match_title,
        match_result,
        hash_diff,
        load_datetime as sat_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__match' as bus_dv_source_model
    from match_details
)

select * from final