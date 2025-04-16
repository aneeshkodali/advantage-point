with

match_details as (
    select
        hk_match,
        load_datetime,
        hash_diff,
        record_source,

        match_title,
        match_result
    from (
        {{ get_latest_record(
            model_ref=ref('raw_dv__sat__match_details'),
            partition_by_col='hk_match',
            order_by_col='load_datetime'
        ) }}
    ) as m
),

-- get unique list of hk
hk_union as (
    (select hk_match from match_details)
),

final as (
    select
        hk_union.hk_match,

        match_details.match_title,
        match_details.match_result,
        match_details.hash_diff as match_details_hash_diff,
        match_details.load_datetime as match_details_load_datetime,
        
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__match' as bus_dv_source_model
    from hk_union
    left join match_details using (hk_match)
)

select * from final