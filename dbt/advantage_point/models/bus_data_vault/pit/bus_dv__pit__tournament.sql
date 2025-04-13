with

tournament_details as (
    select *
    from {{ get_latest_record(
        model_ref="ref('raw_dv__sat__tournament_details')",
        partition_by_col='hk_tournament',
        order_by_col='load_datetime'
    ) }}
),

final as (
    select
        hk_tournament,
        tournament_start_date,
        tournament_surface,
        tournament_draw_size,
        hash_diff,
        load_datetime as sat_load_datetime,
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__tournament' as bus_dv_source_model
    from match_details
)

select * from final