with

tournament_details as (
    select *
    from {{ get_latest_record(
        model_ref=ref('raw_dv__sat__tournament_details'),
        partition_by_col='hk_tournament',
        order_by_col='load_datetime'
    ) }}
),

-- get unique list of hk
hk_union as (
    (select hk_tournament from tournament_details)
),


final as (
    select
        hk_union.hk_tournament,

        tournament_details.tournament_start_date,
        tournament_details.tournament_surface,
        tournament_details.tournament_draw_size,
        tournament_details.hash_diff as tournament_details_hash_diff,
        tournament_details.load_datetime as tournament_details._load_datetime,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__tournament' as bus_dv_source_model
    from hk_union
    left join tournament_details using (hk_tournament)
)

select * from final