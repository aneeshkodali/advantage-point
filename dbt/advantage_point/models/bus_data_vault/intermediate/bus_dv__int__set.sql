with

int_point_scores_parsed as (
    select * from {{ ref('bus_dv__int__point_scores_parsed') }}
),

final as (
    select distinct
        hk_match,
        bk_match,
        set_score_in_match,
        set_score_in_match_server,
        set_score_in_match_receiver,
        set_number_in_match,
        bk_set,

        bus_dv_load_datetime,
        bus_dv_source_model
    from int_point_scores_parsed
)

select * from final