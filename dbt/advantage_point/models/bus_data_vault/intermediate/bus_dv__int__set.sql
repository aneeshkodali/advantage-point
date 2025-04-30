with

int_point_scores_parsed as (
    select * from {{ ref('bus_dv__int__point_scores_parsed') }}
),

tournament_format_history as (
    select * from {{ ref('bus_dv__int__tournament_format_history') }}
),

link_match_tournament as (
    select * from {{ ref('raw_dv__link__match__tournament') }}
),

-- get distinct set rows
sets_distinct as (
    select distinct
        hk_match,
        bk_match,
        set_number_in_match,
        bk_set
    from int_point_scores_parsed
),

final as (
    select
        hk_match,
        bk_match,
        set_number_in_match,
        bk_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__set' as bus_dv_source_model
    from sets_distinct
)

select * from final