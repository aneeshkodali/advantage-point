with

int_set as (
    select * from {{ ref('bus_dv__int__set') }}
),

final as (
    select
        hk_match,
        bk_set,
        is_deciding_set,
        is_tiebreak_set,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__set__match' as bus_dv_source_model
    from sets_is_deciding_set
)

select * from final