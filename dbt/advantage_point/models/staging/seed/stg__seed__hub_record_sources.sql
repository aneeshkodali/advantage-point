with

source as (
    select * from {{ ref('hub_record_sources') }}
),

renamed as (
    select
        hub_name,
        record_source,
        cast(sort_order as int) as sort_order
    from source
)

select * from renamed