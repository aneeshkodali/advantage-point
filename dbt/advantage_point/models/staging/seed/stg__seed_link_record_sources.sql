with

source as (
    select * from {{ ref('link_record_sources') }}
),

renamed as (
    select
        link_name,
        record_source,
        cast(sort_order as int) as sort_order
    from source
)

select * from renamed