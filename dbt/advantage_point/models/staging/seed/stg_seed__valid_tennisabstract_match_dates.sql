with

source as (
    select * from {{ ref('valid_tennisabstract_match_dates') }}
),

renamed as (
    select
        match_url,
        match_date
    from source
)

select * from renamed