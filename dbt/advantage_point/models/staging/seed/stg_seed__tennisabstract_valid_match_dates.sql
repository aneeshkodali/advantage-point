with

source as (
    select * from {{ ref('valid_tennisabstract_match_dates') }}
),

renamed as (
    select
        match_url,
        to_date(cast(valid_match_date as text), 'YYYYMMDD') as match_date
    from source
)

select * from renamed