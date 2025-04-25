with

source as (
    select * from {{ ref('tournamnet_formats') }}
),

renamed as (
    select
        tournament_name,
        tournament_gender,
        format_id,
        cast(coalesce(effective_start_year, '1800') as int) as effective_start_year,
        cast(coalesce(effective_end_year, '3000') as int) as effective_end_year,
        note

    from source
)

select * from renamed