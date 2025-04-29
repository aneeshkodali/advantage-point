with

source as (
    select * from {{ ref('tournament_format_history') }}
),

renamed as (
    select
        tournament_name,
        tournament_gender,
        format_id,
        cast(effective_start_year as int) as effective_start_year,
        cast(effective_end_year as int) as effective_end_year,
        note

    from source
)

select * from renamed