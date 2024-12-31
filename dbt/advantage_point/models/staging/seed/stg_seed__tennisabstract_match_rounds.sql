with

source as (
    select * from {{ ref('match_rounds') }}
),

renamed as (
    select
        match_round,
        match_round_name,
        cast(match_round_sort as int) as match_round_sort
    from source
)

select * from renamed