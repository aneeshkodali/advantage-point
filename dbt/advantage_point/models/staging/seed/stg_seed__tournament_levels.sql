with

source as (
    select * from {{ ref('tournament_levels') }}
),

renamed as (
    select
        tournament_level,
        tournament_level_name
    from source
)

select * from renamed