with

source as (
    select * from {{ ref('tennisabstract_tournament_levels') }}
),

renamed as (
    select
        tournament_level,
        tournament_level_name
    from source
)

select * from renamed