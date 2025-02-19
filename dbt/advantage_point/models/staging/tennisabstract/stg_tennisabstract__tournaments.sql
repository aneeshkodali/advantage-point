with

source as (
    select
        *
    from {{ source('tennisabstract', 'tournaments') }}
),

renamed as (
    select
        tournament_url,
        cast(tournament_year as integer) as tournament_year,
        tournament_name,
        tournament_gender,
        tournament_title,
        to_date(tournament_start_date, 'FMMonth DD, YYYY') as tournament_start_date,
        tournament_surface,
        cast(tournament_draw_size as int) as tournament_draw_size
    from source
)

select * from renamed