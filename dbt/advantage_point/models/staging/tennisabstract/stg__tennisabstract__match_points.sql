with

source as (
    select * from {{ source('tennisabstract', 'match_points') }}
),

/*
note:
'‑' is a non-breaking hyphen
' ' is a non-breaking space
*/
renamed as (
    select
        match_url,
        cast(point_number as int) as point_number_in_match,
        replace("server", ' ', ' ') as point_server,
        replace("sets", '‑', '-') as set_score_in_match,
        replace(games, '‑', '-') as game_score_in_set,
        replace(points, '‑', '-') as point_score_in_game,
        point_description
    from source
)

select * from renamed