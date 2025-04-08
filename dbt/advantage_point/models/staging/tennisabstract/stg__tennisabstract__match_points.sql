with

source as (
    select * from {{ source('tennisabstract', 'match_points') }}
),

matches as (
    select * from {{ ref('stg__tennisabstract__matches') }}
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
),

-- create business key
bk as (
    select
        mp.*,
        {{ generate_match_point_business_key(
            match_date_col='m.match_date',
            match_gender_col='m.match_gender',
            match_tournament_col='m.match_tournament',
            match_round_col='m.match_round',
            match_players_col='m.match_players',
            match_point_col='mp.point_number_in_match'
        ) }} as bk_match_point,
        m.bk_match,
        'tennisabstract__match_points' as record_source
    from renamed as mp
    left join matches as m on mp.match_url = m.match_url
)

select * from bk