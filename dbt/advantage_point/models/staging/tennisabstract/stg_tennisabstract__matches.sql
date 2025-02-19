with

source as (
    select
        *
    from {{ source('tennisabstract', 'matches') }}
),

renamed as (
    select
        match_url,
        cast(match_date as date) as match_date,
        match_gender,
        replace(match_tournament, '_', ' ') as match_tournament,
        match_round,
        replace(match_player_one, '_', ' ') as match_player_one,
        replace(match_player_two, '_', ' ') as match_player_two,
        match_title,
        match_result
    from source
),

-- sort players
matches_players_sort as (
    select
        match_url,
        match_date,
        match_gender,
        match_tournament,
        match_round,
        least(match_player_one, match_player_two) as match_player_one,
        greatest(match_player_one, match_player_two) as match_player_two,
        match_title,
        match_result
)

select * from renamed