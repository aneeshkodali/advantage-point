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

-- create player array (for use in bk creation)
player_array as (
    select
        *,
        {{ sort_player_array(
            player_array='[match_player_one, match_player_two]'
        ) }} as match_players

    from renamed 
),

-- create business key
bk as (
    select
        *,
        {{ generate_match_business_key(
            match_date_col='match_date',
            match_gender_col='match_gender',
            match_tournament_col='match_tournament',
            match_round_col='match_round',
            match_players_col='match_players'
        ) }} as bk_match,
        'tennisabstract__matches' as record_source

    from player_array
)

select * from bk