with

source as (
    select
        *
    from {{ source('tennisabstract', 'matches') }}
),

renamed as (
    select
        match_url,
        cast(
            (
                case
                    when match_url = 'https://www.tennisabstract.com/charting/20170890-W-Toronto-R32-Ashleigh_Barty-Elena_Vesnina.html' then '20170809'
                    else match_date
                end
            ) as date
        ) as match_date,
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
        (
            select array_to_string(array_agg(player order by player), ', ')
            from unnest(array[match_player_one, match_player_two]) as player
        ) as match_players

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
        {{ generate_tournament_business_key(
            tournament_year_col='extract(year from match_date)',
            tournament_gender_col='match_gender',
            tournament_name_col='match_tournament'
        ) }} as bk_tournament,
        'tennisabstract__matches' as record_source

    from player_array
)

select * from bk