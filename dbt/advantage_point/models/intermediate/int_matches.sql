with

tennisabstract_matches as (
    select
        *,
        extract(year from match_date):: int as match_year,
        split_part(match_result, ' d. ', 1) as match_winner
    from {{ ref('stg_tennisabstract__matches') }}
    where is_record_active = true
),

matches as (
    select
        match_url,
        match_gender,
        match_tournament,
        match_date,
        match_year,
        match_round,
        match_title,
        -- creating match_title for use in coalesce (may be better to use this directly?)
        concat(
            match_year || ' ',
            match_tournament || ' ',
            match_round || ': ',
            match_player_one,
            ' vs ',
            match_player_two
        ) as match_title_concat,
        match_winner,
        -- result: {winner} d. {loser} {set score}
        case
            when match_winner = match_player_one then match_player_two
            when match_winner = match_player_two then match_player_one
            else null
        end as match_loser
    from tennisabstract_matches
),

final as (
    select
        match_url,
        match_gender,
        match_tournament,
        match_date,
        match_year,
        match_round,
        case
            when match_title is null then match_title_concat
            when match_title = '404 Not Found' then match_title_concat
            else match_title
        end as match_title,
        match_winner,
        match_loser,
        split_part(match_result, match_loser || ' ', 2) as match_score
    from matches
)

select * from final