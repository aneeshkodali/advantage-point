with

tennisabstract_matches as (
    select
        *,
        split_part(match_result, ' d. ', 1) as match_winner
    from {{ ref('stg_tennisabstract__matches') }}
    where is_record_active = true
),

valid_match_dates as (
    select * from {{ ref('stg_seed__valid_tennisabstract_match_dates')}}
),

-- join to seed data
matches_joined as (
    select
        matches.match_url,
        matches.match_gender,
        matches.match_tournament,
        coalesce(valid_match_dates.match_date, to_date(matches.match_date, 'YYYYMMDD')) as match_date,
        matches.match_round,
        matches.match_title,
        matches.match_result,
        matches.match_winner,
        -- result: {winner} d. {loser} {set score}
        case
            when matches.match_winner = matches.match_player_one then matches.match_player_two
            when matches.match_winner = matches.match_player_two then matches.match_player_one
            else null
        end as match_loser
    from tennisabstract_matches as matches
    left join valid_match_dates on matches.match_url = valid_match_dates.match_url
),

-- get year from match
matches_match_year as (
    select
        *,
        extract(year from match_date):: int as match_year
    from matches_joined
),

-- creating match_title for use in coalesce (may be better to use this directly?)
matches_match_title as (
    select
        *,
        concat(
            matches.match_year || ' ',
            matches.match_tournament || ' ',
            matches.match_round || ': ',
            matches.match_player_one,
            ' vs ',
            matches.match_player_two
        ) as match_title_concat
    from matches_match_year
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
        match_result,
        match_winner,
        match_loser,
        split_part(match_result, match_loser || ' ', 2) as match_score
    from matches_match_title
)

select * from final