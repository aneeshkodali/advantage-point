with

tennisabstract_matches as (
    select
        *,
        extract(year from match_date):: int as match_year
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
        ) as match_title_concat
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
        coalesce(match_title, match_title_concat) as match_title
    from matches
)

select * from final