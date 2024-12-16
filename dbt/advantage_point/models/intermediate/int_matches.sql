with

tennisabstract_matches as (
    select * from {{ ref('stg_tennisabstract__matches') }}
),

matches as (
    select
        match_url,
        match_gender,
        match_tournament,
        match_date,
        match_round,
        match_title
    from tennis_abstract_matches
)

select * from matches