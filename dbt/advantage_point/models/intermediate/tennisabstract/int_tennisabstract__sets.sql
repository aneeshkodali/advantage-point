with

matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
    where match_score is not null
),

-- split sets into own line
match_sets_unnest as (
    select
        match_url,
        UNNEST(STRING_TO_ARRAY(match_score, ' ')) AS set_score
    from matches
),

-- get set number
match_sets_row_number as (
    select
        match_url,
        set_score,
        row_number() over (partition by match_url order by set_score) as set_number_in_match
    from match_sets_unnest
),

final as (
    select
        match_url,
        set_number_in_match
    from match_sets_row_number
)

select * from final