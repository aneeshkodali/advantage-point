with

matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
    where match_result is not null
),

-- split sets into own line
match_sets_unnest as (
    select
        match_url,
        match_result,
        match_winner,
        match_loser,
        UNNEST(STRING_TO_ARRAY(match_score, ' ')) AS set_score
    from matches
),

-- split set score
match_set_scores as (
    select
        match_url,
        match_result,
        match_winner,
        match_loser,
        set_score,
        row_number() over (partition by match_url order by set_score) as set_number_in_match,
        split_part(set_score, '-', 1) as set_score_match_winner,
        split_part(set_score, '-', 2) as set_score_match_loser
    from match_sets_unnest
),
-- convert set scores to integers
match_set_scores_int as (
    select
        match_url,
        match_result,
        match_winner,
        match_loser,
        set_score,
        set_number_in_match,
        set_score_match_winner,
        set_score_match_loser,
        cast(split_part(set_score_match_winner, '(', 1) as int) as set_score_match_winner_int,
        cast(split_part(set_score_match_loser, '(', 1) as int) as set_score_match_loser_int
    from match_set_scores
),
-- compare set scores
match_set_scores_compare as (
    select
        match_url,
        match_result,
        match_winner,
        match_loser,
        set_score,
        set_number_in_match,
        set_score_match_winner,
        set_score_match_loser,
        set_score_match_winner_int,
        set_score_match_loser_int,
        case
            when set_score_match_winner_int > set_score_match_loser_int then match_winner
            when set_score_match_winner_int < set_score_match_loser_int then match_loser
            else null
        end as set_winner,
        case
            when set_score_match_winner_int > set_score_match_loser_int then set_score_match_winner
            when set_score_match_winner_int < set_score_match_loser_int then set_score_match_loser
            else null
        end as set_winner_score
    from match_set_scores_int
),

-- get outcome columns
match_set_scores_set_outcomes as (
    select
        match_url,
        match_result,
        match_winner,
        match_loser,
        set_score,
        set_number_in_match,
        set_score_match_winner,
        set_score_match_loser,
        set_score_match_winner_int,
        set_score_match_loser_int,
        set_winner,
        set_winner_score,
        case
            when set_winner = match_winner then match_loser
            when set_winner = match_loser then match_winner
            else null
        end as set_loser,
        case
            when set_winner_score = set_score_match_winner then set_score_match_loser
            when set_winner_score = set_score_match_loser then set_score_match_winner
            else null
        end as set_loser_score
    from match_set_scores_compare
),

-- union set winner and loser data
match_set_score_union as (
    (
        select
            match_url,
            set_number_in_match,
            set_winner as player_name,
            set_winner_score as set_score,
            1 as is_set_winner
        from match_set_scores_set_outcomes
    )
    union all
    (
        select
            match_url,
            set_number_in_match,
            set_loser as player_name,
            set_loser_score as set_score,
            0 as is_set_winner
        from match_set_scores_set_outcomes
    )
),

final as (
    select
        match_url,
        set_number_in_match,
        player_name,
        set_score,
        is_set_winner
    from match_set_score_union
)

select * from final