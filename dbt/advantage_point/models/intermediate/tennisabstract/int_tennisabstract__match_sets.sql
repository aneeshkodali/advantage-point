with

matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
    where match_score is not null
),

-- split sets into own line
match_sets_unnest as (
    select
        *,
        unnest(string_to_array(match_score, ' ')) AS set_score,
        generate_subscripts(string_to_array(match_score, ' '), 1) AS set_number_in_match
    FROM matches
),

-- parse out set set_score
-- format: {winner score}-{loser score}{optional tiebreak score}
match_sets_set_score_prep as (
    select
        *,
        cast(split_part(set_score, '-', 1) as int) as set_score_match_winner,
        regexp_matches(split_part(set_score, '-', 2), '(\d+)(?:\((\d+)\))?') as set_score_match_loser_regex
    from match_sets_unnest
),

-- parse out set scores from loser regex
match_sets_set_score as (
    select
        *,
        cast(set_score_match_loser_regex[1] as int) as set_score_match_loser,
        cast(set_score_match_loser_regex[2] as int) as set_tiebreaker_score
    from match_sets_set_score_prep
),

-- compare set scores
match_set_scores_compare as (
    select
        *,
        case
            when set_score_match_winner > set_score_match_loser then match_winner
            when set_score_match_winner < set_score_match_loser then match_loser
            else null
        end as set_winner,
        case
            when set_score_match_winner > set_score_match_loser then set_score_match_winner
            when set_score_match_winner < set_score_match_loser then set_score_match_loser
            else null
        end as set_winner_score,
        case
            when set_score_match_winner > set_score_match_loser then concat(set_score_match_winner, '-',set_score_match_loser)
            when set_score_match_winner < set_score_match_loser then concat(set_score_match_loser, '-', set_score_match_winner)
            else null
        end as set_score_concat
    from match_sets_set_score
),

-- get outcome columns
match_set_scores_set_outcomes as (
    select
        *,
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

final as (
    select
        match_url,
        set_number_in_match,
        set_score_concat as set_score,
        set_tiebreaker_score,
        set_winner,
        set_winner_score,
        set_loser,
        set_loser_score
    from match_set_scores_set_outcomes
)

select * from final