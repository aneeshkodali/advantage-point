with

tennisabstract_matches as (
    select * from {{ ref('stg_tennisabstract__matches') }}
    where is_record_active = true
),

matches as (
    select
        match_url,
        match_player_one,
        match_player_two,
        -- match_result is of format: {winner} d. {loser} {score}
        regexp_matches(match_result, '^(.+?) d\. (.+?) (.+)$') AS match_result_parsed
    from tennisabstract_matches
),

-- get players into own rows
matches_union as (
    (
        select
            match_url,
            match_player_one as player_name,
            match_result_parsed
        from matches
    )
    union all
    (
        select
            match_url,
            match_player_two as player_name,
            match_result_parsed
        from matches
    )
)

final as (
    select
        match_url,
        player_name,
        -- compare player to winner
        case
            when player_name = match_result_parsed[1] then true
            when player_name != match_result_parsed[1] then false
            else null
        end as is_match_winner
    from matches_union
)

select * from final