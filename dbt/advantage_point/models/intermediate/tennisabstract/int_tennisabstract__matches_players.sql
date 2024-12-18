with

matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
),

-- get players into own rows
matches_union as (
    (
        select
            match_url,
            match_winner as player_name,
            1 as is_match_winner
        from matches
    )
    union all
    (
        select
            match_url,
            match_loser as player_name,
            0 as is_match_winner
        from matches
    )
),

final as (
    select
        match_url,
        player_name,
        is_match_winner
    from matches_union
)

select * from final