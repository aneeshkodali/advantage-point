with

-- note '‑' is a special character
match_points as (
    select
        *,
        cast(split_part(set_score_in_match, '‑', 1) as int) as set_score_server,
        cast(split_part(set_score_in_match, '‑', 2) as int) as set_score_receiver
    from {{ ref('stg_tennisabstract__match_points') }}
    where is_record_active = true
),

match_sets as (
    select distinct
        match_url,
        set_score_server + set_score_receiver + 1 as set_number_in_match
    from match_points
)

select * from match_sets