{{
    config(
        unique_key='hk_set'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

-- get match columns
-- will be used to construct bk and hk
hub_match as (
    select
        hk_match,
        match_date,
        match_gender,
        match_tournament,
        match_round,
        match_players
    from {{ ref('raw_dv__hub__match') }}
),

sat_point as (

)