{{
    config(
        unique_key='lk_match_player'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
)

select * from hub_record_sources

