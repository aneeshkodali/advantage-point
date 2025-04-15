{{
    config(
        unique_key='lk_match_player'
    )
}}

with

link_record_sources as (
    select * from {{ ref('stg__seed__link_record_sources') }}
),

hub_match as (
    select * from {{ ref('raw_dv__hub__match') }}
),

hub_player as (
    select * from {{ ref('raw_dv__hub__player') }}
),

tennisabstract_matches as (
    select * from {{ ref('stg__tennisabstract__matches') }}
),

-- union players 
tennisabstract_matches_union as (
    select
        bk_match,
        bk_match_player_one as bk_player,
        record_source
    from tennisabstract_matches

    union all

    select
        bk_match,
        bk_match_player_two as bk_player,
        record_source
    from tennisabstract_matches
),

-- union data
records_union as (
    (select * from tennisabstract_matches_union)
),

-- add hub key
records_lkey as (
    select
        {{ generate_match_player_surrogate_key(
            match_business_key_col='lnk.bk_match',
            player_business_key_col='lnk.bk_player'
        ) }} as lk_match_player,

        hub_match.hk_match,
        hub_player.hk_player,

        lnk.bk_match,
        lnk.bk_player,
        lnk.record_source
    from records_union as lnk
    left join hub_match on lnk.bk_match = hub_match.bk_match
    left join hub_player on lnk.bk_player = hub_player.bk_player
),

-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_match_player order by link_rec_src.sort_order) as rn -- assing row number
    from records_lkey as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'raw_dv__link__match_player'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_match_player,
        current_timestamp as load_datetime,
        record_source,

        hk_match,
        bk_match,
        hk_player,
        bk_player

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_match_player = incr.lk_match_player
        ) -- filter for new pk records
        {% endif %}
)

select * from final
