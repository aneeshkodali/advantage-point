{{
    config(
        unique_key='lk_match_tournament'
    )
}}

with

link_record_sources as (
    select * from {{ ref('stg__seed__link_record_sources') }}
),

hub_match as (
    select * from {{ ref('raw_dv__hub__match') }}
),

hub_tournament as (
    select * from {{ ref('raw_dv__hub__tournament') }}
),

tennisabstract_matches as (
    select
        bk_match,
        bk_tournament,
    from {{ ref('stg__tennisabstract__matches') }}
),

-- union data
records_union as (
    (select * from tennisabstract_matches)
),

-- add hub key
records_lkey as (
    select
        {{ generate_match_tournament_surrogate_key(
            match_business_key_col='lnk.bk_match',
            tournament_business_key_col='lnk.bk_tournament'
        ) }} as lk_match_tournament,

        hub_match.hk_match,
        hub_tournament.hk_tournament,

        lnk.bk_match,
        lnk.bk_tournament,
        lnk.record_source
    from records_union as lnk
    left join hub_match on lnk.bk_match = hub_match.bk_match
    left join hub_tournament on lnk.bk_tournament = hub_tournament.bk_tournament
),

-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_match_tournament order by link_rec_src.sort_order) as rn -- assing row number
    from records_lkey as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'raw_dv__link__match__tournament'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_match_tournament,
        current_timestamp as load_datetime,
        record_source,

        hk_match,
        bk_match,
        hk_tournament,
        bk_tournament

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_match_tournament = incr.lk_match_tournament
        ) -- filter for new pk records
        {% endif %}
)

select * from final
