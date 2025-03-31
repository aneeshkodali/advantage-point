{{
    config(
        unique_key='hk_match'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

tennisabstract_matches as (
    select
        bk_match,
        record_source,

        match_date,
        match_gender,
        match_tournament,
        match_round,
        match_players
    from {{ ref('stg__tennisabstract__matches') }}
),

-- union data
records_union as (
    (select * from tennisabstract_matches)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_match_surrogate_key(
            match_business_key_col='bk_match'
        ) }} as hk_match
    from records_union
),

-- add row number to order records
records_rownum as (
    select
        hub.*,
        row_number() over (partition by hub.hk_match order by coalesce(hub_rec_src.sort_order, 9999), hub.record_source) as rn -- assing row number
    from records_hkey as hub
    left join hub_record_sources as hub_rec_src on 1=1
        and hub_rec_src.hub_name = 'hub__matches'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_match,
        bk_match,
        current_timestamp as load_datetime,
        record_source,
        
        match_date,
        match_gender,
        match_tournament,
        match_round,
        match_players
    from records_rownum
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.bk_match = final.bk_match
        ) -- filter for new pk records
        {% endif %}
)

select * from final