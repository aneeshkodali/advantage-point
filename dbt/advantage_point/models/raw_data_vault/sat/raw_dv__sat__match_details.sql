{{
    config(
        unique_key=['hk_match', 'load_datetime']
    )
}}

with

hub_matches as (
    select * from {{ ref('raw_dv__hub__match') }}
),

tennisabstract_matches as (
    select
        bk_match,
        record_source,
        
        match_title,
        match_result
    from {{ ref('stg__tennisabstract__matches') }}
),

-- union data
records_union as (
    (select * from tennisabstract_matches)
),

-- create hash_diff
records_hash as (
    select
        hub_m.hk_match,
        sat.record_source,

        sat.match_title,
        sat.match_result,
        
        {{ dbt_utils.generate_surrogate_key([
            'sat.match_title',
            'sat.match_result',
        ]) }} as hash_diff
    from records_union as sat
    left join hub_matches as hub_m on 1=1
        and sat.bk_match = hub_m.bk_match
),

-- filter for incremental changes
final as (
    select
        hk_match,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        match_title,
        match_result

    from records_hash as incr
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_match = incr.hk_match
                and existing.hash_diff = incr.hash_diff
        )
        {% endif %} 
)

select * from final