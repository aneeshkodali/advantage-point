{{
    config(
        unique_key=['hk_match', 'load_datetime']
    )
}}

with

hub_matches as (
    select * from {{ ref('raw_dv__hub__matches') }}
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
        hub.hk_match,
        sat.*,
        
        {{ dbt_utils.generate_surrogate_key([
            'sat.match_title',
            'sat.match_result',
        ]) }} as hash_diff
    from records_union as sat
    left join hub_matches as hub on 1=1
        and sat.bk_match = hub.bk_match
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

    from records_hash
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_match = final.hk_match
                and existing.hash_diff = final.hash_diff
        )
        {% endif %} 
)

select * from final