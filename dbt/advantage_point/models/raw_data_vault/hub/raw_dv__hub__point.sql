{{
    config(
        unique_key='hk_point'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

tennisabstract_match_points as (
    select
        bk_point,
        record_source,

        bk_match
        point_number_in_match
    from {{ ref('stg__tennisabstract__match_points') }}
),

-- union data
records_union as (
    (select * from tennisabstract_match_points)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_point_surrogate_key(
            point_business_key_col='bk_point'
        ) }} as hk_point
    from records_union
),

-- add row number to order records
records_rownum as (
    select
        hub.*,
        row_number() over (partition by hub.hk_point order by hub_rec_src.sort_order) as rn -- assing row number
    from records_hkey as hub
    left join hub_record_sources as hub_rec_src on 1=1
        and hub_rec_src.hub_name = 'raw_dv__hub__point'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_point,
        bk_point,
        current_timestamp as load_datetime,
        record_source,
        
        bk_match,
        point_number_in_match
    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_point = incr.hk_point
        ) -- filter for new pk records
        {% endif %}
)

select * from final