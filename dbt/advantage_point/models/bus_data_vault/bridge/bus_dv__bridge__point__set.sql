{{
    config(
        unique_key='lk_point_set'
    )
}}

with

link_record_sources as (
    select * from {{ ref('stg__seed__link_record_sources') }}
),

int_point_set as (
    select * from {{ ref('bus_dv__int__point__set') }}
),

-- select columns
-- join to get hk and bk
point_set as (
    select
        {{ generate_point_set_surrogate_key(
            point_business_key_col='bk_point',
            set_business_key_col='bk_set'
        ) }} as lk_point_set,
        
        hk_point,
        bk_point,
        hk_set,
        bk_set,

        point_number_in_set,

        bus_dv_source_model as record_source
    from int_point_set
),

-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_point_set order by link_rec_src.sort_order) as rn -- assing row number
    from point_set as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'bus_dv__bridge__point__set'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_point_set,
        current_timestamp as load_datetime,
        record_source,

        hk_point,
        bk_point,
        hk_set,
        bk_set,
        point_number_in_set,

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_point_set = incr.lk_point_set
        ) -- filter for new pk records
        {% endif %}
)

select * from final