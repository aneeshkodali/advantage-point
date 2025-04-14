{% test all_record_sources_referenced_in_link(model) %}

with

link_record_sources as (
    select distinct
        record_source
    from {{ model }}
),

stg_record_sources as (
    select 
        link_name,
        record_source
    from {{ ref('stg__seed__link_record_sources') }}
),

missing_record_sources as (
    select
        link_rec_src.record_source
    from link_record_sources as link_rec_src
    left join stg_record_sources as stg_rec_src on 1=1
        and link_rec_src.record_source = stg_rec_src.record_source
    where 1=1
        and stg_rec_src.link_name = '{{ model.identifier }}'
        and stg_rec_src.record_source is null
)

select *
from missing_record_sources

{% endtest %}
