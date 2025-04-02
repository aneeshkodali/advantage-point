{% test all_record_sources_referenced_in_hub(hub_model) %}

with

hub_record_sources as (
    select distinct
        record_source
    from {{ hub_model }}
),

stg_record_sources as (
    select 
        hub_name,
        record_source
    from {{ ref('stg__seed__hub_record_sources') }}
),

missing_record_sources as (
    select
        hub_rec_src.record_source
    from hub_record_sources as hub_rec_src
    left join stg_record_sources as stg_rec_src on 1=1
        and hub_rec_src.record_source = stg_rec_src.record_source
    where 1=1
        and stg_rec_src.hub_name = '{{ hub_model.identifier }}'
        and stg_rec_src.record_source is null
)

select *
from missing_record_sources

{% endtest %}
