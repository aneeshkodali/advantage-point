{{ config(
    unique_key = ['hk_shot', 'load_datetime']
) }}

with

hub_shots as (
    select * from {{ ref('bus_dv__hub__shot') }}
),

shot_details as (
    select
        hk_shot,
        bus_dv_source_model as record_source,

        shot_direction,
        shot_result,
        shot_type
    from {{ ref('bus_dv__int__shot') }}
),

-- create hash_diff
records_hash as (
    select
        hub_s.hk_shot,
        sat.record_source,

        sat.shot_direction,
        sat.shot_result,
        sat.shot_type,
        
        {{ dbt_utils.generate_surrogate_key([
            'sat.shot_direction',
            'sat.shot_result',
            'sat.shot_type',
        ]) }} as hash_diff
    from shot_details as sat
    left join hub_shots as hub_s on 1=1
        and sat.hk_shot = hub_s.hk_shot
),

-- filter for incremental changes
final as (
    select
        hk_shot,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        sat.shot_direction,
        sat.shot_result,
        sat.shot_type

    from records_hash as incr
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_shot = incr.hk_shot
                and existing.hash_diff = incr.hash_diff
        )
        {% endif %} 
)

select * from final
