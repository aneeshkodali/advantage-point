{{ config(
    unique_key = ['hk_point', 'load_datetime']
) }}

with

hub_points as (
    select * from {{ ref('raw_dv__hub__point') }}
),

point_details as (
    select
        hk_point,
        bus_dv_source_model as record_source,

        point_side,
        is_break_point,
        is_game_point
    from {{ ref('bus_dv__int__point_details') }}
),

-- create hash_diff
records_hash as (
    select
        hub_p.hk_point,
        sat.record_source,

        sat.point_score_in_game,
        sat.point_side,
        sat.is_break_point,
        sat.is_game_point,
        
        {{ dbt_utils.generate_surrogate_key([
            'sat.point_score_in_game',
            'sat.point_side',
            'sat.is_break_point',
            'sat.is_game_point',
        ]) }} as hash_diff
    from point_details as sat
    left join hub_points as hub_p on 1=1
        and sat.hk_point = hub_p.hk_point
),

-- filter for incremental changes
final as (
    select
        hk_point,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        point_score_in_game,
        point_side,
        is_break_point,
        is_game_point

    from records_hash as incr
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_point = incr.hk_point
                and existing.hash_diff = incr.hash_diff
        )
        {% endif %} 
)

select * from final
