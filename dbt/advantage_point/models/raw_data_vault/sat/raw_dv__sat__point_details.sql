{{
    config(
        unique_key=['hk_point', 'load_datetime']
    )
}}

with

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

hub_match as (
    select * from {{ ref('raw_dv__hub__match') }}
),

tennisabstract_match_points as (
    select
        bk_point,
        record_source,
        
        bk_match,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_description
    from {{ ref('stg__tennisabstract__match_points') }}
),

-- union data
records_union as (
    (select * from tennisabstract_match_points)
),

-- create hash_diff
records_hash as (
    select
        hub_mp.hk_point,
        sat.record_source,

        hub_m.hk_match,
        sat.set_score_in_match,
        sat.game_score_in_set,
        sat.point_score_in_game,
        sat.point_description,

        {{ dbt_utils.generate_surrogate_key([
            'hub_m.hk_match',
            'sat.set_score_in_match',
            'sat.game_score_in_set',
            'sat.point_score_in_game',
            'sat.point_description',
        ]) }} as hash_diff
    from records_union as sat
    left join hub_point as hub_mp on 1=1
        and sat.bk_point = hub_mp.bk_point
    left join hub_match as hub_m on 1=1
        and sat.bk_match = hub_m.bk_match
),

-- filter for incremental changes
final as (
    select
        hk_point,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        hk_match,
        point_server,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_description

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