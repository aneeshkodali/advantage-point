{{
    config(
        unique_key=['hk_player', 'load_datetime']
    )
}}

with

hub_players as (
    select * from {{ ref('raw_dv__hub__player') }}
),

tennisabstract_players as (
    select
        bk_player,
        record_source,

        player_twitter_handle,
        player_wikipedia_id
    from {{ ref('stg__tennisabstract__players') }}
),

-- union data
records_union as (
    (select * from tennisabstract_players)
),

-- create hash diff
records_hash as (
    select
        hub.hk_player,
        sat.record_source,

        sat.player_twitter_handle,
        sat.player_wikipedia_id,

        {{ dbt_utils.generate_surrogate_key([
            'sat.player_twitter_handle',
            'sat.player_wikipedia_id',
        ]) }} as hash_diff
    from records_union as sat
    left join hub_players as hub on 1=1
        and sat.bk_player = hub.bk_player
),

-- filter for incremental changes
final as (
    select
        hk_player,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        player_twitter_handle,
        player_wikipedia_id

    from records_hash as incr
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_player = incr.hk_player
                and existing.hash_diff = incr.hash_diff
        )
        {% endif %} 
)

select * from final