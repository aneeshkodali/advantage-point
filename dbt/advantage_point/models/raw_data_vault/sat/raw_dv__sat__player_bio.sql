{{
    config(
        unique_key=['hk_player', 'load_datetime']
    )
}}

with

tennisabstract_players as (
    select
        player_name,
        player_gender,

        player_full_name,
        player_last_name,
        player_date_of_birth,
        player_country,
        player_hand,
        player_backhand,
        player_height_in_cm,

        'tennisabstract__players' as record_source
    from {{ ref('stg__tennisabstract__players') }}
),

-- union data
records_union as (
    (select * from tennisabstract_players)
),

-- create hashes (surrogate key, hash diff)
records_hash as (
    select
        *,
        {{ generate_player_surrogate_key(
            player_name_col='player_name',
            player_gender_col='player_gender'
        ) }} as hk_player,
        {{ dbt_utils.generate_surrogate_key([
            'player_full_name',
            'player_last_name',
            'player_date_of_birth',
            'player_country',
            'player_hand',
            'player_backhand',
            'player_height_in_cm',
        ]) }} as hash_diff
    from records_union
),

-- filter for incremental changes
final as (
    select
        hk_player,

        player_full_name,
        player_last_name,
        player_date_of_birth,
        player_country,
        player_hand,
        player_backhand,
        player_height_in_cm,

        hash_diff,
        current_timestamp as load_datetime,
        record_source
    from records_hash
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_player = final.hk_player
                and existing.hash_diff = final.hash_diff
        )
        {% endif %} 
)

select * from final