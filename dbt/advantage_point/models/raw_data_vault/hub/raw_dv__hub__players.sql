{{
    config(
        unique_key='hk_player'
    )
}}

with

tennisabstract_players as (
    select
        *,
        'tennisabstract' as record_source
    from {{ ref('stg__tennisabstract__players') }}
),

-- union data
players_union as (
    (
        select
            player_name,
            player_gender,
            record_source
        from tennisabstract_players
    )
),

final as (
    select
        {{ generate_player_surrogate_key(
            player_name_col='player_name',
            player_gender_col='player_gender'
        ) }} as hk_player,
        player_name,
        player_gender,
        current_timestamp as load_datetime,
        record_source
    from players_union
    {% if is_incremental() %}
    where hk_player not in (select hk_player from {{ this }})
    {% endif %}
)

select * from final