{{
    config(
        unique_key=['hub_player', 'load_datetime']
    )
}}

with

hub_players as (
    select * from {{ ref('raw_dv__hub__players') }}
),

tennisabstract_players as (
    select
        *,
        'tennisabstract' as record_source
    from {{ ref('stg__tennisabstract__players') }}
),

-- union data
players_union as (
    select
        {{ generate_player_surrogate_key(
            player_name_col='player_name',
            player_gender_col='player_gender'
        ) }} as hk_player,

        player_full_name,
        player_last_name,
        player_date_of_birth,
        player_country,
        player_hand,
        player_backhand,
        player_height_in_cm,

        {{ dbt_utils.generate_surrogate_key([
            'player_full_name',
            'player_last_name',
            'player_date_of_birth',
            'player_country',
            'player_hand',
            'player_backhand',
            'player_height_in_cm',
        ]) }} as hash_diff,
        record_source
    from (
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

            record_source
        from tennisabstract_players
    ) as p_union
),

-- join to hub
players_joined as (
    select
        p.hk_player,

        p.player_full_name,
        p.player_last_name,
        p.player_date_of_birth,
        p.player_country,
        p.player_hand,
        p.player_backhand,
        p.player_height_in_cm,

        p.hash_diff,
        p.record_source
    from players_union as p
    inner join hub_players as p_hub on p.hk_player = p_hub.hk_player
),

-- filter for incremental changes
players_filtered as (
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
    from players_joined as p
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as p_sat
            where 1=1
                and p_sat.hk_player = p.hk_player
                and p_sat.hash_diff = p.hash_diff
        )
        {% endif %} 
)

select * from players_filtered