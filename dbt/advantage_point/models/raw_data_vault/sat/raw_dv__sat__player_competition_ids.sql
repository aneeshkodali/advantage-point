{{
    config(
        unique_key=['hk_player', 'load_datetime']
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

        player_tour_id,
        player_team_cup_id,
        player_itf_id,

        {{ dbt_utils.generate_surrogate_key([
            'player_tour_id',
            'player_team_cup_id',
            'player_itf_id',
        ]) }} as hash_diff,
        record_source
    from (
        select
            player_name,
            player_gender,

            player_tour_id,
            player_team_cup_id,
            player_itf_id,

            record_source
        from tennisabstract_players
    ) as p_union
),

-- join to hub
players_joined as (
    select
        p.hk_player,

        p.player_tour_id,
        p.player_team_cup_id,
        p.player_itf_id,

        p.hash_diff,
        p.record_source
    from players_union as p
    inner join hub_players as p_hub on p.hk_player = p_hub.hk_player
),

-- filter for incremental changes
players_filtered as (
    select
        hk_player,

        player_tour_id,
        player_team_cup_id,
        player_itf_id,

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