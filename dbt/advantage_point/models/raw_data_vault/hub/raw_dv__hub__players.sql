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
    select
        player_name,
        player_gender,
        record_source,
        hk_player,
        row_number() over (partition by hk_player order by record_source) as rn -- assing row number
    from
    (
        select
            player_name,
            player_gender,
            {{ generate_player_surrogate_key(
                player_name_col='player_name',
                player_gender_col='player_gender'
            ) }} as hk_player,
            record_source
        from tennisabstract_players
    ) as p_union
),

final as (
    select
        hk_player,
        player_name,
        player_gender,
        current_timestamp as load_datetime,
        record_source
    from players_union
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and hk_player not in (
            select hk_player from {{ this }}
        ) -- filter for new pk records
        {% endif %}
)

select * from final