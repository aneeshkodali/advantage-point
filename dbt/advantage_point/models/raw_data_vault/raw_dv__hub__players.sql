{{
    config(
        unique_key='hk_player'
    )
}}

with

tennisabstract_players as (
    select
        {{ generate_player_surrogate_key(
            player_name_col='player_name',
            player_gender_col='player_gender'
        ) }} as hk_player,
        player_name,
        player_gender,
        current_timestamp as load_dts,
        'tennisabstract' as record_source
    from {{ ref('stg__tennisabstract__players') }}
)

select * from tennisabstract_players