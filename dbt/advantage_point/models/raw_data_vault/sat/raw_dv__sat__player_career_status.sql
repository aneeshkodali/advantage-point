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

        is_player_active,
        player_current_singles_rank,
        player_peak_singles_ranking,
        player_first_peak_singles_ranking_date,
        player_last_peak_singles_ranking_date,
        player_current_doubles_ranking,
        player_peak_doubles_ranking,
        player_first_peak_doubles_ranking_date,

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
            'is_player_active',
            'player_current_singles_rank',
            'player_peak_singles_ranking',
            'player_first_peak_singles_ranking_date',
            'player_last_peak_singles_ranking_date',
            'player_current_doubles_ranking',
            'player_peak_doubles_ranking',
            'player_first_peak_doubles_ranking_date',
        ]) }} as hash_diff
    from records_union
),


-- filter for incremental changes
final as (
    select
        hk_player,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        is_player_active,
        player_current_singles_rank,
        player_peak_singles_ranking,
        player_first_peak_singles_ranking_date,
        player_last_peak_singles_ranking_date,
        player_current_doubles_ranking,
        player_peak_doubles_ranking,
        player_first_peak_doubles_ranking_date,

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