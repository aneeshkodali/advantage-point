{{
    config(
        unique_key='hk_player'
    )
}}

with

tennisabstract_players as (
    select
        player_name,
        player_gender,

        'tennisabstract__players' as record_source
    from {{ ref('stg__tennisabstract__players') }}
),

-- union data
records_union as (
    (select * from tennisabstract_players)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_player_surrogate_key(
                player_name_col='player_name',
                player_gender_col='player_gender'
        ) }} as hk_player
    from records_union
),

-- add row number to order records
records_rownum as (
    select
        *,
        row_number() over (partition by hk_player order by record_source) as rn -- assing row number
    from records_hkey
),

final as (
    select
        hk_player,

        player_name,
        player_gender,
        
        current_timestamp as load_datetime,
        record_source
    from records_rownum
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and hk_player not in (
            select hk_player from {{ this }}
        ) -- filter for new pk records
        {% endif %}
)

select * from final