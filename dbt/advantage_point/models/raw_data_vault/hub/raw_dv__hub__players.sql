{{
    config(
        unique_key='hk_player'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
)

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
        hub.*,
        row_number() over (partition by hub.hk_player order by hub_rec_src.sort_order) as rn -- assing row number
    from records_hkey as hub
    left join hub_record_sources as hub_rec_src on 1=1
        and hub_rec_src.hub_name = 'hub__players'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_player,

        current_timestamp as load_datetime,
        record_source,

        player_name,
        player_gender
        
    from records_rownum
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.player_name = final.player_name
                and existing.player_gender = final.player_gender
        ) -- filter for new pk records
        {% endif %}
)

select * from final