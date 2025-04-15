{{
    config(
        unique_key='hk_player'
    )
}}

with

hub_record_sources as (
    select * from {{ ref('stg__seed__hub_record_sources') }}
),

tennisabstract_players as (
    select
        bk_player,
        record_source,

        player_name,
        player_gender
    from {{ ref('stg__tennisabstract__players') }}
),

tennisabstract_match_players as (
    select
        bk_match_player_one,
        bk_match_player_two,
        record_source,
        match_player_one,
        match_player_two,
        match_gender
    from {{ ref('stg__tennisabstract__matches') }}
),

-- union player one and player two
-- get distinct list
tennisabstract_match_players_union as (
    select distinct
        *
    from (
        (
            select
                bk_match_player_one as bk_player,
                record_source,

                match_player_one as player_name,
                match_gender as player_gender
            from tennisabstract_match_players
        )
        union all
        (
            select
                bk_match_player_two as bk_player,
                record_source,

                match_player_two as player_name,
                match_gender as player_gender
            from tennisabstract_match_players
        )
    ) as m_p_union
),

-- union data
records_union as (
    (select * from tennisabstract_players)
    union all
    (select * from tennisabstract_match_players_union)
),

-- add hub key
records_hkey as (
    select
        *,
        {{ generate_player_surrogate_key(
            player_business_key_col='bk_player'
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
        and hub_rec_src.hub_name = 'raw_dv__hub__player'
        and hub.record_source = hub_rec_src.record_source
),

final as (
    select
        hk_player,
        bk_player,
        current_timestamp as load_datetime,
        record_source,

        player_name,
        player_gender
        
    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_player = incr.hk_player
        ) -- filter for new pk records
        {% endif %}
)

select * from final