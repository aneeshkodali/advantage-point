{{
    config(
        unique_key='lk_point_game'
    )
}}

with

link_record_sources as (
    select * from {{ ref('stg__seed__link_record_sources') }}
),

int_point_scores_parsed as (
    select * from {{ ref('bus_dv__int__point_scores_parsed') }}
),

int_point_details as (
    select * from {{ ref('bus_dv__int__point_details') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

hub_game as (
    select * from {{ ref('bus_dv__hub__game') }}
),

-- select columns
-- join to get hk and bk
point_game as (
    select
        {{ generate_point_game_surrogate_key(
            point_business_key_col='hub_point.bk_game',
            game_business_key_col='lnk.bk_game'
        ) }} as lk_point_game,
        
        lnk.hk_point,
        hub_point.bk_point,
        hub_game.hk_game,
        lnk.bk_game,

        lnk.point_score_in_game,
        lnk.point_number_in_game,

        lnk.bus_dv_source_model as record_source
    from int_point_scores_parsed as lnk
    left join hub_point on lnk.hk_point = hub_point.hk_point
    left join hub_game on lnk.bk_game = hub_game.bk_game
),

-- calculate break/game points
point_game_is_winning_point as (
    select
        point_game.hk_point,
        point_game.point_score_in_game,
        point_game.point_number_in_game,
        point_game.bk_game,
        point_game.record_source,
        point_game.point_score_in_game,
        point_game.point_number_in_game,
        int_point_details.is_game_point,
        int_point_details.is_break_point
    from point_game
    left join int_point_details on point_game.hk_point = int_point_details.hk_point
),

-- add row number to order records
records_rownum as (
    select
        lnk.*,
        row_number() over (partition by lnk.lk_point_game order by link_rec_src.sort_order) as rn -- assing row number
    from point_game_is_winning_point as lnk
    left join link_record_sources as link_rec_src on 1=1
        and link_rec_src.link_name = 'bus_dv__bridge__point__game'
        and lnk.record_source = link_rec_src.record_source
),

final as (
    select
        lk_point_game,
        current_timestamp as load_datetime,
        record_source,

        hk_point,
        bk_point,
        hk_game,
        bk_game,
        point_score_in_game,
        point_number_in_game,
        is_game_point,
        is_break_point

    from records_rownum as incr
    where 1=1
        and rn = 1 -- filter for row number
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.lk_point_game = incr.lk_point_game
        ) -- filter for new pk records
        {% endif %}
)

select * from final