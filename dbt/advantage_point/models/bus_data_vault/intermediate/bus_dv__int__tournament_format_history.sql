with

hub_tournaments as (
    select * from {{ ref('raw_dv__hub__tournament') }}
),

tournament_agg as (
    select
        tournament_name,
        tournament_gender,
        min(tournament_year) as min_tournament_year,
        max(tournament_year) as max_tournament_year
    from hub_tournaments
    group by 1, 2
),

tournament_format_history as (
    select * from {{ ref('stg__seed__tournament_format_history')}}
),

tournament_formats as (
    select * from {{ ref('stg__seed__tournament_formats') }}
),

tournament_format_fallback as (
    select * from tournament_formats
    where format_id = 'standard'
),

-- join min/max year
tournament_joined as (
    select
        tfh.tournament_name,
        tfh.tournament_gender,
        tfh.format_id,
        coalesce(
            coalesce(
                tfh.effective_start_year,
                t.min_tournament_year
            ),
            extract(year from current_date)
        ) as effective_start_year,
        coalesce(
            coalesce(
                tfh.effective_end_year,
                t.max_tournament_year
            ),
            extract(year from current_date)
        ) as effective_end_year
    
    from tournament_format_history as tfh
    left join tournament_agg as t on 1=1
        and tfh.tournament_name = t.tournament_name
        and tfh.tournament_gender = t.tournament_gender
),

-- generate rows between years
tournament_rows_gen as (
    select
        t.tournament_name,
        t.tournament_gender,
        t.format_id,
        year_series.year as tournament_year
    from tournament_joined as t
    join lateral generate_series(
        t.effective_start_year, 
        t.effective_end_year
    ) as year_series(year) on true
),

-- generate bk
tournament_bk as (
    select
        *,
        {{ generate_tournament_business_key(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        ) }} as bk_tournament
    from tournament_rows_gen
),

-- join tournament formats
-- coalesce with 'fallback' option
tournament_w_format_joined as (
    select
        hub_t.hk_tournament,
        t.*,
        coalesce(
            tf.best_of_sets,
            tf_fallback.best_of_sets
        ) as best_of_sets,
        coalesce(
            tf.sets_to_win,
            tf_fallback.sets_to_win
        ) as sets_to_win,
        coalesce(
            tf.games_per_set,
            tf_fallback.games_per_set
        ) as games_per_set,
        coalesce(
            tf.tiebreak_trigger_game,
            tf_fallback.tiebreak_trigger_game
        ) as tiebreak_trigger_game,
        coalesce(
            tf.tiebreak_points,
            tf_fallback.tiebreak_points
        ) as tiebreak_points,
        coalesce(
            tf.final_set_tiebreak,
            tf_fallback.final_set_tiebreak
        ) as final_set_tiebreak,
        coalesce(
            tf.final_set_tiebreak_trigger_game,
            tf_fallback.final_set_tiebreak_trigger_game
        ) as final_set_tiebreak_trigger_game,
        coalesce(
            tf.is_ad_scoring,
            tf_fallback.is_ad_scoring
        ) as is_ad_scoring
    -- use hub_tournaments to get ALL tournament records - also get hkey
    from hub_tournaments as hub_t
    -- join tournament_bk to preserve preexisting tournament_format_history
    left join tournament_bk as t on hub_t.bk_tournament = t.bk_tournament
    -- join tournament_formats to get format info
    left join tournament_formats as tf on t.format_id = tf.format_id
    -- join the 'fallback' option if format data doesn't exist for tournament
    left join tournament_format_fallback as tf_fallback on 1=1
),

final as (
    select
        hk_tournament,
        best_of_sets,
        sets_to_win,
        games_per_set,
        tiebreak_trigger_game,
        tiebreak_points,
        final_set_tiebreak,
        final_set_tiebreak_trigger_game,
        is_ad_scoring,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__tournament_format_history' as bus_dv_source_model
    from tournament_w_format_joined
)

select * from final