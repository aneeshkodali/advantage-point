with

source as (
    select * from {{ ref('tournamnet_formats') }}
),

renamed as (
    select
        format_id,
        match_format,
        set_type,
        cast(best_of as int) as best_of,
        cast(sets_to_win as int) as sets_to_win,
        cast(games_per_set as int) as games_per_set,
        tiebreak_type,
        tiebreak_trigger_game,
        tiebreak_points,
        cast(final_set_tiebreak as boolean) as final_set_tiebreak,
        final_set_tiebreak_trigger_game,
        cast(is_ad_scoring as boolean) as is_ad_scoring,
        format_description

    from source
)

select * from renamed