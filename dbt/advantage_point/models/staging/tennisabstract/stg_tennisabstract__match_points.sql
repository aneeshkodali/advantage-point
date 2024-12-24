with

source as (
    select * from {{ source('tennisabstract', 'match_points') }}
),

/*
note:
'‑' is a non-breaking hyphen
*/
renamed as (
    select
        match_url,
        cast(point_number as int) as point_number_in_match,
        "server" as point_server,
        replace("sets", '‑', '-') as set_score_in_match,
        replace(games, '‑', '-') as game_score_in_set,
        replace(points, '‑', '-') as point_score_in_game,
        point_description,
        audit_field_active_flag as is_record_active,
        audit_field_start_datetime_utc as loaded_at
    from source
)

select * from renamed