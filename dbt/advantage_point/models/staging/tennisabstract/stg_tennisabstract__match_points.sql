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
        cast(point_number as int) as point_in_match,
        "server" as point_server,
        "sets" as set_score,
        games as game_score,
        points as point_score,
        point_description,
        audit_field_active_flag as active,
        audit_field_start_datetime_utc as loaded_at
    from source
),

processed as (
    select
        *,
        set_score_server + set_score_receiver + 1 as set_in_match,
        game_score_server + game_score_receiver + 1 as game_in_set
    from renamed
)

select * from processed