with

source as (
    select * from {{ source('tennisabstract', 'match_points') }}
),

renamed as (
    select
        match_url,

        -- sets
        "server" as point_server,
        "sets" as set_score,
        cast(split_part("sets", '-', 1) as int) as set_score_server,
        cast(split_part("sets", '-', 2) as int) as set_score_receiver,

        -- games
        games as game_score,
        cast(split_part(games, '-', 1) as int) as game_score_server,
        cast(split_part(games, '-', 2) as int) as game_score_receiver,
        
        -- points
        cast(point_number as int) as point_in_match,
        points as point_score,
        split_part(points, '-', 1) as point_score_server,
        split_part(points, '-', 2) as point_score_receiver,
        point_description,
        
        -- audit fields
        audit_field_active_flag as active,
        audit_field_start_datetime_utc as loaded_at,
    from source

),

processed as (
    select
        *,
        set_score_server + set_score_receiver + 1 as set_in_match,
        game_score_server + game_score_receiver + 1 as game_in_set,
        row_number() over (partition by match_url order by set_in_match, game_in_set) as game_in_match,
        row_number() over (partition by match_url, set_score, game_score order by point_in_match) as point_in_game,
        row_number() over (partition by match_url, set_score order by point_in_match) as point_in_set,
    from renamed
)

select * from processed