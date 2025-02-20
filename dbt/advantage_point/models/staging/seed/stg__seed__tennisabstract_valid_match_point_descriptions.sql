with

source as (
    select * from {{ ref('tennisabstract_valid_match_point_descriptions') }}
),

renamed as (
    select
        match_url,
        point_number_in_match,
        point_server,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_description_old,
        point_description_new,
        notes
    from source
)

select * from renamed