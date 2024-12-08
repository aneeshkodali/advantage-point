with

source as (
    select * from {{ source('tennisabstract', 'matches') }}
),

renamed as (
    select
        match_url,
        to_date(match_date, 'YYYYMMDD') as match_date,
        match_gender,
        replace(match_tournament, '_', ' ') as match_tournament,
        match_round,
        replace(match_player_one, '_', ' ') as match_player_one,
        replace(match_player_two, '_', ' ') as match_player_two,
        match_title,
        match_result,
        audit_field_active_flag as is_record_active,
        audit_field_start_datetime_utc as loaded_at
    from source
)

select * from renamed