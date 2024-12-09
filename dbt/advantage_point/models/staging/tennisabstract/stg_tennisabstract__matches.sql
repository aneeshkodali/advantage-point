with

source as (
    select * from {{ source('tennisabstract', 'matches') }}
),

seed_valid_match_dates as (
    select * from {{ ref('valid_match_dates') }}
),

renamed as (
    select
        source.match_url,
        to_date(coalesce(valid_match_dates.match_date, source.match_date), 'YYYYMMDD') as match_date,
        source.match_gender,
        replace(source.match_tournament, '_', ' ') as match_tournament,
        source.match_round,
        replace(source.match_player_one, '_', ' ') as match_player_one,
        replace(source.match_player_two, '_', ' ') as match_player_two,
        source.match_title,
        source.match_result,
        source.audit_field_active_flag as is_record_active,
        source.audit_field_start_datetime_utc as loaded_at
    from source
    left join valid_match_dates on source.match_url = valid_match_dates.match_url
)

select * from renamed