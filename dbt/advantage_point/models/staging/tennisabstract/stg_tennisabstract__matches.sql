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
        to_date(coalesce(cast(seed_valid_match_dates.valid_match_date as text), source.match_date), 'YYYYMMDD') as match_date,
        source.match_gender,
        replace(source.match_tournament, '_', ' ') as match_tournament,
        source.match_round,
        replace(source.match_player_one, '_', ' ') as match_player_one,
        replace(source.match_player_two, '_', ' ') as match_player_two,
        source.match_title,
        source.match_result,

        -- {winner} d. {loser} {set score}
        split_part(source.match_result, ' d. ', 1) as match_winner,
        case
            when source.match_winner = source.match_player_one then match_player_two
            when source.match_winner = source.match_player_two then match_player_one
            else null
        end as match_loser,
        source.audit_field_active_flag as is_record_active,
        source.audit_field_start_datetime_utc as loaded_at
    from source
    left join seed_valid_match_dates on source.match_url = seed_valid_match_dates.match_url
),

final as (
    select
        match_url,
        match_date,
        match_gender,
        match_tournament,
        match_round,
        match_player_one,
        match_player_two,
        match_title,
        match_result,
        match_winner,
        match_loser,
        split_part(match_result, match_loser || ' ', 2) as match_score,
        is_record_active,
        loaded_at
    from renamed
)

select * from final