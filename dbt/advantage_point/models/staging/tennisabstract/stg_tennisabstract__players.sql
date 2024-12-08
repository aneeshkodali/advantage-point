with

source as (
    select * from {{ source('tennisabstract', 'players') }}
),

renamed as (
    select
        player_url,
        player_gender,
        replace(photog_credit, "'", "") as photograph_credit,
        {{ convert_rank_to_integer('current_dubs') }} as current_doubles_rank,
        replace(atp_id, "'", "") as tour_id,
        case player_gender
            when 'M' then 'atp_id'
            when 'W' then 'wta_id'
            else null
        end as tour_id_type,
        replace(photog_link, "'", "") as photograph_link,
        replace(fullname, "'", "") as full_name,
        replace(twitter, "'", "") as twitter_handle,
        replace(photog, "'", "") as photograph_credit_abbreviation,
        to_date(peakfirst, 'YYYYMMDD') as first_peak_singles_rank_on,
        chartagg as chart_agg,
        case active
            when '1' then true
            when '0' then false
            else null
        end as is_player_active,
        nameparam as full_name_parameter,
        {{ convert_rank_to_integer('liverank') }} as live_rank,
        shortlist as short_list,
        careerjs as career_js,
        {{ convert_rank_to_integer('peak_dubs') }} as peak_doubles_rank,
        to_date(peakfirst_dubs, 'YYYYMMDD') as first_peak_doubles_rank_on,
        replace(hand, "'", "") as handedness,
        {{ convert_rank_to_integer('currentrank') }} as current_singles_rank,
        replace(country, "'", "") as country_abbreviation,
        replace(backhand, "'", "") as backhand_type,
        to_date(peaklast, 'YYYYMMDD') as last_peak_singles_rank_on,
        replace(dc_id, "'", "") as team_cup_id,
        case player_gender
            when 'M' then 'dc_id'
            when 'W' then 'bjkc_id'
            else null
        end as team_cup_id_type,
        replace(lastname, "'", "") as last_name,
        to_date(dob, 'YYYYMMDD') as date_of_birth,
        cast(ht as int) as height_in_centimeters
        {{ convert_rank_to_integer('peakrank') }} as peak_singles_rank,
        replace(itf_id, "'", "") as itf_id,
        replace(wiki_id, "'", "") as wikipedia_id,
        audit_field_active_flag as is_record_active,
        audit_field_start_datetime_utc as loaded_at
    from source
),

select * from renamed