with

source as (
    select * from {{ source('tennisabstract', 'players') }}
),

renamed as (
    select
        player_url,
        player_gender,
        {{ remove_empty_string_from_source('photog_credit') }} as photograph_credit,
        {{ convert_rank_to_integer('current_dubs') }} as current_doubles_rank,
        {{ remove_empty_string_from_source('atp_id') }} as tour_id,
        {{ remove_empty_string_from_source('photog_link') }} as photograph_link,
        {{ remove_empty_string_from_source('fullname') }} as full_name,
        {{ remove_empty_string_from_source('twitter') }} as twitter_handle,
        {{ remove_empty_string_from_source('photog') }} as photograph,
        to_date(peakfirst, 'YYYYMMDD') as first_peak_singles_rank_on,
        chartagg as chart_agg,
        cast(cast(active as int) as boolean) as is_player_active,
        nameparam as full_name_parameter,
        {{ convert_rank_to_integer('liverank') }} as live_rank,
        shortlist as short_list,
        careerjs as career_js,
        {{ convert_rank_to_integer('peak_dubs') }} as peak_doubles_rank,
        case peakfirst_dubs
            when '''""''' then null
            else to_date(peakfirst_dubs, 'YYYYMMDD')
        end as first_peak_doubles_rank_on,
        {{ remove_empty_string_from_source('hand') }} as handedness,
        {{ convert_rank_to_integer('currentrank') }} as current_singles_rank,
        {{ remove_empty_string_from_source('country') }} as country_abbreviation,
        {{ remove_empty_string_from_source('backhand') }} as backhand_type,
        to_date(peaklast, 'YYYYMMDD') as last_peak_singles_rank_on,
        {{ remove_empty_string_from_source('dc_id') }} as team_cup_id,
        {{ remove_empty_string_from_source('lastname') }} as last_name,
        to_date(dob, 'YYYYMMDD') as date_of_birth,
        cast(ht as int) as height_in_centimeters,
        {{ convert_rank_to_integer('peakrank') }} as peak_singles_rank,
        {{ remove_empty_string_from_source('itf_id') }} as itf_id,
        {{ remove_empty_string_from_source('wiki_id') }} as wikipedia_id
    from source
)

select * from renamed