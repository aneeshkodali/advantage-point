with

source as (
    select * from {{ source('tennisabstract', 'players') }}
),

renamed as (
    select
        player_url,
        player_name,
        player_gender,
        {{ remove_empty_string_from_source('photog_credit') }} as photog_credit,
        {{ convert_rank_to_integer('current_dubs') }} as player_current_doubles_ranking,
        {{ remove_empty_string_from_source('atp_id') }} as player_tour_id,
        {{ remove_empty_string_from_source('photog_link') }} as photog_link,
        {{ remove_empty_string_from_source('fullname') }} as player_full_name,
        {{ remove_empty_string_from_source('twitter') }} as player_twitter_handle,
        {{ remove_empty_string_from_source('photog') }} as photog,
        to_date(peakfirst, 'YYYYMMDD') as player_first_peak_singles_ranking_date,
        chartagg,
        cast(cast(active as int) as boolean) as is_player_active,
        nameparam,
        {{ convert_rank_to_integer('liverank') }} as liverank,
        shortlist,
        careerjs,
        {{ convert_rank_to_integer('peak_dubs') }} as player_peak_doubles_ranking,
        case peakfirst_dubs
            when '''""''' then null
            when '''''' then null
            else to_date(peakfirst_dubs, 'YYYYMMDD')
        end as player_first_peak_doubles_ranking_date,
        {{ remove_empty_string_from_source('hand') }} as player_hand,
        {{ convert_rank_to_integer('currentrank') }} as player_current_singles_rank,
        {{ remove_empty_string_from_source('country') }} as player_country,
        {{ remove_empty_string_from_source('backhand') }} as player_backhand,
        to_date(peaklast, 'YYYYMMDD') as player_last_peak_singles_ranking_date,
        {{ remove_empty_string_from_source('dc_id') }} as player_team_cup_id,
        {{ remove_empty_string_from_source('lastname') }} as player_last_name,
        case dob
            when '''''' then null
            else to_date(dob, 'YYYYMMDD')
        end as player_date_of_birth,
        cast({{ remove_empty_string_from_source('ht') }} as int) as player_height_in_cm,
        {{ convert_rank_to_integer('peakrank') }} as player_peak_singles_ranking,
        {{ remove_empty_string_from_source('itf_id') }} as player_itf_id,
        {{ remove_empty_string_from_source('wiki_id') }} as player_wikipedia_id,
        case lastdate
            when '0' then null
            else to_date(lastdate, 'YYYYMMDD')
        end as player_last_played_date
    from source
)

select * from renamed