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
        {{ convert_rank_to_integer('current_dubs') }} as current_dubs,
        {{ remove_empty_string_from_source('atp_id') }} as player_tour_id,
        {{ remove_empty_string_from_source('photog_link') }} as photog_link,
        {{ remove_empty_string_from_source('fullname') }} as player_full_name,
        {{ remove_empty_string_from_source('twitter') }} as twitter,
        {{ remove_empty_string_from_source('photog') }} as photog,
        to_date(peakfirst, 'YYYYMMDD') as peakfirst,
        chartagg,
        cast(cast(active as int) as boolean) as active,
        nameparam,
        {{ convert_rank_to_integer('liverank') }} as liverank,
        shortlist,
        careerjs,
        {{ convert_rank_to_integer('peak_dubs') }} as peak_dubs,
        case peakfirst_dubs
            when '''""''' then null
            else to_date(peakfirst_dubs, 'YYYYMMDD')
        end as peakfirst_dubs,
        {{ remove_empty_string_from_source('hand') }} as player_hand,
        {{ convert_rank_to_integer('currentrank') }} as currentrank,
        {{ remove_empty_string_from_source('country') }} as player_country,
        {{ remove_empty_string_from_source('backhand') }} as player_backhand,
        to_date(peaklast, 'YYYYMMDD') as peaklast,
        {{ remove_empty_string_from_source('dc_id') }} as player_team_cup_id,
        {{ remove_empty_string_from_source('lastname') }} as player_last_name,
        to_date(dob, 'YYYYMMDD') as player_date_of_birth,
        cast({{ remove_empty_string_from_source('ht') }} as int) as player_height_in_cm,
        {{ convert_rank_to_integer('peakrank') }} as peakrank,
        {{ remove_empty_string_from_source('itf_id') }} as player_itf_id,
        {{ remove_empty_string_from_source('wiki_id') }} as wiki_id
    from source
)

select * from renamed