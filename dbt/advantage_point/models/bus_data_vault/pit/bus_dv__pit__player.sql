with

player_bio as (
    select
        hk_player,
        load_datetime,
        hash_diff,
        record_source,

        player_name,
        player_full_name,
        player_last_name,
        player_date_of_birth,
        player_country,
        player_hand,
        player_backhand,
        player_height_in_cm
    from {{ get_latest_record(
        model_ref="ref('raw_dv__sat__player_bio')",
        partition_by_col='hk_player',
        order_by_col='load_datetime'
    ) }}
),

player_career_status as (
    select
        *
    from {{ get_latest_record(
        model_ref="ref('raw_dv__sat__player_career_status')",
        partition_by_col='hk_player',
        order_by_col='load_datetime'
    ) }}
),

player_competition_ids as (
    select
        *
    from {{ get_latest_record(
        model_ref="ref('raw_dv__sat__player_competition_ids')",
        partition_by_col='hk_player',
        order_by_col='load_datetime'
    ) }}
),

player_media_presence as (
    select
        *
    from {{ get_latest_record(
        model_ref="ref('raw_dv__sat__player_media_presence')",
        partition_by_col='hk_player',
        order_by_col='load_datetime'
    ) }}
),

-- get unique list of hk
hk_union as (
    (select hk_player from player_bio)
    union
    (select hk_player from player_career_status)
    union
    (select hk_player from player_competition_ids)
    union
    (select hk_player from player_media_presence)
),

final as (
    select
        hk_union.hk_player,

        player_bio.player_name,
        player_bio.player_full_name,
        player_bio.player_last_name,
        player_bio.player_date_of_birth,
        player_bio.player_country,
        player_bio.player_hand,
        player_bio.player_backhand,
        player_bio.player_height_in_cm,
        player_bio.hash_diff as player_bio_hash_diff,
        player_bio.load_datetime as player_bio_load_datetime,

        player_career_status.is_player_active,
        player_career_status.player_current_singles_rank,
        player_career_status.player_peak_singles_ranking,
        player_career_status.player_first_peak_singles_ranking_date,
        player_career_status.player_last_peak_singles_ranking_date,
        player_career_status.player_current_doubles_ranking,
        player_career_status.player_peak_doubles_ranking,
        player_career_status.player_first_peak_doubles_ranking_date,
        player_career_status.player_last_match_played_date,
        player_career_status.hash_diff as player_career_status_hash_diff,
        player_career_status.load_datetime as player_career_status_load_datetime,

        player_competition_ids.player_tour_id,
        player_competition_ids.player_team_cup_id,
        player_competition_ids.player_itf_id,
        player_competition_ids.hash_diff as player_competition_ids_hash_diff,
        player_competition_ids.load_datetime as player_competition_ids_load_datetime,

        player_media_presence.player_twitter_handle,
        player_media_presence.player_wikipedia_id,
        player_media_presence.hash_diff as player_media_presence_hash_diff,
        player_media_presence.load_datetime as player_media_presence_load_datetime,

        current_timestamp as bus_dv_load_datetime,
        'bus_dv__pit__player' as bus_dv_source_model
        
    from hk_union
    left join player_bio using (hk_player)
    left join player_career_status using (hk_player)
    left join player_competition_ids using (hk_player)
    left join player_media_presence using (hk_player)
)

select * from final
