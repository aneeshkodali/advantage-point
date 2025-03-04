{{
    config(
        unique_key=['hk_match', 'load_datetime']
    )
}}

with

tennisabstract_matches as (
    select
        match_date,
        match_gender,
        match_tournament,
        match_round,
        {{ create_match_player_sorted_array(
            array[
                match_player_one,
                match_player_two
            ]
        ) }} as match_player_array,

        match_round,
        match_title,
        match_result,

        'tennisabstract__matches' as record_source
    from {{ ref('stg__tennisabstract__matches') }}
),

-- union data
records_union as (
    (select * from tennisabstract_matches)
),

-- create hashes (surrogate key, hash diff)
records_hash as (
    select
        *,
        {{ generate_match_surrogate_key(
            match_date_col='match_date',
            match_gender_col='match_gender',
            match_tournament_col='match_tournament',
            match_round_col='match_round',
            match_player_array_col='match_player_array'
        ) }} as hk_match
        {{ dbt_utils.generate_surrogate_key([
            'match_round',
            'match_title',
            'match_result',
        ]) }} as hash_diff
    from records_union
),

-- filter for incremental changes
final as (
    select
        hk_match,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        match_round,
        match_title,
        match_result

    from records_hash
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_match = final.hk_match
                and existing.hash_diff = final.hash_diff
        )
        {% endif %} 
)

select * from final