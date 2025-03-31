{{
    config(
        unique_key=['hk_tournament', 'load_datetime']
    )
}}

with

tennisabstract_tournaments as (
    select
        tournament_year,
        tournament_gender,
        tournament_name,

        tournament_start_date,
        tournament_surface,
        tournament_draw_size,

        'tennisabstract__tournaments' as record_source
    from {{ ref('stg__tennisabstract__tournaments') }}
),

-- union data
records_union as (
    (select * from tennisabstract_tournaments)
),

-- create hashes (surrogate key, hash diff)
records_hash as (
    select
        *,
        {{ generate_tournament_surrogate_key(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        ) }} as hk_tournament,
        {{ dbt_utils.generate_surrogate_key([
            'tournament_start_date',
            'tournament_surface',
            'tournament_draw_size'
        ]) }} as hash_diff
    from records_union
),

-- filter for incremental changes
final as (
    select
        hk_tournament,

        current_timestamp as load_datetime,
        hash_diff,
        record_source,

        tournament_start_date,
        tournament_surface,
        tournament_draw_size

    from records_hash
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_tournament = final.hk_tournament
                and existing.hash_diff = final.hash_diff
        )
        {% endif %} 
)

select * from final