{{
    config(
        unique_key=['hub_tournament', 'load_datetime']
    )
}}

with

hub_tournaments as (
    select * from {{ ref('raw_dv__hub__tournaments') }}
),

tennisabstract_tournaments as (
    select
        *,
        'tennisabstract' as record_source
    from {{ ref('stg__tennisabstract__tournaments') }}
),

-- union data
tournaments_union as (
    select
        {{ generate_tournament_surrogate_key(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        ) }} as hk_tournament,

        tournament_start_date,
        tournament_surface,
        tournament_draw_size,

        {{ dbt_utils.generate_surrogate_key([
            'tournament_start_date',
            'tournament_surface',
            'tournament_draw_size'
        ]) }} as hash_diff,
        record_source
    from (
        select
            tournament_year,
            tournament_gender,
            tournament_name,

            tournament_start_date,
            tournament_surface,
            tournament_draw_size,

            record_source
        from tennisabstract_tournaments
    ) as t_union
),

-- join to hub
tournaments_joined as (
    select
        t.hk_tournament,

        t.tournament_start_date,
        t.tournament_surface,
        t.tournament_draw_size,

        t.hash_diff,
        t.record_source
    from tournaments_union as t
    inner join hub_tournaments as t_hub on t.hk_tournament = t_hub.hk_tournament
),

-- filter for incremental changes
tournaments_filtered as (
    select
        hk_tournament,

        tournament_start_date,
        tournament_surface,
        tournament_draw_size,
        
        hash_diff,
        current_timestamp as load_datetime,
        record_source
    from tournaments_joined as t
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as t_sat
            where 1=1
                and t_sat.hk_tournament = t.hk_tournament
                and t_sat.hash_diff = t.hash_diff
        )
        {% endif %} 
)

select * from tournaments_filtered