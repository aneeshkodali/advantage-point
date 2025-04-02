{{
    config(
        unique_key=['hk_tournament', 'load_datetime']
    )
}}

with

hub_tournaments as (
    select * from {{ ref('raw_dv__hub__tournaments') }}
),

tennisabstract_tournaments as (
    select
        bk_player,
        record_source,

        tournament_start_date,
        tournament_surface,
        tournament_draw_size
    from {{ ref('stg__tennisabstract__tournaments') }}
),

-- union data
records_union as (
    (select * from tennisabstract_tournaments)
),

-- create hash diff
records_hash as (
    select
        hub.hk_player,
        sat.*,

        {{ dbt_utils.generate_surrogate_key([
            'sat.tournament_start_date',
            'sat.tournament_surface',
            'sat.tournament_draw_size'
        ]) }} as hash_diff
    from records_union as sat
    left join hub_tournaments as hub on 1=1
        and sat.bk_tournament = hub.bk_tournament
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

    from records_hash as incr
    where 1=1
        {% if is_incremental() %}
        and not exists (
            select 1
            from {{ this }} as existing
            where 1=1
                and existing.hk_tournament = incr.hk_tournament
                and existing.hash_diff = incr.hash_diff
        )
        {% endif %} 
)

select * from final