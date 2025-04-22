with

pit_point as (
    select * from {{ ref('bus_dv__pit__point') }}
),

hub_point as (
    select * from {{ ref('raw_dv__hub__point') }}
),

-- split shots into rows (by ;)
shots_exploded as (
  select
    hk_point,
    row_number() over (partition by hk_point order by ordinality) AS shot_number,
    trim(shot_text) AS shot_text
  from pit_point,
  lateral unnest(string_to_array(point_description, '; ')) with ordinality as u(shot_text, ordinality)
),

-- split serves into own rows (by .)
serve_exploded as (
  select
        hk_point,
        shot_number,
        trim(shot_description) as shot_text
    from shots_exploded,
    lateral unnest(string_to_array(shot_text, '.')) as shot_description
    where shot_number = 1
),

-- filter out serve rows
other_shots as (
  select
    hk_point,
    shot_number,
    shot_text
  from shots_exploded
  where shot_number != 1
),

-- union serve rows with non-serve rows
shots_union as (
  (select * from serve_exploded)
  union all
  (select * from other_shots)
),

-- get shot number (factoring for multiple serve rows)
shot_number_w_serve as (
    select
        *,
        row_number() over (partition by hk_point order by shot_number) as shot_number_w_serve
    from shots_union
),

-- get bk
shot_bk as (
    select
        shots.*,
        hub_point.bk_point,
        {{ generate_shot_business_key(
            bk_point_col='hub_point.bk_point',
            set_col='shots.shot_number_w_serve'
        ) }} as bk_shot,
    from shot_number_w_serve as shots
    left join hub_point on shots.hk_point = hub_point.hk_point
),

final as (
    select 
        hk_point,
        shot_number,
        shot_text,
        shot_number_w_serve,
        bk_point,
        bk_shot,
        {{ generate_shot_surrogate_key(
            bk_shot_col='bk_shot'
        ) }} as hk_shot,
        
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__shot' as bus_dv_source_model
    from shots_union
)

select * from final