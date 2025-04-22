with

sat_point_details as (
    select * from {{ ref('raw_dv__sat__point_details') }}
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
  from sat_point_details,
  lateral unnest(string_to_array(point_description, ';')) with ordinality as u(shot_text, ordinality)
),

-- split serves into own rows (by .)
serve_exploded as (
  select
        hk_point,
        shot_number,
        trim(shot_description) as shot_text
    from shots_exploded,
    lateral unnest(string_to_array(shot_text, '.')) as shot_description
    where 1=1
      and shot_number = 1 -- filter for 'serve' rows
      and trim(shot_description) != '' -- filter out blank rows since some rows end with '.'
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
        row_number() over (partition by hk_point order by shot_number) as shot_number_in_point
    from shots_union
),

-- get bk
shot_bk as (
    select
        shots.*,
        hub_point.bk_point,
        {{ generate_shot_business_key(
            bk_point_col='hub_point.bk_point',
            shot_number_col='shots.shot_number_in_point'
        ) }} as bk_shot
    from shot_number_w_serve as shots
    left join hub_point on shots.hk_point = hub_point.hk_point
),

-- get shot attributes
shot_attributes as (
  select
    *,

    (
      regexp_match(
        lower(shot_text),
        'crosscourt|down the line|down the middle|down the t|inside-in|inside-out|to body|wide'
      )
    )[1] as shot_direction,
    case
      -- if double fault (in case logic codes it as 'fault' instead)
      when lower(shot_text) ilike '%double fault%' then 'double fault'
      else (
        regexp_match(
          lower(shot_text),
          'ace|double fault|fault|forced error|service winner|unforced error|winner'
        )
      )[1]
    end as shot_result

  from shot_bk
),

-- get shot_type
shot_type as (
  select
    *,

    case
      -- if shot was point penalty then NULL
      when shot_text ilike '%point%penalty%' then null
      -- if shot was unknown then NULL
      when shot_text ilike '%unknown%' then null
      -- if shot was a 'challenge' then NULL
      when shot_text ilike '%challenge was incorrect%' then null
      -- if shot text is (...) then null
      when left(shot_text, 1) = '(' then null
      -- get text before shot_direction
      when shot_direction is not null then trim(split_part(shot_text, shot_direction, 1))
      -- if not shot_direction, get text before ','
      when shot_direction is null then trim(split_part(shot_text, ',', 1))
      else null      
    end as shot_type

  from shot_attributes
)

final as (
    select 
        hk_point,
        shot_number,
        shot_text,
        shot_number_in_point,
        bk_point,
        bk_shot,
        shot_direction,
        shot_result,
        shot_type,
       
        current_timestamp as bus_dv_load_datetime,
        'bus_dv__int__shot' as bus_dv_source_model
    from shot_type
)

select * from final