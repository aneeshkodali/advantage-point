with

matches as (
  select * from {{ ref('int_tennisabstract__matches') }}
),

match_points as (
  select * from {{ ref('stg_tennisabstract__match_points') }}
  where is_record_active = true
),

-- split scores
-- join players
match_points_scores_split as (
    select
      mp.match_url,
      mp.point_number_in_match,
      mp.point_server,
      
      -- get receiver
      case
        when mp.point_server = m.match_player_one then m.match_player_two
        when mp.point_server = m.match_player_two then m.match_player_one
        else null
      end as point_receiver,
      mp.set_score_in_match,
      cast(split_part(mp.set_score_in_match, '-', 1) as int) as set_score_server,
      cast(split_part(mp.set_score_in_match, '-', 2) as int) as set_score_receiver,
      mp.game_score_in_set,
      cast(split_part(mp.game_score_in_set, '-', 1) as int) as game_score_server,
      cast(split_part(mp.game_score_in_set, '-', 2) as int) as game_score_receiver,
      mp.point_score_in_game,
      split_part(mp.point_score_in_game, '-', 1) as point_score_server,
      split_part(mp.point_score_in_game, '-', 2) as point_score_receiver,

      -- nullify point_description if not valid
      case
        -- if no rally occurred/recorded
        when mp.point_description in ('Point penalty.', 'Unknown.') then null
        -- if rally resulted in a 'challenge'
        when mp.point_description ilike '%challenge was incorrect%' then null
        else mp.point_description
    end as point_description
    from match_points as mp
    left join matches as m on mp.match_url = m.match_url
),

-- add scores
match_points_scores_add as (
  select
    *,
    set_score_server + set_score_receiver + 1 as set_number_in_match,
    game_score_server + game_score_receiver + 1 as game_number_in_set
  from match_points_scores_split
),

-- get running counts
match_points_running_numbers as (
  select
    *,
    dense_rank() over (partition by match_url order by set_number_in_match, game_number_in_set) as game_number_in_match,
    row_number() over (partition by match_url, set_number_in_match order by game_number_in_set, point_number_in_match) as point_number_in_set,
    row_number() over (partition by match_url, set_number_in_match, game_number_in_set order by point_number_in_match) as point_number_in_game
  from match_points_scores_add
),

-- get number of shots (separated by ';')
match_points_rally_shot_count as (
  select
    *,
    array_length(
      regexp_split_to_array(point_description, ';'),
      1
    ) as number_of_shots_in_point
  from match_points_running_numbers
),

-- get last shot in rally
match_points_last_shot as (
  select
    *,
    trim(
      split_part(point_description, ';', number_of_shots_in_point)
    ) as last_shot_in_point
  from match_points_rally_shot_count
),

-- get number of elements in last shot (separated by ',')
-- in case string contains multiple ',' like in the case of double faults (, fault, double fault)
match_points_last_shot_count as (
  select
    *,
    array_length(
      regexp_split_to_array(last_shot_in_point, ','), 
      1
    ) as number_of_elements_in_last_shot
  from match_points_last_shot
),

-- get last shot outcome
-- last element in ',' separated string
-- get rid of any extra strings (ex. '.' and '(...)')
match_points_outcome as (
  select
    *,
    trim(
      split_part(
        replace(
          cast(split_part(last_shot_in_point, ',', number_of_elements_in_last_shot) as text),
          '.',
          ''
        ),
        '(',
        1
      )
    )
    as point_outcome
  from match_points_last_shot_count
),

-- get point winner
match_points_winner as (
  select
    *,
    case
      -- if odd length (server hit last shot)
      when number_of_shots_in_point % 2 != 0 then
        case
          when point_outcome in ('ace', 'service winner', 'winner') then point_server
          when point_outcome in ('double fault', 'forced error', 'unforced error') then point_receiver
          else null
        end
      -- if even length (receiver hit last shot)
      when number_of_shots_in_point % 2 = 0 then
        case
          when point_outcome in ('winner') then point_receiver
          when point_outcome in ('forced error', 'unforced error') then point_server
          else null
        end
      else null
    end as point_winner
    
  from match_points_outcome
),

final as (
  select
    match_url,
    point_number_in_match,

    point_server,
    point_receiver,
    point_description,
    number_of_shots_in_point,

    -- calculate rally length
    case
      -- exclude 'errors'
      when point_outcome in ('double fault', 'forced error', 'unforced error') then number_of_shots_in_point - 1
      -- include 'winners'
      when point_outcome in ('ace', 'service winner', 'winner') then number_of_shots_in_point
      else null
    end as rally_length,

    point_outcome,
    point_winner,
    case
      when point_winner = point_server then point_receiver
      when point_winner != point_server then point_server
      else null
    end as point_loser,

    point_score_in_game,
    point_score_server,
    point_score_receiver,
    point_number_in_set,
    point_number_in_game,

    set_score_in_match,
    set_score_server,
    set_score_receiver,
    set_number_in_match,
    
    game_score_in_set,
    game_score_server,
    game_score_receiver,
    game_number_in_match,
    game_number_in_set
    
  from match_points_winner
)

select * from final