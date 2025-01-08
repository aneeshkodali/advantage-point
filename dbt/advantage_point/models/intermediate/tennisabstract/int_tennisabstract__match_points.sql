with

matches as (
  select * from {{ ref('int_tennisabstract__matches') }}
),

match_points as (
  select * from {{ ref('stg_tennisabstract__match_points') }}
  where is_record_active = true
),

valid_match_point_descriptions as (
  select * from {{ ref('stg_seed__tennisabstract_valid_match_point_descriptions') }}
),

-- split scores
-- join players
-- join valid point descriptions
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

      -- join in valid point descriptions
      coalesce(valid_mp_point_desc.point_description_new, mp.point_description) as point_description
      
    from match_points as mp
    left join matches as m on mp.match_url = m.match_url
    left join valid_match_point_descriptions as valid_mp_point_desc on
          mp.match_url = valid_mp_point_desc.match_url
      and mp.point_number_in_match = valid_mp_point_desc.point_number_in_match
      and mp.point_server = valid_mp_point_desc.point_server
      and mp.set_score_in_match = valid_mp_point_desc.set_score_in_match
      and mp.game_score_in_set = valid_mp_point_desc.game_score_in_set
      and mp.point_score_in_game = valid_mp_point_desc.point_score_in_game
      and mp.point_description = valid_mp_point_desc.point_description_old
),

-- nullify point descriptions
match_points_point_descriptions as (
  select
    *,

    -- nullify point_description if not valid
      case
        -- if no rally occurred/recorded
        when point_description in ('Point penalty.', 'Unknown.') then null
        -- if rally resulted in a 'challenge'
        when point_description ilike '%challenge was incorrect%' then null
        -- if rally does not contain an 'outcome' string
        when not (point_description ilike any (array['%ace%', '%double fault%', '%forced error%', '%unforced error%', '%service winner%', '%winner%'])) then null
        else point_description
    end as point_description_new

  from match_points_scores_split
),

-- add scores
match_points_scores_add as (
  select
    *,
    set_score_server + set_score_receiver + 1 as set_number_in_match,
    game_score_server + game_score_receiver + 1 as game_number_in_set
  from match_points_point_descriptions
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
      regexp_split_to_array(point_description_new, ';'),
      1
    ) as number_of_shots_in_point
  from match_points_running_numbers
),

-- get last shot in rally
match_points_last_shot as (
  select
    *,
    trim(
      split_part(point_description_new, ';', number_of_shots_in_point)
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
-- get rid of any extra strings:
    -- replace '.'
    -- split string by '(' and get anything before '(...)'
    -- if resulting string is '', make null
match_points_outcome as (
  select
    *,
    nullif(
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
        ),
        ''
    )
    as point_result
  from match_points_last_shot_count
),

-- get rally length
match_points_rally_length as (
  select
    *,
    -- calculate rally length
    case
      -- exclude 'errors'
      when point_result in ('double fault', 'forced error', 'unforced error') then number_of_shots_in_point - 1
      -- include 'winners'
      when point_result in ('ace', 'service winner', 'winner') then number_of_shots_in_point
      else null
    end as rally_length
  from match_points_outcome
),

-- get point winner from rally
match_points_winner_rally as (
  select
    *,
    case
      -- if odd length (server hit last shot)
      when number_of_shots_in_point % 2 != 0 then
        case
          when point_result in ('ace', 'service winner', 'winner') then point_server
          when point_result in ('double fault', 'forced error', 'unforced error') then point_receiver
          else null
        end
      -- if even length (receiver hit last shot)
      when number_of_shots_in_point % 2 = 0 then
        case
          when point_result in ('winner') then point_receiver
          when point_result in ('forced error', 'unforced error') then point_server
          else null
        end
      else null
    end as point_winner_rally
    
  from match_points_rally_length
),

-- get point loser from rally
match_points_loser_rally as (
  select
    *,
    case
      when point_winner_rally = point_server then point_receiver
      when point_winner_rally != point_server then point_server
      else null
    end as point_loser_rally
  from match_points_winner_rally
),

-------------------------------------------------------
-- this next section will get the point winner and loser based on the score in the next point

-- get the NEXT point record
-- convert point scores to int
match_points_lead as (
  select
    mp.*,

    -- convert 'AD' to numeric
    cast(replace(mp.point_score_server, 'AD', '41') as int) as point_score_server_int,
    cast(replace(mp.point_score_receiver, 'AD', '41') as int) as point_score_receiver_int,

    mp_next.point_number_in_match as next_point_point_number_in_match,
    mp_next.point_server as next_point_point_server,
    mp_next.point_receiver as next_point_point_receiver,
    -- convert 'AD' to numeric
    cast(replace(mp_next.point_score_server, 'AD', '41') as int) as next_point_point_score_server_int,
    cast(replace(mp_next.point_score_receiver, 'AD', '41') as int) as next_point_point_score_receiver_int,
    
    mp_next.game_number_in_match as next_point_game_number_in_match,
    mp_next.game_score_server as next_point_game_score_server,
    mp_next.game_score_receiver as next_point_game_score_receiver,
    
    mp_next.set_number_in_match as next_point_set_number_in_match,
    mp_next.set_score_server as next_point_set_score_server,
    mp_next.set_score_receiver as next_point_set_score_receiver

  from match_points_loser_rally as mp
  left join match_points_loser_rally as mp_next on
      mp.match_url = mp_next.match_url
      and mp.point_number_in_match + 1 = mp_next.point_number_in_match
),

-- determine point winner based on next point
match_points_winner_next_point as (
  select
    *,
    
    case
      -- -- check if point is last point in match
      -- when point_number_in_match = max(point_number_in_match) over (partition by match_url) then
      --   -- compare server score and receiver score
      --   case
      --     -- when server score higher --> server
      --     when point_score_server_int > point_score_receiver_int then point_server
      --     -- when receiver score higher --> receiver
      --     when point_score_server_int < point_score_receiver_int then point_receiver
      --     else null
      --   end

      -- compare current point game to next point game
      -- next point is in same game as current point --> compare 'point level' data
      when game_number_in_match = next_point_game_number_in_match then
        -- compare current server to next point server
        case
          -- next point server is same as current point server
          when point_server = next_point_point_server then
            -- compare current point server score to next point server score
            case
              -- next server point score is higher --> server won point
              when point_score_server_int < next_point_point_score_server_int then point_server
              -- next server point score is equal --> compare receiver point score in current point and next point
              when point_score_server_int = next_point_point_score_server_int then
                -- compare current point receiver score to next point receiver score
                case
                  -- next receiver point score is higher --> receiver won point
                  when point_score_receiver_int < next_point_point_score_receiver_int then point_receiver
                  -- current receiver point score is higher --> server won point
                  -- could be case when receiver score goes from 'AD' to '40'
                  when point_score_receiver_int > next_point_point_score_receiver_int then point_server
                  else null
                end
              -- next server point is lower --> receiver won point
              -- could be case when server score goes from 'AD' to '40'
              when point_score_server_int > next_point_point_score_server_int then point_receiver
              else null -- may be unnecessary since WHEN statements cover all scenarios
            end
          -- next point server is NOT same as current point server
          -- may be case in a tiebreaker
          when point_server != next_point_point_server then
            -- server becomes next point receiver --> compare current point server score and next point receiver score
            case
              -- next receiver point score is higher --> server won point
              when point_score_server_int < next_point_point_score_receiver_int then point_server
              -- next receiver point score is same --> server lost point and score remaind unchanged
              when point_score_server_int = next_point_point_score_receiver_int then point_receiver
              else null
            end
          else null -- may be unnecessary since WHEN statements cover all scenarios
        end 
      -- next point is in next game --> compare 'game' and maybe 'set' level data
      when game_number_in_match < next_point_game_number_in_match then
        -- compare current game server to next game server
        case
          -- current game server is next game server - scenario shouldn't exist due to 'alternating server' format in tennis
          -- MAYBE there's a tournament in which a server could server subsequent games, perhaps a service game followed by a tiebreak?
          when point_server = next_point_point_server then
            -- compare server game score to next server game score
            case
              -- next server game score is higher --> server won game
              when game_score_server < next_point_game_score_server then point_server
              -- server game score remains unchanged --> receiver won game
              when game_score_server = next_point_game_score_server then point_receiver
              -- server game score is greater then next game score
              -- could be case if start of a new set
              when game_score_server > next_point_game_score_server then
                -- compare current server set score and next server set score
                case
                  -- next server set score is higher --> server won
                  when set_score_server < next_point_set_score_server then point_server
                  -- server set score remains unchanged --> receiver won game
                  when set_score_server = next_point_set_score_server then point_receiver
                  else null
                end
              else null -- may be unnecessary since WHEN statements cover all scenarios
            end
          -- current game server is NOT next game server - should be the case due to 'alternating server' format
          -- server becomes next point receiver
          when point_server != next_point_point_server then
            -- compare current server game score to next game receiver score
            case
              -- next receiver game score is higher --> server won game
              when game_score_server < next_point_game_score_receiver then point_server
              -- server game score remains unchanged --> receiver won game
              when game_score_server = next_point_game_score_receiver then point_receiver
              -- server game score is greater then next game score
              -- could be case if start of a new set
              when game_score_server > next_point_game_score_receiver then
                -- compare set scores of current server and next point receiver
                case
                  -- next point server set score higher --> server won
                  when set_score_server < next_point_set_score_receiver then point_server
                  -- server set score remains unchanged --> receiver won game
                  when set_score_server = next_point_set_score_receiver then point_receiver
                  else null
                end
              else null -- may be unnecessary since WHEN statements cover all scenarios
            end
          else null -- may be unnecessary since WHEN statements cover all scenarios
        end
      else null
    end
    as point_winner_next_point

  from match_points_lead
),

-- determine point loser based on next point
match_points_loser_next_point as (
  select
    *,
    case
      when point_winner_next_point = point_server then point_receiver
      when point_winner_next_point != point_server then point_server
      else null
    end as point_loser_next_point
  from match_points_winner_next_point
),

-- end of section
-------------------------------------------------------

-- get side (of court)
match_points_side as (
  select
    *,

    case
      when point_score_server_int + point_score_receiver_int % 2 = 0 then 'deuce'
      when point_score_server_int + point_score_receiver_int % 2 != 0 then 'ad'
      else null
    end as point_side

  from match_points_loser_next_point
),

final as (
  select
    match_url,
    point_number_in_match,

    point_side,
    point_server,
    point_receiver,
    point_description,
    
    number_of_shots_in_point,
    rally_length,

    point_result,
    point_winner_next_point,
    point_loser_next_point,
    point_winner_rally,
    point_loser_rally,
    coalesce(point_winner_next_point, point_winner_rally) as point_winner,
    coalesce(point_loser_next_point, point_loser_rally) as point_loser,

    point_score_in_game,
    point_score_server,
    point_score_receiver,
    point_number_in_set,
    point_number_in_game,

    game_score_in_set,
    game_score_server,
    game_score_receiver,
    game_number_in_match,
    game_number_in_set,

    set_score_in_match,
    set_score_server,
    set_score_receiver,
    set_number_in_match
    
  from match_points_side
)

select * from final