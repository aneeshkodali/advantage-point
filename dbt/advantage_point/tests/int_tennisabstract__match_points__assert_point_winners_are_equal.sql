/*
Compares the different 'point winner' columns and returns records where columns don't match
*/

with

match_points as (
    select
        match_url,
        point_number_in_match,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_server,
        point_receiver,
        point_description,
        number_of_shots_in_point,
        rally_length,
        point_result,
        coalesce(point_winner_next_point, 'null') as point_winner_next_point,
        coalesce(point_winner_rally, 'null') as point_winner_rally
    from {{ ref('int_tennisabstract__match_points') }}
    where 1=1
        and point_winner_rally is not null -- filter out results where point winner based on rally not found
        and point_winner_next_point is not null -- filter out results where next point not found (incomplete data)
)

select
    *
from match_points
where 1=1
    and point_winner_next_point != point_winner_rally