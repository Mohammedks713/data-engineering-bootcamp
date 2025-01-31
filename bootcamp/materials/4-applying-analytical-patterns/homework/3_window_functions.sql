
-- most games a team has won in a 90 game stretch:
WITH games_trunc AS (
    SELECT
        game_id,
        game_date_est,
        home_team_id,
        visitor_team_id,
        home_team_wins
    FROM games
), game_details_trunc AS (
    SELECT
        game_id,
        team_id
    FROM game_details
    ), unaggregated_joined_table AS (
        SELECT
            t.*,
            game_date_est,
            CASE
                WHEN t.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
                WHEN t.team_id = g.home_team_id AND g.home_team_wins = 0 THEN 0
                WHEN t.team_id = g.visitor_team_id AND g.home_team_wins = 1 THEN 0
                WHEN t.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
            END as team_won
        FROM games_trunc g INNER JOIN game_details_trunc t
        ON g.game_id = t.game_id
), game_sequence AS (
    SELECT
        team_id,
        game_date_est,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date_est) AS game_number,
        team_won as win_flag
    FROM unaggregated_joined_table
), window_aggregates AS (
    SELECT
        team_id,
        game_number,
        SUM(win_flag) OVER (
            PARTITION BY team_id
            ORDER BY game_number
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games
    FROM game_sequence
)
SELECT
    team_id,
    MAX(wins_in_90_games) AS max_wins_in_90_days
FROM window_aggregates
GROUP BY 1
ORDER BY max_wins_in_90_days DESC;



--How many games in a row did LeBron James score over 10 points a game?
WITH games_trunc AS (
    SELECT
        game_id,
        game_date_est
    FROM games
), game_details_trunc AS (
    SELECT
        game_id,
        pts
    FROM game_details
    WHERE player_name = 'LeBron James'
    ), unaggregated_joined_table AS (
        SELECT
            t.*,
            game_date_est
        FROM games_trunc g INNER JOIN game_details_trunc t
        ON g.game_id = t.game_id
), streaks AS (
    SELECT
        game_date_est,
        pts,
        CASE
            WHEN pts > 10 THEN 1
            ELSE 0
        END AS scored_over_10,
        ROW_NUMBER() OVER (ORDER BY game_date_est) -
        ROW_NUMBER() OVER (PARTITION BY CASE WHEN pts > 10 THEN 1 ELSE 0 END ORDER BY game_date_est)
            AS streak_group
    FROM unaggregated_joined_table
)
SELECT
    COUNT(*) AS games_in_streak,
    MIN(game_date_est) AS streak_start,
    MAX(game_date_est) AS streak_end
FROM streaks
WHERE scored_over_10 = 1
GROUP BY streak_group
ORDER BY games_in_streak DESC
LIMIT 1;
