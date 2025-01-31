WITH game_information AS (
    SELECT
        game_id,
        season,
        home_team_id,
        visitor_team_id,
        home_team_wins,
        pts_home,
        pts_away
        FROM games
    ),
    short_game_details AS (
        SELECT
            game_id,
            player_id,
            team_id,
            pts
        FROM game_details
),
ungrouped_game_details AS (
    SELECT
    s.game_id,
    s.player_id,
    s.team_id,
    s.pts,
    g.season,
    CASE
        WHEN s.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
        WHEN s.team_id = g.home_team_id AND g.home_team_wins = 0 THEN 0
        WHEN s.team_id = g.visitor_team_id AND g.home_team_wins = 1 THEN 0
        WHEN s.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
    END as team_won
    FROM game_information g INNER JOIN short_game_details s
    ON g.game_id = s.game_id
),
    grouped_data AS (
        SELECT
            CASE WHEN
                GROUPING(player_id) = 0
                AND GROUPING(team_id) = 0
                THEN 'grouping__player__team'
            WHEN
                GROUPING(player_id) = 0
                AND GROUPING(season) = 0
                THEN 'grouping__player__season'
            WHEN
                GROUPING(team_id) = 0
                THEN 'grouping__team'
            END AS aggregation_type,
            player_id,
            team_id,
            season,
            SUM(pts) as total_player_points,
            SUM(team_won) as total_team_games_won
        FROM ungrouped_game_details
        GROUP BY
            GROUPING SETS (
            (player_id, team_id),
            (player_id, season),
            (team_id)
            )
    )
--Answer questions like who scored the most points playing for one team?
SELECT
    'Most Points by Player for One Team' AS query_type,
    player_id,
    team_id,
    MAX(total_player_points) AS most_points
FROM grouped_data
WHERE aggregation_type = 'grouping__player__team'
GROUP BY player_id, team_id
ORDER BY most_points DESC
LIMIT 1

UNION ALL
-- Answer questions like who scored the most points in one season?
SELECT
    'Most Points Scored by Player in One Season' AS query_type,
    player_id,
    season,
    MAX(total_player_points) AS most_points
FROM grouped_data
WHERE aggregation_type = 'grouping__player__season'
GROUP BY player_id, season
ORDER BY most_points DESC
LIMIT 1

UNION ALL
-- Answer questions like which team has won the most games?
SELECT
    'Team With Most Wins' AS query_type,
    NULL AS player_id,
    team_id,
    MAX(total_team_games_won) AS most_wins
FROM grouped_data
WHERE aggregation_type = 'grouping__team'
GROUP BY team_id
ORDER BY most_wins DESC
LIMIT 1;
