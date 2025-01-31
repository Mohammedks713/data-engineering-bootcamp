WITH last_season AS (
    SELECT * FROM player_state_change
    WHERE year = '1997'
),
    this_season AS (
        SELECT
            player_name,
            draft_year,
            college,
            current_season as year,
            is_active
        FROM players
        WHERE current_season = '1998'
    )
SELECT
    COALESCE(t.player_name, l.player_name) as player_name,
    COALESCE(t.draft_year, l.draft_year) as draft_year,
    COALESCE(t.college, l.college) as college,
    CASE
        WHEN l.state IS NULL THEN 'New'
        WHEN NOT t.is_active AND l.state IN ('New', 'Continued Playing', 'Returned from Retirement') THEN 'Retired'
        WHEN t.is_active AND l.state IN ('New', 'Continued Playing', 'Returned from Retirement') THEN 'Continued Playing'
        WHEN t.is_active AND l.state IN ('Retired') THEN 'Returned from Retirement'
        ELSE 'Stayed Retired'
    END as state,
    COALESCE(t.year, l.year + 1) as year
FROM this_season t FULL OUTER JOIN last_season l
ON t.player_name = l.player_name
AND t.draft_year = l.draft_year
AND t.college = l.college
