SELECT * FROM player_seasons;

-- create struct for season_stats
CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);

--
CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

--DROP TABLE players;
CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    scoring_class scoring_class,
    years_since_last_season INTEGER,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY(player_name, current_season)
);


-- insert
DO $$
DECLARE
    current_year INT := 1996; -- Start year for current_season
    next_year INT := 1997;    -- Start year for season
BEGIN
    WHILE current_year <= 2022 LOOP
        -- Execute the query for the current year pair
        WITH last_season AS (
            SELECT * FROM players
            WHERE current_season = current_year
        ), this_season AS (
            SELECT * FROM player_seasons
            WHERE season = next_year
        )
        INSERT INTO players
        SELECT
            COALESCE(ls.player_name, ts.player_name) as player_name,
            COALESCE(ls.height, ts.height) as height,
            COALESCE(ls.college, ts.college) as college,
            COALESCE(ls.country, ts.country) as country,
            COALESCE(ls.draft_year, ts.draft_year) as draft_year,
            COALESCE(ls.draft_round, ts.draft_round) as draft_round,
            COALESCE(ls.draft_number, ts.draft_number) as draft_number,
            COALESCE(ls.season_stats,
                ARRAY[]::season_stats[]
                ) || CASE WHEN ts.season IS NOT NULL THEN
                    ARRAY[ROW(
                    ts.season,
                    ts.gp,
                    ts.pts,
                    ts.reb,
                    ts.ast)::season_stats]
                    ELSE ARRAY[]::season_stats[] END as season_stats,
            CASE
                WHEN ts.season IS NOT NULL THEN
                    (CASE WHEN ts.pts > 20 THEN 'star'
                        WHEN ts.pts > 15 THEN 'good'
                        WHEN ts.pts > 10 THEN 'average'
                        ELSE 'bad' END)::scoring_class
                ELSE ls.scoring_class
            END as scoring_class,
            CASE WHEN ts.season IS NOT NULL THEN 0
                ELSE COALESCE(ls.years_since_last_season, 0) + 1
            END as years_since_last_season,
            next_year as current_season, -- Correct assignment of integer to current_season
            CASE WHEN ts.season IS NOT NULL THEN true ELSE false END as is_active
        FROM last_season ls
        FULL OUTER JOIN this_season ts
        ON ls.player_name = ts.player_name;

        -- Increment the years for the next iteration
        current_year := current_year + 1;
        next_year := next_year + 1;
    END LOOP;
END $$;

--DROP TABLE players_scd;
-- create players scd
CREATE TABLE players_scd(
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY(player_name, start_season)
);

-- look at all
INSERT INTO players_scd
WITH with_previous AS (
    SELECT
    player_name,
    current_season,
    scoring_class,
    is_active,
    LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
    LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
FROM players
WHERE current_season <= 2021
),
    with_indicators AS (
        SELECT *,
        CASE WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
        FROM with_previous
    ),
    with_streaks AS (
        SELECT *,
               SUM(change_indicator)
               OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
        FROM with_indicators
    )
SELECT player_name,
       scoring_class,
       is_active,
       MIN(current_season) as start_season,
       MAX(current_season) as end_season,
       2021 AS current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, streak_identifier;

--SELECT * FROM players_scd;
CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
                        );


-- incremental scd
WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
    historical_scd AS (
        SELECT
            player_name,
            scoring_class,
            is_active,
            start_season,
            end_season
            FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
    ),
    this_season_data AS (
        SELECT * FROM players
        WHERE current_season = 2022
    ),
    unchanged_records AS (
        SELECT ts.player_name,
               ts.scoring_class,
               ts.is_active,
               ls.start_season,
               ts.current_season as end_season

        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
        WHERE ts.scoring_class = ls.scoring_class
        AND ts.is_active = ls.is_active
    ),
    changed_records AS (
        SELECT ts.player_name,
               UNNEST(ARRAY[
                   ROW(
                        ls.scoring_class,
                       ls.is_active,
                       ls.start_season,
                       ls.end_season
                       )::scd_type,
                   ROW(
                       ts.scoring_class,
                       ts.is_active,
                        ts.current_season,
                       ts.current_season
                       )::scd_type]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
        WHERE (ts.scoring_class <> ls.scoring_class
        OR ts.is_active = ls.is_active)
    ),
    unnested_changed_records AS (
        SELECT player_name,
               (records::scd_type).scoring_class,
               (records::scd_type).is_active,
               (records::scd_type).start_season,
               (records::scd_type).end_season
        FROM changed_records
    ),
    new_records AS (
        SELECT
            ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ts.current_season AS start_season,
            ts.current_season AS end_season
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ts.player_name = ls.player_name
        WHERE ls.player_name IS NULL
    )
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;


-- unnest to get 'back' to old schema
WITH unnested AS (
    SELECT player_name, UNNEST(season_stats)::season_stats as season_stats
FROM players
WHERE current_season = 2001
)
SELECT player_name,
       (season_stats::season_stats).*
         FROM unnested;

SELECT player_name,
       (season_stats[CARDINALITY(season_stats)]::season_stats).pts/
       CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END
FROM players
WHERE current_season = 2001
AND scoring_class = 'star'
ORDER BY 2 DESC;

