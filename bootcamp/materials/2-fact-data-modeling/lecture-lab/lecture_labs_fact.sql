

-- run queries

-- dedupe a table with some duplicates
INSERT INTO fct_game_details
WITH deduped AS (
    SELECT g.game_date_est,
           g.season,
           g.home_team_id,
           gd.*,
           ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id
        ORDER BY g.game_date_est) as row_num
    FROM game_details gd
    JOIN games g on gd.game_id = g.game_id
)
SELECT
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
    -- comment,
    CAST(SPLIT_PART(min, ':', 1 ) AS REAL) +
    CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS m_minutes,
    --min,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as mg3m,
    fg3a as m_fg3a,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_greb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
FROM deduped
WHERE row_num = 1;

-- write ddl fact_game_details
--DROP table fct_game_details;

CREATE TABLE fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOlEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOlEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_mgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_greb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);

SELECT * FROM fct_game_details;

SELECT dim_player_name,
       COUNT(1) AS num_games,
       COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
       CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL)/COUNT(1) As bailed_percentage
FROM fct_game_details
GROUP BY 1
ORDER BY 4 DESC;


-- start lab2

SELECT *
FROM events;


-- create accumulate users table
--DROP TABLE users_cumulated
CREATE TABLE users_cumulated (
    user_id TEXT,
    -- list of dates in the past, the user was active
    dates_active DATE[],
    -- current date for the user
    date DATE,
    PRIMARY KEY (user_id, date)
);

-- fill the table
DO $$
DECLARE
    last_date DATE := DATE('2022-12-31');
    this_date DATE := DATE('2023-01-01');
BEGIN
    WHILE last_date < DATE('2023-02-01') LOOP

        INSERT INTO users_cumulated
        WITH yesterday AS (
            SELECT
                *
            FROM users_cumulated
            WHERE date = last_date
        ),
            today AS (
                SELECT
                    CAST(user_id AS TEXT) AS user_id,
                    DATE(CAST(event_time AS TIMESTAMP)) AS date_active
                FROM events
                WHERE
                    DATE(CAST(event_time AS TIMESTAMP)) = this_date
                AND user_id IS NOT NULL
                GROUP BY 1, 2
            )
        SELECT
            COALESCE(t.user_id, y.user_id) as user_id,
            CASE WHEN y.dates_active IS NULL
                THEN ARRAY[t.date_active]
                WHEN t.date_active IS NULL THEN y.dates_active
                ELSE ARRAY[t.date_active] || y.dates_active
                END as dates_active,
            COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date

        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.user_id = y.user_id;

        last_date := last_date + INTERVAL '1 day';
        this_date := this_date + INTERVAL '1 day';

    END LOOP;
END $$;


SELECT * FROM users_cumulated
ORDER BY user_id
WHERE date = '2023-01-31';


-- generate date-list for 30 days
SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day');


WITH users AS (
    SELECT * FROM users_cumulated
    WHERE date = DATE('2023-01-31')
),
    series AS (
        SELECT *
        FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')
        as series_date
    ),
    place_holder_ints AS (
            SELECT CASE WHEN
                            dates_active @> ARRAY[DATE(series_date)]
                        THEN
                            CAST(POW(2, 32 - (date - DATE(series_Date))) AS BIGINT)
                        ELSE 0
                                 END AS  placeholder_int_value,
                 *
            FROM users
                CROSS JOIN series
    )
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) as active_count,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_monthly_active,
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32))
    & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active,
     BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32))
    & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_daily_active
FROM place_holder_ints
GROUP BY 1;
;

-- final LAB

CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
);

INSERT INTO array_metrics
WITH daily_aggregate AS (
    -- Aggregate daily site hits per user
    SELECT
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-02')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM array_metrics
    WHERE month_start = DATE('2023-01-01')
)
SELECT
    -- Select user_id from either daily_aggregate or yesterday_array
    COALESCE( da.user_id, ya.user_id) AS user_id,
    -- Determine month_start date
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    -- Set metric name to 'site_hits'
    'site_hits' AS metric_name,
    -- Update metric_array based on existing data and new daily aggregates
    CASE
        WHEN ya.metric_array IS NOT NULL THEN
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
        WHEN ya.metric_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)])
                || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO
    UPDATE SET metric_array = EXCLUDED.metric_array;


-- daily aggregate
WITH agg AS (
    SELECT metric_name,
           month_start,
           ARRAY[SUM(metric_array[1]),
               SUM(metric_array[2])] as summed_array
    FROM array_metrics
    GROUP BY 1,2
)
SELECT
    metric_name,
    month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL),
    elem as value
FROM agg
CROSS JOIN UNNEST(agg.summed_array)
WITH ORDINALITY AS a(elem, index);