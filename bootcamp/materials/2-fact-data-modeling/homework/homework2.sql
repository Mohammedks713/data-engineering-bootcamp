
-- Question 1: Deduplicate game_details from Day so there are no duplicates

-- CREATE THE deduped fact table for game_details DDL
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

-- dedupe game_details and insert data into fct_game_details
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
    CAST(SPLIT_PART(min, ':', 1 ) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
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


-- Question 2: DDL for user_devices_cumulated table

CREATE TABLE user_devices_cumulated (
    user_id INTEGER,
    -- dates in past user was active, by device type
    browser_type TEXT,
    device_activity_datelist DATE[],
    --current date for user
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

-- Question 3: cumulative query to generate device_activity_datelist from events

DO $$
DECLARE
    last_date DATE := DATE('2022-12-31');
    this_date DATE := DATE('2023-01-01');
BEGIN
    WHILE last_date < DATE('2023-02-01') LOOP

        INSERT INTO user_devices_cumulated
        WITH YESTERDAY AS (
            SELECT
                *
            FROM user_devices_cumulated
            WHERE date = last_date
        ),
            today AS (
                SELECT
                    e.user_id,
                    d.browser_type as browser_type,
                    DATE(CAST(e.event_time AS TIMESTAMP)) as date_active
                FROM events e INNER JOIN devices d
                ON e.device_id = d.device_id
                WHERE DATE(CAST(event_time AS TIMESTAMP)) = this_date
                AND d.device_id IS NOT NULL
                AND e.user_id IS NOT NULL
                GROUP BY 1,2,3
                --ORDER BY 1,2,3
            )
        SELECT
            COALESCE(t.user_id, y.user_id) as user_id,
            COALESCE(t.browser_type, y.browser_type) as browser_type,
            CASE WHEN y.device_activity_datelist IS NULL
                        THEN ARRAY[t.date_active]
                        WHEN t.date_active IS NULL THEN y.device_activity_datelist
                        ELSE ARRAY[t.date_active] || y.device_activity_datelist
                        END as device_activity_datelist,
            this_date AS date
        FROM today t
        FULL OUTER JOIN yesterday y
        ON t.user_id = y.user_id
        AND t.browser_type = y.browser_type;

        last_date := last_date + INTERVAL '1 day';
        this_date := this_date + INTERVAL '1 day';
    END LOOP;
END $$;


-- Question 4: A datelist generation query. Convert device_activity_datelist column into a datelist_int column

WITH users AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-02-01')
),
    series AS (
        SELECT *
        FROM generate_series(DATE('2023-01-01'), DATE('2023-02-01'), INTERVAL '1 day')
        as series_date
    ),
    place_holder_ints AS (
            SELECT CASE WHEN
                            device_activity_datelist @> ARRAY[DATE(series_date)]
                        THEN
                            CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
                        ELSE 0
                                 END AS  placeholder_int_value,
                 *
            FROM users
                CROSS JOIN series
    )
SELECT
    user_id,
    browser_type,
    device_activity_datelist,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) as datelist_int,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) as active_count,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_monthly_active,
    BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32))
    & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_weekly_active,
     BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32))
    & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 as dim_is_daily_active
FROM place_holder_ints
GROUP BY 1,2, 3
ORDER BY 1, 2, 3;

-- Question 5: DDL for hosts_cumulated table
SELECT * FROM events;

CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE,
    PRIMARY KEY (host, date)
);

-- Question 6: The incremental query to generate host_activity_datelist

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM hosts_cumulated
    WHERE date = '2023-01-02'
),
    today AS (
        SELECT
            host,
            DATE(CAST(event_time AS TIMESTAMP)) as date_active
        FROM events
        WHERE
            DATE(CAST(event_time AS TIMESTAMP)) = '2023-01-03'
        AND
            host IS NOT NULL
        GROUP BY 1,2
                )
    SELECT
        COALESCE(t.host, y.host) as host,
        CASE WHEN y.host_activity_datelist IS NULL
                        THEN ARRAY[t.date_active]
                        WHEN t.date_active IS NULL THEN y.host_activity_datelist
                        ELSE ARRAY[t.date_active] || y.host_activity_datelist
                        END as host_activity_datelist,
        '2023-01-03' AS date
    FROM today t FULL OUTER JOIN yesterday y
    ON t.host = y.host;


-- Question 7: A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced (
    month TIMESTAMP,
    host TEXT,
    hits INTEGER[],
    unique_visitors INTEGER [],
    PRIMARY KEY (host, month)
);


-- Question 8: An incremental query that loads host_activity_reduced day-by-day

INSERT INTO host_activity_reduced
WITH yesterday_array AS (
    SELECT
        *
    FROM host_activity_reduced
    WHERE month = DATE_TRUNC('month', '2023-01-01'::DATE)
), daily_aggregate AS (
    SELECT
        host,
        DATE(event_time) AS date,
        COUNT(1) as daily_hits,
        COUNT(DISTINCT user_id) as daily_unique_visitors
    FROM events
    WHERE DATE(event_time) = '2023-01-01'
    GROUP BY 1, 2
)
SELECT
    COALESCE(DATE_TRUNC('month', da.date::DATE), ya.month) as month,
    COALESCE(da.host, ya.host) as host,
    CASE
        WHEN ya.hits IS NOT NULL THEN
            ya.hits || ARRAY[COALESCE(da.daily_hits,0)]
        WHEN ya.hits IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (da.date - DATE(DATE_TRUNC('month', da.date)), 0)])
                || ARRAY[COALESCE(da.daily_hits,0)]
    END AS hits,
    CASE
        WHEN ya.unique_visitors IS NOT NULL THEN
            ya.unique_visitors || ARRAY[COALESCE(da.daily_unique_visitors,0)]
        WHEN ya.unique_visitors IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (da.date - DATE(DATE_TRUNC('month', da.date)), 0)])
                || ARRAY[COALESCE(da.daily_unique_visitors,0)]
    END AS unique_visitors
FROM daily_aggregate da
FULL OUTER JOIN
    yesterday_array ya
ON da.host = ya.host;


