-- fill actors_history_scd for all years; maintain status for each year so both historical and present

INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT
    actorid,
    actor,
    current_year,
    quality_class,
    is_active,
    LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) as previous_quality_class,
    LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) as previous_is_active
FROM actors
),
    with_indicators AS (
        SELECT *,
        CASE WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
        FROM with_previous
    ),
    with_streaks AS (
        SELECT *,
               SUM(change_indicator)
               OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
        FROM with_indicators
    )
SELECT
       actorid,
        actor,
       quality_class,
       is_active,
       MIN(current_year) as start_date,
       MAX(current_year) as end_date,
       2021 as current_year
FROM with_streaks
GROUP BY actorid,actor, streak_identifier, is_active, quality_class
ORDER BY actorid, actor, streak_identifier;
