

-- populate actors table 1970 is min year 2021 is max year

DO $$
DECLARE
    last_year INT := 1969; -- Start with the minimum year
    this_year INT := 1970; -- Next year
BEGIN
    WHILE last_year < 2021 LOOP
        -- Insert data for the current year
        INSERT INTO actors (actorid, actor, current_year, films, quality_class, is_active)
        -- get data for last year if feasible
        WITH last_season AS (
            SELECT * FROM actors
            WHERE current_year = last_year
        ),
            this_season AS (
            SELECT
            actorid,
            actor,
            year,
            ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
            AVG(rating) AS avg_rating
            FROM actor_films
            WHERE year = this_year
            GROUP BY 1,2,3
        )
        SELECT
            COALESCE(ts.actorid, ls.actorid) AS actorid,
            COALESCE(ts.actor, ls.actor) AS actor,
            this_year AS current_year,
            CASE WHEN ls.films IS NULL THEN ts.films
                ELSE ls.films || ts.films
            END AS films,
            CASE
                WHEN ts.avg_rating IS NULL THEN ls.quality_class
                WHEN ts.avg_rating > 8 THEN 'star'
                WHEN ts.avg_rating > 7 THEN 'good'
                WHEN ts.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            CASE
                WHEN ts.year IS NOT NULL THEN TRUE ELSE FALSE
            END AS is_active
        FROM last_season ls
        FULL OUTER JOIN this_season ts
        ON ls.actorid = ts.actorid
        WHERE ts.year = this_year OR ls.current_year = last_year;

        -- Move to the next year
        last_year := last_year + 1;
        this_year := this_year + 1;
    END LOOP;
END $$;

