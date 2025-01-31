
-- Create the DDL for actors_history_scd: Table which will track quality_class and is_Active for each actor
CREATE TABLE actors_history_scd(
    actorid TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INT,
    end_date INT,
    current_year INT,
    PRIMARY KEY (actorid, start_date)
);