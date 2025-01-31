
--films
CREATE TYPE films AS (
    film TEXT,
    votes INT,
    rating REAL,
    filmid TEXT);

-- quality class
CREATE TYPE quality_class as ENUM ('star', 'good', 'average', 'bad');

-- create actors table
CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    current_year INT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
PRIMARY KEY (actorid, current_year)
);
