-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);


SELECT geodata::json->>'country', COUNT(*) FROM processed_events
GROUP BY 1
ORDER BY 2 DESC;

