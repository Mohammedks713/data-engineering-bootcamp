-- SQL Script for Analyzing Sessionized Event Data in PostgreSQL

-- Query 1: Average number of events per session for a specific host
-- This query calculates the average number of events per session for a given host (e.g., 'techcreator.io').
-- Replace 'techcreator.io' with the desired host as needed.
SELECT
    host,
    AVG(num_events) AS avg_events_per_session
FROM
    processed_events_aggregated
WHERE
    host LIKE '%techcreator.io%'
GROUP BY
    host;

-- Query 2: Comparison of average events per session across all hosts
-- This query provides the average number of events per session for each host in the table.
SELECT
    host,
    AVG(num_events) AS avg_events_per_host
FROM
    processed_events_aggregated
GROUP BY
    host;

-- Additional Query: Total number of sessions per host
-- This query calculates the total number of sessions for each host in the table.
SELECT
    host,
    COUNT(*) AS total_sessions
FROM
    processed_events_aggregated
GROUP BY
    host;

-- Notes:
-- Ensure that the 'processed_events_aggregated' table exists and is populated before running these queries.
-- These queries are designed to be robust and handle multiple hosts dynamically.
-- Adjust the WHERE clause in Query 1 to match specific hosts or criteria if needed.