-- Step 1: Repair the table to register new partitions from S3
MSCK REPAIR TABLE nyc_taxi_db.emr_trip_data;



-- Step 3: Run a sanity check count query
WITH glue_counts AS (
    SELECT year, month, day, COUNT(*) as glue_count
    FROM nyc_taxi_db.trip_data
    WHERE cab_type = 'yellow' AND year = 2024 AND month = 1
    GROUP BY year, month, day
),
emr_counts AS (
    SELECT year, month, day, COUNT(*) as emr_count
    FROM nyc_taxi_db.emr_trip_data
    WHERE cab_type = 'yellow' AND year = 2024 AND month = 1
    GROUP BY year, month, day
)
SELECT
    COALESCE(g.year, e.year) as year,
    COALESCE(g.month, e.month) as month,
    COALESCE(g.day, e.day) as day,
    g.glue_count,
    e.emr_count,
    CASE WHEN g.glue_count = e.emr_count THEN 'MATCH' ELSE 'MISMATCH' END as status
FROM glue_counts g
FULL OUTER JOIN emr_counts e
    ON g.year = e.year AND g.month = e.month AND g.day = e.day
ORDER BY day;

--Step 2: Descibe tables
DESCRIBE EXTENDED nyc_taxi_db.emr_trip_data;
