Iceberg
-- ============================================================
-- NYC Taxi Benchmarks — Jan 1, 2025
-- Iceberg table:  nyc_taxi_wh.trip_data
-- Legacy table:   teo_nyc_taxi_db.trip_data
-- Zone lookups:   nyc_taxi_wh.taxi_zone_lookup / teo_nyc_taxi_db.taxi_zone_lookup
--
-- Tips:
--  • Run each pair (Iceberg vs Legacy) back-to-back.
--  • In Athena, note Runtime and Data scanned for each.
--  • Run each twice (keep best of two) to reduce cold-start noise.
-- ============================================================


-- ============================================================
-- 1) Row count (pure prune test)
-- ============================================================

-- Iceberg
SELECT COUNT(*) AS rows
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02';

--Time in queue:
103 ms
Run time:
983 ms
Data scanned:

--with OPTIMIZE
Time in queue:
117 ms
Run time:
1.641 sec
Data scanned:


-- Legacy
SELECT COUNT(*) AS rows
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv');

Time in queue:
102 ms
Run time:
1.364 sec
Data scanned:
-
-- ============================================================
-- 2) Trips by cab_type
-- ============================================================

-- Iceberg
SELECT cab_type, COUNT(*) AS trips
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02'
GROUP BY cab_type
ORDER BY trips DESC;

Time in queue:
115 ms
Run time:
2.897 sec
Data scanned:
0.33 KB

-- with OPTIMIZE
Time in queue:
103 ms
Run time:
1.096 sec
Data scanned:
0.13 KB

-- Legacy
SELECT cab_type, COUNT(*) AS trips
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv')
GROUP BY cab_type
ORDER BY trips DESC;

Time in queue:
92 ms
Run time:
1.446 sec
Data scanned:
-
-- ============================================================
-- 3) Hourly volume (date_trunc)
-- ============================================================

-- Iceberg
SELECT date_trunc('hour', pickup_datetime) AS hour, COUNT(*) AS trips
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= TIMESTAMP '2025-01-01 00:00:00'
  AND pickup_datetime <  TIMESTAMP '2025-01-02 00:00:00'
GROUP BY 1
ORDER BY 1;

Time in queue:
141 ms
Run time:
2.985 sec
Data scanned:
723.70 KB

--with optimize
Time in queue:
118 ms
Run time:
1.196 sec
Data scanned:
628.75 KB


-- Legacy
SELECT date_trunc('hour', pickup_datetime) AS hour, COUNT(*) AS trips
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv')
GROUP BY 1
ORDER BY 1;

Time in queue:
114 ms
Run time:
1.204 sec
Data scanned:
748.07 KB


-- ============================================================
-- 5) Fare stats (avg + percentiles)
-- ============================================================

-- Iceberg
SELECT
  AVG(fare_amount) AS avg_fare,
  approx_percentile(fare_amount, 0.5) AS p50,
  approx_percentile(fare_amount, 0.9) AS p90
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02';

  Time in queue:
106 ms
Run time:
1.198 sec
Data scanned:
149.56 KB

--with optimized
Time in queue:
106 ms
Run time:
1.884 sec
Data scanned:
170.64 KB
-- Legacy
SELECT
  AVG(fare_amount) AS avg_fare,
  approx_percentile(fare_amount, 0.5) AS p50,
  approx_percentile(fare_amount, 0.9) AS p90
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv');

Time in queue:
111 ms
Run time:
1.233 sec
Data scanned:
255.81 KB
-- ============================================================
-- 6) Payment type breakdown
-- ============================================================

-- Iceberg
SELECT payment_type, COUNT(*) AS trips
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02'
GROUP BY payment_type
ORDER BY trips DESC;

Time in queue:
87 ms
Run time:
1.52 sec
Data scanned:
18.87 KB

--with optimized
Time in queue:
101 ms
Run time:
1.099 sec
Data scanned:
20.45 KB


-- Legacy
SELECT payment_type, COUNT(*) AS trips
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv')
GROUP BY payment_type
ORDER BY trips DESC;

Time in queue:
166 ms
Run time:
1.042 sec
Data scanned:
78.10 KB
-- ============================================================
-- 7) Filtered slice on non-partition column (file skipping)
-- ============================================================

-- Iceberg
SELECT COUNT(*) AS trips
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02'
  AND pulocationid IN (132, 138, 237);

Time in queue:
108 ms
Run time:
2.219 sec
Data scanned:
100.80 KB

--with optimized

Time in queue:
98 ms
Run time:
1.051 sec
Data scanned:
97.76 KB

-- Legacy
SELECT COUNT(*) AS trips
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv')
  AND pulocationid IN (132, 138, 237);

Time in queue:
102 ms
Run time:
1.141 sec
Data scanned:
189.27 KB

-- ============================================================
-- 8) Data quality ratios
-- ============================================================

-- Iceberg
SELECT
  SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS zero_dist_ratio,
  SUM(CASE WHEN fare_amount   < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS neg_fare_ratio
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02';

Time in queue:
126 ms
Run time:
1.682 sec
Data scanned:
293.35 KB

--with optimized
Time in queue:
108 ms
Run time:
1.315 sec
Data scanned:
314.97 KB

-- Legacy
SELECT
  SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS zero_dist_ratio,
  SUM(CASE WHEN fare_amount   < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS neg_fare_ratio
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv');

Time in queue:
103 ms
Run time:
1.276 sec
Data scanned:
564.29 KB

-- ============================================================
-- 9) Heavier aggregation (stress)
-- ============================================================

-- Iceberg
SELECT cab_type,
       AVG(trip_distance) AS avg_dist,
       AVG(total_amount)  AS avg_total
FROM nyc_taxi_wh.trip_data
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime <  DATE '2025-01-02'
GROUP BY cab_type
HAVING COUNT(*) > 1000
ORDER BY avg_total DESC;

Time in queue:
99 ms
Run time:
2.888 sec
Data scanned:
332.40 KB

--with optimized

Time in queue:
78 ms
Run time:
1.164 sec
Data scanned:
345.31 KB

-- Legacy
SELECT cab_type,
       AVG(trip_distance) AS avg_dist,
       AVG(total_amount)  AS avg_total
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2025 AND month = 1 AND day = 1
  AND cab_type IN ('yellow','green','fhv')
GROUP BY cab_type
HAVING COUNT(*) > 1000
ORDER BY avg_total DESC;

Time in queue:
119 ms
Run time:
1.191 sec
Data scanned:
641.60 KB


-- ============================================================
-- (Optional) After several 2025 loads, compact Iceberg small files
-- Then re-run tests #1–#3 and #7 to see improvements.
-- ============================================================
-- OPTIMIZE nyc_taxi_wh.trip_data REWRITE DATA USING BIN_PACK;

OPTIMIZE nyc_taxi_wh.trip_data
REWRITE DATA
USING BIN_PACK;

