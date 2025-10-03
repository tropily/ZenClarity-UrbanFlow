
-- Snowflake Query to get cost and performance for specific queries
-- $2 percredit
--

WITH params AS (
  SELECT 2.00::NUMBER(10,4) AS credit_price_usd
),
ids AS (
  SELECT column1::STRING AS query_id FROM VALUES
    ('01bf5b9b-0000-35f8-000d-928b0023509e'),
    ('01bf5b9e-0000-3592-000d-928b002331f2'),
    ('01bf5b9f-0000-3571-000d-928b0022b29e'),
    ('01bf5ba0-0000-3511-000d-928b0022432a'),
    ('01bf5ba1-0000-3592-000d-928b0023320a'),
    ('01bf5c3d-0000-3571-000d-928b0022b392'),
    ('01bf5c55-0000-3511-000d-928b002243fe')
),
q AS (
  SELECT
    h.query_id,
    h.start_time,
    h.end_time,
    h.execution_time                      AS exec_ms,
    h.bytes_scanned,
    h.warehouse_name
  FROM snowflake.account_usage.query_history h
  JOIN ids USING (query_id)
),
a AS (
  SELECT
    query_id,
    credits_attributed_compute            AS credits_compute
  FROM snowflake.account_usage.query_attribution_history
  WHERE query_id IN (SELECT query_id FROM ids)
)
SELECT
  q.query_id,
  q.start_time,
  q.exec_ms,
  q.bytes_scanned,
  a.credits_compute,
  ROUND(a.credits_compute * (SELECT credit_price_usd FROM params), 4) AS cost_usd
FROM q
LEFT JOIN a USING (query_id)
ORDER BY q.start_time;



-- Redshift Serverless daily reconciliation + per-query cost attribution
-- Region: us-east-1   Price: $0.375 per RPU-hour
-- Local day to reconcile: 2025-09-28 (America/Los_Angeles)


-- 0) Params (Pacific day → UTC) + price
-- Redshift Serverless us-east-1 | price = $0.375 per RPU-hour
WITH myq AS (
  SELECT q.query_id, q.start_time, q.end_time
  FROM sys_query_history q
  WHERE q.query_id IN (13406292,13406377,13406396,13406417,13406443,13811973,14015045)
),
-- usage minutes bounded around  queries (pad to catch resume)
minutes AS (
  SELECT
    u.start_time AS minute_start,
    u.end_time   AS minute_end,
    u.charged_seconds::float AS billed_seconds
  FROM sys_serverless_usage u
  WHERE u.end_time   >  (SELECT MIN(start_time) - INTERVAL '2 minutes' FROM myq)
    AND u.start_time <  (SELECT MAX(end_time)   + INTERVAL '1 minute'  FROM myq)
),
-- count ALL queries (everyone) and YOUR queries overlapping each minute
minute_conc AS (
  SELECT
    m.minute_start,
    m.minute_end,
    m.billed_seconds,
    COALESCE(COUNT(DISTINCT aq.query_id), 0) AS running_all,
    COALESCE(COUNT(DISTINCT mq.query_id), 0) AS running_my
  FROM minutes m
  LEFT JOIN sys_query_history aq
    ON aq.start_time < m.minute_end
   AND aq.end_time   > m.minute_start
  LEFT JOIN myq mq
    ON mq.start_time < m.minute_end
   AND mq.end_time   > m.minute_start
  GROUP BY m.minute_start, m.minute_end, m.billed_seconds
),
-- exact attribution where there ARE queries running: split by running_all, but give share only to YOUR overlaps
attrib_exact AS (
  SELECT
    q.query_id,
    SUM(mc.billed_seconds / NULLIF(mc.running_all,0)) AS rpu_seconds_exact
  FROM myq q
  JOIN minute_conc mc
    ON q.start_time < mc.minute_end
   AND q.end_time   > mc.minute_start
  WHERE mc.running_all > 0
  GROUP BY q.query_id
),
-- resume/idle minutes = billed but NOBODY running
resume_minutes AS (
  SELECT minute_start, minute_end, billed_seconds
  FROM minute_conc
  WHERE billed_seconds > 0 AND running_all = 0
),
-- pair each resume minute with ALL of your queries that start after it,
-- keep only the FIRST one via ROW_NUMBER()
resume_match AS (
  SELECT
    rm.minute_start,
    rm.minute_end,
    rm.billed_seconds,
    q.query_id,
    q.start_time,
    ROW_NUMBER() OVER (PARTITION BY rm.minute_start, rm.minute_end
                       ORDER BY q.start_time) AS rn
  FROM resume_minutes rm
  JOIN myq q
    ON q.start_time >= rm.minute_end
),
resume_alloc AS (
  SELECT query_id, SUM(billed_seconds) AS rpu_seconds_resume
  FROM resume_match
  WHERE rn = 1
  GROUP BY query_id
),
final AS (
  SELECT
    q.query_id,
    COALESCE(a.rpu_seconds_exact, 0) + COALESCE(r.rpu_seconds_resume, 0) AS rpu_seconds_attributed
  FROM myq q
  LEFT JOIN attrib_exact a USING (query_id)
  LEFT JOIN resume_alloc r USING (query_id)
)
SELECT
  'redshift_serverless' AS engine,
  q.query_id,
  q.start_time,
  DATEDIFF(milliseconds, q.start_time, q.end_time) AS exec_ms,
  f.rpu_seconds_attributed,
  ROUND((f.rpu_seconds_attributed / 3600.0) * 0.375, 4) AS cost_usd
FROM final f
JOIN myq q USING (query_id)
ORDER BY q.start_time;

