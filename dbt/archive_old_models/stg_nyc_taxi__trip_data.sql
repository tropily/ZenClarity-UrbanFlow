{{ config(
    enabled = target.name != 'snowflake_iceberg'
) }}


SELECT
  vendorid AS vendor_id,
  pickup_datetime AS pickup_at,
  dropoff_datetime AS dropoff_at,
  store_and_fwd_flag,
  ratecodeid,
  CAST(pulocationid AS BIGINT) AS pickup_location_id,
  CAST(dolocationid AS BIGINT) AS dropoff_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge,
  airport_fee,
  cab_type,

  -- Date parts: Use existing columns or calculate them
  {% if target.type == 'spark' %}
    -- Glue already has year, month, day columns
    year AS pickup_year,
    month AS pickup_month,
    day AS pickup_day,
  {% else %}
    -- Snowflake: calculate from pickup_datetime
    YEAR(pickup_datetime) AS pickup_year,
    MONTH(pickup_datetime) AS pickup_month,
    DAY(pickup_datetime) AS pickup_day,
  {% endif %}

  -- Handle platform-specific columns
  {% if target.type == 'spark' %}
    NULL AS filename_loaded,      -- Not in Glue table
    NULL AS load_timestamp        -- Not in Glue table
  {% else %}
    filename_loaded,              -- Exists in Snowflake
    load_timestamp               -- Exists in Snowflake
  {% endif %}

FROM {{ source('nyc_taxi_db', 'trip_data') }}
