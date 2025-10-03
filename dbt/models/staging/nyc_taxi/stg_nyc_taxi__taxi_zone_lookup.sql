SELECT
  -- Handle different column names between platforms
  {% if target.type == 'spark' %}
    locationid AS location_id,  -- Glue has 'locationid'
  {% else %}
    location_id,                -- Snowflake has 'LOCATION_ID'
  {% endif %}
  borough,
  zone,
  service_zone,
  {% if target.type != 'spark' %}
    filename_loaded  -- Only exists in Snowflake/Redshift
  {% else %}
    NULL AS filename_loaded  -- Add as NULL for Spark
  {% endif %}
FROM {{ source('nyc_taxi_db', 'taxi_zone_lookup') }}
