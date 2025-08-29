SELECT
  location_id,
  borough,
  zone,
  service_zone,
  filename_loaded
FROM {{ source('nyc_taxi_db', 'taxi_zone_lookup') }}