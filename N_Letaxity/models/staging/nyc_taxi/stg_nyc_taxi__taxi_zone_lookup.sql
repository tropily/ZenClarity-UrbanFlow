SELECT
  location_id,
  borough,
  zone,
  service_zone,
  filename_loaded
FROM {{ source('nyc_taxi_batch', 'taxi_zone_lookup') }}
