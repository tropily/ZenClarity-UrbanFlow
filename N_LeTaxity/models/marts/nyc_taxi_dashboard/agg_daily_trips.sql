SELECT 
    DATE(t.pickup_at) AS pickup_date,
    HOUR(t.pickup_at) AS pickup_hour,
    z.zone AS pickup_zone,
    COUNT(*) AS total_trips
FROM {{ ref('stg_nyc_taxi__trip_data') }} AS t
LEFT JOIN {{ ref('stg_nyc_taxi__taxi_zone_lookup') }} AS z
    ON t.pickup_location_id = z.location_id
GROUP BY 1, 2, 3
