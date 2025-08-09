-- models/marts/avg_passenger_count_by_cab_type.sql

SELECT
    cab_type,
    ROUND(AVG(passenger_count), 2) AS average_passenger_count
FROM {{ ref('stg_nyc_taxi__trip_data') }}
GROUP BY cab_type
ORDER BY cab_type