select
  trip_id,
  pickup_datetime  as pickup_at,
  dropoff_datetime as dropoff_at,
  pulocationid     as pickup_location_id,
  dolocationid     as dropoff_location_id,
  passenger_count,
  fare_amount,
  payment_type,
  event_time,
  lambda_received_time,
  inserted_at
from {{ source('nyc_taxi_stream', 'taxi_streaming_trips') }}

