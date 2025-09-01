with s as (
  select * from {{ ref('stg_nyc_taxi__streaming_trips') }}
),
zones as (
  select
    location_id,
    zone    as pickup_zone,
    borough as pickup_borough
  from {{ ref('stg_nyc_taxi__taxi_zone_lookup') }}
)

select
  s.trip_id,
  s.pickup_at,
  s.dropoff_at,
  s.pickup_location_id,
  s.dropoff_location_id,
  z.pickup_zone,
  z.pickup_borough,
  s.passenger_count,
  s.fare_amount,
  s.payment_type,
  s.event_time,
  s.lambda_received_time,
  s.inserted_at,

  -- aligned with int_nyc__trip_zone
  cast(s.pickup_at as date)                            as pickup_date,
  cast(date_trunc('month', s.pickup_at) as date)       as pickup_month,
  to_char(date_trunc('month', s.pickup_at), 'YYYY-MM') as pickup_month_ym,
  extract(hour from s.pickup_at)                       as pickup_hour,
  datediff('minute', s.pickup_at, s.dropoff_at)        as trip_duration_min,

  -- streaming-specific
  datediff('second', s.lambda_received_time, s.inserted_at)                        as delay_time_seconds,
  round(datediff('second', s.lambda_received_time, s.inserted_at)::numeric/60.0,2) as delay_time_minutes

from s
left join zones z
  on s.pickup_location_id = z.location_id
where s.dropoff_at >= s.pickup_at
  and s.pickup_at::date >= dateadd(day, -{{ var('streaming_days_back', 1) }}, current_date)