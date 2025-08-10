-- switch to 'table' later if needed
{{ config(materialized='view') }}


with trips as (
  select
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    cab_type
  from {{ ref('stg_nyc_taxi__trip_data') }}
),
zones as (
  select
    location_id,
    zone       as pickup_zone,
    borough    as pickup_borough
  from {{ ref('stg_nyc_taxi__taxi_zone_lookup') }}
)

select
  t.*,
  z.pickup_zone,
  z.pickup_borough,
  date(t.pickup_at)                            as pickup_date,
  to_char(date_trunc('month', t.pickup_at), 'YYYY-MM') as pickup_month_ym,
  extract(hour from t.pickup_at)               as pickup_hour,
  datediff('minute', t.pickup_at, t.dropoff_at) as trip_duration_min
from trips t
left join zones z
  on t.pickup_location_id = z.location_id
where t.dropoff_at >= t.pickup_at  -- basic data sanity 
--and t.passenger_count > 0