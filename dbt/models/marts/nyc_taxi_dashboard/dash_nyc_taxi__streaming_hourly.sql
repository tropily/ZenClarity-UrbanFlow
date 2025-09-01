{{ config(materialized='view', tags=['mart','nyc_taxi','streaming']) }}

select
  date_trunc('hour', pickup_at) as hour,
  count(*)                      as trips,
  sum(fare_amount)              as fare_total,
  avg(nullif(passenger_count,0)) as avg_passenger_count
from {{ ref('int_nyc__streaming_enriched') }}
group by 1
order by 1
