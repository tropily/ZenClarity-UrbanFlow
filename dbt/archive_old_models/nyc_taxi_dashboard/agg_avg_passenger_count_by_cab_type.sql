{{ config(materialized='view') }}


select
  cab_type,
  avg(passenger_count) as avg_passenger_count
from {{ ref('int_nyc__trip_zone') }}
where passenger_count is not null
group by 1
