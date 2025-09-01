{{ config(materialized='view') }}

select
  cab_type,
  pickup_month,
  avg(trip_duration_min) as avg_trip_duration_min
from {{ ref('int_nyc__trip_zone') }}
where trip_duration_min is not null
group by 1,2
