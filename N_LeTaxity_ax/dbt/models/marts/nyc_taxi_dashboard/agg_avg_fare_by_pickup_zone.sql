{{ config(materialized='view') }}

select
  pickup_zone,
  avg(fare_amount) as avg_fare
from {{ ref('int_nyc__trip_zone') }}
where fare_amount is not null
group by 1
