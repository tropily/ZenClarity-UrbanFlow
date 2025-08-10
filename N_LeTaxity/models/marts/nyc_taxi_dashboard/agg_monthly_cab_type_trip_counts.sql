{{ config(materialized='view') }}

select
  cab_type,
  pickup_month,
  count(*) as trip_count
from {{ ref('int_nyc__trip_zone') }}
group by 1,2
