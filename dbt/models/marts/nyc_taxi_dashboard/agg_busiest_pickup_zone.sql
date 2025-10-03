{{ config(materialized='view') }}

with zone_counts as (
  select pickup_zone, count(*) as total_trips
  from {{ ref('int_nyc__trip_zone') }}
  group by 1
),
ranked_overall as (
  select pickup_zone, total_trips,
         dense_rank() over (order by total_trips desc) as rnk
  from zone_counts
),
zone_counts_month as (
  select pickup_month, pickup_zone, count(*) as total_trips
  from {{ ref('int_nyc__trip_zone') }}
  group by 1,2
),
ranked_month as (
  select pickup_month, pickup_zone, total_trips,
         dense_rank() over (partition by pickup_month order by total_trips desc) as rnk
  from zone_counts_month
)

select 'overall' as grain,
       {% if target.type == 'spark' %}
         CAST(NULL AS DATE) as pickup_month,
       {% else %}
         null::date as pickup_month,
       {% endif %}
       pickup_zone, total_trips, rnk
from ranked_overall
where rnk = 1

union all

select 'by_month' as grain, pickup_month, pickup_zone, total_trips, rnk
from ranked_month
where rnk = 1
