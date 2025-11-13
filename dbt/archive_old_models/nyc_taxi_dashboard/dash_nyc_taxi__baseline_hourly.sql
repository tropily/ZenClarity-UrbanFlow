{{ config(
    enabled = target.name != 'snowflake_iceberg'
) }}

{{ config(materialized='view', tags=['mart','nyc_taxi','baseline']) }}

with chosen_day as (
  {% if var('baseline_date', none) %}
    -- quote the var safely and cast to date (prevents empty results)
    select {{ "'" ~ var('baseline_date') ~ "'" }}::date as d
  {% else %}
    -- fallback: latest baseline day present in data
    select max(date(pickup_at)) as d
    from {{ ref('int_nyc__trip_zone') }}
  {% endif %}
)
select
  date_trunc('hour', pickup_at)                         as hour,
  count(*)                                              as trips,
  sum(fare_amount)                                      as fare_total,
  avg(nullif(passenger_count, 0))                       as avg_passenger_count
from {{ ref('int_nyc__trip_zone') }}, chosen_day
where date(pickup_at) = chosen_day.d
group by 1
order by 1