{{ config(materialized='view', tags=['mart','nyc_taxi','compare']) }}

select 'stream' as series, hour, trips, fare_total, avg_passenger_count
from {{ ref('dash_nyc_taxi__streaming_hourly') }}
union all
select 'baseline' as series, hour, trips, fare_total, avg_passenger_count
from {{ ref('dash_nyc_taxi__baseline_hourly') }}
order by hour, series
