# 🌆 ZenClarity-UrbanFlow — dbt Models

**Layers**: staging → intermediate → marts  
**Targets**: Redshift Serverless + Snowflake (via portable macros)  
**Focus**: clear lineage, tested models, and cross-warehouse benchmarks.

- Sources: *NYC Taxi trip data*, *Taxi zone lookup*
- Marts (dashboard-ready): `agg_avg_fare_by_pickup_zone`, `agg_busiest_pickup_zone`, `agg_monthly_cab_type_trip_counts`, `agg_avg_passenger_count_by_cab_type`, `agg_avg_trip_duration_by_cab_type_month`
- Consumers: Streamlit dashboard, ad-hoc benchmarks
