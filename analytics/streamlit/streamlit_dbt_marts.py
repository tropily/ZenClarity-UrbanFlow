import os
import streamlit as st
import pandas as pd
import boto3
import json
import redshift_connector

# ----------------------------------------------
# Redshift & AWS Secrets Manager Configuration
# ----------------------------------------------
REGION = 'us-east-1'
SECRET_NAME = 'teo_developer/lambda/redshift'

REDSHIFT_HOST = 'teo-nyc-workgroup.667137120741.us-east-1.redshift-serverless.amazonaws.com'
REDSHIFT_PORT = 5439
REDSHIFT_DB = 'nyc_taxi_db'

# dbt mart schema (Redshift): dev_viper_mart
DBT_MART_SCHEMA = os.getenv('DBT_MART_SCHEMA', 'dev_viper_mart')

# ----------------------------------------------
# Get Redshift credentials from Secrets Manager
# ----------------------------------------------
@st.cache_resource
def get_redshift_credentials():
    client = boto3.client('secretsmanager', region_name=REGION)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response['SecretString'])
    return secret['username'], secret['password']

# ----------------------------------------------
# Redshift connection helper (kept same style)
# ----------------------------------------------
def _rs_conn():
    username, password = get_redshift_credentials()
    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=username,
        password=password
    )

def _run_query(sql: str) -> pd.DataFrame:
    with _rs_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)

# ----------------------------------------------
# Streamlit UI
# ----------------------------------------------
st.set_page_config(page_title="ZenClarity-UrbanFlow — dbt Marts", layout="wide")
st.title("🚕 ZenClarity-UrbanFlow — Dashboard (dbt Marts)")
st.caption(f"Source: Redshift • Schema: {DBT_MART_SCHEMA}")

st.markdown("This page queries **dbt mart** tables directly (no ad-hoc aggregates).")

# 1) Avg fare by pickup zone
st.subheader("💰 Avg Fare by Pickup Zone")
df_fare = _run_query(f"""
    select pickup_zone, avg_fare
    from {DBT_MART_SCHEMA}.agg_avg_fare_by_pickup_zone
    order by avg_fare desc
    limit 20
""")
st.dataframe(df_fare, use_container_width=True)

# 2) Busiest pickup zones (overall)
st.subheader("🚦 Busiest Pickup Zones (Overall)")
df_busy = _run_query(f"""
    select pickup_zone, total_trips
    from {DBT_MART_SCHEMA}.agg_busiest_pickup_zone
    where grain = 'overall'
    order by total_trips desc
    limit 20
""")
st.dataframe(df_busy, use_container_width=True)

# 3) Monthly trip counts by cab type
st.subheader("🗓️ Monthly Trip Counts by Cab Type")
df_monthly = _run_query(f"""
    select pickup_month, cab_type, trip_count
    from {DBT_MART_SCHEMA}.agg_monthly_cab_type_trip_counts
    order by pickup_month, cab_type
""")
st.dataframe(df_monthly, use_container_width=True)

# 4) Avg passenger count by cab type
st.subheader("🧍 Avg Passenger Count by Cab Type")
df_pass = _run_query(f"""
    select cab_type, avg_passenger_count
    from {DBT_MART_SCHEMA}.agg_avg_passenger_count_by_cab_type
    order by avg_passenger_count desc
""")
st.dataframe(df_pass, use_container_width=True)

# 5) Avg trip duration by cab type / month
st.subheader("⏱️ Avg Trip Duration (min) by Cab Type / Month")
df_dur = _run_query(f"""
    select pickup_month, cab_type, avg_trip_duration_min
    from {DBT_MART_SCHEMA}.agg_avg_trip_duration_by_cab_type_month
    order by pickup_month, cab_type
""")
st.dataframe(df_dur, use_container_width=True)

st.info("Tip: Set DBT_MART_SCHEMA env var if you need to point at a different schema.")
