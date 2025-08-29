import streamlit as st
import pandas as pd
import boto3
import json
import redshift_connector
import time
from datetime import timedelta, datetime
import pytz

# ----------------------------------------------
# Redshift & AWS Secrets Manager Configuration
# ----------------------------------------------
REGION = 'us-east-1'
SECRET_NAME = 'teo_developer/lambda/redshift'
REDSHIFT_HOST = 'teo-nyc-workgroup.667137120741.us-east-1.redshift-serverless.amazonaws.com'
REDSHIFT_PORT = 5439
REDSHIFT_DB = 'nyc_taxi_db'
REDSHIFT_SCHEMA = 'public'
CURRENT_VIEW = 'taxi_trip_simulated_today_vw'
BASELINE_VIEW = 'taxi_trip_top_traffic_vw'
NYC_TZ = pytz.timezone('America/New_York')

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
# Load data from Redshift
# ----------------------------------------------
@st.cache_data(ttl=60)
def load_data():
    username, password = get_redshift_credentials()
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=username,
        password=password
    )
    cursor = conn.cursor()

    # Load today's simulated streaming data
    query_current = f"""
        SELECT pickup_datetime, pickup_zone, fare_amount, passenger_count
        FROM {REDSHIFT_SCHEMA}.{CURRENT_VIEW}
        WHERE pickup_datetime <= GETDATE()
    """
    cursor.execute(query_current)
    cols = [col[0] for col in cursor.description]
    today_data = pd.DataFrame(cursor.fetchall(), columns=cols)

    # Load baseline data from Dec 13, 2024
    query_baseline = f"""
        SELECT pickup_datetime, pickup_zone, fare_amount, passenger_count
        FROM {REDSHIFT_SCHEMA}.{BASELINE_VIEW}
    """
    cursor.execute(query_baseline)
    cols = [col[0] for col in cursor.description]
    baseline_data = pd.DataFrame(cursor.fetchall(), columns=cols)

    cursor.close()
    conn.close()
    return today_data, baseline_data

# ----------------------------------------------
# Streamlit UI Setup
# ----------------------------------------------
st.set_page_config(page_title="Nle Taxity Dashboard", layout="wide")
st.title("🚕 NLe_Taxity Live Streaming Dashboard")
st.caption("Compare Real-Time Simulation vs. Historical Peak | Refreshes every 60s")

# ----------------------------------------------
# Load Data
# ----------------------------------------------
today_df, baseline_df = load_data()

if today_df.empty or baseline_df.empty:
    st.warning("No data available to display KPIs or trends.")
    st.stop()

# ----------------------------------------------
# Preprocessing
# ----------------------------------------------
today_df['pickup_datetime'] = pd.to_datetime(today_df['pickup_datetime']).dt.tz_localize('UTC').dt.tz_convert(NYC_TZ)
baseline_df['pickup_datetime'] = pd.to_datetime(baseline_df['pickup_datetime']).dt.tz_localize('UTC').dt.tz_convert(NYC_TZ)

now = datetime.now(NYC_TZ)

# Filter to only pickup_datetime up to now
current_hour = now.hour
current_minute = now.minute

today_df = today_df[today_df['pickup_datetime'] <= now]

# Extract hour for grouping
today_df['hour'] = today_df['pickup_datetime'].dt.hour
baseline_df['hour'] = baseline_df['pickup_datetime'].dt.hour

# ----------------------------------------------
# KPI Calculation
# ----------------------------------------------
# Baseline
baseline_hour = baseline_df[baseline_df['hour'] == current_hour]
baseline_trips = baseline_hour.shape[0]
baseline_fare = baseline_hour['fare_amount'].sum()
baseline_passenger = baseline_hour['passenger_count'].mean()

# Today
today_hour = today_df[today_df['hour'] == current_hour]
today_trips = today_hour.shape[0]
today_fare = today_hour['fare_amount'].sum()
today_passenger = today_hour['passenger_count'].mean()

# Delta calculation function
def calc_delta(current, base):
    if base == 0:
        return "N/A"
    change = (current - base) / base * 100
    arrow = "🔼" if change >= 0 else "🔽"
    return f"{arrow} {change:.1f}%"

# ----------------------------------------------
# KPI Dashboard
# ----------------------------------------------
st.subheader(f"📊 Real-Time KPIs vs Baseline — {now.strftime('%Y-%m-%d %H:%M')} NYC Time")
col1, col2, col3 = st.columns(3)
col1.metric("🚖 Trips", today_trips, calc_delta(today_trips, baseline_trips))
col2.metric("💰 Total Fare ($)", f"${today_fare:,.2f}", calc_delta(today_fare, baseline_fare))
col3.metric("🧍‍♂️ Avg Passenger Count", f"{today_passenger:.2f}", calc_delta(today_passenger, baseline_passenger))

# ----------------------------------------------
# Hourly Aggregated Comparison Chart
# ----------------------------------------------
st.subheader("📈 Hourly Trip and Fare Comparison")

# Aggregate by hour for baseline and today
today_hourly = today_df.groupby('hour').agg(trip_count=('pickup_datetime', 'count'), fare_total=('fare_amount', 'sum')).reset_index()
baseline_hourly = baseline_df.groupby('hour').agg(trip_count=('pickup_datetime', 'count'), fare_total=('fare_amount', 'sum')).reset_index()

# Merge them
total_df = pd.merge(baseline_hourly, today_hourly, on='hour', suffixes=('_baseline', '_today'), how='outer').fillna(0)

st.bar_chart(total_df.set_index('hour')[['trip_count_baseline', 'trip_count_today']])
st.bar_chart(total_df.set_index('hour')[['fare_total_baseline', 'fare_total_today']])

# ----------------------------------------------
# Show recent trips
# ----------------------------------------------
st.subheader("📝 Recent Streaming Trips")
st.dataframe(today_df.sort_values('pickup_datetime', ascending=False).head(10))

# ----------------------------------------------
# Auto-refresh every 60 seconds
# ----------------------------------------------
if "run" in st.query_params:
    last_run = int(st.query_params["run"])
    now_run = int(time.time())
    if now_run - last_run >= 60:
        st.query_params["run"] = str(now_run)
        st.rerun()
else:
    st.query_params["run"] = str(int(time.time()))

