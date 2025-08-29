import streamlit as st 
import pandas as pd
import boto3
import json
import redshift_connector
import time
from datetime import timedelta, datetime

# ----------------------------------------------
# Redshift & AWS Secrets Manager Configuration
# ----------------------------------------------
REGION = 'us-east-1'
SECRET_NAME = 'teo_developer/lambda/redshift'
REDSHIFT_HOST = 'teo-nyc-workgroup.667137120741.us-east-1.redshift-serverless.amazonaws.com'
REDSHIFT_PORT = 5439
REDSHIFT_DB = 'nyc_taxi_db'
REDSHIFT_SCHEMA = 'public'
REDSHIFT_VIEW = 'taxi_streaming_trips_vw'

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
# Query recent streaming trip data from Redshift
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
    query = f"""
        SELECT 
            event_time, trip_id, pickup_zone, delay_time_minutes, fare_amount, passenger_count
        FROM {REDSHIFT_SCHEMA}.{REDSHIFT_VIEW}
        WHERE event_time >= GETDATE() - INTERVAL '120 minutes'
    """
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    cursor.close()
    conn.close()
    return df

# ----------------------------------------------
# Streamlit UI Setup
# ----------------------------------------------
st.set_page_config(page_title="Nle Taxity Dashboard", layout="wide")
st.title("🚕 NLe_Taxity Live Streaming Dashboard")
st.caption("Streaming Redshift Data | Auto-refresh every 60 seconds")

# ----------------------------------------------
# Load data
# ----------------------------------------------
data = load_data()

if data.empty:
    st.warning("No streaming trip records found in the last 10 minutes.")
    st.stop()

# ----------------------------------------------
# Preprocessing
# ----------------------------------------------
data['event_time'] = pd.to_datetime(data['event_time'])
data['minute'] = data['event_time'].dt.floor('min')

# ----------------------------------------------
# Zone Filter Dropdown
# ----------------------------------------------
zones = ['All'] + sorted(data['pickup_zone'].dropna().unique().tolist())
selected_zone = st.selectbox("📍 Filter by Pickup Zone:", zones)

if selected_zone != 'All':
    data = data[data['pickup_zone'] == selected_zone]

# ----------------------------------------------
# 🧩 Custom CSS to enlarge KPI fonts
# ----------------------------------------------
st.markdown("""
    <style>
    div[data-testid="metric-container"] {
        font-size: 28px !important;
    }
    div[data-testid="metric-container"] > label {
        font-size: 18px !important;
    }
    </style>
""", unsafe_allow_html=True)

# ----------------------------------------------
# KPI Calculations
# ----------------------------------------------
total_trips = len(data)
avg_delay = round(data['delay_time_minutes'].mean(), 2) if not data.empty else 0
total_fare = round(data['fare_amount'].sum(), 2)
total_passengers = int(data['passenger_count'].sum())
busiest_zone = data['pickup_zone'].value_counts().idxmax() if not data.empty else "N/A"
avg_fare_per_passenger = round(total_fare / total_passengers, 2) if total_passengers > 0 else 0
time_range_min = (data['event_time'].max() - data['event_time'].min()).total_seconds() / 60
trip_rate = round(total_trips / time_range_min, 2) if time_range_min > 0 else 0

# ----------------------------------------------
# KPI Display (7 KPIs)
# ----------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)
k6, k7 = st.columns(2)

k1.metric("🚖 Total Trips", total_trips)
k2.metric("⏱ Avg Delay (min)", avg_delay)
k3.metric("🗺 Busiest Zone", busiest_zone)
k4.metric("💰 Total Fare ($)", total_fare)
k5.metric("👥 Total Passengers", total_passengers)
k6.metric("💸 Avg Fare / Passenger", f"${avg_fare_per_passenger}")
k7.metric("📊 Trips per Minute", trip_rate)

# ----------------------------------------------
# Show next refresh time
# ----------------------------------------------
next_refresh = datetime.now() + timedelta(seconds=60)
st.caption(f"🔄 Next refresh at: {next_refresh.strftime('%H:%M:%S')}")

# ----------------------------------------------
# Cumulative Line Chart
# ----------------------------------------------
trip_counts = data.groupby('minute').size().reset_index(name='trip_count')
trip_counts = trip_counts.sort_values('minute')
trip_counts['cumulative'] = trip_counts['trip_count'].cumsum()

st.line_chart(trip_counts.set_index('minute')[['cumulative']])

# ----------------------------------------------
# Show recent trip rows
# ----------------------------------------------
st.write("📋 Recent Trips", data.sort_values('event_time', ascending=False).head(10))

# ----------------------------------------------
# ⏱️ Auto-refresh logic every 60 seconds (safe placement)
# ----------------------------------------------
if "run" in st.query_params:
    last_run = int(st.query_params["run"])
    now = int(time.time())
    if now - last_run >= 60:
        st.query_params["run"] = str(now)
        st.rerun()
else:
    st.query_params["run"] = str(int(time.time()))
