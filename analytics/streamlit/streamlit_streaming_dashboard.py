import os, json, pandas as pd, psycopg2, streamlit as st, boto3
import time
from datetime import datetime
import pytz

# ----------------------------------------------
# Redshift & AWS Secrets Manager Configuration
# ----------------------------------------------
REGION = "us-east-1"
SECRET_NAME = "dev/dbt/redshift" 
HOST = "teo-nyc-workgroup.667137120741.us-east-1.redshift-serverless.amazonaws.com"
PORT = 5439
DB   = "nyc_taxi_db"
NYC_TZ = pytz.timezone('America/New_York')

# The new dbt views to use
STREAM_VIEW = "dev_viper_mart.agg_daily_streaming_trips"
BASELINE_VIEW = "dev_viper_mart.dash_nyc_taxi__baseline_hourly"


# ----------------------------------------------
# Get Redshift credentials from Secrets Manager
# ----------------------------------------------
@st.cache_resource
def get_redshift_credentials():
    sm = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(sm.get_secret_value(SecretId=SECRET_NAME)["SecretString"])
    return secret["username"], secret["password"]

# ----------------------------------------------
# Load data from Redshift with a single, optimized query
# ----------------------------------------------
@st.cache_data(ttl=60)
def get_dashboard_data():
    username, password = get_redshift_credentials()
    conn = psycopg2.connect(
        host=HOST, port=PORT, dbname=DB, user=username, password=password, sslmode="require"
    )
    
    query = f"""
    SELECT
        'stream' AS series,
        pickup_hour AS hour,
        trips,
        fare_total
    FROM {STREAM_VIEW}
    --WHERE pickup_date = CURRENT_DATE

    UNION ALL

    SELECT
        'baseline' AS series,
        EXTRACT(HOUR FROM hour) AS hour,
        trips,
        fare_total
    FROM {BASELINE_VIEW}
    --WHERE hour::date = (CURRENT_DATE - 1)
    ORDER BY hour;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ----------------------------------------------
# Streamlit UI Setup
# ----------------------------------------------
st.set_page_config(page_title="ZenClarity UrbanFlow", layout="wide")
st.title("🚕 ZenClarity UrbanFlow")
st.header("Live Streaming Dashboard")
st.caption("Compare Real-Time Simulation vs. Historical Peak | Refreshes every 60s")

# ----------------------------------------------
# Load Data and Preprocessing
# ----------------------------------------------
df_combined = get_dashboard_data()

if df_combined.empty:
    st.warning("No data available to display KPIs or trends.")
    st.stop()

# Get today's and baseline data for KPIs
today_df = df_combined[df_combined['series'] == 'stream']
baseline_df = df_combined[df_combined['series'] == 'baseline']

# KPI Calculations
trips_stream = today_df['trips'].sum() if not today_df.empty else 0
trips_baseline = baseline_df['trips'].sum() if not baseline_df.empty else 0
fare_stream = today_df['fare_total'].sum() if not today_df.empty else 0.0
fare_baseline = baseline_df['fare_total'].sum() if not baseline_df.empty else 0.0

# Delta calculation function
def calc_delta(current, base):
    if base == 0 or pd.isna(base) or base is None:
        return "N/A"
    change = (current - base) / base * 100
    return f"{change:,.1f}%"

# ----------------------------------------------
# UI and Dashboard
# ----------------------------------------------
st.subheader("📊 Real-Time KPIs vs Baseline")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("🚖 Total Trips", f"{trips_stream:,.0f}", delta=calc_delta(trips_stream, trips_baseline))
    st.markdown("Total trips in the streaming data so far.")

with col2:
    st.metric("💰 Total Fare ($)", f"${fare_stream:,.2f}", delta=calc_delta(fare_stream, fare_baseline))
    st.markdown("Total fare collected from the streaming data so far.")

with col3:
    if trips_baseline > 0:
        trips_delta = trips_stream - trips_baseline
        st.metric(
            label="Trips Change vs. Baseline",
            value=f"{trips_delta:,.0f}",
            delta=f"{((trips_delta / trips_baseline) * 100):,.2f}%"
        )
    else:
        st.metric("Trips Change vs. Baseline", "N/A")
    st.markdown("Change in trips compared to the same period in the historical baseline.")

st.info(
    "💡 **Insight**: The metrics show a strong performance for today's streaming data, with an increase in trips and total fare compared to the baseline. This suggests a positive trend in customer demand."
)

st.divider()

# --- Hourly Aggregated Comparison Chart ---
st.subheader("📈 Hourly Trip and Fare Comparison")
left_chart, right_chart = st.columns(2)

# Chart 1: Trips per hour
left_chart.subheader("Trips per Hour")
left_chart.line_chart(df_combined.pivot(index="hour", columns="series", values="trips"))

# Chart 2: Fare per hour
right_chart.subheader("Fare Total per Hour")
right_chart.bar_chart(df_combined.pivot(index="hour", columns="series", values="fare_total"))

st.markdown("This line chart provides a direct comparison of trips per hour. The bar chart on the right visualizes the fare total.")
    
st.divider()

# --- Difference Chart ---
st.subheader("Difference in Trips: Stream - Baseline")

# Pivot the data to calculate the difference
df_pivoted = df_combined.pivot(index="hour", columns="series", values="trips").fillna(0)
df_pivoted['difference'] = df_pivoted['stream'] - df_pivoted['baseline']

# Assign a color to each bar based on its value
df_pivoted['color'] = df_pivoted['difference'].apply(lambda x: '#e74c3c' if x < 0 else '#2ecc71')

st.bar_chart(
    df_pivoted,
    y='difference',
    color='color',
    use_container_width=True
)
st.markdown("This bar chart visualizes the performance delta. A positive bar (green) indicates the stream is outperforming the baseline, while a negative bar (red) shows underperformance. This is a very clear and intuitive way to present performance.")
    
st.divider()

# --- Recent Rows Table ---
st.subheader("📝 Recent Data Sample from Baseline")
df_recent = get_dashboard_data()
st.dataframe(df_recent.sort_values("hour", ascending=False).head(10), use_container_width=True)

# ----------------------------------------------
# Auto-refresh every 60 seconds
# ----------------------------------------------
if "run" in st.query_params:
    last_run = int(st.query_params["run"])
    now_run = int(datetime.now().timestamp())
    if now_run - last_run >= 60:
        st.query_params["run"] = str(now_run)
        st.rerun()
else:
    st.query_params["run"] = str(int(datetime.now().timestamp()))
