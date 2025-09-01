import os, json, pandas as pd, psycopg2, streamlit as st, boto3

# --- Settings (edit if needed) ---
REGION = "us-east-1"
SECRET_NAME = "dev/dbt/redshift"  # <- your existing secret
HOST = "teo-nyc-workgroup.667137120741.us-east-1.redshift-serverless.amazonaws.com"
PORT = 5439
DB   = "nyc_taxi_db"
SCHEMA = os.getenv("DBT_MART_SCHEMA", "dev_viper_mart")

VIEW_UNION = f"{SCHEMA}.dash_nyc_taxi__stream_vs_baseline_hourly"
VIEW_STREAM = f"{SCHEMA}.dash_nyc_taxi__streaming_hourly"
VIEW_BASE   = f"{SCHEMA}.dash_nyc_taxi__baseline_hourly"

@st.cache_resource
def get_redshift_credentials():
    sm = boto3.client("secretsmanager", region_name=REGION)
    secret = json.loads(sm.get_secret_value(SecretId=SECRET_NAME)["SecretString"])
    return secret["username"], secret["password"]

@st.cache_data(ttl=60)
def read_sql_df(sql: str) -> pd.DataFrame:
    user, pwd = get_redshift_credentials()
    with psycopg2.connect(
        host=HOST, port=PORT, dbname=DB, user=user, password=pwd, sslmode="require"
    ) as conn:
        return pd.read_sql(sql, conn)

st.set_page_config(page_title="ZenClarity • Streaming vs Baseline", layout="wide")
st.title("🚕 Streaming vs Baseline — Hourly")

# Data
df_union = read_sql_df(
    f"select series, hour, trips, fare_total, avg_passenger_count from {VIEW_UNION} order by hour"
)

if df_union.empty:
    st.warning("No data found in the marts. Rebuild dbt marts or check GRANTs.")
    st.stop()

# Charts
left, right = st.columns(2)
left.subheader("Trips per Hour")
left.line_chart(df_union.pivot(index="hour", columns="series", values="trips"))

right.subheader("Fare Total per Hour")
right.bar_chart(df_union.pivot(index="hour", columns="series", values="fare_total"))

st.subheader("Recent Rows")
st.dataframe(df_union.tail(48), use_container_width=True)
