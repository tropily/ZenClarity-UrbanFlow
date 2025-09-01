import streamlit as st
import pandas as pd
import psycopg2
import boto3
import json

# Redshift & AWS Secrets Manager details
REGION = 'us-east-1'
SECRET_NAME = 'teo_developer/lambda/redshift'
REDSHIFT_HOST = 'teo-nyc-workgroup.667137120741.us-east-1.redshift-serverless.amazonaws.com'
REDSHIFT_PORT = 5439
REDSHIFT_DB = 'nyc_taxi_db'
REDSHIFT_SCHEMA = 'public'
REDSHIFT_VIEW = 'taxi_streaming_trips_vw'

# Fetch Redshift credentials from Secrets Manager
@st.cache_resource
def get_redshift_credentials():
    client = boto3.client('secretsmanager', region_name=REGION)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response['SecretString'])
    return secret['username'], secret['password']

# Load data from Redshift view
@st.cache_data(ttl=10)  # refresh every 10 seconds
def load_data():
    username, password = get_redshift_credentials()

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=username,
        password=password
    )
    query = f"""
        SELECT *
        FROM {REDSHIFT_SCHEMA}.{REDSHIFT_VIEW}
        WHERE trip_start_time >= NOW() - INTERVAL '10 minutes'
        ORDER BY trip_start_time DESC
        LIMIT 1000;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Streamlit UI
st.set_page_config(page_title="NYC Taxi Live Stream", layout="wide")
st.title("🚕 NYC Taxi Trips - Live Dashboard")
st.caption("Streaming Redshift data from the last 10 minutes")

# Load and display data
data = load_data()

# Show line chart
if not data.empty:
    st.line_chart(data[['trip_start_time', 'trip_distance']].set_index('trip_start_time'))
else:
    st.warning("No recent trips found.")
