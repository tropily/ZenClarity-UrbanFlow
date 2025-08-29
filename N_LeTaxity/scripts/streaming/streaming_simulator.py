import json
import time
import random
import boto3
import pandas as pd
from datetime import datetime, timezone
from decimal import Decimal

# Constants
FIREHOSE_STREAM_NAME = 'taxi_streaming_raw_data'
REGION = 'us-east-1'
DYNAMODB_TABLE_NAME = 'trip_streaming_log'
PARQUET_S3_PATH = 's3://teo-nyc-taxi/processed/trip_data/cab_type=yellow/year=2024/month=12/day=13/'
SIMULATED_HOUR = 8  # Simulate trips as if they happened at 8 AM
BURST_INTERVAL_SEC = 5  # Delay between bursts

# AWS Clients
firehose = boto3.client('firehose', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamo_table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Load historical Parquet data
print("📦 Loading historical data from S3...")
df = pd.read_parquet(PARQUET_S3_PATH)
df = df[df['pickup_datetime'].dt.hour == SIMULATED_HOUR]

# Clean data
df = df.fillna({
    "passenger_count": 1,
    "fare_amount": 0,
    "payment_type": 1,
    "pulocationid": 0,
    "dolocationid": 0
})
df = df.reset_index(drop=True)

print(f"✅ Loaded {len(df)} historical records for hour {SIMULATED_HOUR}")
print("🚀 Streaming historical taxi trips to Firehose...\n")

def send_to_firehose(record):
    try:
        fare = record.get("fare_amount")
        if pd.isna(fare):
            print("[⚠️ Skipped] NaN fare_amount")
            return

        firehose_record = {
            "trip_id": f"cab_{random.randint(100000, 999999)}",
            "pickup_datetime": record["pickup_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            "dropoff_datetime": record["dropoff_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            "PULocationID": int(record.get("pulocationid")),
            "DOLocationID": int(record.get("dolocationid")),
            "passenger_count": int(record.get("passenger_count")),
            "fare_amount": float(fare),
            "payment_type": int(record.get("payment_type")),
            "event_time": datetime.now(timezone.utc).isoformat()  # ⬅️ Fixed timezone
        }

        response = firehose.put_record(
            DeliveryStreamName=FIREHOSE_STREAM_NAME,
            Record={'Data': json.dumps(firehose_record) + "\n"}
        )
        print(f"[Sent] trip_id={firehose_record['trip_id']} time={firehose_record['event_time']}")

    except Exception as e:
        print(f"[Firehose Error] {e}")

def log_to_dynamodb(record):
    try:
        item = {
            "trip_id": f"cab_{random.randint(100000, 999999)}",
            "pickup_datetime": record["pickup_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            "dropoff_datetime": record["dropoff_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            "PULocationID": int(record.get("pulocationid")),
            "DOLocationID": int(record.get("dolocationid")),
            "passenger_count": int(record.get("passenger_count")),
            "fare_amount": Decimal(str(round(float(record.get("fare_amount")), 2))),
            "payment_type": int(record.get("payment_type")),
            "event_time": datetime.now(timezone.utc).isoformat()
        }
        dynamo_table.put_item(Item=item)

    except Exception as e:
        print(f"[DynamoDB Error] {e}")

# Main loop
if __name__ == "__main__":
    while True:
        for _ in range(10):  # Adjust burst size if needed
            row = df.sample(1).iloc[0]
            send_to_firehose(row)
            log_to_dynamodb(row)
        time.sleep(BURST_INTERVAL_SEC)
