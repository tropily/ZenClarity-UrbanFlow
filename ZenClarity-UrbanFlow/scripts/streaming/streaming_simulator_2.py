import json
import time
import random
import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# Constants
FIREHOSE_STREAM_NAME = 'taxi_streaming_raw_data'
REGION = 'us-east-1'
DYNAMODB_TABLE_NAME = 'trip_streaming_log'
TRIPS_PER_HOUR = 5000
BURST_INTERVAL_SEC = 5

# Simulated time shift (in hours)
SIMULATED_HOUR = 8  # Always simulate as if it's 8 AM

# AWS Clients
firehose = boto3.client('firehose', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamo_table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Top 10 high-volume pickup zones (Yellow Cab)
top_pickup_zones = [161, 236, 186, 234, 138, 132, 143, 229, 125, 87]
possible_dropoff_zones = top_pickup_zones

def get_dynamic_burst_size():
    if SIMULATED_HOUR in [8, 17, 18]:
        return 15
    elif SIMULATED_HOUR in range(7, 10) or SIMULATED_HOUR in range(16, 19):
        return 10
    else:
        return 5

def generate_trip_event():
    now = datetime.now(timezone.utc)
    simulated_now = now.replace(hour=SIMULATED_HOUR, minute=random.randint(0, 59), second=random.randint(0, 59))

    event_time = now.isoformat()
    pickup_offset = random.randint(0, 2)
    pickup_time = simulated_now + timedelta(minutes=pickup_offset)
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 20))

    trip = {
        "trip_id": f"cab_{random.randint(100000, 999999)}",
        "pickup_datetime": pickup_time.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": dropoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "PULocationID": random.choice(top_pickup_zones),
        "DOLocationID": random.choice(possible_dropoff_zones),
        "passenger_count": random.randint(1, 4),
        "fare_amount": Decimal(str(round(random.uniform(5.0, 75.0), 2))),
        "payment_type": random.choice([1, 2, 3, 4, 5, 6]),
        "event_time": event_time
    }

    return trip

def send_to_firehose(record):
    firehose_record = record.copy()
    firehose_record["fare_amount"] = float(record["fare_amount"])

    response = firehose.put_record(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Record={'Data': json.dumps(firehose_record) + "\n"}
    )

    status_code = response['ResponseMetadata']['HTTPStatusCode']
    print(f"[Sent] {record['event_time']} trip_id={record['trip_id']} status={status_code}")

def log_to_dynamodb(record):
    try:
        dynamo_table.put_item(Item=record)
    except Exception as e:
        print(f"[DynamoDB Error] trip_id={record['trip_id']}: {e}")

if __name__ == "__main__":
    print("🚖 Streaming NYC Taxi trips to Firehose and logging to DynamoDB...\n")

    while True:
        burst_size = get_dynamic_burst_size()
        for _ in range(burst_size):
            trip = generate_trip_event()
            send_to_firehose(trip)
            log_to_dynamodb(trip)
        time.sleep(BURST_INTERVAL_SEC)
