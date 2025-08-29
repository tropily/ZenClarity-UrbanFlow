import json
import base64
from datetime import datetime
from collections import OrderedDict

def lambda_handler(event, context):
    output = []

    for record in event['records']:
        try:
            # 🔹 Decode input
            decoded_data = base64.b64decode(record['data']).decode('utf-8').strip()

            if not decoded_data:
                raise ValueError("Empty decoded string")

            raw = json.loads(decoded_data)

            # 🔹 Check required fields
            required_fields = [
                "trip_id", "pickup_datetime", "dropoff_datetime",
                "passenger_count", "fare_amount", "payment_type"
            ]
            missing_fields = [f for f in required_fields if f not in raw]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")

            # 🔹 Parse and format timestamps
            pickup_dt = datetime.strptime(raw['pickup_datetime'], "%Y-%m-%d %H:%M:%S")
            dropoff_dt = datetime.strptime(raw['dropoff_datetime'], "%Y-%m-%d %H:%M:%S")

            event_time_raw = raw.get('event_time')
            if event_time_raw:
                try:
                    parsed_event_time = datetime.strptime(event_time_raw, "%Y-%m-%dT%H:%M:%S.%f%z")
                except ValueError:
                    parsed_event_time = datetime.strptime(event_time_raw, "%Y-%m-%dT%H:%M:%S%z")
                event_time = parsed_event_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                event_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            lambda_received_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # 🔹 Build the final payload (consistent order, no nulls)
            payload = OrderedDict()
            payload['trip_id'] = raw['trip_id']
            payload['pickup_datetime'] = pickup_dt.strftime("%Y-%m-%d %H:%M:%S")
            payload['dropoff_datetime'] = dropoff_dt.strftime("%Y-%m-%d %H:%M:%S")
            payload['pulocationid'] = raw.get('PULocationID', -1)
            payload['dolocationid'] = raw.get('DOLocationID', -1)
            payload['passenger_count'] = raw['passenger_count']
            payload['fare_amount'] = raw['fare_amount']
            payload['payment_type'] = raw['payment_type']
            payload['event_time'] = event_time
            payload['lambda_received_time'] = lambda_received_time
            payload['year'] = pickup_dt.year
            payload['month'] = pickup_dt.month
            payload['day'] = pickup_dt.day
            payload['hour'] = pickup_dt.hour

            # ✅ Log full payload before encoding
            print("✅ Final payload before encoding:")
            print(json.dumps(payload, indent=2))
            print("🧾 Payload keys:", list(payload.keys()))

            # 🔹 Encode for Firehose
            encoded_data = base64.b64encode((json.dumps(payload) + "\n").encode("utf-8")).decode("utf-8")

            # 🔹 Build output record
            output_record = {
                "recordId": record["recordId"],
                "result": "Ok",
                "data": encoded_data,
                "metadata": {
                    "partitionKeys": {
                        "year": str(payload["year"]),
                        "month": str(payload["month"]),
                        "day": str(payload["day"]),
                        "hour": str(payload["hour"])
                    }
                }
            }

        except Exception as e:
            print(json.dumps({
                "recordId": record.get("recordId", "unknown"),
                "status": "error",
                "error": str(e)
            }))
            output_record = {
                "recordId": record.get("recordId", "unknown"),
                "result": "ProcessingFailed",
                "data": record.get("data", "")
            }

        output.append(output_record)

    return {"records": output}
