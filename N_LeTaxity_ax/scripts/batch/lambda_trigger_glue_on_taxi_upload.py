import json
import re
from datetime import datetime
import boto3
import os

from pipeline_logger import log_pipeline_stage

# Step Functions client
sfn = boto3.client("stepfunctions")

# Step Function ARN (replace with your actual one!)
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:667137120741:stateMachine:step_function_nyc_taxi_monthly_batch"
def lambda_handler(event, context):
    print("🚀 Event Received:", json.dumps(event))

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        print(f"📦 Object Key: {object_key}")

        # Match expected pattern
        pattern = r"^raw/(yellow|green|fhv)_tripdata_(\d{4})-(\d{2})\.parquet$"
        match = re.match(pattern, object_key)

        if not match:
            log_pipeline_stage(
                pipeline_id="invalid_filename_" + object_key,
                stage="transform_start",
                pipeline_name="nyc_taxi_batch",
                pipeline_type="batch",
                executor="lambda",
                status="FAILED",
                timestamp=datetime.utcnow().isoformat(),
                s3_input=f"s3://{bucket}/{object_key}",
                details={"error": "Invalid file name pattern"}
            )
            continue

        cab_type, year, month = match.groups()
        pipeline_id = f"{cab_type}_tripdata_{year}-{month}"
        timestamp = datetime.utcnow().isoformat()

        # ✅ Log transform start
        log_pipeline_stage(
            pipeline_id=pipeline_id,
            stage="transform_start",
            pipeline_name="nyc_taxi_batch",
            pipeline_type="batch",
            executor="lambda",
            status="STARTED",
            timestamp=timestamp,
            s3_input=f"s3://{bucket}/{object_key}",
            details={"trigger": "s3_put_event"}
        )

        # ✅ Trigger Step Function
        try:
            response = sfn.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                input=json.dumps({
                    "cab_type": cab_type,
                    "year": year,
                    "month": month,
                    "pipeline_id": pipeline_id
                })
            )
            print(f"✅ Step Function started: {response['executionArn']}")
        except Exception as e:
            print(f"❌ Failed to start Step Function: {e}")
            log_pipeline_stage(
                pipeline_id=pipeline_id,
                stage="step_function_start",
                pipeline_name="nyc_taxi_batch",
                pipeline_type="batch",
                executor="lambda",
                status="FAILED",
                timestamp=datetime.utcnow().isoformat(),
                details={"error": str(e)}
            )
            raise

        return {
            "cab_type": cab_type,
            "year": year,
            "month": month,
            "pipeline_id": pipeline_id
        }

    log_pipeline_stage(
        pipeline_id="unrecognized_event",
        stage="transform_start",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="lambda",
        status="FAILED",
        timestamp=datetime.utcnow().isoformat(),
        details={"error": "No valid tripdata files in event", "event": json.dumps(event)}
    )

    return {"error": "No valid records found"}
