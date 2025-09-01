import boto3
import os
import json
from decimal import Decimal, InvalidOperation
from datetime import datetime
from botocore.exceptions import ClientError

# === ENVIRONMENT VARIABLES ===
DYNAMO_TABLE = os.environ.get("CONTROL_TABLE", "pipeline_execution_control_table")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")  # Optional

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

def safe_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)

def log_pipeline_stage(
    pipeline_id,
    stage,
    pipeline_name,
    pipeline_type,
    executor,
    status,
    timestamp,
    record_count=None,
    s3_input=None,
    s3_output=None,
    db_table=None,
    details=None
):
    table = dynamodb.Table(DYNAMO_TABLE)

    item = {
        "pipeline_id": pipeline_id,
        "stage": stage,
        "pipeline_name": pipeline_name,
        "pipeline_type": pipeline_type,
        "executor": executor,
        "status": status,
        "timestamp": timestamp,
    }

    if record_count is not None:
        item["record_count"] = safe_decimal(record_count)
    if s3_input:
        item["s3_input"] = s3_input
    if s3_output:
        item["s3_output"] = s3_output
    if db_table:
        item["db_table"] = db_table
    if details:
        item["details"] = details

    # Write to DynamoDB
    try:
        table.put_item(Item=item)
        print(f"✅ Logged to control table: {pipeline_id} [{stage}] - {status}")
    except ClientError as e:
        print(f"❌ Failed to log to DynamoDB: {e}")
        raise

    # Optional: Send SNS on failure
    if status == "FAILED" and SNS_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"[FAILED] {pipeline_id} at {stage}",
                Message=json.dumps(item, indent=2),
            )
            print("📣 Failure notification sent via SNS.")
        except Exception as e:
            print(f"❌ Failed to send SNS alert: {e}")
