import os
import json
import boto3

sns = boto3.client("sns")

# Get the SNS topic from environment variable
TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

def lambda_handler(event, context):
    status = event.get("status", "UNKNOWN")
    cab_type = event.get("cab_type", "N/A")
    year = event.get("year", "N/A")
    month = event.get("month", "N/A")
    pipeline_id = event.get("pipeline_id", f"{cab_type}_tripdata_{year}-{month}")
    error = event.get("error", "None")

    subject = f"[NYC Taxi Pipeline] {status} - {pipeline_id}"

    message = f"""
🚦 NYC Taxi Data Pipeline Execution Report

Pipeline ID: {pipeline_id}
Status     : {status}

📅 Date     : {year}-{month}
🚕 Cab Type : {cab_type}

ℹ️  Details:
{json.dumps(event, indent=2) if status == "FAILED" else "See Step Function logs for full details."}

🕒 Timestamp: {context.timestamp if hasattr(context, 'timestamp') else 'N/A'}
"""

    try:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"✅ Notification sent to {TOPIC_ARN}")
        return {"message": "Notification sent", "status": status}
    except Exception as e:
        print(f"❌ Failed to send SNS notification: {str(e)}")
        return {"error": str(e)}
