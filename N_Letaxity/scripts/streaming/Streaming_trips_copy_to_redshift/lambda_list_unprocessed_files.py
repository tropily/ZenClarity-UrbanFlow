import boto3
import os
from datetime import datetime, timedelta

s3 = boto3.client('s3')
ddb = boto3.client('dynamodb')

def list_recent_files(bucket, prefix, table_name, lookback_minutes=5):
    """
    List .parquet files uploaded in the past `lookback_minutes` that haven't been processed yet.
    """
    recent_keys = []
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=lookback_minutes)

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue

            last_modified = obj['LastModified']
            if last_modified < cutoff:
                continue

            pipeline_id = key  # Use full S3 key as pipeline_id
            if not is_pipeline_processed(table_name, pipeline_id):
                recent_keys.append(key)

    return recent_keys

def is_pipeline_processed(table_name, pipeline_id):
    try:
        response = ddb.get_item(
            TableName=table_name,
            Key={'pipeline_id': {'S': pipeline_id}}
        )
        return 'Item' in response
    except Exception as e:
        print(f"Error checking DynamoDB: {e}")
        return False
