import boto3
from datetime import datetime

ddb = boto3.client('dynamodb')

def mark_pipeline_success(table_name, pipeline_id, pipeline_type, dataset_name, s3_uri):
    try:
        ddb.put_item(
            TableName=table_name,
            Item={
                'pipeline_id': {'S': pipeline_id},
                'pipeline_type': {'S': pipeline_type},
                'dataset_name': {'S': dataset_name},
                's3_uri': {'S': s3_uri},
                'status': {'S': 'success'},
                'copied_at': {'S': datetime.utcnow().isoformat()},
                'notes': {'S': 'Ingested successfully by Lambda'}
            }
        )
        print(f"✅ Logged to DynamoDB: {pipeline_id}")
    except Exception as e:
        print(f"❌ Failed to write to DynamoDB for {pipeline_id}: {e}")
