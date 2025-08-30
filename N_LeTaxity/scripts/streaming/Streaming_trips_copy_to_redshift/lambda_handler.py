import os
from list_unprocessed_files import list_recent_files
from dynamo_tracker import mark_pipeline_success
from copy_to_redshift import run_redshift_copy

S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ['S3_PREFIX']
DYNAMO_TABLE = os.environ['DYNAMO_TABLE']
SECRET_ARN = os.environ['SECRET_ARN']
WORKGROUP = os.environ['WORKGROUP_NAME']
DATABASE = os.environ['DATABASE_NAME']

DATASET_NAME = "nyc_taxi_streaming"
PIPELINE_TYPE = "streaming"


def lambda_handler(event, context):
    print("🚀 Lambda started")

    new_files = list_recent_files(S3_BUCKET, S3_PREFIX, DYNAMO_TABLE)
    print(f"🧾 Found {len(new_files)} new files to process.")

    for key in new_files:
        s3_uri = f"s3://{S3_BUCKET}/{key}"
        pipeline_id = key

        try:
            run_redshift_copy(SECRET_ARN, WORKGROUP, DATABASE, s3_uri)
            mark_pipeline_success(DYNAMO_TABLE, pipeline_id, PIPELINE_TYPE, DATASET_NAME, s3_uri)
        except Exception as e:
            print(f"❌ Failed to process file {key}: {e}")

    print("✅ Lambda completed")
