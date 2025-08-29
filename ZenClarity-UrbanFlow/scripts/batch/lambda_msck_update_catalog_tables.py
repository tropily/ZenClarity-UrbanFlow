import boto3
import json
import traceback
from datetime import datetime
import os

from pipeline_logger import log_pipeline_stage  # ensure this is part of Lambda ZIP

athena_client = boto3.client('athena')

DATABASE_NAME = "teo_nyc_taxi_db"
TABLES = ["trip_data"]
ATHENA_OUTPUT = "s3://teo-nyc-taxi/Athena/output_result/"

def run_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE_NAME},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    return response['QueryExecutionId']

def lambda_handler(event, context):
    print("✅ Event received:")
    print(json.dumps(event, indent=2))

    pipeline_id = event.get("pipeline_id", f"catalog_refresh_{datetime.utcnow().strftime('%Y-%m-%d')}")
    timestamp = datetime.utcnow().isoformat()

    # ✅ Log start
    log_pipeline_stage(
        pipeline_id=pipeline_id,
        stage="msck_repair",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="lambda",
        status="STARTED",
        timestamp=timestamp,
        details={"tables": TABLES}
    )

    try:
        query_ids = []
        for table in TABLES:
            query = f"MSCK REPAIR TABLE {DATABASE_NAME}.{table};"
            print(f"🛠 Running: {query}")
            query_id = run_query(query)
            print(f"✅ QueryExecutionId: {query_id}")
            query_ids.append({table: query_id})

        log_pipeline_stage(
            pipeline_id=pipeline_id,
            stage="msck_repair",
            pipeline_name="nyc_taxi_batch",
            pipeline_type="batch",
            executor="lambda",
            status="SUCCEEDED",
            timestamp=datetime.utcnow().isoformat(),
            details={"query_ids": query_ids}
        )

        return {"status": "MSCK repair successful", "queries": query_ids}

    except Exception as e:
        log_pipeline_stage(
            pipeline_id=pipeline_id,
            stage="msck_repair",
            pipeline_name="nyc_taxi_batch",
            pipeline_type="batch",
            executor="lambda",
            status="FAILED",
            timestamp=datetime.utcnow().isoformat(),
            details={"error": str(e), "trace": traceback.format_exc()}
        )
        raise
