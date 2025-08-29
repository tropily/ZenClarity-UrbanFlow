import boto3
import time

redshift = boto3.client('redshift-data')

def execute_sql(secret_arn, workgroup, database, sql):
    resp = redshift.execute_statement(
        SecretArn=secret_arn,
        WorkgroupName=workgroup,
        Database=database,
        Sql=sql
    )
    statement_id = resp["Id"]
    wait_for_completion(statement_id)
    return statement_id

def wait_for_completion(statement_id):
    while True:
        response = redshift.describe_statement(Id=statement_id)
        status = response["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            if status == "FAILED":
                raise Exception(f"SQL statement failed: {response.get('Error')}")
            elif status == "ABORTED":
                raise Exception("SQL statement was aborted.")
            break
        time.sleep(1)

def build_insert_sql(cab_type, staging_table, final_table):
    base_columns = [
        "vendorid", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag", "ratecodeid",
        "pulocationid", "dolocationid", "passenger_count", "trip_distance", "fare_amount",
        "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "payment_type", "congestion_surcharge"
    ]

    if cab_type == "yellow":
        target_columns = base_columns + ["ehail_fee", "trip_type", "cab_type"]
        select_columns = base_columns + ["NULL AS ehail_fee", "NULL AS trip_type", "'yellow' AS cab_type"]
    elif cab_type == "green":
        target_columns = base_columns + ["ehail_fee", "trip_type", "airport_fee", "cab_type"]
        select_columns = base_columns + ["ehail_fee", "trip_type", "NULL AS airport_fee", "'green' AS cab_type"]
    else:
        raise ValueError(f"Unsupported cab_type: {cab_type}")

    insert_cols = ", ".join(target_columns)
    select_cols = ", ".join([f"s.{col}" if " AS " not in col else col for col in select_columns])

    return f"""
        INSERT INTO {final_table} ({insert_cols})
        SELECT {select_cols}
        FROM {staging_table} s
        LEFT JOIN {final_table} t
          ON s.vendorid = t.vendorid
         AND s.pickup_datetime = t.pickup_datetime
         AND s.pulocationid = t.pulocationid
         AND s.dolocationid = t.dolocationid
        WHERE t.vendorid IS NULL;
    """

def get_row_count(secret_arn, workgroup, database, table_name):
    sql = f"SELECT COUNT(*) FROM {table_name};"
    statement_id = execute_sql(secret_arn, workgroup, database, sql)
    while True:
        try:
            result = redshift.get_statement_result(Id=statement_id)
            return int(result["Records"][0][0]["longValue"])
        except redshift.exceptions.InvalidRequestException as e:
            if "Statement is not in Finished state" in str(e):
                time.sleep(0.5)
            else:
                raise

def run_copy_pipeline(cab_type, secret_arn, workgroup, database, s3_path, pipeline_id):
    staging_table = f"public.{cab_type}_trip_data_staging"
    final_table = "public.taxi_trip_data"

    print(f"🧹 Truncating staging table: {staging_table}")
    execute_sql(secret_arn, workgroup, database, f"TRUNCATE TABLE {staging_table};")

    copy_sql = f"""
        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE 'arn:aws:iam::667137120741:role/teo_redshift_service_role'
        FORMAT AS PARQUET;
    """
    print("📄 COPY SQL:\n", copy_sql)
    execute_sql(secret_arn, workgroup, database, copy_sql)

    staging_count = get_row_count(secret_arn, workgroup, database, staging_table)
    print(f"🧮 Rows in staging after COPY: {staging_count}")

    if staging_count == 0:
        raise Exception("🚫 No records found in staging table after COPY.")

    insert_sql = build_insert_sql(cab_type, staging_table, final_table)
    print("📥 Running INSERT into final table...")
    print("🧾 INSERT SQL:\n", insert_sql)
    execute_sql(secret_arn, workgroup, database, insert_sql)

    final_count = get_row_count(secret_arn, workgroup, database, final_table)
    print(f"✅ Final table row count: {final_count}")

    return {
        "staging_rows": staging_count,
        "final_row_count": final_count
    }
