import boto3
import os

redshift = boto3.client('redshift-data')

def run_redshift_copy(secret_arn, workgroup, database, s3_uri):
    staging_table = "public.staging_taxi_streaming_trips"
    final_table = "public.taxi_streaming_trips"

    copy_sql = f"""
    COPY {staging_table}
    FROM '{s3_uri}'
    IAM_ROLE 'arn:aws:iam::667137120741:role/teo_redshift_service_role'
    FORMAT AS PARQUET;
    """

    insert_sql = f"""
    INSERT INTO {final_table} (
        trip_id, pickup_datetime, dropoff_datetime,
        pulocationid, dolocationid, passenger_count,
        fare_amount, payment_type, year, month, day, hour
    )
    SELECT
        s.trip_id,
        s.pickup_datetime,
        s.dropoff_datetime,
        s.pulocationid,
        s.dolocationid,
        s.passenger_count,
        s.fare_amount,
        s.payment_type,
        EXTRACT(YEAR FROM s.pickup_datetime),
        EXTRACT(MONTH FROM s.pickup_datetime),
        EXTRACT(DAY FROM s.pickup_datetime),
        EXTRACT(HOUR FROM s.pickup_datetime)
    FROM {staging_table} s
    LEFT JOIN {final_table} t ON s.trip_id = t.trip_id
    WHERE t.trip_id IS NULL;

    DELETE FROM {staging_table};
    """

    try:
        print("🚀 Copying into staging table...")
        redshift.execute_statement(
            SecretArn=secret_arn,
            WorkgroupName=workgroup,
            Database=database,
            Sql=copy_sql
        )

        print("✅ Running dedup & insert into final table...")
        redshift.execute_statement(
            SecretArn=secret_arn,
            WorkgroupName=workgroup,
            Database=database,
            Sql=insert_sql
        )

    except Exception as e:
        print(f"❌ Redshift COPY/INSERT failed: {e}")
        raise
