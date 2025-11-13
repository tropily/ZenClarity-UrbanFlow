#!/usr/bin/env python3
"""
EMR PySpark job — process trip data (base stage, no zone joins)

Version: 3 - Iceberg Implementation (Partition Evolution and ACID compliance) <--- HIGHLIGHTED
Version: 2 - Partitioning Fix (cab_type/day removed, now year/month only)
Version: 1 - Optimization (AQE, Repartition fix, Python refactor)
Version: 0 baseline

EMR/Glue PySpark job — write NYC Taxi trips to Iceberg (2025+)

- Reads a single month from S3 raw: {cab_type}_tripdata_{YYYY}-{MM}.parquet
- Standardizes timestamp columns (tpep_/lpep_ -> pickup/dropoff)
- Adds missing FHV fields to keep a stable schema
- Filters to requested year/month
- Adds cab_type as a REAL column (persisted in Parquet)
- Lowercases all column names, drops metadata
- Writes to Iceberg table glue_catalog.nyc_taxi_wh.trip_data (PARTITIONED BY day(pickup_datetime))

Example:
  spark-submit --deploy-mode cluster \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.glue_catalog.warehouse=s3://teo-nyc-taxi/warehouse/ \
    s3://teo-nyc-taxi/scripts/emr-jobs/emr_process_trip_data_iceberg.py \
      --cab_type yellow --year 2025 --month 1 \
      --raw_prefix s3://teo-nyc-taxi/raw/
"""

"""
EMR/Glue PySpark job — write NYC Taxi trips to Iceberg (2025+)
Adds smoke checks and prints the Spark/YARN applicationId for easy `yarn logs`.

Required Spark confs (pass via --conf):
  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  spark.sql.catalog.glue_catalog.warehouse=s3://teo-nyc-taxi/warehouse/
"""
import argparse
import time
from pyspark.sql import SparkSession, functions as F, types as T

# ---- Canonical target schema: column -> Spark type ----
TARGET_TYPES = {
    "vendorid": T.IntegerType(),
    "cab_type": T.StringType(),
    "pickup_datetime": T.TimestampType(),
    "dropoff_datetime": T.TimestampType(),
    "store_and_fwd_flag": T.StringType(),
    "ratecodeid": T.LongType(),             # bigint
    "pulocationid": T.IntegerType(),
    "dolocationid": T.IntegerType(),
    "passenger_count": T.LongType(),        # bigint
    "trip_distance": T.DoubleType(),
    "fare_amount": T.DoubleType(),
    "extra": T.DoubleType(),
    "mta_tax": T.DoubleType(),
    "tip_amount": T.DoubleType(),
    "tolls_amount": T.DoubleType(),
    "ehail_fee": T.DoubleType(),            # may be absent in many feeds
    "improvement_surcharge": T.DoubleType(),
    "total_amount": T.DoubleType(),
    "payment_type": T.LongType(),           # bigint
    "trip_type": T.LongType(),              # may be absent (NULL ok)
    "congestion_surcharge": T.DoubleType(),
    "airport_fee": T.DoubleType(),
}

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cab_type", required=True, choices=["yellow", "green", "fhv"])
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--month", required=True, type=int)
    ap.add_argument("--raw_prefix", default="s3://teo-nyc-taxi/raw/")
    ap.add_argument("--coalesce", type=int, default=0, help="Optional: coalesce before write (0=skip)")
    ap.add_argument("--smoke", type=int, default=1, help="Run smoke checks before write (1=yes, 0=no)")
    return ap.parse_args()

def standardize_timestamp_cols(df, cab_type: str):
    if cab_type == "yellow":
        if "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        if "tpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    elif cab_type == "green":
        if "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        if "lpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    return df

def add_missing_fhv_cols(df):
    """Minimal alignment for FHV to match taxi schema."""
    needs = {
        "trip_distance": T.DoubleType(),
        "fare_amount": T.DoubleType(),
        "passenger_count": T.LongType(),  # bigint in target
    }
    for c, t in needs.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
    return df

def ensure_target_schema(df):
    """
    Ensure every column in TARGET_TYPES exists and has the expected type.
    Missing columns are added as NULLs cast to the target type.
    Existing columns are cast to the target type to avoid subtle mismatches.
    """
    for col_name, dtype in TARGET_TYPES.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(dtype))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df

def main():
    args = parse_args()
    if not (1 <= args.month <= 12):
        raise ValueError("month must be between 1 and 12")

    raw_path = f"{args.raw_prefix}{args.cab_type}_tripdata_{args.year}-{args.month:02d}.parquet"

    start_time = time.time()
    spark = (
        SparkSession.builder
        .appName(f"tripdata_to_iceberg_{args.cab_type}_{args.year}_{args.month:02d}")
        .getOrCreate()
    )
    sc = spark.sparkContext

    # Print YARN app info for quick `yarn logs`
    app_id = getattr(sc, "applicationId", "<unknown>")
    ui_url = getattr(sc, "uiWebUrl", "<no ui url>")
    print(f"[APP] Spark/YARN applicationId = {app_id}")
    print(f"[APP] Spark UI = {ui_url}")
    print(f"[APP] For logs: yarn logs -applicationId {app_id} | less")

    # Runtime/write tuning
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("parquet.enable.dictionary", "false")
    spark.conf.set("parquet.writer.version", "v1")

    print(f"[INFO] Reading {raw_path}")
    df = spark.read.parquet(raw_path)

    # Normalize timestamps by cab type
    df = standardize_timestamp_cols(df, args.cab_type)

    # FHV minimal alignment
    if args.cab_type == "fhv":
        df = add_missing_fhv_cols(df)

    # Ensure pickup_datetime exists
    if "pickup_datetime" not in df.columns:
        raise ValueError("pickup_datetime column not found after standardization.")

    # Filter to requested month (guardrails)
    df = df.filter(
        (F.year(F.col("pickup_datetime")) == F.lit(int(args.year))) &
        (F.month(F.col("pickup_datetime")) == F.lit(int(args.month)))
    )

    # Persist cab_type as a REAL column
    df = df.withColumn("cab_type", F.lit(args.cab_type).cast(T.StringType()))

    # Lower-case column names; drop Spark metadata if present
    df = df.select(*[F.col(c).alias(c.lower()) for c in df.columns])
    for m in ("_metadata", "_spark_metadata"):
        if m in df.columns:
            df = df.drop(m)

    # Enforce canonical schema (adds missing cols as NULLs, casts types)
    df = ensure_target_schema(df)

    # Final output ordering to match Iceberg DDL
    cols_out = list(TARGET_TYPES.keys())
    df_out = df.select(*cols_out).withColumn("ingestion_ts", F.current_timestamp())

    # Optional file count tuning
    if args.coalesce and args.coalesce > 0:
        df_out = df_out.coalesce(args.coalesce)

    # -------------------- SMOKE CHECKS --------------------
    if args.smoke:
        print("[SMOKE] Iceberg/Spark confs:")
        for k in [
            "spark.sql.extensions",
            "spark.sql.catalog.glue_catalog",
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "spark.sql.catalog.glue_catalog.warehouse",
        ]:
            print(k, "=", spark.conf.get(k, "<missing>"))

        print("[SMOKE] Output schema:")
        df_out.printSchema()

        print("[SMOKE] Sample count on df_out.limit(1000):", df_out.limit(1000).count())

        try:
            print("[SMOKE] DESCRIBE glue_catalog.nyc_taxi_wh.trip_data")
            spark.sql("DESCRIBE TABLE glue_catalog.nyc_taxi_wh.trip_data").show(truncate=False)
            print("[SMOKE] SELECT count(*) FROM glue_catalog.nyc_taxi_wh.trip_data")
            spark.sql("SELECT count(*) AS c FROM glue_catalog.nyc_taxi_wh.trip_data").show()
        except Exception as e:
            print("[SMOKE][WARN] Catalog/table probe failed:", repr(e))
    # -----------------------------------------------------

    print("[INFO] Writing to Iceberg table glue_catalog.nyc_taxi_wh.trip_data")
    try:
        # Either API is fine; writeTo(...) can yield clearer errors.
        # df_out.write.format("iceberg").mode("append").saveAsTable("glue_catalog.nyc_taxi_wh.trip_data")
        df_out.writeTo("glue_catalog.nyc_taxi_wh.trip_data").append()
    except Exception as e:
        print("[ERROR] Iceberg write failed:", repr(e))
        print(f"[HINT] Verify confs, permissions, and table existence. For more logs:")
        print(f"[HINT] yarn logs -applicationId {app_id} | less")
        raise

    end_time = time.time()
    print(f"[BENCHMARK] Total job time: {end_time - start_time:.3f} sec")
    print("[DONE] Write complete.")
    spark.stop()

if __name__ == "__main__":
    main()
