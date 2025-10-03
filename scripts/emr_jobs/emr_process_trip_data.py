#!/usr/bin/env python3
"""
EMR PySpark job — process trip data (base stage, no zone joins)

Mirrors the Glue job behavior with 3 cab types (yellow/green/fhv):
- Reads a single month from S3 raw: {cab_type}_tripdata_{YEAR}-{MM}.parquet
- Normalizes pickup/dropoff timestamp column names by cab type
- Adds missing columns for FHV to keep a stable schema
- Filters to the requested YEAR/MONTH
- Adds partitions: cab_type, year, month, day (from pickup_datetime)
- Lower-cases all column names, drops metadata columns
- Writes partitioned Parquet (append) to a TEST destination on S3

Example (EMR step):
  spark-submit --deploy-mode cluster s3://teo-nyc-taxi/scripts/emr-jobs/emr_process_trip_data.py \
    --cab_type yellow --year 2024 --month 1 \
    --raw_prefix s3://teo-nyc-taxi/raw/ \
    --dest_prefix s3://teo-nyc-taxi/processed/emr/trip_data/
"""

import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cab_type", required=True, choices=["yellow", "green", "fhv"])
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--month", required=True, type=int)
    ap.add_argument("--raw_prefix", default="s3://teo-nyc-taxi/raw/")
    ap.add_argument("--dest_prefix", default="s3://teo-nyc-taxi/processed/emr/trip_data/")
    ap.add_argument("--coalesce", type=int, default=10)
    return ap.parse_args()

def standardize_timestamp_cols(df, cab_type: str):
    """
    Make sure we have 'pickup_datetime' and 'dropoff_datetime' columns.
    - yellow: tpep_* -> pickup/dropoff
    - green : lpep_* -> pickup/dropoff
    - fhv   : usually already has pickup_datetime/dropoff_datetime (if missing we’ll still proceed)
    """
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
    # fhv: leave as-is; many fhv files already use pickup_datetime/dropoff_datetime
    return df

def add_missing_fhv_cols(df):
    """Add null-typed columns so fhv aligns with yellow/green base schema."""
    needs = {
        "trip_distance": T.DoubleType(),
        "fare_amount": T.DoubleType(),
        "passenger_count": T.IntegerType(),
    }
    for c, t in needs.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
    return df

def lower_cols(df):
    return df.toDF(*[c.lower() for c in df.columns])

def main():
    args = parse_args()
    if args.month < 1 or args.month > 12:
        raise ValueError("month must be between 1 and 12")

    raw_path = f"{args.raw_prefix}{args.cab_type}_tripdata_{args.year}-{args.month:02d}.parquet"
    dest_path = args.dest_prefix.rstrip("/") + "/"

    spark = (
        SparkSession.builder
        .appName(f"emr_process_trip_data_{args.cab_type}_{args.year}_{args.month:02d}")
        .getOrCreate()
    )

    # Write behavior to mirror Glue tuning
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("parquet.enable.dictionary", "false")
    spark.conf.set("parquet.writer.version", "v1")

    print(f"[INFO] Reading {raw_path}")
    df = spark.read.parquet(raw_path)

    # Canonicalize timestamp columns based on cab type
    df = standardize_timestamp_cols(df, args.cab_type)

    # FHV may miss some numeric fields present in taxi datasets
    if args.cab_type == "fhv":
        df = add_missing_fhv_cols(df)

    # Ensure pickup_datetime exists for filtering/partitioning
    if "pickup_datetime" not in df.columns:
        raise ValueError("pickup_datetime column not found after standardization.")

    # Filter to requested month (guard against broad inputs)
    df = df.filter(
        (F.year(F.col("pickup_datetime")) == F.lit(int(args.year))) &
        (F.month(F.col("pickup_datetime")) == F.lit(int(args.month)))
    )

    # Add partition columns and typed constants
    df = (
        df.withColumn("cab_type", F.lit(args.cab_type).cast(T.StringType()))
          .withColumn("year", F.lit(args.year).cast(T.IntegerType()))
          .withColumn("month", F.lit(args.month).cast(T.IntegerType()))
          .withColumn("day", F.dayofmonth(F.col("pickup_datetime")).cast(T.IntegerType()))
    )

    # Lowercase columns & drop metadata if present
    df = lower_cols(df)
    for m in ("_metadata", "_spark_metadata"):
        if m in df.columns:
            df = df.drop(m)

    print("[INFO] Normalized schema:")
    df.printSchema()

    # Write partitioned Parquet (append) to the TEST destination
    print(f"[INFO] Writing to {dest_path}")
    (df.repartition(args.coalesce, "cab_type", "year", "month", "day")
       .write
       .mode("append")
       .partitionBy("cab_type", "year", "month", "day")
       .parquet(dest_path))

    print("[DONE] Write complete.")
    spark.stop()

if __name__ == "__main__":
    main()

