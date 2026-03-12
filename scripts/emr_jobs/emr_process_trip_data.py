#!/usr/bin/env python3
"""
EMR PySpark job — process trip data (base stage, no zone joins)

Version: 3 - Iceberg Implementation (Partition Evolution and ACID compliance) <--- HIGHLIGHTED
Version: 2 - Partitioning Fix (cab_type/day removed, now year/month only)
Version: 1 - Optimization (AQE, Repartition fix, Python refactor)
Version: 0 baseline

Mirrors the Glue job behavior with 3 cab types (yellow/green/fhv):
- Reads a single month from S3 raw: {cab_type}_tripdata_{YEAR}-{MM}.parquet
- Normalizes pickup/dropoff timestamp column names by cab type
- Adds missing columns for FHV to keep a stable schema
- Filters to the requested YEAR/MONTH
- Adds partitions: cab_type, year, month, day (from pickup_datetime)
- Lower-cases all column names, drops metadata columns
- Writes partitioned Parquet (append) to a TEST destination on S3

Example (EMR step):
  spark-submit --deploy-mode cluster s3://***/scripts/emr-jobs/emr_process_trip_data.py \
    --cab_type yellow --year 2025 --month 1 \
    --raw_prefix s3://***/raw/ \
    --dest_prefix s3://***/processed/emr/trip_data_iceberg/ <--- HIGHLIGHTED: New Iceberg Destination
"""

import argparse
from pyspark.sql import SparkSession, functions as F, types as T
import time


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cab_type", required=True, choices=["yellow", "green", "fhv"])
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--month", required=True, type=int)
    ap.add_argument("--raw_prefix", default="s3://***/raw/")
    ap.add_argument("--dest_prefix", default="s3://***/processed/emr/trip_data_iceberg/") # HIGHLIGHTED: Default updated for Iceberg
    ap.add_argument("--coalesce", type=int, default=10)
    return ap.parse_args()

def standardize_timestamp_cols(df, cab_type: str):
    """
    Make sure we have 'pickup_datetime' and 'dropoff_datetime' columns.
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


def main():
    args = parse_args()
    if args.month < 1 or args.month > 12:
        raise ValueError("month must be between 1 and 12")

    raw_path = f"{args.raw_prefix}{args.cab_type}_tripdata_{args.year}-{args.month:02d}.parquet"
    dest_path = args.dest_prefix.rstrip("/") + "/"

# --- START TIMING BLOCK (S-1.2.6.8) ---
    start_time = time.time()
    spark = (
        SparkSession.builder
        .appName(f"emr_process_trip_data_{args.cab_type}_{args.year}_{args.month:02d}_ICEBERG_V3")
        # REMOVED: .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2")
        .getOrCreate()
    )

    # --- S-1.2.8.1: ICEBERG/GLUE CATALOG CONFIGURATION (Transferred from JSON) ---
    # These properties define the Glue Catalog as the Iceberg metastore
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", dest_path)
    spark.conf.set("spark.sql.catalog.glue_catalog.type", "hive")
    spark.conf.set("spark.sql.catalog.default", "glue_catalog")

    # Write behavior to mirror Glue tuning
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.adaptive.enabled", "true") # V1: Enable AQE
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

    # --- ADD PARTITION COLUMNS AND TYPED CONSTANTS ---
    df = (
        # V2 Partitioning Fix: Restore cab_type as a column for analytical grouping
        df.withColumn("cab_type", F.lit(args.cab_type).cast(T.StringType()))
        .withColumn("year", F.lit(args.year).cast(T.IntegerType()))
        .withColumn("month", F.lit(args.month).cast(T.IntegerType()))
        # V2 Partitioning Fix: Day is omitted to reduce Small Files Problem
    )


    # Lowercase columns (Optimized: uses Spark expressions, eliminates Python overhead on Driver) --V2 Enhancements
    df = df.select(*[F.col(c).alias(c.lower()) for c in df.columns])

# Drop metadata if present (CORRECTED LOOP SYNTAX)
    for m in ("_metadata", "_spark_metadata"):
        if m in df.columns:
            df = df.drop(m)

    print("[INFO] Normalized schema:")
    df.printSchema()

# ... (Continuing inside the main() function) ...

    # --- S-1.2.8.3: ICEBERG WRITER LOGIC ---
    print(f"[INFO] Writing to Iceberg table: trip_data_iceberg at {dest_path}")
    (df
       .write
       .format("iceberg") # Instructs Spark to use the Iceberg library
       .mode("overwrite") # Use 'overwrite' for the first run to create the table structure
       .partitionBy("year", "month") # V2 Partitioning Fix implemented here
       .saveAsTable("trip_data_iceberg") # Registers the table in the Glue Catalog
    )

    # --- END TIMING BLOCK (S-1.2.6.8) ---
    end_time = time.time()
    print(f"[BENCHMARK] Total Optimized Job Time: {end_time - start_time:.3f} seconds")
    # --- END TIMING BLOCK ---

    print("[DONE] Write complete.")
    spark.stop()

if __name__ == "__main__":
    main()

