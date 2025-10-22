import sys
import time
from datetime import datetime
import traceback
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, to_timestamp, lit, dayofmonth, year, month
from pyspark.sql.types import StringType, IntegerType, DoubleType

# === Initialize Spark Context ===
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ✅ Add flat .zip file (no /python folder inside)
sc.addPyFile("s3://teo-nyc-taxi/scripts/glue-jobs/libs/pipeline_logger_flat.zip")

# debug
import sys
print("🔍 PYTHONPATH:", sys.path)

# ✅ Now import your logger
from pipeline_logger import log_pipeline_stage


# ✅ Import after adding the PyFile
from pipeline_logger import log_pipeline_stage

# ✅ Timestamp
TIMESTAMP = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')

# ✅ Job arguments
try:
    args = getResolvedOptions(sys.argv, ['CAB_TYPE', 'YEAR', 'MONTH'])
    CAB_TYPE = args['CAB_TYPE'].lower()
    YEAR = args['YEAR']
    MONTH = args['MONTH']
except Exception as e:
    print(f"❌ Missing arguments: {str(e)}")
    sys.exit(1)

# ✅ Pipeline ID
pipeline_id = f"{CAB_TYPE}_tripdata_{YEAR}-{MONTH}"
timestamp = datetime.utcnow().isoformat()

# ✅ Log job start
log_pipeline_stage(
    pipeline_id=pipeline_id,
    stage="transform",
    pipeline_name="nyc_taxi_batch",
    pipeline_type="batch",
    executor="glue",
    status="STARTED",
    timestamp=timestamp,
    details={"job": "glue_process_taxi_data"}
)

# ✅ S3 paths
RAW_DATA_PATH = f"s3://teo-nyc-taxi/raw/{CAB_TYPE}_tripdata_{YEAR}-{MONTH}.parquet"
PROCESSED_DATA_PATH = "s3://teo-nyc-taxi/processed/trip_data/"

# ✅ Local + remote log paths (optional)
LOG_S3_PATH = f"s3://teo-nyc-taxi/logs/{TIMESTAMP}_glue_nyc_taxi_processing.log"
LOCAL_LOG_PATH = f"/tmp/{TIMESTAMP}_glue_nyc_taxi_processing.log"

# ✅ Local logger (optional)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOCAL_LOG_PATH), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
def log_event(level, message, **kwargs):
    logger.log(getattr(logging, level.upper()), {
        "timestamp": datetime.utcnow().isoformat(),
        "message": message,
        "details": kwargs
    })

# ✅ Initialize Glue Context
start_time = time.time()
# sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
log_event("INFO", "✅ Glue Spark session started.")

# ✅ Read raw data
try:
    df = spark.read.parquet(RAW_DATA_PATH)
    log_event("INFO", "✅ Loaded raw data", path=RAW_DATA_PATH, count=df.count())
except Exception as e:
    log_pipeline_stage(
        pipeline_id=pipeline_id,
        stage="transform",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="glue",
        status="FAILED",
        timestamp=datetime.utcnow().isoformat(),
        s3_input=RAW_DATA_PATH,
        details={"error": str(e), "trace": traceback.format_exc()}
    )
    sys.exit(1)

# ✅ Standardize timestamp columns
if CAB_TYPE == "yellow":
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
elif CAB_TYPE == "green":
    df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

# ✅ Add missing columns for FHV
if CAB_TYPE == "fhv":
    df = df.withColumn("trip_distance", lit(None).cast(DoubleType())) \
           .withColumn("fare_amount", lit(None).cast(DoubleType())) \
           .withColumn("passenger_count", lit(None).cast(IntegerType()))

# ✅ Filter by month
try:
    df = df.filter(
        (year(col("pickup_datetime")) == int(YEAR)) & 
        (month(col("pickup_datetime")) == int(MONTH))
    )
    log_event("INFO", "✅ Filtered to selected month")
except Exception as e:
    log_pipeline_stage(
        pipeline_id=pipeline_id,
        stage="transform",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="glue",
        status="FAILED",
        timestamp=datetime.utcnow().isoformat(),
        details={"error": str(e), "trace": traceback.format_exc()}
    )
    sys.exit(1)

# ✅ Enrich with partition columns
try:
    df_enriched = df.withColumn("year", lit(YEAR).cast(IntegerType())) \
                    .withColumn("month", lit(MONTH).cast(IntegerType())) \
                    .withColumn("day", dayofmonth(col("pickup_datetime")).cast(IntegerType())) \
                    .withColumn("cab_type", lit(CAB_TYPE).cast(StringType()))
except Exception as e:
    log_pipeline_stage(
        pipeline_id=pipeline_id,
        stage="transform",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="glue",
        status="FAILED",
        timestamp=datetime.utcnow().isoformat(),
        details={"error": str(e), "trace": traceback.format_exc()}
    )
    sys.exit(1)

# ✅ Final formatting
df_enriched = df_enriched.toDF(*[col.lower() for col in df_enriched.columns])
for colname in ["_metadata", "_spark_metadata"]:
    if colname in df_enriched.columns:
        df_enriched = df_enriched.drop(colname)

# ✅ Write partitioned output
try:
    df_enriched.repartition(10, "cab_type", "year", "month", "day") \
        .write \
        .option("spark.sql.parquet.compression.codec", "snappy") \
        .option("parquet.enable.dictionary", "false") \
        .option("parquet.writer.version", "v1") \
        .mode("append") \
        .partitionBy("cab_type", "year", "month", "day") \
        .parquet(PROCESSED_DATA_PATH)

    log_pipeline_stage(
        pipeline_id=pipeline_id,
        stage="transform",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="glue",
        status="SUCCEEDED",
        timestamp=datetime.utcnow().isoformat(),
        record_count=df_enriched.count(),
        s3_output=PROCESSED_DATA_PATH,
        details={"partition_by": ["cab_type", "year", "month", "day"]}
    )
except Exception as e:
    log_pipeline_stage(
        pipeline_id=pipeline_id,
        stage="transform",
        pipeline_name="nyc_taxi_batch",
        pipeline_type="batch",
        executor="glue",
        status="FAILED",
        timestamp=datetime.utcnow().isoformat(),
        details={"error": str(e), "trace": traceback.format_exc()}
    )
    sys.exit(1)

# ✅ Stop Spark session
log_event("INFO", "✅ Completed Glue job")
spark.stop()
