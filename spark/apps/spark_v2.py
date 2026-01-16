from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "kafka:9092"
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"

print("Starting Spark Processor...")
spark = SparkSession.builder \
    .appName("HealthcareRealTimeMonitor") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- DEFINE SCHEMA ---
schema = StructType() \
    .add("event_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("patient_id", StringType()) \
    .add("patient_name", StringType()) \
    .add("age", IntegerType()) \
    .add("gender", StringType()) \
    .add("bed_id", StringType()) \
    .add("vital_id", StringType()) \
    .add("vital_name", StringType()) \
    .add("value", FloatType()) \
    .add("units", StringType())

# --- READ STREAM ---
print("Connecting to Kafka...")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "patient_vitals") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# LOGIC: DETECT HIGH Heart rate
alerts = parsed_stream.withColumn(
    "alert_status",
    when((col("vital_name") == "Heart rate") & (col("value") > 100), "CRITICAL: HIGH HR")
    .when((col("vital_name") == "Heart rate") & (col("value") < 50), "CRITICAL: LOW HR")
    .otherwise("NORMAL")
).filter(col("alert_status") != "NORMAL")

#OUTPUT TO CONSOLE 
query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

#OUTPUT TO MINIO 
storage_query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "s3a://processed-data/vitals_logs/") \
    .option("checkpointLocation", "s3a://processed-data/checkpoints/") \
    .start()

spark.streams.awaitAnyTermination()