import os
import shutil
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class IoTSensorMetricsPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency injection of SparkSession.
        Ensures Separation of Concerns (Stateless Processing).
        """
        self.spark = spark_session

    def process_and_aggregate(self, df):
        """
        Core Logic 1: Transform & Aggregate metrics
        Requirements:
        1. Extract the date from "timestamp" as a String in "yyyy-MM-dd" format, 
           naming this new column "date".
        2. Filter out anomalous rows where "reading" is greater than 120.0 
           (malfunctioning sensors) or "reading" is negative.
        3. Group by "sensor_type", "date", and "device_id".
        4. Calculate the average reading, rounded to 2 decimal places, 
           and alias it as "avg_reading".
        """
        # ================== START EDITING YOUR TRANSFORMATION LOGIC ==================
        aggregated_df = df\
            .select(
                "*",
                F.to_date("timestamp", "yyyy-MM-dd").alias("date"),
            )\
            .where(
                (F.col("reading") <= 120.0) | (F.col('reading') >= 0.0)
            )\
            .groupBy(
                "sensor_type",
                "date",
                "device_id")\
            .agg(
                F.round(F.avg("reading"), 2).alias("avg_reading")
            )
        # ================== END EDITING YOUR TRANSFORMATION LOGIC ====================
        return aggregated_df

    def optimize_and_write(self, df, output_path):
        """
        Core Logic 2: Prevent the Small File Problem during partitioned writing.

        Why this is critical:
        If we directly call df.write.partitionBy("sensor_type", "date").save(), 
        Spark will write at least one file per partition directory for *every* executor task.
        With hundreds of tasks, this causes the disastrous "Small File Problem".

        Requirements:
        1. Repartition the DataFrame by the exact partitioning columns: "sensor_type" and "date".
           This forces Spark to shuffle the data so that all records for a specific 
           partition directory are handled by a single dedicated task, resulting 
           in exactly ONE neat Parquet file per partition folder.
        2. Write the optimized DataFrame out as Parquet format.
        3. Partition the output on disk by ["sensor_type", "date"].
        4. Use "overwrite" mode.
        """
        # ================== START EDITING YOUR WRITING LOGIC ==================
        # TODO: Optimize partition layout by repartitioning on partition keys
        # TODO: Write out to parquet with partitionBy("sensor_type", "date")
        df.repartition('sensor_type', 'date')\
            .write\
            .mode('overwrite')\
            .format("parquet")\
            .save(output_path)

        # ================== END EDITING YOUR WRITING LOGIC ====================


# ==============================================================================
# Local Test Scaffold
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day5_Small_File_Mitigation_Test") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create a temporary directory locally to simulate writing to AWS S3
    temp_output_dir = os.path.join(
        tempfile.gettempdir(), "day5_sensor_gold_lake")
    if os.path.exists(temp_output_dir):
        shutil.rmtree(temp_output_dir)

    # Mock Raw IoT Sensor Data streams (Bronze layer)
    raw_data = [
        # Normal rows
        ("device_01", "temperature", "2026-06-22 10:15:00", 35.5),
        ("device_01", "temperature", "2026-06-22 11:20:00", 36.5),
        ("device_02", "temperature", "2026-06-22 09:00:00", 22.1),
        ("device_03", "humidity",    "2026-06-22 15:45:00", 65.2),
        ("device_03", "humidity",    "2026-06-22 16:30:00", 64.8),
        # Anomalous rows (should be filtered)
        # Extreme temperature anomaly
        ("device_01", "temperature", "2026-06-22 10:20:00", 150.0),
        ("device_02", "temperature", "2026-06-22 10:25:00", -10.0),  # Negative anomaly
        # Multi-day data
        ("device_01", "temperature", "2026-06-23 08:30:00", 38.0)
    ]

    schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reading", DoubleType(), True)
    ])

    # Ingesting raw streams
    input_df = spark.createDataFrame(raw_data, schema=schema) \
                    .withColumn("timestamp", F.to_timestamp("timestamp"))

    print("--- 1. Raw Input IoT Stream (Bronze) ---")
    input_df.show(truncate=False)

    # Instantiate the processing pipeline
    pipeline = IoTSensorMetricsPipeline(spark)

    # Run Stage 1: Transformation & Aggregation
    processed_df = pipeline.process_and_aggregate(input_df)
    print("--- 2. Processed & Aggregated Stream (Silver) ---")
    processed_df.show(truncate=False)

    # Run Stage 2: Optimized partitioned writing
    print(
        f"[INFO] Writing optimized files to local simulated Lake: {temp_output_dir}")
    pipeline.optimize_and_write(processed_df, temp_output_dir)

    # Verification: Check file layouts inside the partitioned lake
    print("--- 3. Verifying Output Partition Directories & File Counts ---")

    def scan_recursive(dir_path):
        for root, dirs, files in os.walk(dir_path):
            # We filter out hidden metadata files from Spark
            parquet_files = [f for f in files if f.endswith(
                ".parquet") and not f.startswith(".")]
            if parquet_files:
                relative_path = os.path.relpath(root, dir_path)
                print(
                    f"Partition Folder: [{relative_path}] -> Contains {len(parquet_files)} file(s): {parquet_files}")

    scan_recursive(temp_output_dir)

    # Cleanup temp directory
    if os.path.exists(temp_output_dir):
        shutil.rmtree(temp_output_dir)

    spark.stop()
