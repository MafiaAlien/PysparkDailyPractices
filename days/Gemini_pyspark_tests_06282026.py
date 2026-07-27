# Pure English production pipeline setup for Streaming Watermarks
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import shutil


class RealTimeOrderProcessor:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Active Session.
        Decoupled state architecture for high-availability stream tracking.
        """
        self.spark = spark_session

    def process_stream_dsl(self, streaming_df):
        """
        Track 1: PySpark Structured Streaming DataFrame DSL Path.

        Requirements:
        1. Apply a 20-minute watermark threshold on the chronological "event_time" column.
        2. Group the micro-batches by a 10-minute tumbling window on "event_time".
        3. Aggregate sum("amount") as "total_sales" and count("order_id") as "order_count".
        4. Round "total_sales" to 2 decimal places to maintain currency schema hygiene.
        """
        # ================== START DSL STREAM EDITING ==================

        optimized_dsl_df = streaming_df\
            .withWatermark("event_time", "20 minutes")\
            .groupBy(F.window("event_time", "10 minutes"))\
            .agg(
                F.round(F.sum('amount'), 2).alias("total_sales"),
                F.count('order_id').alias("order_count"))

        # ================== END DSL STREAM EDITING ====================
        return optimized_dsl_df

    def process_stream_sql(self, streaming_df):
        """
        Track 2: Pure Spark SQL Path over Streaming Temporary View.

        Requirements:
        1. Watermarking MUST be initialized on the streaming DataFrame via DSL first 
           before view registration (Spark SQL catalog constraint). Apply the 20-minute watermark.
        2. Register the watermarked relation into the catalog view: .createOrReplaceTempView("view_orders").
        3. Execute a self.spark.sql() query grouping by the native streaming SQL expression:
           window(event_time, "10 minutes").
        4. Capture window, and map ROUND(SUM(amount), 2) AS total_sales,
           and COUNT(order_id) AS order_count.
        """
        # ================== START SQL STREAM EDITING ==================
        stream_df_with_watermark = streaming_df.withWatermark(
            "event_time", "20 minutes")
        stream_df_with_watermark.createOrReplaceTempView("view_orders")

        sql_query = """
        SELECT
            window(event_time, '10 minutes') AS time_window,
            ROUND(SUM(amount), 2) AS total_sales,
            COUNT(order_id) AS order_count
        FROM view_orders
        GROUP BY window(event_time, '10 minutes')
        """
        # ================== END SQL STREAM EDITING ====================
        return self.spark.sql(sql_query)


# ==============================================================================
# Local Execution Test Scaffold (Micro-batch Simulation Reactor)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day11_Streaming_Watermark_DualTrack") \
        .master("local[*]") \
        .getOrCreate()

    # Enforce strict ERROR log filtering per custom specifications
    spark.sparkContext.setLogLevel("ERROR")

    # Define explicit ingest schema
    order_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("amount", DoubleType(), True)
    ])

    # Setup a local disk workspace directory to simulate directory-source streaming ingestion
    mock_landzone = "./mock_stream_landzone"
    if os.path.exists(mock_landzone):
        shutil.rmtree(mock_landzone)
    os.makedirs(mock_landzone)

    # Spawn the reactive unbound stream monitor
    streaming_input_df = spark.readStream \
        .schema(order_schema) \
        .json(mock_landzone)

    processor = RealTimeOrderProcessor(spark)

    # Fire up Path A and Path B compilers to register logic trees into Catalyst
    dsl_stream_df = processor.process_stream_dsl(streaming_input_df)
    sql_stream_df = processor.process_stream_sql(streaming_input_df)

    print("\n" + "="*80)
    print("🌊 PYSPARK STRUCTURED STREAMING REACTOR COMPILED SUCCESSFULLY")
    print("="*80)
    print(
        f"[INFO] Streaming input is now listening to local file directory: {mock_landzone}")
    print("[INFO] Both DSL and SQL execution plan trees are actively registered inside Catalyst.")
    print("[INFO] Ready for streaming micro-batch emission simulation or code validation review.")
    print("="*80 + "\n")

    # Graceful shutdown configuration
    spark.stop()
    if os.path.exists(mock_landzone):
        shutil.rmtree(mock_landzone)
