from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class PlanOptimizationPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Session.
        Decoupled framework for high-throughput dual-track execution plan auditing.
        """
        self.spark = spark_session

    def process_and_optimize_dsl(self, events_df, devices_df):
        """
        Track 1: PySpark DataFrame DSL Implementation.

        Requirements:
        1. Cache/Persist the incoming "events_df" immediately.
        2. Force a Broadcast Hash Join (BHJ) on "device_id" using F.broadcast(devices_df).
        3. Group by "device_model" and calculate aggregations:
           - Average of "signal_strength" as "avg_signal" (rounded to 2 decimal places).
           - Approximate distinct count of "event_id" as "total_events" using HLL algorithm.
        """
        # ================== START EDITING YOUR DSL OPTIMIZATION ==================

        events_df.cache()

        optimized_dsl_df = events_df.join(
            F.broadcast(devices_df),
            on='device_id',
            how="inner"
        ).groupBy('device_model')\
         .agg(
            F.round(F.avg(F.col('signal_strength')), 2).alias('avg_signal'),
            F.approx_count_distinct('event_id').alias('total_events')
        )

        # ================== END EDITING YOUR DSL OPTIMIZATION ====================
        return optimized_dsl_df

    def process_and_optimize_sql(self, events_df, devices_df):
        """
        Track 2: Pure Spark SQL Implementation.

        Requirements:
        1. Register both dataframes into Spark's Session Catalog as Temp Views:
           "view_events" and "view_devices".
        2. Invoke the native SQL command to cache the hot table: "CACHE TABLE view_events".
        3. Write a standard SQL block incorporating the optimizer hint syntax:
           "/*+ BROADCAST(d) */" to forcefully trigger broadcast optimization on the dim alias.
        4. Apply ROUND(AVG(...), 2) and APPROX_COUNT_DISTINCT(...) metrics mapping.
        """
        # ================== START EDITING YOUR SQL OPTIMIZATION ==================

        events_df.createOrReplaceTempView("view_events")
        devices_df.createOrReplaceTempView("view_devices")

        # 2. Trigger explicit memory cache using native Spark SQL command
        self.spark.sql("CACHE TABLE view_events")

        # 3. Execute optimized query leveraging standard SQL optimizer hints
        # Notice the /*+ BROADCAST(d) */ hint block - this is the physical magic!
        optimized_sql = """
            SELECT
                /*+ BROADCAST(d) */
                d.device_model,
                ROUND(AVG(e.signal_strength), 2) AS avg_signal,
                APPROX_COUNT_DISTINCT(e.event_id) AS total_events
            FROM view_events e
            INNER JOIN view_devices d 
            ON e.device_id = d.device_id
            GROUP BY d.device_model
        """

        # 4. Fire the engine and return stateless projection
        return self.spark.sql(optimized_sql)

        # ================== END EDITING YOUR SQL OPTIMIZATION ====================
        return optimized_sql_df


# ==============================================================================
# Local Execution Test Scaffold (Dual-Track Plan Auditing Engine)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day10_Dual_Track_Optimization_Audit") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Ingestion Core Schemas
    events_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("signal_strength", DoubleType(), True)
    ])

    devices_schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("device_model", StringType(), True)
    ])

    # Ingest Raw Mock Distributed Partitions
    raw_events = [
        ("evt_alpha",  "dev_01", -45.5),
        ("evt_beta",   "dev_02", -60.2),
        ("evt_gamma",  "dev_01", -42.0),
        ("evt_delta",  "dev_03", -88.9),
        ("evt_epsilon", "dev_02", -55.1)
    ]

    raw_devices = [
        ("dev_01", "Edge_Pro_X"),
        ("dev_02", "Gateway_Lite_M"),
        ("dev_03", "Industrial_Hub_V")
    ]

    events_df = spark.createDataFrame(raw_events, schema=events_schema)
    devices_df = spark.createDataFrame(raw_devices, schema=devices_schema)

    # Instantiate the dual-track pipeline engine
    pipeline = PlanOptimizationPipeline(spark)

    print("\n" + "="*80)
    print("🔥 EXECUTION PATH A: PIPELINE DATA RUNNING VIA PYSPARK DSL")
    print("="*80)
    result_dsl = pipeline.process_and_optimize_dsl(events_df, devices_df)
    result_dsl.show(truncate=False)
    print("--> DSL Catalyst Physical Blueprint:")
    result_dsl.explain(mode="formatted")

    print("\n" + "="*80)
    print("🚀 EXECUTION PATH B: PIPELINE DATA RUNNING VIA PURE SPARK SQL")
    print("="*80)
    result_sql = pipeline.process_and_optimize_sql(events_df, devices_df)
    result_sql.show(truncate=False)
    print("--> SQL Catalyst Physical Blueprint:")
    result_sql.explain(mode="formatted")

    spark.stop()
