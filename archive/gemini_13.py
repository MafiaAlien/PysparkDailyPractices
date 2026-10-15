# Pure English production pipeline setup for Rolling Window Manipulation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class ClickstreamManipulationPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Active Session.
        Engineered for high-volume scrolling distinct metrics tracking.
        """
        self.spark = spark_session

    def calculate_rolling_uv_dsl(self, clickstream_df):
        """
        Track 1: PySpark DataFrame DSL Path.

        Requirements:
        1. Leverage Spark 3.0+'s sliding window capability inside .groupBy().
           - Use F.window() on the "timestamp" column.
           - Set the window duration to "7 days".
           - Set the sliding step to "1 day".
        2. Group the data by BOTH "url" and the newly spawned sliding window block.
        3. Perform a distinct count aggregation over "user_id", naming the output column "rolling_uv".
        4. Sort the final output by "url" ascending, and window.start ascending for presentation hygiene.
        """
        # ================== START DSL DATA MANIPULATION ==================

        optimized_dsl_df = (
            clickstream_df
            .groupBy(
                F.col("url"),
                F.window('timestamp', '7 day', '1 days')
            )
            .agg(F.approx_count_distinct('user_id').alias('rolling_uv'))
            .select(
                F.col("url"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("rolling_uv"),
            )
            .orderBy("url", "window_start")
        )

        # ================== END DSL DATA MANIPULATION ====================
        return optimized_dsl_df

    def calculate_rolling_uv_sql(self, clickstream_df):
        """
        Track 2: Pure Spark SQL Path over Temporary View.

        Requirements:
        1. Register the incoming dataframe into the session catalog as "view_clickstream".
        2. Write a native Spark SQL string executing group-by window logic:
           "GROUP BY url, window(timestamp, '7 days', '1 day')"
        3. Extract url, window.start AS window_start, window.end AS window_end, 
           and COUNT(DISTINCT user_id) AS rolling_uv.
        4. Apply alphabetical and chronological order sorting.
        """
        # ================== START SQL DATA MANIPULATION ==================
        clickstream_df.createOrReplaceTempView("view_clickstream")
        query = """
            SELECT 
                url,
                window.start AS window_start,
                window.end AS window_end,
                COUNT(DISTINCT user_id) AS rolling_uv
            FROM view_clickstream
            GROUP BY url, window(timestamp, '7 days', '1 day')
            ORDER BY url ASC, window_start ASC
        """

        # ================== END SQL DATA MANIPULATION ====================
        return self.spark.sql(query)


# ==============================================================================
# Local Execution Test Scaffold (Chronological Timeline Simulator)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day13_Rolling_Distinct_UV_Manipulation") \
        .master("local[*]") \
        .getOrCreate()

    # Mute infrastructure logs to enforce console purity
    spark.sparkContext.setLogLevel("ERROR")

    # Ingestion Clickstream Schema
    click_schema = StructType([
        StructField("url", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Ingest chronological click events stretching across multiple days
    # This mock data simulates users visiting pages between June 20 and June 25
    raw_clickstream = [
        ("/home",    "user_adam",  "2026-06-20 10:00:00"),
        # Same user, separate day
        ("/home",    "user_adam",  "2026-06-21 11:30:00"),
        ("/home",    "user_eve",   "2026-06-21 14:15:00"),  # New user lands
        ("/profile", "user_bob",   "2026-06-22 09:05:00"),
        # Repeating user inside the window
        ("/home",    "user_eve",   "2026-06-24 18:00:00"),
        # New user expanding the count
        ("/home",    "user_dawn",  "2026-06-25 04:20:00"),
        ("/profile", "user_bob",   "2026-06-25 22:10:00")
    ]

    # Convert strings to actual timestamp types during dataframe creation
    input_df = spark.createDataFrame(raw_clickstream, schema=click_schema) \
                    .withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

    print("\n" + "="*80)
    print("📋 RAW INGESTED CLICKSTREAM TRAFFIC (BRONZE LAYER)")
    print("="*80)
    input_df.show(truncate=False)

    # Initialize the high-performance manipulation reactor
    pipeline = ClickstreamManipulationPipeline(spark)

    print("\n" + "="*80)
    print("🔥 PATH A RESULTS: ROLLING 7-DAY DISTINCT UV VIA DSL SLIDING WINDOW")
    print("="*80)
    dsl_metrics = pipeline.calculate_rolling_uv_dsl(input_df)
    dsl_metrics.show(truncate=False)

    print("\n" + "="*80)
    print("🚀 PATH B RESULTS: ROLLING 7-DAY DISTINCT UV VIA PURE SPARK SQL")
    print("="*80)
    sql_metrics = pipeline.calculate_rolling_uv_sql(input_df)
    sql_metrics.show(truncate=False)

    spark.stop()
