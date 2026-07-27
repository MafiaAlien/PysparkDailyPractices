from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.types import *


class FinancialTuningPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Session.
        Decoupled architecture for stateless enterprise data processing.
        """
        self.spark = spark_session

    def calculate_rolling_spend(self, df):
        """
        Core Logic: Optimize Window Frame to eliminate Memory Spill risks.

        Requirements:
        1. Convert the "order_time" column into a LongType unix timestamp (in seconds)
           named "time_seconds". This is critical because Spark's rangeBetween() 
           requires a numeric sequence to evaluate chronological sliding distances.
        2. Define an optimized Window Specification:
           - Partition by "user_id".
           - Order by "time_seconds" in ascending order.
           - Define a sliding range frame that spans exactly the PAST 7 DAYS up to the current row.
           - Hint: 7 days in seconds = 7 * 24 * 3600 = 604800 seconds. 
             Use .rangeBetween(-604800, W.currentRow).
        3. Compute the rolling sum of "amount", round it to 2 decimal places, 
           and alias it as "rolling_7d_spend".
        4. Drop the helper column "time_seconds" before returning to preserve Schema Hygiene.
        """
        # ================== START EDITING YOUR OPTIMIZATION LOGIC ==================
        window_spec_7days_rolling = W\
            .partitionBy('user_id')\
            .orderBy(F.col("time_seconds").asc())\
            .rangeBetween(-604800, 0)

        optimized_df = df\
            .select(
                "user_id",
                'order_time',
                F.unix_timestamp(F.col('order_time')).alias('time_seconds'),
                'amount',
            ).withColumn(
                'rolling_7d_spend',
                F.sum(F.col("amount"))
                .over(window_spec_7days_rolling)
            )\
            .drop('time_seconds')\
            .select(
                'user_id',
                'order_time',
                'amount',
                'rolling_7d_spend'
            )

        # ================== END EDITING YOUR OPTIMIZATION LOGIC ====================
        return optimized_df


# ==============================================================================
# Local Execution Test Scaffold (Stateless Data Injections)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day6_Rolling_Window_Spill_Tuning") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    # Mock Raw VIP High-Frequency Transaction Streams (Bronze Layer)
    raw_orders = [
        # User 101: Purchases spread across multiple days
        ("usr_101", "2026-06-01 10:00:00", 100.0),
        ("usr_101", "2026-06-02 12:00:00", 50.0),  # Within 7 days of order 1
        # Cumulative: 100 + 50 + 200 = 350
        ("usr_101", "2026-06-05 14:00:00", 200.0),
        # Expired previous window! Should only sum 80.0
        ("usr_101", "2026-06-15 09:00:00", 80.0),

        # User 102: Simultaneous orders or rapid consecutive streams
        ("usr_102", "2026-06-20 18:00:00", 500.0),
        ("usr_102", "2026-06-22 19:00:00", 300.0)  # Cumulative: 500 + 300 = 800
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("order_time", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])

    input_df = spark.createDataFrame(raw_orders, schema=schema) \
                    .withColumn("order_time", F.to_timestamp("order_time"))

    print("--- 1. Raw Transaction Input Streams (Bronze Layer) ---")
    input_df.printSchema()
    input_df.show(truncate=False)

    # Initialize the high-performance tuning reactor
    pipeline = FinancialTuningPipeline(spark)
    output_df = pipeline.calculate_rolling_spend(input_df)

    print("--- 2. Final Spill-Free Rolling Summary Table (Gold Fact) ---")
    output_df.printSchema()
    
    output_df.show(truncate=False)

    spark.stop()
