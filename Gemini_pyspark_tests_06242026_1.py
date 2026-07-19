from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.types import *


class CustomerSnapshotPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Session.
        Decoupled architecture for stateless enterprise data warehousing.
        """
        self.spark = spark_session

    def upsert_customer_profiles(self, history_df, delta_df):
        """
        Core Logic: Simulate an Upsert/Merge (SCD Type 1 Update) using core Spark DSL.

        Requirements:
        1. Combine the historical customer records (history_df) and the incoming 
           daily incremental batch (delta_df) into a single processing stream.
           Hint: Use DataFrame union() or unionByName().
        2. Define a Window Specification partitioned by the business key "customer_id",
           ordered by the operational timestamp "updated_at" in DESCENDING order.
        3. Apply F.row_number().over() to generate a chronological rank named "rank_id".
           Since the window is ordered by updated_at DESC, the newest record for any 
           duplicate customer_id will always receive a rank_id of 1.
        4. Filter the dataframe to strictly keep rows where rank_id == 1. This drops 
           the old historical rows that have been overridden by the new delta updates.
        5. Drop the helper column "rank_id" to achieve absolute Schema Hygiene.
        """
        # ================== START EDITING YOUR UPSERT LOGIC ==================
        window_spec = W.partitionBy(
            'customer_id').orderBy(F.desc('updated_at'))
        final_snapshot_df = history_df\
            .union(delta_df)\
            .select(
                '*',
                F.row_number().over(window_spec).alias('rank_id')
            )\
            .where(F.col('rank_id') == 1)\
            .drop('rank_id')\
            .select(
                'customer_id',
                'email',
                'tier',
                'updated_at'
            )

        # ================== END EDITING YOUR UPSERT LOGIC ====================
        return final_snapshot_df


# ==============================================================================
# Local Execution Test Scaffold (Stateless Data Injections)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day7_Incremental_Delta_Upsert_Test") \
        .master("local[*]") \
        .getOrCreate()

    # 1. Setup Base Customer Schema
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        # VIP Tiers: Bronze, Silver, Gold
        StructField("tier", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])

    # 2. Mock Data A: Current historical base snapshot in our Lakehouse (Silver Layer)
    historical_base_data = [
        ("cust_001", "alice@gmail.com", "Bronze", "2026-06-20 09:00:00"),
        ("cust_002", "bob@gmail.com",   "Silver", "2026-06-21 10:30:00"),
        ("cust_003", "charlie@com",      "Gold",   "2026-06-22 14:15:00")
    ]

    history_df = spark.createDataFrame(historical_base_data, schema=customer_schema) \
                      .withColumn("updated_at", F.to_timestamp("updated_at"))

    print("--- 1. Current Historical Base Snapshot Table (Silver) ---")
    history_df.show(truncate=False)

    # 3. Mock Data B: Incoming T+1 incremental daily batch updates (Bronze layer Ingestion)
    # Notice:
    # - cust_001 upgraded their tier from Bronze to Gold! (Update)
    # - cust_004 is a completely brand new customer registering today! (Insert)
    daily_delta_data = [
        ("cust_001", "alice@gmail.com", "Gold",
         "2026-06-24 08:00:00"),  # Tier Update!
        ("cust_004", "david@gmail.com", "Bronze",
         "2026-06-24 11:45:00")  # New Record Insert!
    ]

    delta_df = spark.createDataFrame(daily_delta_data, schema=customer_schema) \
                    .withColumn("updated_at", F.to_timestamp("updated_at"))

    print("--- 2. Incoming Daily Incremental Delta Batch (Bronze) ---")
    delta_df.show(truncate=False)

    # 4. Fire up the processing machinery
    pipeline = CustomerSnapshotPipeline(spark)
    latest_snapshot_df = pipeline.upsert_customer_profiles(
        history_df, delta_df)

    print("--- 3. Final Merged & Upserted Customer Profiles Snapshot (Gold) ---")
    latest_snapshot_df.printSchema()
    latest_snapshot_df.show(truncate=False)

    spark.stop()
