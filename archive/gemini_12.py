# Pure English production pipeline setup for Lakehouse Time Travel Audits
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import shutil

class LakehouseAuditPipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Active Session.
        Decoupled engine engineered for ACID transaction log rollbacks.
        """
        self.spark = spark_session

    def audit_price_anomaly_dsl(self, delta_path, historical_version=0):
        """
        Track 1: PySpark DataFrame DSL Pathway.
        
        Requirements:
        1. Read the LATEST (currently corrupted) state of the Delta table from "delta_path".
        2. Read the HISTORICAL pristine state of the Delta table from "delta_path" 
           by enforcing the option("versionAsOf", historical_version).
        3. Inner Join the current DataFrame and the historical DataFrame on "product_id".
        4. Project: "product_id", current price AS "current_price", 
           historical price AS "original_price".
        5. Add a calculated column: "price_diff" mapped as (current_price - original_price).
        6. Filter out rows where "price_diff" equals 0.0 (only keep the corrupted anomalies).
        """
        # ================== START DSL TIME TRAVEL EDITING ==================
        current_df = self.spark.read.format("delta").load(delta_path)
        historical_df = self.spark.read.format("delta").option("versionAsOf", historical_version).load(delta_path)
        
        cur_clean = current_df.select(
            F.col("product_id"),
            F.col("price").alias("current_price")
        )

        hist_clean = historical_df.select(
            F.col("product_id").alias('his_prod_id'),
            F.col("price").alias("original_price")
        )

        anomaly_dsl_df = cur_clean.join(
            hist_clean, 
            F.col('product_id') == F.col('his_prod_id'),
            "inner")\
                .withColumn(
                    "price_diff",
                    F.col('current_price') - F.col('original_price')
                )\
                .filter(
                    F.col('price_diff') != 0.0
                ).select(
                    "product_id",
                    "current_price",
                    "original_price",
                    "price_diff"
                )
        
        # ================== END DSL TIME TRAVEL EDITING ====================
        return anomaly_dsl_df

    def audit_price_anomaly_sql(self, table_name, historical_version=0):
        """
        Track 2: Pure Spark SQL Pathway.
        
        Requirements:
        1. Notice that the latest state is already registered as an active table ("products_corrupted").
        2. To read a historical version in Spark SQL natively, leverage the syntax:
           "SELECT * FROM table_name VERSION AS OF version_number".
        3. Write an inner join query between the latest table and the historical snapshot on "product_id".
        4. Calculate "price_diff" as (latest.price - historical.price).
        5. Filter out unaffected data (price_diff != 0.0) and return the Projection.
        """
        # ================== START SQL TIME TRAVEL EDITING ==================
        
        sql_query = f"""
            SELECT 
                t1.product_id,
                t1.price AS current_price,
                t2.price AS original_price,
                t1.price - t2.price AS price_diff
            FROM 
            {table_name} t1
            INNER JOIN
            {table_name} VERSION AS OF {historical_version} t2
            ON t1.product_id = t2.product_id
            WHERE 
                t1.product_id = t2.product_id AND (t1.price - t2.price) != 0
        """
        # ================== END SQL TIME TRAVEL EDITING ====================
        return self.spark.sql(sql_query)


# ==============================================================================
# Local Execution Test Scaffold (ACID Transaction Simulator)
# ==============================================================================
if __name__ == "__main__":
    # Initialize high-performance Lakehouse local node with dynamic Delta Jars
    spark = SparkSession.builder \
        .appName("Day12_Delta_Lake_TimeTravel_Audit") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Enforce absolute terminal output purity
    spark.sparkContext.setLogLevel("ERROR")

    # Local workspace setup for Delta Engine tracking
    delta_warehouse = "./mock_delta_warehouse/products_gold"
    if os.path.exists("./mock_delta_warehouse"):
        shutil.rmtree("./mock_delta_warehouse")

    # Core table schema definition
    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True)
    ])

    # --- VERSION 0: Pristine Safe State Ingestion ---
    initial_snapshot = [
        ("prod_mac",   1999.00, "Electronics"),
        ("prod_phone",  999.00, "Electronics"),
        ("prod_chair",  150.00, "Furniture")
    ]
    v0_df = spark.createDataFrame(initial_snapshot, schema=product_schema)
    v0_df.write.format("delta").mode("overwrite").save(delta_warehouse)

    # --- VERSION 1: Malicious Corrupted Update Injection ---
    corrupted_snapshot = [
        ("prod_mac",   2500.00, "Electronics"),  # Wrong price jump (+501.0)
        ("prod_phone",  999.00, "Electronics"),  # Unaffected pristine rows
        ("prod_chair",   45.00, "Furniture")     # Wrong price drop (-105.0)
    ]
    v1_df = spark.createDataFrame(corrupted_snapshot, schema=product_schema)
    v1_df.write.format("delta").mode("overwrite").save(delta_warehouse)

    # Register the latest corrupted state into the Session Catalog for Path B
    spark.read.format("delta").load(delta_warehouse).createOrReplaceTempView("products_corrupted")

    # Initialize the Audit Pipeline Reactor
    auditor = LakehouseAuditPipeline(spark)

    print("\n" + "="*80)
    print("🔥 AUDIT PATH A: ANOMALY DISCOVERY VIA PYSPARK DSL Snapshots")
    print("="*80)
    dsl_anomalies = auditor.audit_price_anomaly_dsl(delta_warehouse, historical_version=0)
    dsl_anomalies.show(truncate=False)

    print("\n" + "="*80)
    print("🚀 AUDIT PATH B: ANOMALY DISCOVERY VIA PURE SPARK SQL TimeTravel")
    print("="*80)
    sql_anomalies = auditor.audit_price_anomaly_sql("products_corrupted", historical_version=0)
    sql_anomalies.show(truncate=False)

    # Graceful file systems cleanup
    spark.stop()
    if os.path.exists("./mock_delta_warehouse"):
        shutil.rmtree("./mock_delta_warehouse")