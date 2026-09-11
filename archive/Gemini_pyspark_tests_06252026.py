from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class SalesCubePipeline:
    def __init__(self, spark_session: SparkSession):
        """
        Dependency Injection of Session.
        Decoupled architecture for high-performance Gold reporting layers.
        """
        self.spark = spark_session

    def generate_hierarchical_report(self, df):
        """
        Core Logic: Build a Multi-dimensional Rollup Report with Clean Labels.

        Requirements:
        1. Apply hierarchical dimensions aggregation using the .rollup() operator.
           - The hierarchy must flow from broad to specific: "region", then "category".
        2. Inside the aggregation (.agg()):
           - Calculate the sum of "revenue", round it to 2 decimal places, 
             and alias it as "total_revenue".
           - Generate a tracking column using F.grouping_id() to identify 
             the exact aggregation level, and alias it as "agg_level_id".
        3. Handle the Spark-generated NULL values (which represent Grand Totals or Subtotals):
           - In the "region" column: If it represents a Grand Total (where agg_level_id is 3), 
             replace NULL with the string literal "ALL_REGIONS". Otherwise, keep the original region.
           - In the "category" column: If it represents a Subtotal or Grand Total 
             (where agg_level_id is 1 or 3), replace NULL with "ALL_CATEGORIES". 
             Otherwise, keep the original category.
           - Hint: Use F.when() expressions driven by the value of "agg_level_id".
        4. Sort the final report by "agg_level_id" ascending, then "region", then "category".
        """
        # ================== START EDITING YOUR ROLLUP LOGIC ==================

        final_report_df = df\
            .rollup(
                'region',
                'category',
            ).agg(
                F.round(F.sum('revenue'), 2).alias('total_revenue'),
                F.grouping_id().alias('agg_level_id')
            )\
            .select(
                F.when(F.col('agg_level_id') == 3, 'ALL_REGIONS')
                .otherwise(F.col("region")).alias('region'),
                F.when(F.col('agg_level_id').isin(1, 3), 'ALL_CATEGORIES')
                .otherwise(F.col('category')).alias('category'),
                'total_revenue',
                'agg_level_id',
            )\
            .orderBy(
                F.asc('agg_level_id'),
                F.col('region'),
                F.col('category')
            )

        # ================== END EDITING YOUR ROLLUP LOGIC ====================
        return final_report_df


# ==============================================================================
# Local Execution Test Scaffold (Stateless Data Injections)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day8_Multi_Dimensional_Rollup_Test") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # 1. Setup Base Sales Schema
    sales_schema = StructType([
        StructField("region", StringType(), True),
        StructField("category", StringType(), True),
        StructField("revenue", DoubleType(), True)
    ])

    # 2. Mock Raw Base Fact Data (Silver Layer)
    raw_sales_data = [
        ("North_AMER", "Electronics", 50000.0),
        ("North_AMER", "Electronics", 35000.0),
        ("North_AMER", "Furniture",   20000.0),
        ("APAC",       "Electronics", 80000.0),
        ("APAC",       "Furniture",   45000.0),
        ("APAC",       "Furniture",   15000.0)
    ]

    input_df = spark.createDataFrame(raw_sales_data, schema=sales_schema)

    print("--- 1. Raw Input Sales Fact Stream (Silver Layer) ---")
    input_df.show(truncate=False)

    # 3. Fire up the reporting machinery
    pipeline = SalesCubePipeline(spark)
    report_df = pipeline.generate_hierarchical_report(input_df)

    print("--- 2. Final Multi-Dimensional Hierarchical Report (Gold Layer) ---")
    report_df.printSchema()
    report_df.show(truncate=False)

    spark.stop()
