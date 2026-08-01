from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class SaltingJoinPipeline:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def process_salted_join(self, transaction_df, seller_df, salt_bins=4):
        """
        Core Logic: Mitigate Data Skew via Salting Pattern

        Requirements:
        1. Salt the Fact Table (transaction_df):
           - Add a new column named "salted_seller_id".
           - For each row, concatenate the original "seller_id" with a random integer 
             between 0 and (salt_bins - 1). 
           - Hint: Use F.concat(), F.lit("_"), and F.floor(F.rand() * salt_bins).

        2. Explode the Dimension Table (seller_df):
           - To match the random salts, we must replicate the dimension rows.
           - Generate an array column containing all possible salt values: [0, 1, 2, ..., salt_bins-1].
           - Hint: Use F.array([F.lit(i) for i in range(salt_bins)]).
           - Explode this array column so each seller row replicates 'salt_bins' times.
           - Create a column named "salted_seller_id" in this table by concatenating 
             "seller_id", "_", and the exploded salt value.

        3. Execute the Salted Join:
           - Join the two processed DataFrames on "salted_seller_id".
           - Select only the required columns for the final fact table:
             "tx_id", "seller_id", "seller_name", "amount".
           - Ensure no helper/salted columns are leaked to the downstream output.
        """
        # ================== START EDITING YOUR LOGIC ==================

        # Step 1: Salt the transaction DataFrame
        salted_txn_df = transaction_df\
            .withColumn('salt', F.floor(F.rand() * salt_bins).try_cast(IntegerType()))\

        # Step 2: Replicate and salt the seller DataFrame
        replicated_seller_df = seller_df\
            .withColumn('salt', F.explode(F.sequence(F.lit(0), F.lit(salt_bins - 1))))

        # Step 3: Perform the salted join and select final columns
        final_joined_df = salted_txn_df.join(
            replicated_seller_df,
            on=['seller_id', 'salt'],
            how='inner'
        ).drop('salt')\
            .select(
            'tx_id',
            'seller_id',
            'amount'
        )

        # ================== END EDITING YOUR LOGIC ====================
        return final_joined_df


# ==============================================================================
# Local Test Execution Scaffold (Stateless Data Injections)
# ==============================================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Day4_Data_Skew_Salting_Test") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # Mock Data A: Highly skewed transaction logs
    raw_tx_data = [
        ("tx_001", "seller_mega", 150.0),
        ("tx_002", "seller_mega", 99.5),
        ("tx_003", "seller_mega", 430.0),
        # "seller_mega" causes heavy data skew
        ("tx_004", "seller_mega", 25.0),
        ("tx_005", "seller_small", 12.0),
        ("tx_006", "seller_medium", 85.0)
    ]

    tx_schema = StructType([
        StructField("tx_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])

    input_txn_df = spark.createDataFrame(raw_tx_data, schema=tx_schema)

    # Mock Data B: Seller dimension table
    raw_seller_data = [
        ("seller_mega", "Mega Global Store Inc"),
        ("seller_small", "Small Boutique"),
        ("seller_medium", "Medium Retail Retail")
    ]

    seller_schema = StructType([
        StructField("seller_id", StringType(), True),
        StructField("seller_name", StringType(), True)
    ])

    input_seller_df = spark.createDataFrame(
        raw_seller_data, schema=seller_schema)

    print("--- 1. Raw Input Transaction Data (Skewed Bronze) ---")
    input_txn_df.show(truncate=False)

    # Trigger the pipeline execution with 4 salt bins
    pipeline = SaltingJoinPipeline(spark)
    output_df = pipeline.process_salted_join(
        input_txn_df, input_seller_df, salt_bins=4)

    print("--- 2. Final Optimized Join Data (Balanced Gold Fact) ---")
    output_df.show(truncate=False)

    # Verify execution plan to confirm Shuffle Exchange changes
    print("--- 3. Spark Optimized Execution Plan ---")
    output_df.explain()
